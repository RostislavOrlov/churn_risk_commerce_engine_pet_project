from __future__ import annotations

import logging
import time
from typing import List, Optional, Set
import httpx
from jose import jwt, jwk
from pydantic import BaseModel, Field
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from redis.asyncio import Redis
import asyncio

from ..config import settings
from .exceptions import SecurityException

logger = logging.getLogger("rtc_security")
bearer_scheme = HTTPBearer(auto_error=False)


class AuthenticatedPrincipal(BaseModel):
    """Унифицированный контекст субъекта (пользователь OIDC или M2M-сервис)."""

    sub: str
    jti: Optional[str] = None
    username: str = "unknown"
    client_id: str = "unknown"
    is_service_account: bool = False
    roles: Set[str] = Field(default_factory=set)
    scopes: Set[str] = Field(default_factory=set)
    exp: int = 0


class KeycloakTokenValidator:
    def __init__(self) -> None:
        self.jwks: dict = {}
        self.last_jwks_fetch: float = 0.0
        self.realm_base_url = f"{settings.KEYCLOAK_SERVER_URL}/realms/{settings.KEYCLOAK_REALM}"
        self.certs_url = f"{self.realm_base_url}/protocol/openid-connect/certs"
        self.token_url = f"{self.realm_base_url}/protocol/openid-connect/token"
        self.logout_url = f"{self.realm_base_url}/protocol/openid-connect/logout"

    async def init_jwks(self) -> None:
        if not settings.AUTH_ENABLED:
            logger.warning("[SECURITY] AUTH_ENABLED=False. Пропуск инициализации JWKS Keycloak.")
            return
        try:
            async with httpx.AsyncClient(timeout=settings.KEYCLOAK_TIMEOUT_SEC) as client:
                response = await client.get(self.certs_url)
                response.raise_for_status()
                self.jwks = response.json()
                self.last_jwks_fetch = time.time()
                logger.info("[SECURITY] JWKS public keys loaded from Keycloak.")
        except Exception as e:
            logger.critical(f"[SECURITY][FATAL] JWKS initialization failed: {e}")
            raise SecurityException(f"Identity Provider JWKS failed: {e}")

    async def _get_public_key_async(self, token: str) -> dict:
        try:
            unverified_header = jwt.get_unverified_header(token)
        except Exception:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid JWT header structure")

        kid = unverified_header.get("kid")
        if not kid:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing 'kid' claim in JWT")

        for key in self.jwks.get("keys", []):
            if key.get("kid") == kid:
                return key

        if (time.time() - self.last_jwks_fetch) > 60.0:
            logger.warning(f"[SECURITY] Key ID '{kid}' not cached. Triggering lazy JWKS reload...")
            await self.init_jwks()
            for key in self.jwks.get("keys", []):
                if key.get("kid") == kid:
                    return key

        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Matching JWKS public key not found")

    async def _is_token_revoked(self, jti: Optional[str]) -> bool:
        """Проверяет, не отозван ли access_token через Redis Blacklist при logout."""
        if not jti:
            return False

        from ..main import db_state

        if not db_state.redis_pool:
            return False

        redis_client = Redis(connection_pool=db_state.redis_pool)
        try:
            exists = await redis_client.exists(f"rtc:auth:blacklist:{jti}")
            return bool(exists)
        except Exception as err:
            # Fail-open при сбое Redis, чтобы не положить авторизацию всего сервиса
            logger.warning(f"[SECURITY][BLACKLIST_BYPASS] Redis check skipped: {err}")
            return False
        finally:
            await redis_client.aclose()

    async def revoke_token_in_blacklist(self, jti: Optional[str], exp: int) -> None:
        """Добавляет jti отозванного access_token в Redis до момента его естественного истечения."""
        if not jti:
            return

        ttl = max(1, int(exp - time.time()))
        from ..main import db_state

        if not db_state.redis_pool:
            return

        redis_client = Redis(connection_pool=db_state.redis_pool)
        try:
            await redis_client.set(f"rtc:auth:blacklist:{jti}", "REVOKED", ex=ttl)
            logger.info(f"[SECURITY] Access token jti={jti} revoked in Redis blacklist for {ttl}s.")
        finally:
            await redis_client.aclose()

    async def __call__(
        self, credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme)
    ) -> AuthenticatedPrincipal:
        if not settings.AUTH_ENABLED:
            return AuthenticatedPrincipal(
                sub="dev-bypass-user",
                username="dev_operator",
                client_id="local-dev",
                roles={"OPERATOR", "DISPATCHER", "CRITICAL_OPS", "ROUTING_ENGINE"},
                scopes={"openid", "profile", "logistics:reroute", "topology:read"},
                exp=int(time.time()) + 3600,
            )

        if not credentials or not credentials.credentials:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing Bearer token")

        token = credentials.credentials
        try:
            public_key_dict = await self._get_public_key_async(token)
            public_key = jwk.construct(public_key_dict)

            decode_options = {"verify_aud": settings.KEYCLOAK_VERIFY_AUDIENCE}
            payload = jwt.decode(
                token,
                public_key.to_pem().decode("utf-8"),
                algorithms=[settings.JWT_ALGORITHM],
                audience=settings.KEYCLOAK_CLIENT_ID if settings.KEYCLOAK_VERIFY_AUDIENCE else None,
                issuer=self.realm_base_url,
                options=decode_options,
            )
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Access token expired")
        except jwt.JWTClaimsError as claims_err:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Invalid JWT claims: {claims_err}")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Token verification failed: {e}")

        # Проверка доверенного клиента (azp)
        azp = payload.get("azp", "")
        if settings.KEYCLOAK_ALLOWED_AZP and azp not in settings.KEYCLOAK_ALLOWED_AZP:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Client '{azp}' is not in allowed authorized parties",
            )

        # Проверка Redis Blacklist (мгновенный logout)
        jti = payload.get("jti")
        if await self._is_token_revoked(jti):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has been revoked")

        realm_roles = payload.get("realm_access", {}).get("roles", [])
        client_roles = (
            payload.get("resource_access", {})
            .get(settings.KEYCLOAK_CLIENT_ID, {})
            .get("roles", [])
        )
        raw_scopes = payload.get("scope", "")
        scopes_set = set(raw_scopes.split()) if isinstance(raw_scopes, str) else set()

        preferred_username = payload.get("preferred_username") or payload.get("clientId") or payload.get("sub", "")
        is_m2m = str(preferred_username).startswith("service-account-") or "clientId" in payload

        return AuthenticatedPrincipal(
            sub=str(payload.get("sub", "")),
            jti=jti,
            username=str(preferred_username),
            client_id=str(azp or "unknown"),
            is_service_account=is_m2m,
            roles=set(realm_roles + client_roles),
            scopes=scopes_set,
            exp=int(payload.get("exp", 0)),
        )


keycloak_auth_bearer = KeycloakTokenValidator()


class RoleChecker:
    """Проверяет наличие хотя бы одной требуемой роли RBAC или OAuth2 scope."""

    def __init__(self, allowed_roles: List[str], required_scopes: Optional[List[str]] = None):
        self.allowed_roles = set(allowed_roles)
        self.required_scopes = set(required_scopes or [])

    def __call__(
        self, principal: AuthenticatedPrincipal = Depends(keycloak_auth_bearer)
    ) -> AuthenticatedPrincipal:
        has_role = bool(principal.roles & self.allowed_roles)
        has_scope = bool(self.required_scopes and (principal.scopes & self.required_scopes))

        if not (has_role or has_scope):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied for '{principal.username}'. Required roles: {sorted(self.allowed_roles)}",
            )
        return principal


class BackendServiceTokenManager:
    """
    Управляет собственным M2M-токеном самого бэкенда (OAuth2 Client Credentials Flow)
    для исходящих интеграций.
    """

    def __init__(self) -> None:
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0
        self._lock = asyncio.Lock()
        self.token_url = f"{settings.KEYCLOAK_SERVER_URL}/realms/{settings.KEYCLOAK_REALM}/protocol/openid-connect/token"

    async def get_service_token(self) -> Optional[str]:
        if not settings.AUTH_ENABLED:
            return None

        # Отдаем закэшированный токен, если до истечения осталось больше 30 секунд
        if self._access_token and time.time() < (self._expires_at - 30.0):
            return self._access_token

        async with self._lock:
            if self._access_token and time.time() < (self._expires_at - 30.0):
                return self._access_token

            try:
                async with httpx.AsyncClient(timeout=settings.KEYCLOAK_TIMEOUT_SEC) as client:
                    response = await client.post(
                        self.token_url,
                        data={
                            "grant_type": "client_credentials",
                            "client_id": settings.KEYCLOAK_CLIENT_ID,
                            "client_secret": settings.KEYCLOAK_CLIENT_SECRET,
                        },
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                    )
                    response.raise_for_status()
                    data = response.json()

                    self._access_token = data["access_token"]
                    expires_in = int(data.get("expires_in", 300))
                    self._expires_at = time.time() + expires_in
                    logger.info(
                        f"[SECURITY][M2M_SELF] Backend '{settings.KEYCLOAK_CLIENT_ID}' authenticated in Keycloak "
                        f"(ttl={expires_in}s)."
                    )
                    return self._access_token
            except Exception as exc:
                logger.error(f"[SECURITY][M2M_SELF_FAIL] Failed to obtain own M2M token: {exc}")
                raise SecurityException(f"Backend M2M self-authentication failed: {exc}")


backend_m2m_client = BackendServiceTokenManager()
