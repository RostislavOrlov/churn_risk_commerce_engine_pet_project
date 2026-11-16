from __future__ import annotations

import logging
from typing import Optional
import httpx
from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, status

from ...config import settings
from ...core.security import keycloak_auth_bearer, AuthenticatedPrincipal

logger = logging.getLogger("rtc_auth_router")
router = APIRouter(prefix="/auth", tags=["Authentication & Token Lifecycle"])


class UserLoginRequest(BaseModel):
    username: str = Field(..., min_length=2, max_length=128, examples=["dispatcher_ivanov"])
    password: str = Field(..., min_length=4, max_length=128)


class M2MTokenRequest(BaseModel):
    client_id: str = Field(..., min_length=2, max_length=128, examples=["online-routing-engine-client"])
    client_secret: str = Field(..., min_length=4, max_length=256)


class RefreshTokenRequest(BaseModel):
    refresh_token: str = Field(..., min_length=10)


class LogoutRequest(BaseModel):
    refresh_token: Optional[str] = Field(default=None, description="Refresh токен для закрытия сессии в Keycloak")


class TokenPairResponse(BaseModel):
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "Bearer"
    expires_in: int
    refresh_expires_in: Optional[int] = None
    scope: str = ""


class PrincipalProfileResponse(BaseModel):
    sub: str
    username: str
    client_id: str
    is_service_account: bool
    roles: list[str]
    scopes: list[str]


async def _post_keycloak_form(url: str, form_data: dict) -> dict:
    try:
        async with httpx.AsyncClient(timeout=settings.KEYCLOAK_TIMEOUT_SEC) as client:
            response = await client.post(
                url,
                data=form_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            if response.status_code in (400, 401):
                err_detail = response.json().get("error_description", "Invalid credentials or expired token")
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=err_detail)
            response.raise_for_status()
            return response.json() if response.content else {}
    except HTTPException:
        raise
    except httpx.TimeoutException:
        raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail="Keycloak authentication timeout")
    except Exception as exc:
        logger.error(f"[AUTH][KEYCLOAK_ERROR] {exc}")
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Identity Provider unavailable")


@router.post(
    "/login",
    response_model=TokenPairResponse,
    status_code=status.HTTP_200_OK,
    summary="Аутентификация пользователя (выдача пары Access + Refresh JWT)",
)
async def login_user(payload: UserLoginRequest) -> TokenPairResponse:
    token_data = await _post_keycloak_form(
        keycloak_auth_bearer.token_url,
        {
            "grant_type": "password",
            "client_id": settings.KEYCLOAK_CLIENT_ID,
            "client_secret": settings.KEYCLOAK_CLIENT_SECRET,
            "username": payload.username,
            "password": payload.password,
            "scope": "openid profile email",
        },
    )
    logger.info(f"[AUTH][LOGIN_SUCCESS] username={payload.username}")
    return TokenPairResponse(**token_data)


@router.post(
    "/refresh",
    response_model=TokenPairResponse,
    status_code=status.HTTP_200_OK,
    summary="Обновление пары токенов по Refresh Token (Refresh Token Rotation)",
)
async def refresh_access_token(payload: RefreshTokenRequest) -> TokenPairResponse:
    token_data = await _post_keycloak_form(
        keycloak_auth_bearer.token_url,
        {
            "grant_type": "refresh_token",
            "client_id": settings.KEYCLOAK_CLIENT_ID,
            "client_secret": settings.KEYCLOAK_CLIENT_SECRET,
            "refresh_token": payload.refresh_token,
        },
    )
    return TokenPairResponse(**token_data)


@router.post(
    "/logout",
    status_code=status.HTTP_200_OK,
    summary="Выход из системы и мгновенный отзыв Access/Refresh токенов",
)
async def logout_user(
    payload: LogoutRequest,
    principal: AuthenticatedPrincipal = Depends(keycloak_auth_bearer),
) -> dict:
    # 1. Мгновенно блокируем текущий access_token по jti в Redis Blacklist
    await keycloak_auth_bearer.revoke_token_in_blacklist(jti=principal.jti, exp=principal.exp)

    # 2. Завершаем SSO-сессию и отзываем refresh_token в Keycloak
    if payload.refresh_token and settings.AUTH_ENABLED:
        try:
            await _post_keycloak_form(
                keycloak_auth_bearer.logout_url,
                {
                    "client_id": settings.KEYCLOAK_CLIENT_ID,
                    "client_secret": settings.KEYCLOAK_CLIENT_SECRET,
                    "refresh_token": payload.refresh_token,
                },
            )
        except Exception as err:
            logger.warning(f"[AUTH][KEYCLOAK_LOGOUT_WARN] username={principal.username}: {err}")

    logger.info(f"[AUTH][LOGOUT_SUCCESS] username={principal.username}")
    return {"status": "LOGGED_OUT", "username": principal.username}


@router.get(
    "/me",
    response_model=PrincipalProfileResponse,
    status_code=status.HTTP_200_OK,
    summary="Профиль и роли текущего аутентифицированного субъекта",
)
async def get_current_principal(
    principal: AuthenticatedPrincipal = Depends(keycloak_auth_bearer),
) -> PrincipalProfileResponse:
    return PrincipalProfileResponse(
        sub=principal.sub,
        username=principal.username,
        client_id=principal.client_id,
        is_service_account=principal.is_service_account,
        roles=sorted(principal.roles),
        scopes=sorted(principal.scopes),
    )
