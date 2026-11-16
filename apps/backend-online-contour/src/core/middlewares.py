import time
import json
import logging
import uuid
from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from redis.asyncio import Redis

from ..config import settings

logger = logging.getLogger("rtc_middleware")

IGNORED_LOG_PATHS = {"/healthz", "/readyz", "/health/live", "/health/ready", "/metrics"}


class OpenTelemetryTraceMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        start_time = time.perf_counter()

        trace_id = request.headers.get("X-Trace-ID") or request.headers.get("X-Request-ID")
        if not trace_id:
            trace_id = str(uuid.uuid4())

        request.state.trace_id = trace_id

        try:
            response = await call_next(request)
        except Exception as e:
            duration = (time.perf_counter() - start_time) * 1000
            logger.error(
                f"unhandled_exception | trace_id={trace_id} | method={request.method} | "
                f"path={request.url.path} | duration_ms={duration:.2f} | error={str(e)}"
            )
            raise e

        duration = (time.perf_counter() - start_time) * 1000
        response.headers["X-Request-ID"] = trace_id
        response.headers["X-Trace-ID"] = trace_id
        response.headers["X-Response-Time-MS"] = f"{duration:.2f}"

        if request.url.path not in IGNORED_LOG_PATHS:
            logger.info(
                f"http_request | trace_id={trace_id} | method={request.method} | "
                f"path={request.url.path} | status_code={response.status_code} | duration_ms={duration:.2f}"
            )

        return response


class IdempotencyKeyMiddleware(BaseHTTPMiddleware):
    """
    Гарантирует 24-часовую идемпотентность мутирующих POST-запросов
    по заголовку X-Idempotency-Key или Idempotency-Key через Redis SET NX.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method != "POST":
            return await call_next(request)

        idem_key = request.headers.get("X-Idempotency-Key") or request.headers.get("Idempotency-Key")
        if not idem_key:
            return await call_next(request)

        from ..main import db_state

        if not db_state.redis_pool:
            return await call_next(request)

        redis_client = Redis(connection_pool=db_state.redis_pool)
        storage_key = f"rtc:idempotency:{request.url.path}:{idem_key}"

        try:
            cached = await redis_client.get(storage_key)
            if cached:
                decoded = json.loads(cached.decode("utf-8") if isinstance(cached, bytes) else cached)
                if decoded.get("status") == "IN_PROGRESS":
                    return JSONResponse(
                        status_code=status.HTTP_409_CONFLICT,
                        content={"error_code": "IDEMPOTENCY_CONFLICT", "message": "Request is already in progress"},
                    )
                return JSONResponse(
                    status_code=decoded["status_code"],
                    content=decoded["body"],
                    headers={"X-Idempotent-Replay": "true"},
                )

            acquired = await redis_client.set(
                storage_key, json.dumps({"status": "IN_PROGRESS"}), nx=True, ex=30
            )
            if not acquired:
                return JSONResponse(
                    status_code=status.HTTP_409_CONFLICT,
                    content={"error_code": "IDEMPOTENCY_CONFLICT", "message": "Concurrent duplicate request"},
                )

            response = await call_next(request)

            if 200 <= response.status_code < 300:
                body_bytes = b""
                async for chunk in response.body_iterator:
                    body_bytes += chunk

                try:
                    parsed_body = json.loads(body_bytes.decode("utf-8"))
                    await redis_client.set(
                        storage_key,
                        json.dumps({"status": "DONE", "status_code": response.status_code, "body": parsed_body}),
                        ex=settings.IDEMPOTENCY_TTL_SEC,
                    )
                except Exception:
                    await redis_client.delete(storage_key)

                return Response(
                    content=body_bytes,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=response.media_type,
                )

            await redis_client.delete(storage_key)
            return response
        except Exception as err:
            logger.warning(f"[IDEMPOTENCY] Bypass due to Redis error: {err}")
            return await call_next(request)
        finally:
            await redis_client.aclose()
