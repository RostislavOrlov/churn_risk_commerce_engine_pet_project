from __future__ import annotations

import asyncio
import contextlib
import logging
from time import time, monotonic
from typing import Any, Coroutine, Tuple
from fastapi import FastAPI, Response, status
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send
from sqlalchemy import text
from redis.asyncio import ConnectionPool, Redis
from neo4j import AsyncGraphDatabase
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from .config import settings
from .api.routes import api_router
from .core.security import keycloak_auth_bearer, backend_m2m_client
from .core.middlewares import OpenTelemetryTraceMiddleware, IdempotencyKeyMiddleware
from .core.metrics import PrometheusMetricsMiddleware
from .core.exceptions import RealTimeCommerceException
from .repositories.postgres_client import pg_manager
from .services.analytics import ml_serving_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PRE_STOP_DELAY_SEC = 2.0
DRAIN_TIMEOUT_SEC = 15.0
RESOURCE_CLOSE_TIMEOUT_SEC = 5.0
HEALTH_PATHS = {"/healthz", "/readyz", "/health/live", "/health/ready", "/metrics"}


class ApplicationState:
    def __init__(self) -> None:
        self.redis_pool: ConnectionPool | None = None
        self.neo4j_driver = None

        self.is_ready: bool = False
        self.is_shutting_down: bool = False
        self.reject_new_requests: bool = False

        # Load Shedding (Bulkhead) & Graceful Draining
        self.concurrency_semaphore: asyncio.Semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_REQUESTS)
        self.in_flight_requests: int = 0
        self.all_requests_done: asyncio.Event = asyncio.Event()
        self.all_requests_done.set()
        self.background_tasks: set[asyncio.Task] = set()

        # Кэш активного /readyz (timestamp, is_healthy, details)
        self._readiness_cache: Tuple[float, bool, dict] = (0.0, False, {})

    def spawn_background_task(self, coro: Coroutine[Any, Any, Any], name: str | None = None) -> asyncio.Task:
        if self.reject_new_requests:
            raise RuntimeError("Невозможно запустить фоновую задачу: сервис в процессе остановки")
        task = asyncio.create_task(coro, name=name)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
        return task


db_state = ApplicationState()


class PureASGILifecycleMiddleware:
    """
    Pure ASGI слой для Graceful Shutdown (Draining) и мгновенного Load Shedding при перегрузке.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path in HEALTH_PATHS:
            await self.app(scope, receive, send)
            return

        if db_state.reject_new_requests:
            response = JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error_code": "SERVICE_UNAVAILABLE", "message": "Инстанс выведен из ротации"},
                headers={"Connection": "close"},
            )
            await response(scope, receive, send)
            return

        if db_state.concurrency_semaphore.locked():
            response = JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "error_code": "LOAD_SHEDDING_ACTIVE",
                    "message": f"Concurrency limit ({settings.MAX_CONCURRENT_REQUESTS}) exceeded",
                },
                headers={"Retry-After": "1"},
            )
            await response(scope, receive, send)
            return

        async with db_state.concurrency_semaphore:
            db_state.in_flight_requests += 1
            db_state.all_requests_done.clear()
            try:
                await self.app(scope, receive, send)
            finally:
                db_state.in_flight_requests -= 1
                if db_state.in_flight_requests == 0:
                    db_state.all_requests_done.set()


async def _check_dependencies_health() -> Tuple[bool, dict]:
    """Выполняет активную проверку Redis (PING) и Postgres Replica (SELECT 1) с микрокэшем."""
    now = monotonic()
    cached_ts, cached_ok, cached_details = db_state._readiness_cache
    if (now - cached_ts) < settings.READINESS_CACHE_TTL_SEC:
        return cached_ok, cached_details

    checks = {"redis": "DOWN", "postgres_replica": "DOWN"}

    # 1. Проверка Redis PING
    if db_state.redis_pool:
        client = Redis(connection_pool=db_state.redis_pool)
        try:
            pong = await asyncio.wait_for(client.ping(), timeout=0.15)
            if pong:
                checks["redis"] = "UP"
        except Exception as exc:
            checks["redis"] = f"ERROR: {type(exc).__name__}"
        finally:
            await client.aclose()

    # 2. Проверка Postgres Replica SELECT 1
    if pg_manager.replica_session_factory:
        try:
            async with pg_manager.replica_session_factory() as session:
                await asyncio.wait_for(session.execute(text("SELECT 1")), timeout=0.20)
                checks["postgres_replica"] = "UP"
        except Exception as exc:
            checks["postgres_replica"] = f"ERROR: {type(exc).__name__}"

    is_ok = checks["redis"] == "UP" and checks["postgres_replica"] == "UP"
    db_state._readiness_cache = (now, is_ok, checks)
    return is_ok, checks


async def _close_postgres() -> None:
    try:
        await asyncio.wait_for(pg_manager.dispose(), timeout=RESOURCE_CLOSE_TIMEOUT_SEC)
    except Exception as exc:
        logging.error(f"[LIFESPAN] Ошибка закрытия PostgreSQL Master/Replica: {exc}")


async def _close_redis() -> None:
    if not db_state.redis_pool:
        return
    try:
        await asyncio.wait_for(db_state.redis_pool.disconnect(), timeout=RESOURCE_CLOSE_TIMEOUT_SEC)
        logging.info("[LIFESPAN] Пул Redis корректно закрыт.")
    except Exception as exc:
        logging.error(f"[LIFESPAN] Ошибка при закрытии Redis: {exc}")


async def _close_neo4j() -> None:
    if not db_state.neo4j_driver:
        return
    try:
        await asyncio.wait_for(db_state.neo4j_driver.close(), timeout=RESOURCE_CLOSE_TIMEOUT_SEC)
        logging.info("[LIFESPAN] Драйвер Neo4j корректно закрыт.")
    except Exception as exc:
        logging.error(f"[LIFESPAN] Ошибка при закрытии Neo4j: {exc}")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("[LIFESPAN] Инициализация ресурсов рантайма...")
    try:
        pg_manager.init_engines()

        db_state.redis_pool = ConnectionPool.from_url(
            settings.REDIS_URL,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
            socket_timeout=settings.REDIS_TIMEOUT_SEC,
        )

        db_state.neo4j_driver = AsyncGraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
        )

        await keycloak_auth_bearer.init_jwks()
        await backend_m2m_client.get_service_token()
        await asyncio.to_thread(ml_serving_service.load_model)
        await asyncio.to_thread(ml_serving_service.warm_up)

        db_state.is_ready = True
        logging.info("[LIFESPAN] Сервис готов к приему трафика (READY).")
        yield

    finally:
        logging.info("[LIFESPAN] Запуск процедуры Graceful Shutdown...")
        db_state.is_ready = False
        db_state.is_shutting_down = True

        await asyncio.sleep(PRE_STOP_DELAY_SEC)
        db_state.reject_new_requests = True

        if db_state.in_flight_requests > 0:
            try:
                await asyncio.wait_for(db_state.all_requests_done.wait(), timeout=DRAIN_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                logging.warning(f"[LIFESPAN] Таймаут Draining! Незавершенных запросов: {db_state.in_flight_requests}")

        if db_state.background_tasks:
            _, pending = await asyncio.wait(db_state.background_tasks, timeout=DRAIN_TIMEOUT_SEC)
            for task in pending:
                task.cancel()

        await asyncio.gather(_close_postgres(), _close_redis(), _close_neo4j(), return_exceptions=True)
        logging.info("[LIFESPAN] Graceful Shutdown полностью завершен.")


app = FastAPI(
    title=settings.PROJECT_NAME,
    lifespan=lifespan,
    docs_url=None if settings.ENV == "production" else "/docs",
    redoc_url=None if settings.ENV == "production" else "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(IdempotencyKeyMiddleware)
app.add_middleware(PrometheusMetricsMiddleware)
app.add_middleware(OpenTelemetryTraceMiddleware)
app.add_middleware(PureASGILifecycleMiddleware)


@app.exception_handler(RealTimeCommerceException)
async def realtime_commerce_exception_handler(request, exc: RealTimeCommerceException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error_code": exc.code, "message": exc.message},
    )


@app.get("/metrics", status_code=status.HTTP_200_OK, include_in_schema=False)
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/healthz", status_code=status.HTTP_200_OK, include_in_schema=False)
@app.get("/health/live", status_code=status.HTTP_200_OK, include_in_schema=False)
async def liveness_check() -> dict:
    return {"status": "UP", "timestamp": int(time())}


@app.get("/readyz", include_in_schema=False)
@app.get("/health/ready", include_in_schema=False)
async def readiness_check() -> Response:
    if not db_state.is_ready or db_state.is_shutting_down:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "NOT_READY", "reason": "SHUTDOWN_OR_BOOTING", "timestamp": int(time())},
        )

    deps_ok, checks = await _check_dependencies_health()
    if not deps_ok:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "DEGRADED_NOT_READY", "dependencies": checks, "timestamp": int(time())},
        )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"status": "READY", "dependencies": checks, "timestamp": int(time())},
    )

@app.get("/ui", include_in_schema=False)
async def control_tower_ui():
    return FileResponse("src/static/control_tower.html")

app.include_router(api_router, prefix=settings.API_V1_STR)
