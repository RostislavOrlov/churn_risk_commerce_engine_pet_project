import time
from fastapi import Request, Response
from prometheus_client import Counter, Histogram, Gauge
from starlette.middleware.base import BaseHTTPMiddleware

HTTP_REQUESTS_TOTAL = Counter(
    "rtc_online_http_requests_total",
    "Общее количество входящих HTTP запросов",
    ["method", "endpoint", "status_code"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "rtc_online_http_request_duration_seconds",
    "Гистограмма задержки (latency) HTTP запросов в секундах",
    ["method", "endpoint"],
    buckets=(0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5),
)

ML_INFERENCE_DURATION_SECONDS = Histogram(
    "rtc_online_ml_inference_duration_seconds",
    "Время выполнения онлайн-инференса CatBoost в секундах",
    ["model_name", "degraded_mode"],
    buckets=(0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100),
)

CIRCUIT_BREAKER_STATE = Gauge(
    "rtc_online_circuit_breaker_state",
    "Текущий статус предохранителей инфраструктуры (0 = CLOSED, 1 = HALF-OPEN, 2 = OPEN)",
    ["resource_name"],
)

CIRCUIT_BREAKER_FAILURES_TOTAL = Counter(
    "rtc_online_circuit_breaker_failures_total",
    "Количество зафиксированных сбоев контура предохранителей",
    ["resource_name", "exception_type"],
)

CACHE_OPERATIONS_TOTAL = Counter(
    "rtc_online_cache_operations_total",
    "Метрики эффективности гибридного кэша L1/L2",
    ["namespace", "layer", "result"],
)

ML_SHADOW_SCORE_DIFF = Histogram(
    "rtc_online_ml_shadow_score_diff",
    "Абсолютное расхождение вероятностей между Champion и Challenger моделями",
    ["champion_model", "challenger_model"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.10, 0.20, 0.50, 1.0),
)


class PrometheusMetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path in ("/metrics", "/healthz", "/readyz"):
            return await call_next(request)

        start_time = time.perf_counter()
        route = request.scope.get("route")
        endpoint = route.path if route else request.url.path
        method = request.method

        try:
            response = await call_next(request)
            status_code = str(response.status_code)
            return response
        except Exception:
            status_code = "500"
            raise
        finally:
            duration = time.perf_counter() - start_time
            HTTP_REQUESTS_TOTAL.labels(method=method, endpoint=endpoint, status_code=status_code).inc()
            HTTP_REQUEST_DURATION_SECONDS.labels(method=method, endpoint=endpoint).observe(duration)
