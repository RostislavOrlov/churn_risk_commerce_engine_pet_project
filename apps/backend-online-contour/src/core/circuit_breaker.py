import time
import logging
from typing import Callable, Any

from ..config import settings
from .metrics import CIRCUIT_BREAKER_STATE, CIRCUIT_BREAKER_FAILURES_TOTAL

logger = logging.getLogger("rtc_circuit_breaker")


class CircuitBreakerOpenException(Exception):
    pass


class HighAvailabilityCircuitBreaker:
    STATE_MAP = {"CLOSED": 0, "HALF-OPEN": 1, "OPEN": 2}

    def __init__(
        self,
        name: str,
        failure_threshold: int = settings.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
        recovery_timeout: float = settings.CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SEC,
        half_open_max_calls: int = 3,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self.failure_count = 0
        self._half_open_calls = 0      # Сколько пробных запросов уже запущено в HALF-OPEN
        self._half_open_successes = 0  # Сколько из них вернулись с успехом
        self.state = "CLOSED"
        self.last_state_change = time.time()
        CIRCUIT_BREAKER_STATE.labels(resource_name=self.name).set(0)

    def _set_state(self, new_state: str) -> None:
        self.state = new_state
        self.last_state_change = time.time()
        self._half_open_calls = 0
        self._half_open_successes = 0
        CIRCUIT_BREAKER_STATE.labels(resource_name=self.name).set(self.STATE_MAP[new_state])

    def allow_request(self) -> bool:
        now = time.time()

        if self.state == "CLOSED":
            return True

        if self.state == "OPEN":
            if (now - self.last_state_change) > self.recovery_timeout:
                self._set_state("HALF-OPEN")
                self._half_open_calls = 1
                logger.warning(
                    f"[CIRCUIT_BREAKER][{self.name}] Переключение в HALF-OPEN. "
                    f"Пробный запрос разрешен (1/{self.half_open_max_calls})."
                )
                return True
            return False

        if self._half_open_calls < self.half_open_max_calls:
            self._half_open_calls += 1
            return True

        if (now - self.last_state_change) > self.recovery_timeout:
            self._set_state("HALF-OPEN")
            self._half_open_calls = 1
            logger.warning(
                f"[CIRCUIT_BREAKER][{self.name}] Таймаут проверки в HALF-OPEN. Перезапуск пробы."
            )
            return True

        return False

    def record_success(self) -> None:
        if self.state == "HALF-OPEN":
            self._half_open_successes += 1
            if self._half_open_successes >= self.half_open_max_calls:
                self.failure_count = 0
                self._set_state("CLOSED")
                logger.info(
                    f"[CIRCUIT_BREAKER][{self.name}] Все {self.half_open_max_calls} пробных запросов успешны. "
                    f"Переключение в CLOSED."
                )
            return

        self.failure_count = 0
        if self.state != "CLOSED":
            self._set_state("CLOSED")

    def record_failure(self, error: Exception) -> None:
        self.failure_count += 1
        CIRCUIT_BREAKER_FAILURES_TOTAL.labels(
            resource_name=self.name, exception_type=type(error).__name__
        ).inc()
        logger.error(
            f"[CIRCUIT_BREAKER][{self.name}] Сбой ({self.failure_count}/{self.failure_threshold}): {error}"
        )

        if self.state == "HALF-OPEN" or (
            self.state == "CLOSED" and self.failure_count >= self.failure_threshold
        ):
            self._set_state("OPEN")
            logger.critical(
                f"[CIRCUIT_BREAKER][{self.name}] КРИТИЧЕСКИЙ СБОЙ -> OPEN. Изоляция на {self.recovery_timeout}с."
            )

    async def call_async(
        self, func: Callable[..., Any], fallback_func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        if not self.allow_request():
            return fallback_func(*args, **kwargs)
        try:
            result = await func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure(e)
            return fallback_func(*args, **kwargs)


class GracefulDegradationEngine:
    """Генератор безопасных дефолтных векторов признаков при недоступности Redis/Neo4j."""

    @staticmethod
    def get_fallback_user_features(user_id: str):
        from ..models.cache_entities import UserOnlineFeaturesModel

        return UserOnlineFeaturesModel(
            user_id=user_id,
            is_fallback=True,
        )
