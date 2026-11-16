import json
import logging
import asyncio
from datetime import datetime, timezone
import httpx
import redis
from celery import Task

from ..config import settings
from .celery_app import celery_app

logger = logging.getLogger("rtc_celery_workers")

DLQ_REDIS_LIST_KEY = "rtc:dlq:tms_reroute"
DLQ_MAX_ITEMS = 5000


def _push_to_redis_dlq(record: dict) -> None:
    try:
        r_client = redis.Redis.from_url(settings.REDIS_URL, socket_timeout=1.0)
        try:
            r_client.lpush(DLQ_REDIS_LIST_KEY, json.dumps(record, default=str))
            r_client.ltrim(DLQ_REDIS_LIST_KEY, 0, DLQ_MAX_ITEMS - 1)
        finally:
            r_client.close()
    except Exception as redis_err:
        logger.critical(f"[CELERY_DLQ][REDIS_WRITE_FAIL] Не удалось записать в Redis DLQ: {redis_err}")


class ResilientTMSTask(Task):

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        dlq_payload = {
            "failed_task_id": str(task_id),
            "task_name": self.name,
            "args": list(args),
            "kwargs": dict(kwargs),
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "failed_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        logger.critical(
            f"[CELERY_DLQ][TASK_MOVED_TO_DLQ] task_id={task_id} исчерпала все ретраи и переведена в dead_letter: {exc}"
        )

        park_failed_tms_command_dlq.apply_async(args=[dlq_payload], queue="dead_letter")

        _push_to_redis_dlq(dlq_payload)
        super().on_failure(exc, task_id, args, kwargs, einfo)


async def _async_send_tms_network_call(
    incident_id: str,
    order_id: str,
    failing_node_id: str,
    bypass_hub_id: str,
    assigned_team: str,
) -> None:
    from ..core.security import backend_m2m_client

    payload = {
        "incident_id": incident_id,
        "order_id": order_id,
        "failing_node_id": failing_node_id,
        "bypass_hub_id": bypass_hub_id,
        "trigger_source": "REALTIME_COMMERCE_ENGINE",
        "routing_action": "BYPASS_FAILING_NODE",
        "execution_priority": "CRITICAL",
        "assigned_dispatch_team": assigned_team,
    }

    headers = {"Content-Type": "application/json"}
    service_token = await backend_m2m_client.get_service_token()
    if service_token:
        headers["Authorization"] = f"Bearer {service_token}"

    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.post(settings.TMS_WEBHOOK_URL, json=payload, headers=headers)
        response.raise_for_status()
        logger.info(f"[CELERY_WORKER][TMS_SYNC_SUCCESS] Команда Reroute доставлена в TMS для инцидента {incident_id}")


@celery_app.task(
    name="src.tasks.workers.dispatch_reroute_command_tms",
    base=ResilientTMSTask,
    bind=True,
    autoretry_for=(httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError),
    retry_backoff=2,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=3,
)
def dispatch_reroute_command_tms(
    self,
    incident_id: str,
    order_id: str,
    failing_node_id: str,
    bypass_hub_id: str,
    assigned_team: str,
) -> str:
    attempt = self.request.retries + 1
    logger.info(
        f"[CELERY_WORKER] Попытка {attempt}/{self.max_retries + 1} отправки Reroute в TMS для инцидента: {incident_id}"
    )

    try:
        asyncio.run(
            _async_send_tms_network_call(
                incident_id=incident_id,
                order_id=order_id,
                failing_node_id=failing_node_id,
                bypass_hub_id=bypass_hub_id,
                assigned_team=assigned_team,
            )
        )
        return f"SUCCESS_INCIDENT_{incident_id}_DISPATCHED"
    except httpx.HTTPStatusError as http_err:
        if 400 <= http_err.response.status_code < 500:
            logger.error(
                f"[CELERY_WORKER][NON_RETRYABLE_4XX] TMS отклонил запрос ({http_err.response.status_code}). "
                "Прямой сброс в DLQ."
            )
            raise RuntimeError(f"Non-retryable TMS 4xx error: {http_err}") from http_err
        raise


@celery_app.task(name="src.tasks.workers.park_failed_tms_command_dlq", queue="dead_letter")
def park_failed_tms_command_dlq(dlq_payload: dict) -> str:
    logger.warning(f"[DEAD_LETTER_QUEUE] Зафиксирована невыполненная команда TMS: {dlq_payload.get('failed_task_id')}")
    return "PARKED_IN_DLQ"
