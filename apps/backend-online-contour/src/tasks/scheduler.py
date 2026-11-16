import logging
from .celery_app import celery_app

logger = logging.getLogger("rtc_celery_scheduler")


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        300.0,
        verify_infrastructure_health_heartbeat.s(),
        name="rtc_online_verify_infrastructure_health_heartbeat_every_5_min",
    )


@celery_app.task(name="src.tasks.scheduler.verify_infrastructure_health_heartbeat")
def verify_infrastructure_health_heartbeat() -> str:
    logger.info("[SCHEDULER][HEARTBEAT] Фоновая проверка готовности Celery-воркеров выполнена.")
    return "HEARTBEAT_HEALTH_OK"
