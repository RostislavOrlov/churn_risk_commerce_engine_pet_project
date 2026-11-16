from celery import Celery
from kombu import Queue
from ..config import settings

celery_app = Celery(
    "realtime_commerce_workers",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Europe/Moscow",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    task_time_limit=30,
    task_soft_time_limit=15,
    task_queues=(
        Queue("default"),
        Queue("critical_ops"),
        Queue("dead_letter"),
    ),
    task_default_queue="default",
    task_routes={
        "src.tasks.workers.dispatch_reroute_command_tms": {"queue": "critical_ops"},
        "src.tasks.workers.park_failed_tms_command_dlq": {"queue": "dead_letter"},
    },
)

celery_app.autodiscover_tasks(["src.tasks"])
