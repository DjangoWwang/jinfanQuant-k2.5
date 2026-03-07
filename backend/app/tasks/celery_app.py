"""Celery application for background task processing."""

from celery import Celery

from app.config import settings

celery_app = Celery(
    "jinfan_tasks",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Shanghai",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    result_expires=86400,  # 24h
    task_routes={
        "app.tasks.analysis.*": {"queue": "analysis"},
        "app.tasks.scraping.*": {"queue": "scraping"},
        "app.tasks.reports.*": {"queue": "reports"},
    },
)

celery_app.autodiscover_tasks(["app.tasks"])
