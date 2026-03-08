"""Celery application for background task processing."""

import logging

from celery import Celery
from celery.schedules import crontab

from app.config import settings

logger = logging.getLogger(__name__)


def _parse_time(time_str: str) -> tuple[int, int]:
    """Parse 'HH:MM' string into (hour, minute) tuple with validation."""
    try:
        hour, minute = map(int, time_str.strip().split(":"))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"out of range: hour={hour}, minute={minute}")
        return hour, minute
    except (ValueError, IndexError) as e:
        raise ValueError(
            f"Invalid BEAT time format '{time_str}', expected 'HH:MM' (00:00-23:59)"
        ) from e


celery_app = Celery(
    "jinfan_tasks",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    # Celery stores internal times as UTC; crontab schedules are interpreted
    # in the configured timezone (Asia/Shanghai), which is the recommended setup.
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
        # ETL / ingestion tasks route to the scraping queue
        "app.tasks.analysis.refresh_nav_incremental": {"queue": "scraping"},
        "app.tasks.analysis.refresh_nav_full": {"queue": "scraping"},
        "app.tasks.analysis.daily_data_refresh": {"queue": "scraping"},
    },
)

# ---------------------------------------------------------------------------
# Celery Beat periodic schedule
# ---------------------------------------------------------------------------
if settings.BEAT_ENABLED:
    refresh_h, refresh_m = _parse_time(settings.BEAT_DAILY_REFRESH_TIME)
    risk_h, risk_m = _parse_time(settings.BEAT_RISK_CHECK_TIME)
    nav_h, nav_m = _parse_time(settings.BEAT_NAV_CALC_TIME)

    celery_app.conf.beat_schedule = {
        "daily-data-refresh": {
            "task": "app.tasks.analysis.daily_data_refresh",
            "schedule": crontab(hour=refresh_h, minute=refresh_m, day_of_week="1-5"),
            "options": {"queue": "scraping"},
        },
        "daily-risk-check": {
            "task": "app.tasks.analysis.scheduled_risk_check",
            "schedule": crontab(hour=risk_h, minute=risk_m, day_of_week="1-5"),
            "options": {"queue": "analysis"},
        },
        "daily-nav-calc": {
            "task": "app.tasks.analysis.scheduled_nav_calc",
            "schedule": crontab(hour=nav_h, minute=nav_m, day_of_week="1-5"),
            "options": {"queue": "analysis"},
        },
    }
    logger.info(
        "Celery Beat enabled: refresh=%s, risk=%s, nav=%s (Mon-Fri)",
        settings.BEAT_DAILY_REFRESH_TIME,
        settings.BEAT_RISK_CHECK_TIME,
        settings.BEAT_NAV_CALC_TIME,
    )

celery_app.autodiscover_tasks(["app.tasks"])
