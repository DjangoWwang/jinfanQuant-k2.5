"""Background analysis tasks (factor exposure, backtest, report generation)."""

from __future__ import annotations

import asyncio
import logging
from datetime import date

from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


_worker_loop = None


def _run_async(coro):
    """Run an async function from sync Celery task using a shared event loop."""
    global _worker_loop
    if _worker_loop is None or _worker_loop.is_closed():
        _worker_loop = asyncio.new_event_loop()
    return _worker_loop.run_until_complete(coro)


@celery_app.task(bind=True, name="app.tasks.analysis.run_backtest_async", max_retries=2)
def run_backtest_async(self, config_dict: dict) -> dict:
    """Run portfolio backtest in background.

    Args:
        config_dict: BacktestConfigSchema as dict.

    Returns:
        dict with backtest_id and status.
    """
    async def _run():
        from app.database import async_session
        from app.schemas.portfolio import BacktestConfigSchema
        from app.services.portfolio_service import portfolio_service

        config = BacktestConfigSchema(**config_dict)
        async with async_session() as db:
            result = await portfolio_service.run_backtest(db, config)
            return {"backtest_id": result.get("backtest_id"), "status": "completed"}

    try:
        return _run_async(_run())
    except Exception as exc:
        logger.exception("Backtest task failed")
        raise self.retry(exc=exc, countdown=30)


@celery_app.task(bind=True, name="app.tasks.analysis.compute_factor_exposure", max_retries=2)
def compute_factor_exposure(self, product_id: int, window: int = 52) -> dict:
    """Compute factor exposure for a product in background."""
    async def _run():
        from app.database import async_session
        from app.services.attribution_service import attribution_service

        async with async_session() as db:
            result = await attribution_service.get_factor_exposure(db, product_id, window=window)
            return result

    try:
        return _run_async(_run())
    except Exception as exc:
        logger.exception("Factor exposure task failed: product_id=%s", product_id)
        raise self.retry(exc=exc, countdown=30)


@celery_app.task(bind=True, name="app.tasks.analysis.generate_report_async", max_retries=1)
def generate_report_async(self, product_id: int, config: dict) -> dict:
    """Generate PDF report in background."""
    async def _run():
        from app.database import async_session
        from app.services.report_service import report_service

        async with async_session() as db:
            pdf_bytes = await report_service.generate_report(db, product_id, config)
            # Save to temp file and return path
            import tempfile
            import os
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf", prefix=f"report_{product_id}_")
            tmp.write(pdf_bytes)
            tmp.close()
            return {"file_path": tmp.name, "product_id": product_id, "status": "completed"}

    try:
        return _run_async(_run())
    except Exception as exc:
        logger.exception("Report generation failed: product_id=%s", product_id)
        raise self.retry(exc=exc, countdown=60)
