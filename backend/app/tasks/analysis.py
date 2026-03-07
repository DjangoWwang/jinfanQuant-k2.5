"""Background analysis & ingestion tasks (factor exposure, backtest, report, ETL)."""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Any

from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


def _run_async(coro):
    """Run an async function from sync Celery task using a fresh event loop."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


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
def generate_report_async(
    self,
    product_id: int,
    config: dict,
    report_format: str = "pdf",
    requested_by: int | None = None,
) -> dict:
    """Generate PDF or Excel report in background.

    Args:
        product_id: Product ID.
        config: Dict with report_type, period_start, period_end, benchmark_id.
        report_format: "pdf" (default) or "excel".

    Returns:
        dict with task_id, file_path, format, status.
    """
    async def _run():
        import os
        from datetime import datetime

        from app.config import settings
        from app.database import async_session
        from app.services.report_service import report_service

        # Ensure output directory exists
        output_dir = os.path.abspath(settings.REPORT_OUTPUT_DIR)
        os.makedirs(output_dir, exist_ok=True)

        async with async_session() as db:
            if report_format == "excel":
                file_bytes = await report_service.generate_excel_report(
                    db, product_id, config,
                )
                suffix = ".xlsx"
            else:
                # Extract params for PDF generation
                from datetime import date as date_type

                period_start = config.get("period_start")
                period_end = config.get("period_end")
                if isinstance(period_start, str):
                    period_start = date_type.fromisoformat(period_start)
                if isinstance(period_end, str):
                    period_end = date_type.fromisoformat(period_end)

                file_bytes = await report_service.generate_report(
                    db,
                    product_id=product_id,
                    report_type=config.get("report_type", "monthly"),
                    period_start=period_start,
                    period_end=period_end,
                    benchmark_id=config.get("benchmark_id"),
                )
                suffix = ".pdf"

            # Build filename with timestamp
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"report_{product_id}_{ts}{suffix}"
            file_path = os.path.join(output_dir, filename)

            with open(file_path, "wb") as f:
                f.write(file_bytes)

            return {
                "task_id": self.request.id,
                "file_path": file_path,
                "format": report_format,
                "product_id": product_id,
                "requested_by": requested_by,
                "status": "completed",
            }

    try:
        return _run_async(_run())
    except Exception as exc:
        logger.exception(
            "Report generation failed: product_id=%s, format=%s",
            product_id, report_format,
        )
        raise self.retry(exc=exc, countdown=60)


# ======================================================================
# ETL / Ingestion tasks
# ======================================================================

@celery_app.task(bind=True, name="app.tasks.analysis.refresh_nav_incremental", max_retries=2)
def refresh_nav_incremental(self, fund_ids: list[int] | None = None) -> dict:
    """增量更新基金净值。

    Args:
        fund_ids: 指定基金ID列表；为 None 时更新所有 nav_status='has_data' 的基金。

    Returns:
        {"processed": N, "failed": M, "errors": [...]}
    """
    async def _run() -> dict:
        from sqlalchemy import select
        from app.database import async_session
        from app.crawler.fof99.client import Fof99Client
        from app.models.fund import Fund
        from app.services.ingestion_service import IngestionService

        client = Fof99Client()
        await client.login()
        try:
            service = IngestionService(client)
            async with async_session() as db:
                # 确定要处理的基金列表
                if fund_ids:
                    result = await db.execute(
                        select(Fund).where(Fund.id.in_(fund_ids))
                    )
                else:
                    result = await db.execute(
                        select(Fund)
                        .where(Fund.nav_status == "has_data")
                        .where(Fund.fof99_fund_id.isnot(None))
                        .where(Fund.status == "active")
                        .order_by(Fund.id)
                    )
                funds = list(result.scalars().all())

                total = len(funds)
                processed, failed = 0, 0
                errors: list[str] = []

                logger.info("增量净值更新: 共 %d 只基金待处理", total)

                for fund in funds:
                    try:
                        await service.ingest_fund_nav(
                            db, fund_id=fund.id, incremental=True, delay=0.5
                        )
                        processed += 1
                    except Exception as e:
                        failed += 1
                        err_msg = f"fund_id={fund.id} ({fund.fund_name[:20]}): {e}"
                        errors.append(err_msg)
                        logger.warning("增量更新失败: %s", err_msg)

                    # 更新任务进度
                    self.update_state(
                        state="PROGRESS",
                        meta={"processed": processed, "failed": failed, "total": total},
                    )

                await db.commit()

                logger.info("增量净值更新完成: 处理=%d, 失败=%d, 总数=%d", processed, failed, total)
                return {"processed": processed, "failed": failed, "errors": errors[:50]}
        finally:
            await client.close()

    try:
        return _run_async(_run())
    except Exception as exc:
        logger.exception("增量净值更新任务异常")
        raise self.retry(exc=exc, countdown=60)


@celery_app.task(bind=True, name="app.tasks.analysis.refresh_nav_full", max_retries=1)
def refresh_nav_full(self, fund_ids: list[int]) -> dict:
    """全量重新下载基金净值（删除已有数据 + 重新爬取）。

    Args:
        fund_ids: 必须指定基金ID列表。

    Returns:
        {"processed": N, "failed": M, "errors": [...]}
    """
    async def _run() -> dict:
        from app.database import async_session
        from app.crawler.fof99.client import Fof99Client
        from app.services.ingestion_service import IngestionService

        client = Fof99Client()
        await client.login()
        try:
            service = IngestionService(client)
            total = len(fund_ids)
            processed, failed = 0, 0
            errors: list[str] = []

            logger.info("全量净值重建: 共 %d 只基金待处理", total)

            for fid in fund_ids:
                try:
                    # Per-fund transaction: delete+rebuild is atomic per fund
                    async with async_session() as db:
                        async with db.begin():
                            await service.ingest_fund_nav_full(db, fund_id=fid, delay=0.5)
                    processed += 1
                except Exception as e:
                    failed += 1
                    err_msg = f"fund_id={fid}: {e}"
                    errors.append(err_msg)
                    logger.warning("全量重建失败: %s", err_msg)

                self.update_state(
                    state="PROGRESS",
                    meta={"processed": processed, "failed": failed, "total": total},
                )

            logger.info("全量净值重建完成: 处理=%d, 失败=%d, 总数=%d", processed, failed, total)
            return {"processed": processed, "failed": failed, "errors": errors[:50]}
        finally:
            await client.close()

    try:
        return _run_async(_run())
    except Exception as exc:
        logger.exception("全量净值重建任务异常")
        raise self.retry(exc=exc, countdown=60)


@celery_app.task(bind=True, name="app.tasks.analysis.daily_data_refresh", max_retries=1)
def daily_data_refresh(self) -> dict:
    """每日定时数据刷新: 增量更新活跃基金净值，然后执行数据质量检查。

    Returns:
        {"nav_result": {...}, "quality_updated": N}
    """
    async def _run() -> dict:
        from sqlalchemy import select, func
        from app.database import async_session
        from app.crawler.fof99.client import Fof99Client
        from app.models.fund import Fund
        from app.services.ingestion_service import IngestionService

        client = Fof99Client()
        await client.login()
        try:
            service = IngestionService(client)
            async with async_session() as db:
                # 阶段1: 增量刷新所有活跃且有数据的基金
                result = await db.execute(
                    select(Fund)
                    .where(Fund.status == "active")
                    .where(Fund.nav_status == "has_data")
                    .where(Fund.fof99_fund_id.isnot(None))
                    .order_by(Fund.id)
                )
                funds = list(result.scalars().all())

                total = len(funds)
                processed, failed = 0, 0
                errors: list[str] = []

                logger.info("每日数据刷新 - 阶段1(净值更新): 共 %d 只基金", total)

                for fund in funds:
                    try:
                        await service.ingest_fund_nav(
                            db, fund_id=fund.id, incremental=True, delay=0.5
                        )
                        processed += 1
                    except Exception as e:
                        failed += 1
                        err_msg = f"fund_id={fund.id}: {e}"
                        errors.append(err_msg)
                        logger.warning("每日刷新-净值更新失败: %s", err_msg)

                    self.update_state(
                        state="PROGRESS",
                        meta={
                            "phase": "nav_refresh",
                            "processed": processed,
                            "failed": failed,
                            "total": total,
                        },
                    )

                nav_result = {"processed": processed, "failed": failed, "errors": errors[:50]}

                # 阶段2: 数据质量检查
                logger.info("每日数据刷新 - 阶段2(数据质量检查)")
                quality_updated = 0

                result = await db.execute(
                    select(Fund)
                    .where(Fund.status == "active")
                    .where(Fund.nav_status == "has_data")
                    .order_by(Fund.id)
                )
                active_funds = list(result.scalars().all())

                for fund in active_funds:
                    try:
                        await service._update_data_quality(db, fund)
                        quality_updated += 1
                    except Exception as e:
                        logger.warning("数据质量检查失败 fund_id=%d: %s", fund.id, e)

                self.update_state(
                    state="PROGRESS",
                    meta={"phase": "quality_check", "quality_updated": quality_updated},
                )

                await db.commit()

                logger.info("每日数据刷新完成: 净值更新=%d/%d, 质量检查=%d",
                            processed, total, quality_updated)
                return {
                    "nav_result": nav_result,
                    "quality_updated": quality_updated,
                }
        finally:
            await client.close()

    try:
        return _run_async(_run())
    except Exception as exc:
        logger.exception("每日数据刷新任务异常")
        raise self.retry(exc=exc, countdown=120)
