"""爬虫数据入库服务 — 将火富牛爬取数据写入数据库。

流程: Fof99Client → Scraper → 标准化 → DB upsert
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from typing import Any

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.crawler.fof99.client import Fof99Client
from app.crawler.fof99.fund_scraper import FundScraper
from app.crawler.fof99.nav_scraper import NavScraper, detect_frequency
from app.crawler.fof99.index_scraper import IndexScraper
from app.models.fund import Fund, NavHistory
from app.models.benchmark import Benchmark, IndexNav
from app.models.scrape import ScrapeJob

logger = logging.getLogger(__name__)


class IngestionService:
    """将爬虫数据导入数据库的服务层。"""

    def __init__(self, client: Fof99Client) -> None:
        self._client = client
        self._fund_scraper = FundScraper(client)
        self._nav_scraper = NavScraper(client)
        self._index_scraper = IndexScraper(client)

    # ------------------------------------------------------------------
    # 基金列表入库
    # ------------------------------------------------------------------

    async def ingest_fund_list(
        self,
        db: AsyncSession,
        strategy_ids: list[int] | None = None,
        max_pages: int = 100,
        skip_no_nav: bool = True,
    ) -> dict[str, int]:
        """爬取基金列表并入库。

        Args:
            skip_no_nav: 跳过没有最新净值的基金(默认True，大量基金无数据)

        Returns:
            {"created": N, "updated": M, "skipped": K}
        """
        job = ScrapeJob(job_type="fund_list", status="running", started_at=datetime.utcnow())
        db.add(job)
        await db.flush()

        try:
            raw_funds = await self._fund_scraper.fetch_all_advanced(
                strategy_ids=strategy_ids, max_pages=max_pages
            )
            logger.info("爬取到 %d 只基金", len(raw_funds))

            created, updated, skipped = 0, 0, 0

            for raw in raw_funds:
                filing_number = raw.get("filing_number", "").strip()
                fund_name = raw.get("fund_name", "").strip()

                if not fund_name:
                    skipped += 1
                    continue

                # 跳过无净值的基金（39万基金中大多数无数据）
                if skip_no_nav and not raw.get("latest_nav"):
                    skipped += 1
                    continue

                # 按 fof99_fund_id 或 filing_number 查找已有记录
                encode_id = raw.get("encode_id", "")
                existing = None

                if encode_id:
                    result = await db.execute(
                        select(Fund).where(Fund.fof99_fund_id == encode_id)
                    )
                    existing = result.scalar_one_or_none()

                if not existing and filing_number:
                    result = await db.execute(
                        select(Fund).where(Fund.filing_number == filing_number)
                    )
                    existing = result.scalar_one_or_none()

                if existing:
                    # 更新现有记录
                    existing.fund_name = fund_name
                    if filing_number:
                        existing.filing_number = filing_number
                    existing.strategy_type = raw.get("strategy_type") or existing.strategy_type
                    existing.strategy_sub = raw.get("strategy_sub") or existing.strategy_sub
                    if raw.get("inception_date"):
                        existing.inception_date = _parse_date(raw["inception_date"])
                    if raw.get("latest_nav") is not None:
                        existing.latest_nav = raw["latest_nav"]
                    if raw.get("latest_nav_date"):
                        existing.latest_nav_date = _parse_date(raw["latest_nav_date"])
                    if encode_id:
                        existing.fof99_fund_id = encode_id
                    updated += 1
                else:
                    # 创建新记录
                    fund = Fund(
                        fund_name=fund_name,
                        filing_number=filing_number or None,
                        manager_name=raw.get("advisor") or "",
                        inception_date=_parse_date(raw.get("inception_date")),
                        strategy_type=raw.get("strategy_type") or None,
                        strategy_sub=raw.get("strategy_sub") or None,
                        latest_nav=raw.get("latest_nav"),
                        latest_nav_date=_parse_date(raw.get("latest_nav_date")),
                        nav_frequency="daily",
                        data_source="fof99",
                        fof99_fund_id=encode_id or None,
                    )
                    db.add(fund)
                    created += 1

            await db.flush()

            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.records_added = created
            job.records_updated = updated
            logger.info("基金列表入库完成: 新增=%d, 更新=%d, 跳过=%d", created, updated, skipped)

        except Exception as e:
            job.status = "failed"
            job.completed_at = datetime.utcnow()
            job.error_log = str(e)[:2000]
            logger.exception("基金列表入库失败")
            raise

        return {"created": created, "updated": updated, "skipped": skipped}

    # ------------------------------------------------------------------
    # 基金净值入库
    # ------------------------------------------------------------------

    async def ingest_fund_nav(
        self,
        db: AsyncSession,
        fund_id: int | None = None,
        limit: int | None = None,
        incremental: bool = True,
        delay: float = 0.5,
    ) -> dict[str, int]:
        """爬取基金净值并入库。

        Args:
            fund_id: 指定单只基金ID，None则全量
            limit: 最多处理几只基金
            incremental: True则只拉增量(最新日期之后)
            delay: 每只基金间的延迟秒数(防封)

        Returns:
            {"funds_processed": N, "records_upserted": M, "errors": K}
        """
        job = ScrapeJob(job_type="fund_nav", status="running", started_at=datetime.utcnow())
        db.add(job)
        await db.flush()

        try:
            if fund_id:
                funds = []
                result = await db.execute(select(Fund).where(Fund.id == fund_id))
                f = result.scalar_one_or_none()
                if f:
                    funds = [f]
            else:
                query = (
                    select(Fund)
                    .where(Fund.status == "active")
                    .where(Fund.fof99_fund_id.isnot(None))
                    .order_by(Fund.id)
                )
                if limit:
                    query = query.limit(limit)
                result = await db.execute(query)
                funds = list(result.scalars().all())

            logger.info("准备爬取 %d 只基金的净值", len(funds))

            processed, total_records, errors = 0, 0, 0

            for fund in funds:
                try:
                    encode_id = fund.fof99_fund_id
                    if not encode_id:
                        continue

                    # 增量模式：只爬最新日期之后的数据
                    start_date = None
                    if incremental and fund.latest_nav_date:
                        start_date = fund.latest_nav_date.isoformat()

                    nav_records, freq = await self._nav_scraper.fetch_nav_with_frequency(
                        encode_id, start_date
                    )

                    if not nav_records:
                        processed += 1
                        continue

                    # 更新频率
                    if freq in ("daily", "weekly"):
                        fund.nav_frequency = freq

                    # 写入nav_history
                    count = await self._upsert_nav_batch(db, fund.id, nav_records)
                    total_records += count

                    # 更新latest_nav
                    last = nav_records[-1]
                    fund.latest_nav = last.get("unit_nav")
                    fund.latest_nav_date = _parse_date(last.get("nav_date"))

                    processed += 1
                    logger.info(
                        "基金 %s (%s): %d条净值, 频率=%s",
                        fund.fund_name[:20], encode_id, count, freq
                    )

                    if delay > 0:
                        await asyncio.sleep(delay)

                except Exception as e:
                    errors += 1
                    logger.warning("基金 %s 净值爬取失败: %s", fund.id, e)

            await db.flush()

            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.records_added = total_records
            logger.info("净值入库完成: 处理=%d, 记录=%d, 错误=%d", processed, total_records, errors)

        except Exception as e:
            job.status = "failed"
            job.completed_at = datetime.utcnow()
            job.error_log = str(e)[:2000]
            logger.exception("净值入库失败")
            raise

        return {"funds_processed": processed, "records_upserted": total_records, "errors": errors}

    async def _upsert_nav_batch(
        self, db: AsyncSession, fund_id: int, records: list[dict[str, Any]]
    ) -> int:
        """批量upsert净值记录。"""
        if not records:
            return 0

        rows = []
        for r in records:
            nav_date = _parse_date(r["nav_date"])
            if not nav_date:
                continue
            rows.append({
                "fund_id": fund_id,
                "nav_date": nav_date,
                "unit_nav": r.get("unit_nav"),
                "cumulative_nav": r.get("cumulative_nav"),
                "daily_return": r.get("change_pct"),
                "data_source": "fof99",
            })

        stmt = pg_insert(NavHistory).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_nav_history_fund_date",
            set_={
                "unit_nav": stmt.excluded.unit_nav,
                "cumulative_nav": stmt.excluded.cumulative_nav,
                "daily_return": stmt.excluded.daily_return,
            },
        )
        await db.execute(stmt)
        return len(rows)

    # ------------------------------------------------------------------
    # 指数数据入库
    # ------------------------------------------------------------------

    async def ingest_index_list(
        self,
        db: AsyncSession,
    ) -> dict[str, int]:
        """爬取指数列表并入库到benchmarks表。"""
        raw_list = await self._index_scraper.fetch_index_list()
        logger.info("爬取到 %d 个指数", len(raw_list))

        created, updated = 0, 0
        for raw in raw_list:
            index_code = raw.get("index_code", "").strip()
            if not index_code:
                continue

            result = await db.execute(
                select(Benchmark).where(Benchmark.index_code == index_code)
            )
            existing = result.scalar_one_or_none()

            if existing:
                existing.index_name = raw.get("index_name") or existing.index_name
                existing.category = raw.get("category") or existing.category
                updated += 1
            else:
                bm = Benchmark(
                    index_code=index_code,
                    index_name=raw.get("index_name", ""),
                    category=raw.get("category", ""),
                    data_source="fof99",
                    is_public=True,
                )
                db.add(bm)
                created += 1

        await db.flush()
        logger.info("指数列表入库: 新增=%d, 更新=%d", created, updated)
        return {"created": created, "updated": updated}

    async def ingest_index_nav(
        self,
        db: AsyncSession,
        index_code: str | None = None,
        limit: int | None = None,
        delay: float = 0.5,
    ) -> dict[str, int]:
        """爬取指数历史净值并入库到index_nav表。"""
        if index_code:
            result = await db.execute(
                select(Benchmark).where(Benchmark.index_code == index_code)
            )
            benchmarks = [b for b in [result.scalar_one_or_none()] if b]
        else:
            query = select(Benchmark).where(Benchmark.is_active.is_(True)).order_by(Benchmark.id)
            if limit:
                query = query.limit(limit)
            result = await db.execute(query)
            benchmarks = list(result.scalars().all())

        logger.info("准备爬取 %d 个指数的历史净值", len(benchmarks))

        processed, total_records, errors = 0, 0, 0

        # 需要 index_list 来获取 source_id (viewv2 需要的fid)
        raw_index_list = await self._index_scraper.fetch_index_list()
        code_to_source_id = {r["index_code"]: r["source_id"] for r in raw_index_list}

        for bm in benchmarks:
            try:
                source_id = code_to_source_id.get(bm.index_code)
                if not source_id:
                    logger.warning("指数 %s 无source_id，跳过", bm.index_code)
                    continue

                prices = await self._index_scraper.fetch_index_history(source_id)
                if not prices:
                    processed += 1
                    continue

                rows = [{
                    "index_code": bm.index_code,
                    "nav_date": p["date"],
                    "nav_value": p["close"],
                    "daily_return": p.get("change_pct"),
                } for p in prices]

                stmt = pg_insert(IndexNav).values(rows)
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_index_nav_code_date",
                    set_={
                        "nav_value": stmt.excluded.nav_value,
                        "daily_return": stmt.excluded.daily_return,
                    },
                )
                await db.execute(stmt)
                total_records += len(rows)
                processed += 1

                logger.info("指数 %s: %d条净值", bm.index_code, len(rows))

                if delay > 0:
                    await asyncio.sleep(delay)

            except Exception as e:
                errors += 1
                logger.warning("指数 %s 净值爬取失败: %s", bm.index_code, e)

        await db.flush()
        logger.info("指数净值入库: 处理=%d, 记录=%d, 错误=%d", processed, total_records, errors)
        return {"indices_processed": processed, "records_upserted": total_records, "errors": errors}


def _parse_date(value: Any) -> date | None:
    """安全解析日期字符串。"""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None
