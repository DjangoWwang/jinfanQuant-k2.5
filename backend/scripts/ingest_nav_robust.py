"""稳健版NAV入库脚本 — 分批处理，逐只提交，智能限流+背压控制。

特性:
1. 基于nav_status字段选择待处理基金（跳过has_data和no_data）
2. 每只基金独立session+commit
3. 识别RateLimitError并触发全局背压暂停
4. 入库后更新nav_status标记
5. 支持断点续传(--resume-from)

用法:
    cd backend
    python scripts/ingest_nav_robust.py
    python scripts/ingest_nav_robust.py --delay 1.0 --retry 2
    python scripts/ingest_nav_robust.py --resume-from 500
    python scripts/ingest_nav_robust.py --status  # 仅查看状态
"""

import argparse
import asyncio
import logging
import os
import sys
import time
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

os.environ["DEBUG"] = "false"

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.crawler.fof99.client import Fof99Client, RateLimitError
from app.crawler.fof99.nav_scraper import NavScraper, detect_frequency
from app.database import async_session
from app.models.fund import Fund, NavHistory

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("nav_robust")
logger.setLevel(logging.INFO)


async def get_pending_funds(resume_from: int = 0) -> list[tuple[int, str, str]]:
    """获取待处理基金（nav_status为pending/rate_limited/fetch_failed）。"""
    async with async_session() as db:
        result = await db.execute(text("""
            SELECT f.id, f.fof99_fund_id, f.fund_name
            FROM funds f
            WHERE f.fof99_fund_id IS NOT NULL
            AND f.status = 'active'
            AND (f.nav_status IS NULL OR f.nav_status IN ('pending', 'rate_limited', 'fetch_failed'))
            AND f.id >= :resume_from
            ORDER BY
                CASE f.nav_status
                    WHEN 'rate_limited' THEN 0
                    WHEN 'fetch_failed' THEN 1
                    ELSE 2
                END,
                f.id
        """), {"resume_from": resume_from})
        return [(row[0], row[1], row[2]) for row in result.all()]


async def update_nav_status(fund_id: int, status: str) -> None:
    """更新基金的nav_status。"""
    async with async_session() as db:
        await db.execute(
            text("UPDATE funds SET nav_status = :status WHERE id = :fid"),
            {"status": status, "fid": fund_id},
        )
        await db.commit()


async def ingest_single_fund(
    scraper: NavScraper,
    fund_id: int,
    fof99_id: str,
    fund_name: str,
) -> int:
    """为单只基金拉取净值并入库。返回入库记录数。RateLimitError不在此处理。"""
    nav_records, freq = await scraper.fetch_nav_with_frequency(fof99_id)
    if not nav_records:
        return 0

    async with async_session() as db:
        rows = []
        for r in nav_records:
            nav_date_str = r.get("nav_date", "")
            if not nav_date_str:
                continue
            try:
                nav_date = date.fromisoformat(nav_date_str[:10])
            except (ValueError, TypeError):
                continue
            rows.append({
                "fund_id": fund_id,
                "nav_date": nav_date,
                "unit_nav": r.get("unit_nav"),
                "cumulative_nav": r.get("cumulative_nav"),
                "daily_return": r.get("change_pct"),
                "data_source": "fof99",
            })

        if not rows:
            return 0

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

        fund = await db.get(Fund, fund_id)
        if fund:
            if freq in ("daily", "weekly"):
                fund.nav_frequency = freq
            last = nav_records[-1]
            fund.latest_nav = last.get("unit_nav")
            try:
                fund.latest_nav_date = date.fromisoformat(last.get("nav_date", "")[:10])
            except (ValueError, TypeError):
                pass
            fund.nav_status = "has_data"

        await db.commit()
        return len(rows)


async def show_status():
    """显示当前各状态基金数量。"""
    async with async_session() as db:
        result = await db.execute(text("""
            SELECT
                COALESCE(nav_status, 'null') as status,
                count(*) as cnt
            FROM funds
            WHERE fof99_fund_id IS NOT NULL
            GROUP BY nav_status
            ORDER BY cnt DESC
        """))
        print("\n基金NAV状态分布:")
        print("-" * 30)
        for row in result.all():
            print(f"  {row[0]:15s}: {row[1]:5d}")

        result2 = await db.execute(text("SELECT count(DISTINCT fund_id), count(*) FROM nav_history"))
        row = result2.one()
        print(f"\nnav_history: {row[0]}只基金, {row[1]}条记录")


async def main():
    parser = argparse.ArgumentParser(description="稳健版NAV入库（含限流背压）")
    parser.add_argument("--delay", type=float, default=0.5, help="基础请求间延迟(秒)")
    parser.add_argument("--retry", type=int, default=2, help="失败重试次数")
    parser.add_argument("--limit", type=int, default=0, help="最多处理几只(0=全部)")
    parser.add_argument("--resume-from", type=int, default=0, help="从指定fund_id开始")
    parser.add_argument("--status", action="store_true", help="仅显示状态统计")
    args = parser.parse_args()

    if args.status:
        await show_status()
        return

    funds = await get_pending_funds(args.resume_from)
    total = len(funds)
    if args.limit > 0:
        funds = funds[:args.limit]

    logger.info("共 %d 只基金待入库 (本次处理 %d 只)", total, len(funds))

    if not funds:
        logger.info("所有基金已处理，无需操作")
        await show_status()
        return

    client = Fof99Client()
    logger.info("正在登录火富牛...")
    await client.login()
    logger.info("登录成功")

    scraper = NavScraper(client)

    success, failed, empty, rate_limited, total_records = 0, 0, 0, 0, 0
    consecutive_failures = 0
    start_time = time.time()

    try:
        for i, (fund_id, fof99_id, fund_name) in enumerate(funds):
            # 背压检查: 连续失败过多时暂停
            if consecutive_failures >= 10:
                logger.error("连续失败%d次，终止执行。使用 --resume-from %d 继续", consecutive_failures, fund_id)
                break
            if consecutive_failures >= 5:
                pause = 120
                logger.warning("连续失败%d次，暂停%d秒...", consecutive_failures, pause)
                await asyncio.sleep(pause)

            count = 0
            last_error = None
            is_rate_limit = False

            for attempt in range(args.retry + 1):
                try:
                    count = await ingest_single_fund(scraper, fund_id, fof99_id, fund_name)
                    last_error = None
                    break
                except RateLimitError as e:
                    is_rate_limit = True
                    last_error = e
                    if attempt < args.retry:
                        wait = 15 * (attempt + 1)  # 15, 30, 45...
                        logger.warning("限流 %d (%s) attempt %d, 等%ds", fund_id, fund_name[:15], attempt + 1, wait)
                        await asyncio.sleep(wait)
                except Exception as e:
                    last_error = e
                    if attempt < args.retry:
                        wait = 5 * (attempt + 1)
                        logger.warning("错误 %d (%s) attempt %d, 等%ds: %s", fund_id, fund_name[:15], attempt + 1, wait, e)
                        await asyncio.sleep(wait)

            # 处理结果
            if last_error:
                if is_rate_limit:
                    rate_limited += 1
                    await update_nav_status(fund_id, "rate_limited")
                    consecutive_failures += 1
                else:
                    failed += 1
                    await update_nav_status(fund_id, "fetch_failed")
                    consecutive_failures += 1
            elif count > 0:
                success += 1
                total_records += count
                consecutive_failures = 0  # 重置
            else:
                empty += 1
                await update_nav_status(fund_id, "no_data")
                consecutive_failures = 0  # 无数据不算失败

            # 进度日志
            if (i + 1) % 10 == 0 or i == len(funds) - 1:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                eta = (len(funds) - i - 1) / rate if rate > 0 else 0
                logger.info(
                    "进度: %d/%d (%.0f%%) | 成功=%d 空=%d 限流=%d 失败=%d | 记录=%d | ETA %.0f秒",
                    i + 1, len(funds), (i + 1) / len(funds) * 100,
                    success, empty, rate_limited, failed, total_records, eta
                )

            if args.delay > 0 and not last_error:
                await asyncio.sleep(args.delay)

    finally:
        await client.close()

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("入库完成! 耗时 %.1f秒", elapsed)
    logger.info("成功: %d 只 (%d 条记录)", success, total_records)
    logger.info("无数据: %d 只", empty)
    logger.info("限流: %d 只 (下次优先重试)", rate_limited)
    logger.info("失败: %d 只", failed)


if __name__ == "__main__":
    asyncio.run(main())
