"""每日净值更新定时任务 — 增量更新现有基金的最新净值。

功能:
1. 日频基金: 每个交易日更新（仅拉最新数据点）
2. 周频基金: 每周六更新
3. 失败重试: 上次rate_limited/fetch_failed的优先重试
4. 完成后重新计算数据质量评分

用法:
    cd backend
    python scripts/daily_update.py                # 自动判断今天该更新什么
    python scripts/daily_update.py --force-all     # 强制更新全部
    python scripts/daily_update.py --limit 100     # 限制更新数量
    python scripts/daily_update.py --dry-run       # 仅预览不执行
"""

import argparse
import asyncio
import logging
import os
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ["DEBUG"] = "false"

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.crawler.fof99.client import Fof99Client, RateLimitError
from app.crawler.fof99.nav_scraper import NavScraper
from app.database import async_session
from app.models.fund import Fund, NavHistory

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("daily_update")
logger.setLevel(logging.INFO)


async def is_trading_day(target_date: date) -> bool:
    """检查是否为交易日。"""
    async with async_session() as db:
        result = await db.execute(text(
            "SELECT is_trading_day FROM trading_calendar WHERE cal_date = :d"
        ), {"d": target_date})
        row = result.first()
        return bool(row and row[0])


async def get_funds_to_update(force_all: bool = False) -> list[tuple]:
    """获取需要更新的基金列表。"""
    today = date.today()
    weekday = today.weekday()  # 0=Mon, 6=Sun

    async with async_session() as db:
        if force_all:
            result = await db.execute(text("""
                SELECT f.id, f.fof99_fund_id, f.fund_name, f.nav_frequency, f.latest_nav_date
                FROM funds f
                WHERE f.fof99_fund_id IS NOT NULL
                AND f.status = 'active'
                AND f.nav_status = 'has_data'
                ORDER BY f.latest_nav_date ASC NULLS FIRST
            """))
        else:
            # 智能选择:
            # 1. rate_limited/fetch_failed 优先重试
            # 2. 日频基金: latest_nav_date < today - 1
            # 3. 周频基金: 仅在周六(weekday=5)或周日(6)更新
            conditions = []
            conditions.append("f.nav_status IN ('rate_limited', 'fetch_failed')")

            if weekday in (5, 6):  # 周六/周日更新周频
                conditions.append(
                    "(f.nav_frequency = 'weekly' AND (f.latest_nav_date IS NULL OR f.latest_nav_date < :cutoff_weekly))"
                )
            # 交易日更新日频 (非周末都尝试)
            if weekday < 5:
                conditions.append(
                    "(f.nav_frequency = 'daily' AND (f.latest_nav_date IS NULL OR f.latest_nav_date < :cutoff_daily))"
                )

            where_clause = " OR ".join(conditions)
            result = await db.execute(text(f"""
                SELECT f.id, f.fof99_fund_id, f.fund_name, f.nav_frequency, f.latest_nav_date
                FROM funds f
                WHERE f.fof99_fund_id IS NOT NULL
                AND f.status = 'active'
                AND f.nav_status IN ('has_data', 'rate_limited', 'fetch_failed')
                AND ({where_clause})
                ORDER BY
                    CASE f.nav_status
                        WHEN 'rate_limited' THEN 0
                        WHEN 'fetch_failed' THEN 1
                        ELSE 2
                    END,
                    f.latest_nav_date ASC NULLS FIRST
            """), {
                "cutoff_daily": today - timedelta(days=2),
                "cutoff_weekly": today - timedelta(days=8),
            })

        return [(r[0], r[1], r[2], r[3], r[4]) for r in result.all()]


async def update_single_fund(
    scraper: NavScraper,
    fund_id: int,
    fof99_id: str,
    fund_name: str,
    latest_date: date | None,
) -> int:
    """增量更新单只基金的净值。返回新增记录数。"""
    nav_records, freq = await scraper.fetch_nav_with_frequency(fof99_id)
    if not nav_records:
        return 0

    # 只保留latest_date之后的新数据
    new_records = []
    for r in nav_records:
        nav_date_str = r.get("nav_date", "")
        if not nav_date_str:
            continue
        try:
            nav_date = date.fromisoformat(nav_date_str[:10])
        except (ValueError, TypeError):
            continue
        if latest_date and nav_date <= latest_date:
            continue
        new_records.append({
            "fund_id": fund_id,
            "nav_date": nav_date,
            "unit_nav": r.get("unit_nav"),
            "cumulative_nav": r.get("cumulative_nav"),
            "daily_return": r.get("change_pct"),
            "data_source": "fof99",
        })

    if not new_records:
        return 0

    async with async_session() as db:
        stmt = pg_insert(NavHistory).values(new_records)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_nav_history_fund_date",
            set_={
                "unit_nav": stmt.excluded.unit_nav,
                "cumulative_nav": stmt.excluded.cumulative_nav,
                "daily_return": stmt.excluded.daily_return,
            },
        )
        await db.execute(stmt)

        # 更新基金的最新净值
        fund = await db.get(Fund, fund_id)
        if fund:
            last = nav_records[-1]
            fund.latest_nav = last.get("unit_nav")
            try:
                fund.latest_nav_date = date.fromisoformat(last.get("nav_date", "")[:10])
            except (ValueError, TypeError):
                pass
            if freq in ("daily", "weekly"):
                fund.nav_frequency = freq

        await db.commit()
        return len(new_records)


async def main():
    parser = argparse.ArgumentParser(description="每日净值增量更新")
    parser.add_argument("--force-all", action="store_true", help="强制更新全部基金")
    parser.add_argument("--limit", type=int, default=0, help="最多更新几只(0=全部)")
    parser.add_argument("--delay", type=float, default=0.5, help="请求间延迟(秒)")
    parser.add_argument("--retry", type=int, default=2, help="失败重试次数")
    parser.add_argument("--dry-run", action="store_true", help="仅预览不执行")
    args = parser.parse_args()

    today = date.today()
    weekday = today.strftime("%A")
    logger.info("每日更新启动: %s (%s)", today, weekday)

    funds = await get_funds_to_update(args.force_all)
    if args.limit > 0:
        funds = funds[:args.limit]

    logger.info("需要更新: %d 只基金", len(funds))

    if args.dry_run:
        for fid, _, name, freq, last_date in funds[:20]:
            print(f"  {fid:5d} {name[:25]:25s} {freq or '?':7s} 最新: {last_date}")
        if len(funds) > 20:
            print(f"  ... 还有 {len(funds)-20} 只")
        return

    if not funds:
        logger.info("无需更新")
        return

    client = Fof99Client()
    await client.login()
    scraper = NavScraper(client)

    success, failed, no_new, total_records = 0, 0, 0, 0
    start_time = time.time()

    try:
        for i, (fund_id, fof99_id, fund_name, freq, latest_date) in enumerate(funds):
            count = 0
            last_error = None

            for attempt in range(args.retry + 1):
                try:
                    count = await update_single_fund(scraper, fund_id, fof99_id, fund_name, latest_date)
                    last_error = None
                    break
                except RateLimitError as e:
                    last_error = e
                    if attempt < args.retry:
                        wait = 15 * (attempt + 1)
                        logger.warning("限流 %d (%s) attempt %d, 等%ds", fund_id, fund_name[:15], attempt + 1, wait)
                        await asyncio.sleep(wait)
                except Exception as e:
                    last_error = e
                    if attempt < args.retry:
                        wait = 5 * (attempt + 1)
                        logger.warning("错误 %d (%s): %s", fund_id, fund_name[:15], e)
                        await asyncio.sleep(wait)

            if last_error:
                failed += 1
            elif count > 0:
                success += 1
                total_records += count
            else:
                no_new += 1

            if (i + 1) % 50 == 0 or i == len(funds) - 1:
                elapsed = time.time() - start_time
                logger.info(
                    "进度: %d/%d | 更新=%d 无新=%d 失败=%d | 新记录=%d | %.0f秒",
                    i + 1, len(funds), success, no_new, failed, total_records, elapsed
                )

            if args.delay > 0 and not last_error:
                await asyncio.sleep(args.delay)

    finally:
        await client.close()

    elapsed = time.time() - start_time
    logger.info("=" * 50)
    logger.info("更新完成! 耗时 %.1f秒", elapsed)
    logger.info("有新数据: %d 只 (%d 条记录)", success, total_records)
    logger.info("无新数据: %d 只", no_new)
    logger.info("失败: %d 只", failed)


if __name__ == "__main__":
    asyncio.run(main())
