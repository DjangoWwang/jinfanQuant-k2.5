"""基金池扩展脚本 — 每天从火富牛39万基金池中发现新基金并入库。

流程:
1. 从advancedList分页获取基金列表(每页300只)
2. 过滤: 跳过已入库的基金(按fof99_fund_id去重)
3. 新基金写入funds表
4. 拉取NAV历史并入库
5. 记录已处理到第几页(断点续传)

用法:
    cd backend
    python scripts/expand_fund_pool.py                    # 默认每天处理1000只新基金
    python scripts/expand_fund_pool.py --daily-limit 500  # 每天500只
    python scripts/expand_fund_pool.py --start-page 100   # 从第100页开始
    python scripts/expand_fund_pool.py --status            # 查看进度
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ["DEBUG"] = "false"

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.crawler.fof99.client import Fof99Client, RateLimitError
from app.crawler.fof99.fund_scraper import FundScraper
from app.crawler.fof99.nav_scraper import NavScraper
from app.database import async_session
from app.models.fund import Fund, NavHistory

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("expand_pool")
logger.setLevel(logging.INFO)

# 进度文件
PROGRESS_FILE = Path(__file__).parent / ".expand_progress.json"


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    return {"last_page": 0, "total_discovered": 0, "total_with_nav": 0, "last_run": None}


def save_progress(progress: dict):
    progress["last_run"] = str(date.today())
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False, indent=2), encoding="utf-8")


async def get_existing_fof99_ids() -> set[str]:
    """获取已入库的所有fof99_fund_id。"""
    async with async_session() as db:
        result = await db.execute(text(
            "SELECT fof99_fund_id FROM funds WHERE fof99_fund_id IS NOT NULL"
        ))
        return {r[0] for r in result.all()}


async def insert_new_fund(fund_data: dict) -> int | None:
    """将新基金写入funds表，返回fund_id。如已存在返回None。"""
    async with async_session() as db:
        stmt = pg_insert(Fund.__table__).values(
            fund_name=fund_data.get("fund_name", ""),
            filing_number=fund_data.get("filing_number") or None,
            manager_name=fund_data.get("advisor", ""),
            inception_date=fund_data.get("inception_date"),
            strategy_type=fund_data.get("strategy_type", ""),
            strategy_sub=fund_data.get("strategy_sub", ""),
            latest_nav=fund_data.get("latest_nav"),
            latest_nav_date=fund_data.get("latest_nav_date"),
            data_source="fof99",
            fof99_fund_id=fund_data["encode_id"],
            status="active",
            nav_status="pending",
        ).on_conflict_do_nothing(index_elements=["filing_number"])
        result = await db.execute(stmt)
        await db.commit()

        if result.rowcount == 0:
            return None

        # 获取刚插入的fund_id
        r = await db.execute(text(
            "SELECT id FROM funds WHERE fof99_fund_id = :fid"
        ), {"fid": fund_data["encode_id"]})
        row = r.first()
        return row[0] if row else None


async def ingest_fund_nav(
    scraper: NavScraper, fund_id: int, fof99_id: str
) -> int:
    """拉取单只基金的NAV并入库。返回记录数。"""
    nav_records, freq = await scraper.fetch_nav_with_frequency(fof99_id)
    if not nav_records:
        return 0

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

    async with async_session() as db:
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
    progress = load_progress()
    async with async_session() as db:
        r = await db.execute(text("""
            SELECT COALESCE(nav_status, 'null'), count(*)
            FROM funds GROUP BY nav_status ORDER BY count(*) DESC
        """))
        print("\n基金状态分布:")
        total = 0
        for row in r.all():
            print(f"  {row[0]:15s}: {row[1]:6d}")
            total += row[1]
        print(f"  {'总计':15s}: {total:6d}")

    print(f"\n扩展进度:")
    print(f"  上次处理到: 第{progress['last_page']}页")
    print(f"  已发现新基金: {progress['total_discovered']}")
    print(f"  有NAV数据: {progress['total_with_nav']}")
    print(f"  上次运行: {progress['last_run']}")
    print(f"  39万基金约需: {390000 // 300}页 (每页300)")
    remaining_pages = 390000 // 300 - progress['last_page']
    print(f"  剩余: ~{remaining_pages}页")


async def main():
    parser = argparse.ArgumentParser(description="基金池扩展 — 每天发现新基金")
    parser.add_argument("--daily-limit", type=int, default=1000, help="每天处理的新基金数量上限")
    parser.add_argument("--start-page", type=int, default=0, help="从第几页开始(覆盖进度)")
    parser.add_argument("--delay", type=float, default=0.5, help="请求间延迟(秒)")
    parser.add_argument("--status", action="store_true", help="仅显示进度")
    args = parser.parse_args()

    if args.status:
        await show_status()
        return

    progress = load_progress()
    start_page = args.start_page if args.start_page > 0 else progress["last_page"] + 1

    logger.info("基金池扩展启动: 从第%d页开始, 每天上限%d只", start_page, args.daily_limit)

    # 获取已有的fof99_id集合
    existing_ids = await get_existing_fof99_ids()
    logger.info("已入库基金: %d 只", len(existing_ids))

    client = Fof99Client()
    await client.login()
    fund_scraper = FundScraper(client)
    nav_scraper = NavScraper(client)

    new_discovered = 0
    new_with_nav = 0
    new_no_nav = 0
    failed = 0
    page = start_page
    start_time = time.time()

    try:
        while new_discovered < args.daily_limit:
            # 获取一页基金列表
            try:
                result = await fund_scraper.advanced_search(page=page, pagesize=300)
            except RateLimitError:
                logger.warning("获取基金列表限流, 等60秒...")
                await asyncio.sleep(60)
                continue
            except Exception as e:
                logger.error("获取基金列表失败: %s", e)
                break

            batch = result.get("list", [])
            total_in_api = result.get("total", 0)

            if not batch:
                logger.info("已到达最后一页(第%d页)", page)
                break

            # 过滤已入库的
            normalized = [fund_scraper._normalize_advanced(f) for f in batch]
            new_funds = [f for f in normalized if f["encode_id"] not in existing_ids]

            logger.info("第%d页: %d只, 新基金%d只 (总%d/%d)",
                       page, len(batch), len(new_funds), new_discovered, args.daily_limit)

            for fund_data in new_funds:
                if new_discovered >= args.daily_limit:
                    break

                # 1. 插入基金元数据
                fund_id = await insert_new_fund(fund_data)
                if fund_id is None:
                    existing_ids.add(fund_data["encode_id"])
                    continue

                existing_ids.add(fund_data["encode_id"])
                new_discovered += 1

                # 2. 拉取NAV
                try:
                    count = await ingest_fund_nav(nav_scraper, fund_id, fund_data["encode_id"])
                    if count > 0:
                        new_with_nav += 1
                    else:
                        new_no_nav += 1
                        # 标记为no_data
                        async with async_session() as db:
                            await db.execute(text(
                                "UPDATE funds SET nav_status = 'no_data' WHERE id = :fid"
                            ), {"fid": fund_id})
                            await db.commit()
                except RateLimitError:
                    failed += 1
                    logger.warning("NAV限流 %d (%s), 等30秒", fund_id, fund_data["fund_name"][:15])
                    await asyncio.sleep(30)
                except Exception as e:
                    failed += 1
                    logger.warning("NAV错误 %d: %s", fund_id, e)

                if args.delay > 0:
                    await asyncio.sleep(args.delay)

                if new_discovered % 50 == 0:
                    elapsed = time.time() - start_time
                    logger.info(
                        "发现: %d | 有NAV=%d 无NAV=%d 失败=%d | %.0f秒",
                        new_discovered, new_with_nav, new_no_nav, failed, elapsed
                    )

            page += 1

            # 保存进度
            progress["last_page"] = page
            progress["total_discovered"] += new_discovered
            progress["total_with_nav"] += new_with_nav
            save_progress(progress)

    finally:
        await client.close()

    # 最终保存
    progress["last_page"] = page
    save_progress(progress)

    elapsed = time.time() - start_time
    logger.info("=" * 50)
    logger.info("扩展完成! 耗时 %.1f秒", elapsed)
    logger.info("新发现: %d 只", new_discovered)
    logger.info("有NAV: %d 只", new_with_nav)
    logger.info("无NAV: %d 只", new_no_nav)
    logger.info("失败: %d 只", failed)
    logger.info("已处理到第 %d 页", page)


if __name__ == "__main__":
    asyncio.run(main())
