"""基金池扩展脚本 — 每天从火富牛39万基金池中发现新基金并入库。

流程:
1. 从advancedList分页获取基金列表(每页300只)
2. 过滤: 跳过已入库的基金(按fof99_fund_id去重)
3. 新基金写入funds表
4. 拉取NAV历史并入库
5. 过滤: 跳过NAV为空的基金, 跳过2026年无数据的基金
6. 入库后立即计算数据质量评分
7. 记录已处理到第几页(断点续传)

用法:
    cd backend
    python scripts/expand_fund_pool.py                    # 默认每天处理1000只新基金
    python scripts/expand_fund_pool.py --daily-limit 500  # 每天500只
    python scripts/expand_fund_pool.py --start-page 100   # 从第100页开始
    python scripts/expand_fund_pool.py --status            # 查看进度
    python scripts/expand_fund_pool.py --no-quality-filter # 不过滤2026年无数据的
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


def _parse_date(val) -> date | None:
    """安全解析日期字符串或date对象。"""
    if val is None:
        return None
    if isinstance(val, date):
        return val
    try:
        return date.fromisoformat(str(val)[:10])
    except (ValueError, TypeError):
        return None


async def insert_new_fund(fund_data: dict) -> int | None:
    """将新基金写入funds表，返回fund_id。如已存在返回None。"""
    async with async_session() as db:
        stmt = pg_insert(Fund.__table__).values(
            fund_name=fund_data.get("fund_name", ""),
            filing_number=fund_data.get("filing_number") or None,
            manager_name=fund_data.get("advisor", ""),
            inception_date=_parse_date(fund_data.get("inception_date")),
            strategy_type=fund_data.get("strategy_type", ""),
            strategy_sub=fund_data.get("strategy_sub", ""),
            latest_nav=fund_data.get("latest_nav"),
            latest_nav_date=_parse_date(fund_data.get("latest_nav_date")),
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
) -> tuple[int, str | None, date | None]:
    """拉取单只基金的NAV并入库。返回(记录数, 频率, 最新日期)。"""
    nav_records, freq = await scraper.fetch_nav_with_frequency(fof99_id)
    if not nav_records:
        return 0, None, None

    rows = []
    latest_nav_date = None
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
        if latest_nav_date is None or nav_date > latest_nav_date:
            latest_nav_date = nav_date

    if not rows:
        return 0, None, None

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
        return len(rows), freq, latest_nav_date


def compute_quick_quality(
    record_count: int,
    nav_frequency: str | None,
    first_date: date | None,
    last_date: date | None,
    nav_records: list[dict] | None = None,
) -> tuple[int, list[str]]:
    """快速计算数据质量评分(简化版, 不查DB)。

    返回 (score, tags)
    """
    tags = []
    today = date.today()

    total_days = (last_date - first_date).days if first_date and last_date else 0

    # 1. 数据量 (25分)
    if nav_frequency == "daily":
        expected = total_days * 5 / 7 * 0.8
    else:
        expected = total_days / 7 * 0.9
    if expected > 0:
        coverage = record_count / max(expected, 1)
        score_quantity = min(25, int(coverage * 25))
    else:
        score_quantity = 0

    if record_count < 50:
        score_quantity = min(score_quantity, 10)
        tags.append("sparse")

    # 2. 连续性 (25分)
    if total_days > 365:
        score_continuity = 25
    elif total_days > 180:
        score_continuity = 20
    elif total_days > 90:
        score_continuity = 15
    elif total_days > 30:
        score_continuity = 10
    else:
        score_continuity = 5
        tags.append("short_history")

    if total_days < 365 and "short_history" not in tags:
        tags.append("short_history")

    if last_date and (today - last_date).days > 30:
        score_continuity -= 5
        tags.append("stale")

    # 3. 稳定性 (25分) — 简化: 检查NAV跳变
    score_stability = 25
    if nav_records and len(nav_records) > 1:
        jumps_20 = 0
        prev_nav = None
        for r in nav_records:
            nav = r.get("unit_nav")
            if nav and nav > 0 and prev_nav and prev_nav > 0:
                change = abs(nav - prev_nav) / prev_nav
                if change > 0.2:
                    jumps_20 += 1
            prev_nav = nav
        if jumps_20 > 10:
            score_stability = 5
            tags.append("jumpy")
        elif jumps_20 > 3:
            score_stability = 15
            tags.append("jumpy")
        elif jumps_20 > 0:
            score_stability = 22

    # 4. 完整性 (25分) — 简化: 检查NULL比例
    score_completeness = 25
    if nav_records:
        null_count = sum(1 for r in nav_records
                        if r.get("unit_nav") is None or r.get("cumulative_nav") is None)
        null_pct = null_count / len(nav_records) if nav_records else 0
        score_completeness = max(0, int((1 - null_pct) * 25))

    total_score = max(0, min(100, score_quantity + score_continuity + score_stability + score_completeness))
    if total_score >= 80:
        tags.append("high_quality")

    tags = list(dict.fromkeys(tags))
    return total_score, tags


async def update_fund_quality(fund_id: int, score: int, tags: list[str]):
    """将质量评分写入数据库。"""
    async with async_session() as db:
        await db.execute(text("""
            UPDATE funds
            SET data_quality_score = :score,
                data_quality_tags = :tags
            WHERE id = :fid
        """), {
            "score": score,
            "tags": ",".join(tags) if tags else None,
            "fid": fund_id,
        })
        await db.commit()


async def delete_fund_and_nav(fund_id: int):
    """删除不符合条件的基金及其NAV数据。"""
    async with async_session() as db:
        await db.execute(text("DELETE FROM nav_history WHERE fund_id = :fid"), {"fid": fund_id})
        await db.execute(text("DELETE FROM funds WHERE id = :fid"), {"fid": fund_id})
        await db.commit()


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

        # 质量评分分布
        r = await db.execute(text("""
            SELECT
                CASE
                    WHEN data_quality_score >= 80 THEN '高质量(>=80)'
                    WHEN data_quality_score >= 60 THEN '中等(60-79)'
                    WHEN data_quality_score IS NOT NULL THEN '低质量(<60)'
                    ELSE '未评分'
                END as quality,
                count(*)
            FROM funds
            WHERE nav_status = 'has_data'
            GROUP BY quality ORDER BY quality
        """))
        print("\n数据质量分布 (has_data):")
        for row in r.all():
            print(f"  {row[0]:15s}: {row[1]:6d}")

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
    parser.add_argument("--no-quality-filter", action="store_true",
                       help="不过滤2026年无数据的基金(默认过滤)")
    args = parser.parse_args()

    if args.status:
        await show_status()
        return

    progress = load_progress()
    start_page = args.start_page if args.start_page > 0 else progress["last_page"] + 1
    quality_filter = not args.no_quality_filter

    logger.info("基金池扩展启动: 从第%d页开始, 每天上限%d只, 质量过滤=%s",
                start_page, args.daily_limit, quality_filter)

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
    skipped_no_2026 = 0
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

            if not batch:
                logger.info("已到达最后一页(第%d页)", page)
                break

            # 过滤已入库的
            normalized = [fund_scraper._normalize_advanced(f) for f in batch]
            new_funds = [f for f in normalized if f["encode_id"] not in existing_ids]

            logger.info("第%d页: %d只, 新基金%d只 (已处理%d/%d)",
                       page, len(batch), len(new_funds), new_discovered, args.daily_limit)

            for fund_data in new_funds:
                if new_discovered >= args.daily_limit:
                    break

                # 预过滤: 如果火富牛返回的latest_nav为空,跳过
                if not fund_data.get("latest_nav"):
                    existing_ids.add(fund_data["encode_id"])
                    new_no_nav += 1
                    continue

                # 预过滤: 如果最新净值日期在2025年之前,跳过(2026年无数据)
                if quality_filter:
                    latest_date_str = fund_data.get("latest_nav_date")
                    if latest_date_str:
                        try:
                            latest_d = date.fromisoformat(str(latest_date_str)[:10])
                            if latest_d.year < 2026:
                                existing_ids.add(fund_data["encode_id"])
                                skipped_no_2026 += 1
                                continue
                        except (ValueError, TypeError):
                            pass

                # 1. 插入基金元数据
                fund_id = await insert_new_fund(fund_data)
                if fund_id is None:
                    existing_ids.add(fund_data["encode_id"])
                    continue

                existing_ids.add(fund_data["encode_id"])
                new_discovered += 1

                # 2. 拉取NAV
                try:
                    nav_records_raw = None
                    count, freq, latest_nav_date = await ingest_fund_nav(
                        nav_scraper, fund_id, fund_data["encode_id"]
                    )

                    if count == 0:
                        # NAV为空 — 删除刚插入的基金记录
                        await delete_fund_and_nav(fund_id)
                        new_no_nav += 1
                        logger.debug("跳过 %s: NAV为空", fund_data["fund_name"][:20])
                        continue

                    # 3. 检查2026年是否有数据
                    if quality_filter and latest_nav_date and latest_nav_date.year < 2026:
                        await delete_fund_and_nav(fund_id)
                        skipped_no_2026 += 1
                        logger.debug("跳过 %s: 2026年无数据(最新%s)",
                                    fund_data["fund_name"][:20], latest_nav_date)
                        continue

                    new_with_nav += 1

                    # 4. 计算并保存数据质量评分
                    first_date = None
                    try:
                        first_date_str = fund_data.get("inception_date")
                        if first_date_str:
                            first_date = date.fromisoformat(str(first_date_str)[:10])
                    except (ValueError, TypeError):
                        pass

                    score, tags = compute_quick_quality(
                        record_count=count,
                        nav_frequency=freq,
                        first_date=first_date,
                        last_date=latest_nav_date,
                    )
                    await update_fund_quality(fund_id, score, tags)

                except RateLimitError:
                    failed += 1
                    logger.warning("NAV限流 %d (%s), 等30秒", fund_id, fund_data["fund_name"][:15])
                    # 标记为rate_limited而不是删除
                    async with async_session() as db:
                        await db.execute(text(
                            "UPDATE funds SET nav_status = 'rate_limited' WHERE id = :fid"
                        ), {"fid": fund_id})
                        await db.commit()
                    await asyncio.sleep(30)
                except Exception as e:
                    failed += 1
                    logger.warning("NAV错误 %d: %s", fund_id, e)

                if args.delay > 0:
                    await asyncio.sleep(args.delay)

                if new_discovered % 50 == 0:
                    elapsed = time.time() - start_time
                    logger.info(
                        "发现: %d | 有NAV=%d 无NAV=%d 无2026=%d 失败=%d | %.0f秒",
                        new_discovered, new_with_nav, new_no_nav, skipped_no_2026, failed, elapsed
                    )

            page += 1

            # 保存进度
            progress["last_page"] = page
            progress["total_discovered"] = progress.get("total_discovered", 0) + len(
                [f for f in new_funds if f["encode_id"] in existing_ids]
            )
            progress["total_with_nav"] = progress.get("total_with_nav", 0) + new_with_nav
            save_progress(progress)

    finally:
        await client.close()

    # 最终保存
    progress["last_page"] = page
    save_progress(progress)

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("扩展完成! 耗时 %.1f秒", elapsed)
    logger.info("新发现: %d 只 (尝试入库)", new_discovered)
    logger.info("有NAV且2026有数据: %d 只 (已保留)", new_with_nav)
    logger.info("NAV为空: %d 只 (已跳过)", new_no_nav)
    logger.info("2026年无数据: %d 只 (已跳过)", skipped_no_2026)
    logger.info("失败: %d 只", failed)
    logger.info("已处理到第 %d 页", page)


if __name__ == "__main__":
    asyncio.run(main())
