"""并发抓取调度器 — 多worker并行抓取基金数据。

核心思路:
- 多个worker共享一个任务队列
- 每个worker有独立的Fof99Client（可同账号/不同账号）
- 全局限流感知: 任一worker触发限流 → 全局暂停
- 支持两种模式: 增量更新 + 扩展新基金

架构:
  TaskQueue ← [fund_id_1, fund_id_2, ...]
      ↓
  Worker-1 (client-1) ─┐
  Worker-2 (client-2) ─┤→ 限流信号 → 全局暂停
  Worker-3 (client-3) ─┘
      ↓
  ResultCollector → 统计+日志

用法:
    cd backend
    # 增量更新 (2个worker)
    python scripts/crawl_concurrent.py update --workers 2

    # 扩展新基金 (2个worker)
    python scripts/crawl_concurrent.py expand --workers 2 --daily-limit 3000

    # 查看状态
    python scripts/crawl_concurrent.py status
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import date, timedelta
from functools import partial
from pathlib import Path

# 强制无缓冲输出
print = partial(print, flush=True)

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
logger = logging.getLogger("crawl_concurrent")
logger.setLevel(logging.INFO)

# 账号配置
# 火富牛机构账号最多20个设备，但为避免占满，默认用主账号+可控数量
# 备用账号 (132开头) 不用于爬虫，避免占用其设备位
import hashlib

_ACCOUNTS = [
    {
        "username": os.getenv("FOF99_USERNAME", ""),
        "password": os.getenv("FOF99_PASSWORD", ""),
        "device_id": os.getenv("FOF99_DEVICE_ID", ""),
    },
]
# 火富牛实际限20台设备，但保守用10个避免占满
MAX_DEVICES_PER_ACCOUNT = 10


def generate_device_id(account_index: int, slot: int) -> str:
    """为每个账号的每个设备槽位生成确定性的DEVICE_ID。"""
    primary_did = _ACCOUNTS[account_index].get("device_id", "")
    if slot == 0 and primary_did:
        return primary_did
    seed = f"jinfan_acct{account_index}_slot{slot}_2026"
    return hashlib.md5(seed.encode()).hexdigest()


def get_worker_account(worker_index: int) -> dict:
    """获取worker的账号配置。按轮询分配到多个账号，每账号最多3个设备。"""
    acct_idx = worker_index // MAX_DEVICES_PER_ACCOUNT
    slot = worker_index % MAX_DEVICES_PER_ACCOUNT
    # 超出账号数则循环使用
    acct_idx = acct_idx % len(_ACCOUNTS)
    acct = _ACCOUNTS[acct_idx]
    return {
        "username": acct["username"],
        "password": acct["password"],
        "device_id": generate_device_id(acct_idx, slot),
    }


def max_workers_available() -> int:
    """可用的最大worker数 = 账号数 × 每账号设备数。"""
    return len(_ACCOUNTS) * MAX_DEVICES_PER_ACCOUNT

# 全局限流事件
rate_limit_event = asyncio.Event()
rate_limit_event.set()  # 初始为非限流状态

# 统计
stats = {
    "success": 0, "no_new": 0, "failed": 0, "rate_limited": 0,
    "total_records": 0, "skipped_no_nav": 0, "skipped_no_2026": 0,
}
stats_lock = asyncio.Lock()

PROGRESS_FILE = Path(__file__).parent / ".expand_progress.json"


def _parse_date(val) -> date | None:
    if val is None:
        return None
    if isinstance(val, date):
        return val
    try:
        return date.fromisoformat(str(val)[:10])
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Worker — 增量更新模式
# ---------------------------------------------------------------------------

async def update_worker(
    worker_id: int,
    queue: asyncio.Queue,
    scraper: NavScraper,
    base_delay: float,
):
    """从队列取基金并增量更新NAV。"""
    current_delay = base_delay
    await asyncio.sleep(worker_id * 0.3)

    while True:
        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        fund_id, fof99_id, fund_name, freq, latest_date = item

        # 等待限流解除
        await rate_limit_event.wait()

        try:
            nav_records, detected_freq = await scraper.fetch_nav_with_frequency(fof99_id)

            if not nav_records:
                async with stats_lock:
                    stats["no_new"] += 1
                queue.task_done()
                continue

            # 过滤新数据
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
                async with stats_lock:
                    stats["no_new"] += 1
            else:
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
                    fund = await db.get(Fund, fund_id)
                    if fund:
                        last = nav_records[-1]
                        fund.latest_nav = last.get("unit_nav")
                        try:
                            fund.latest_nav_date = date.fromisoformat(last["nav_date"][:10])
                        except (ValueError, TypeError):
                            pass
                        if detected_freq in ("daily", "weekly"):
                            fund.nav_frequency = detected_freq
                        if fund.nav_status in ("rate_limited", "fetch_failed"):
                            fund.nav_status = "has_data"
                    await db.commit()

                async with stats_lock:
                    stats["success"] += 1
                    stats["total_records"] += len(new_records)

            # 成功后逐步降低延迟
            current_delay = max(base_delay, current_delay * 0.95)

        except RateLimitError:
            async with stats_lock:
                stats["rate_limited"] += 1

            # 通知全局暂停
            rate_limit_event.clear()
            wait = 20
            logger.warning("[W%d] 限流, 全局暂停%ds, fund=%d (%s)",
                          worker_id, wait, fund_id, fund_name[:15])
            await asyncio.sleep(wait)
            rate_limit_event.set()

            # 限流后增加延迟
            current_delay = min(current_delay * 1.5, 8.0)

            # 重新入队
            await queue.put(item)

        except Exception as e:
            logger.warning("[W%d] 错误 fund=%d: %s", worker_id, fund_id, e)
            async with stats_lock:
                stats["failed"] += 1

        queue.task_done()
        await asyncio.sleep(current_delay)


# ---------------------------------------------------------------------------
# Worker — 扩展新基金模式
# ---------------------------------------------------------------------------

async def expand_worker(
    worker_id: int,
    queue: asyncio.Queue,
    scraper: NavScraper,
    base_delay: float,
    quality_filter: bool,
):
    """从队列取新基金, 拉NAV, 过滤, 入库。"""
    current_delay = base_delay
    # 错开启动时间，避免所有worker同时请求
    await asyncio.sleep(worker_id * 0.3)

    while True:
        try:
            fund_data = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        await rate_limit_event.wait()

        fund_name = fund_data.get("fund_name", "?")[:20]

        # 预过滤: latest_nav为空
        if not fund_data.get("latest_nav"):
            async with stats_lock:
                stats["skipped_no_nav"] += 1
            queue.task_done()
            continue

        # 预过滤: 2026年无数据
        if quality_filter:
            latest_str = fund_data.get("latest_nav_date")
            ld = _parse_date(latest_str)
            if ld and ld.year < 2026:
                async with stats_lock:
                    stats["skipped_no_2026"] += 1
                queue.task_done()
                continue

        # 插入基金元数据
        try:
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
                    queue.task_done()
                    continue

                r = await db.execute(text(
                    "SELECT id FROM funds WHERE fof99_fund_id = :fid"
                ), {"fid": fund_data["encode_id"]})
                row = r.first()
                fund_id = row[0] if row else None

            if fund_id is None:
                queue.task_done()
                continue
        except Exception as e:
            logger.warning("[W%d] 插入失败 %s: %s", worker_id, fund_name, e)
            queue.task_done()
            continue

        # 拉取NAV
        try:
            nav_records, freq = await scraper.fetch_nav_with_frequency(fund_data["encode_id"])

            if not nav_records:
                # NAV为空, 删除
                async with async_session() as db:
                    await db.execute(text("DELETE FROM funds WHERE id = :fid"), {"fid": fund_id})
                    await db.commit()
                async with stats_lock:
                    stats["skipped_no_nav"] += 1
                queue.task_done()
                continue

            # 解析并入库NAV
            rows = []
            latest_nav_date = None
            for r in nav_records:
                nd = _parse_date(r.get("nav_date"))
                if not nd:
                    continue
                rows.append({
                    "fund_id": fund_id,
                    "nav_date": nd,
                    "unit_nav": r.get("unit_nav"),
                    "cumulative_nav": r.get("cumulative_nav"),
                    "daily_return": r.get("change_pct"),
                    "data_source": "fof99",
                })
                if latest_nav_date is None or nd > latest_nav_date:
                    latest_nav_date = nd

            if not rows:
                async with async_session() as db:
                    await db.execute(text("DELETE FROM funds WHERE id = :fid"), {"fid": fund_id})
                    await db.commit()
                async with stats_lock:
                    stats["skipped_no_nav"] += 1
                queue.task_done()
                continue

            # 检查2026年数据
            if quality_filter and latest_nav_date and latest_nav_date.year < 2026:
                async with async_session() as db:
                    await db.execute(text("DELETE FROM funds WHERE id = :fid"), {"fid": fund_id})
                    await db.commit()
                async with stats_lock:
                    stats["skipped_no_2026"] += 1
                queue.task_done()
                continue

            # 入库NAV
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
                    fund.nav_status = "has_data"
                    fund.latest_nav = nav_records[-1].get("unit_nav")
                    fund.latest_nav_date = latest_nav_date
                    if freq in ("daily", "weekly"):
                        fund.nav_frequency = freq

                    # 快速质量评分
                    first_d = _parse_date(fund_data.get("inception_date"))
                    total_days = (latest_nav_date - first_d).days if first_d and latest_nav_date else 0
                    s1 = min(25, int(len(rows) / max(total_days * 5 / 7 * 0.8, 1) * 25)) if total_days > 0 else 10
                    s2 = 25 if total_days > 365 else (20 if total_days > 180 else (15 if total_days > 90 else 5))
                    s3 = 22  # 默认稳定
                    null_pct = sum(1 for r in rows if r["unit_nav"] is None) / len(rows) if rows else 0
                    s4 = max(0, int((1 - null_pct) * 25))
                    score = max(0, min(100, s1 + s2 + s3 + s4))
                    tags = []
                    if len(rows) < 50: tags.append("sparse")
                    if total_days < 365: tags.append("short_history")
                    if score >= 80: tags.append("high_quality")
                    fund.data_quality_score = score
                    fund.data_quality_tags = ",".join(tags) or None

                await db.commit()

            async with stats_lock:
                stats["success"] += 1
                stats["total_records"] += len(rows)

            current_delay = max(base_delay, current_delay * 0.95)

        except RateLimitError:
            async with stats_lock:
                stats["rate_limited"] += 1
            rate_limit_event.clear()
            wait = 20
            logger.warning("[W%d] 限流, 全局暂停%ds, %s", worker_id, wait, fund_name)
            await asyncio.sleep(wait)
            rate_limit_event.set()
            current_delay = min(current_delay * 1.5, 8.0)
            # 标记为rate_limited
            async with async_session() as db:
                await db.execute(text(
                    "UPDATE funds SET nav_status = 'rate_limited' WHERE id = :fid"
                ), {"fid": fund_id})
                await db.commit()

        except Exception as e:
            logger.warning("[W%d] NAV错误 %s: %s", worker_id, fund_name, e)
            async with stats_lock:
                stats["failed"] += 1

        queue.task_done()
        await asyncio.sleep(current_delay)


# ---------------------------------------------------------------------------
# 主命令: update
# ---------------------------------------------------------------------------

async def cmd_update(args):
    """增量更新所有基金NAV。"""
    today = date.today()
    logger.info("增量更新启动: %s, %d个worker", today, args.workers)

    async with async_session() as db:
        result = await db.execute(text("""
            SELECT f.id, f.fof99_fund_id, f.fund_name, f.nav_frequency, f.latest_nav_date
            FROM funds f
            WHERE f.fof99_fund_id IS NOT NULL
            AND f.status = 'active'
            AND f.nav_status IN ('has_data', 'rate_limited', 'fetch_failed')
            ORDER BY
                CASE f.nav_status WHEN 'rate_limited' THEN 0 WHEN 'fetch_failed' THEN 1 ELSE 2 END,
                f.latest_nav_date ASC NULLS FIRST
        """))
        funds = result.all()

    if args.limit > 0:
        funds = funds[:args.limit]

    logger.info("需要更新: %d 只基金", len(funds))

    if not funds:
        return

    # 创建任务队列
    queue = asyncio.Queue()
    for f in funds:
        await queue.put(f)

    # 创建clients和workers (受限于账号×设备数)
    n_workers = min(args.workers, max_workers_available())
    clients = []
    workers = []
    start_time = time.time()

    for i in range(n_workers):
        acct = get_worker_account(i)
        client = Fof99Client(
            username=acct["username"],
            password=acct["password"],
            device_id=acct["device_id"],
        )
        await client.login()
        clients.append(client)
        scraper = NavScraper(client)
        workers.append(update_worker(i, queue, scraper, args.delay))
        logger.info("Worker-%d 就绪 (device: %s...)", i, acct["device_id"][:8])

    # 启动进度监控
    async def monitor():
        while not queue.empty() or any(not w.done() for w in tasks):
            total = len(funds)
            done = total - queue.qsize()
            elapsed = time.time() - start_time
            rate = done / elapsed if elapsed > 0 else 0
            eta = (total - done) / rate if rate > 0 else 0
            logger.info(
                "进度: %d/%d (%.0f%%) | 成功=%d 无新=%d 失败=%d 限流=%d | 新记录=%d | %.0f秒 ETA %.0f秒",
                done, total, done / total * 100 if total > 0 else 0,
                stats["success"], stats["no_new"], stats["failed"], stats["rate_limited"],
                stats["total_records"], elapsed, eta
            )
            await asyncio.sleep(30)

    tasks = [asyncio.create_task(w) for w in workers]
    monitor_task = asyncio.create_task(monitor())

    try:
        await asyncio.gather(*tasks)
    finally:
        monitor_task.cancel()
        for c in clients:
            await c.close()

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("更新完成! 耗时 %.1f秒 (%.1f分钟)", elapsed, elapsed / 60)
    logger.info("有新数据: %d 只 (%d 条记录)", stats["success"], stats["total_records"])
    logger.info("无新数据: %d 只", stats["no_new"])
    logger.info("失败: %d 只 (限流: %d)", stats["failed"], stats["rate_limited"])


# ---------------------------------------------------------------------------
# 主命令: expand
# ---------------------------------------------------------------------------

async def cmd_expand(args):
    """扩展基金池。"""
    logger.info("基金池扩展: %d个worker, 上限%d只", args.workers, args.daily_limit)

    existing_ids = set()
    async with async_session() as db:
        result = await db.execute(text(
            "SELECT fof99_fund_id FROM funds WHERE fof99_fund_id IS NOT NULL"
        ))
        existing_ids = {r[0] for r in result.all()}
    logger.info("已入库: %d 只", len(existing_ids))

    # 加载进度
    progress = {"last_page": 0, "total_discovered": 0, "total_with_nav": 0, "last_run": None}
    if PROGRESS_FILE.exists():
        progress = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    start_page = args.start_page if args.start_page > 0 else progress["last_page"] + 1

    # 使用一个client获取基金列表
    acct0 = get_worker_account(0)
    list_client = Fof99Client(
        username=acct0["username"],
        password=acct0["password"],
        device_id=acct0["device_id"],
    )
    await list_client.login()
    fund_scraper = FundScraper(list_client)

    # 收集新基金到队列
    queue = asyncio.Queue()
    page = start_page
    collected = 0
    quality_filter = not args.no_quality_filter

    logger.info("从第%d页开始收集基金列表...", start_page)

    empty_streak = 0  # 连续无新基金的页数
    MAX_EMPTY_STREAK = 20  # 连续20页没新基金才停止

    while collected < args.daily_limit:
        try:
            # price_date 降序: 最近有净值更新的基金排前面，效率最高
            result = await fund_scraper.advanced_search(
                page=page, pagesize=300,
                order_by="price_date", order=1,  # 1=降序，活跃基金优先
            )
        except RateLimitError:
            logger.warning("列表限流, 等60s")
            await asyncio.sleep(60)
            continue
        except Exception as e:
            logger.error("列表获取失败: %s", e)
            break

        batch = result.get("list", [])
        if not batch:
            logger.info("到达最后一页: 第%d页", page)
            break

        normalized = [fund_scraper._normalize_advanced(f) for f in batch]
        new_funds = [f for f in normalized if f["encode_id"] not in existing_ids]

        if not new_funds:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_STREAK:
                logger.info("连续%d页无新基金，停止收集 (页=%d)", MAX_EMPTY_STREAK, page)
                break
        else:
            empty_streak = 0

        # 列表阶段预过滤: price_date 为空或2026年前的直接跳过
        skipped_list = 0
        for f in new_funds:
            if collected >= args.daily_limit:
                break
            existing_ids.add(f["encode_id"])
            if quality_filter:
                pd = _parse_date(f.get("latest_nav_date"))
                if pd is None or pd.year < 2026:
                    skipped_list += 1
                    async with stats_lock:
                        stats["skipped_no_2026"] += 1
                    continue
            await queue.put(f)
            collected += 1

        logger.info("第%d页: %d只, 新%d只, 跳过%d只(无2026), 队列=%d",
                     page, len(batch), len(new_funds), skipped_list, collected)
        page += 1
        await asyncio.sleep(0.5)

    await list_client.close()

    logger.info("共收集 %d 只新基金, 开始NAV抓取...", collected)

    # 创建workers抓取NAV (受限于账号×设备数)
    n_workers = min(args.workers, max_workers_available())
    clients = []
    workers_list = []
    start_time = time.time()

    for i in range(n_workers):
        acct = get_worker_account(i)
        client = Fof99Client(
            username=acct["username"],
            password=acct["password"],
            device_id=acct["device_id"],
        )
        await client.login()
        clients.append(client)
        scraper = NavScraper(client)
        workers_list.append(expand_worker(i, queue, scraper, args.delay, quality_filter))
        logger.info("Worker-%d 就绪 (device: %s...)", i, acct["device_id"][:8])

    async def monitor():
        while not queue.empty() or any(not w.done() for w in tasks):
            done = collected - queue.qsize()
            elapsed = time.time() - start_time
            rate = done / elapsed if elapsed > 0 else 0
            eta = queue.qsize() / rate if rate > 0 else 0
            logger.info(
                "NAV进度: %d/%d | 入库=%d 无NAV=%d 无2026=%d 失败=%d 限流=%d | 记录=%d | %.0f秒 ETA %.0f秒",
                done, collected,
                stats["success"], stats["skipped_no_nav"], stats["skipped_no_2026"],
                stats["failed"], stats["rate_limited"],
                stats["total_records"], elapsed, eta
            )
            await asyncio.sleep(30)

    tasks = [asyncio.create_task(w) for w in workers_list]
    monitor_task = asyncio.create_task(monitor())

    try:
        await asyncio.gather(*tasks)
    finally:
        monitor_task.cancel()
        for c in clients:
            await c.close()

    # 保存进度
    progress["last_page"] = page
    progress["total_discovered"] = progress.get("total_discovered", 0) + collected
    progress["total_with_nav"] = progress.get("total_with_nav", 0) + stats["success"]
    progress["last_run"] = str(date.today())
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False, indent=2), encoding="utf-8")

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("扩展完成! 耗时 %.1f秒 (%.1f分钟)", elapsed, elapsed / 60)
    logger.info("新入库: %d 只 (%d 条NAV记录)", stats["success"], stats["total_records"])
    logger.info("跳过(无NAV): %d 只", stats["skipped_no_nav"])
    logger.info("跳过(无2026): %d 只", stats["skipped_no_2026"])
    logger.info("失败: %d 只 (限流: %d)", stats["failed"], stats["rate_limited"])
    logger.info("已处理到第 %d 页", page)


# ---------------------------------------------------------------------------
# 主命令: status
# ---------------------------------------------------------------------------

async def cmd_status(args):
    async with async_session() as db:
        r = await db.execute(text("""
            SELECT COALESCE(nav_status, 'null'), count(*) FROM funds
            GROUP BY nav_status ORDER BY count(*) DESC
        """))
        print("\n基金状态分布:")
        total = 0
        for row in r.all():
            print(f"  {row[0]:15s}: {row[1]:6d}")
            total += row[1]
        print(f"  {'总计':15s}: {total:6d}")

        r = await db.execute(text("""
            SELECT
                CASE
                    WHEN data_quality_score >= 80 THEN '高质量(>=80)'
                    WHEN data_quality_score >= 60 THEN '中等(60-79)'
                    WHEN data_quality_score IS NOT NULL THEN '低质量(<60)'
                    ELSE '未评分'
                END, count(*)
            FROM funds WHERE nav_status = 'has_data'
            GROUP BY 1 ORDER BY 1
        """))
        print("\n数据质量 (has_data):")
        for row in r.all():
            print(f"  {row[0]:15s}: {row[1]:6d}")

        r = await db.execute(text("SELECT COUNT(*), MIN(nav_date), MAX(nav_date) FROM nav_history"))
        row = r.first()
        print(f"\nNAV记录: {row[0]:,d}条, {row[1]} ~ {row[2]}")

    progress = {"last_page": 0}
    if PROGRESS_FILE.exists():
        progress = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    print(f"\n扩展进度: 第{progress.get('last_page', 0)}页, "
          f"发现{progress.get('total_discovered', 0)}只, "
          f"有NAV{progress.get('total_with_nav', 0)}只")

    # 展示上次survey结果
    survey_file = Path(__file__).parent / ".survey_result.json"
    if survey_file.exists():
        survey = json.loads(survey_file.read_text(encoding="utf-8"))
        c = survey.get("counts", {})
        print(f"\n上次普查 ({survey.get('scan_date', '?')}):")
        print(f"  2026年活跃: {c.get('year_2026', 0):,}只 ({survey.get('active_pct', 0)}%)")
        print(f"  2025年:     {c.get('year_2025', 0):,}只")
        print(f"  2024年:     {c.get('year_2024', 0):,}只")
        print(f"  更早:       {c.get('before_2024', 0):,}只")
        print(f"  无数据:     {c.get('no_price_date', 0):,}只")

    # 查询火富牛平台总基金数
    try:
        acct = get_worker_account(0)
        client = Fof99Client(
            username=acct["username"],
            password=acct["password"],
            device_id=acct["device_id"],
        )
        await client.login()
        scraper = FundScraper(client)
        result = await scraper.advanced_search(page=1, pagesize=1)
        remote_total = result.get("total", 0)
        await client.close()
        coverage = total / remote_total * 100 if remote_total > 0 else 0
        remaining = remote_total - total
        pages_done = progress.get("last_page", 0)
        total_pages = (remote_total + 299) // 300  # pagesize=300
        print(f"\n火富牛平台总量: {remote_total:,}只")
        print(f"已入库覆盖率:   {total:,}/{remote_total:,} = {coverage:.1f}%")
        print(f"待抓取:         {remaining:,}只")
        print(f"列表页进度:     {pages_done}/{total_pages}页")
    except Exception as e:
        print(f"\n火富牛总量查询失败: {e}")


# ---------------------------------------------------------------------------
# 主命令: cleanup — 清理火富牛设备绑定
# ---------------------------------------------------------------------------

async def cmd_cleanup(args):
    """清理火富牛账号的已登录设备，保留当前使用的 device_id。"""
    keep_ids = set()
    # 保留所有 worker 会用到的 device_id
    for i in range(max_workers_available()):
        acct = get_worker_account(i)
        keep_ids.add(acct["device_id"])

    acct = get_worker_account(0)
    client = Fof99Client(
        username=acct["username"],
        password=acct["password"],
        device_id=acct["device_id"],
    )
    await client.login()

    devices = await client.list_devices()
    print(f"\n当前已登录设备: {len(devices)} 台 (上限20)")

    removed = 0
    kept = 0
    for dev in devices:
        did = dev.get("browser_code", "")
        rec_id = dev.get("id")
        last_login = dev.get("last_login_time", "")
        if did in keep_ids:
            print(f"  [保留] {did[:12]}... {last_login} (worker设备)")
            kept += 1
        elif args.dry_run:
            print(f"  [待删] {did[:12]}... {last_login}")
            removed += 1
        else:
            await client.unbind_device(rec_id)
            print(f"  [已删] {did[:12]}... {last_login}")
            removed += 1

    await client.close()
    action = "将删除" if args.dry_run else "已删除"
    print(f"\n{action}: {removed} 台, 保留: {kept} 台")
    if args.dry_run and removed > 0:
        print("使用 --force 执行实际删除")


# ---------------------------------------------------------------------------
# 主命令: survey — 扫描火富牛全量基金，统计数据分布
# ---------------------------------------------------------------------------

async def cmd_survey(args):
    """扫描火富牛全量基金列表，按 price_date 统计数据分布。

    不下载NAV数据，只翻页读取列表元数据中的 price_date 字段。
    """
    acct = get_worker_account(0)
    client = Fof99Client(
        username=acct["username"],
        password=acct["password"],
        device_id=acct["device_id"],
    )
    await client.login()
    scraper = FundScraper(client)

    # 先查总数
    result = await scraper.advanced_search(page=1, pagesize=1)
    remote_total = result.get("total", 0)
    total_pages = (remote_total + 299) // 300
    print(f"\n火富牛平台总量: {remote_total:,}只, 共{total_pages}页")
    print(f"开始扫描... (预计{total_pages * 0.5 / 60:.0f}分钟)\n")

    # 统计桶
    counts = {
        "no_price_date": 0,       # price_date 为空
        "before_2024": 0,         # 2024年之前
        "year_2024": 0,           # 2024年
        "year_2025": 0,           # 2025年
        "year_2026": 0,           # 2026年
    }
    strategy_counts = {}          # 按策略分类统计
    total_scanned = 0
    start_time = time.time()

    start_page = args.start_page if args.start_page > 0 else 1
    page = start_page

    while page <= total_pages:
        try:
            result = await scraper.advanced_search(
                page=page, pagesize=300,
                order_by="price_date", order=1,  # 降序，有数据的在前面
            )
        except RateLimitError:
            logger.warning("限流, 等30s")
            await asyncio.sleep(30)
            continue
        except Exception as e:
            logger.error("第%d页失败: %s", page, e)
            break

        batch = result.get("list", [])
        if not batch:
            break

        for f in batch:
            total_scanned += 1
            pd_str = f.get("price_date") or ""
            pd = _parse_date(pd_str)
            strat = f.get("strategy_one") or "未分类"

            if pd is None:
                counts["no_price_date"] += 1
            elif pd.year >= 2026:
                counts["year_2026"] += 1
            elif pd.year == 2025:
                counts["year_2025"] += 1
            elif pd.year == 2024:
                counts["year_2024"] += 1
            else:
                counts["before_2024"] += 1

            # 按策略分类统计有2026数据的
            if pd and pd.year >= 2026:
                strategy_counts[strat] = strategy_counts.get(strat, 0) + 1

        # 每50页输出进度
        if page % 50 == 0 or page == total_pages:
            elapsed = time.time() - start_time
            rate = total_scanned / elapsed if elapsed > 0 else 0
            eta = (remote_total - total_scanned) / rate if rate > 0 else 0
            logger.info(
                "扫描进度: %d/%d页 (%d只) | 有2026=%d 无数据=%d | %.0f秒 ETA %.0f秒",
                page, total_pages, total_scanned,
                counts["year_2026"], counts["no_price_date"],
                elapsed, eta
            )

        page += 1
        await asyncio.sleep(args.delay)

    await client.close()

    elapsed = time.time() - start_time

    # 输出报告
    print("=" * 60)
    print(f"火富牛基金数据分布报告")
    print(f"扫描: {total_scanned:,}只 / {remote_total:,}只, 耗时{elapsed/60:.1f}分钟")
    print("=" * 60)

    active_2026 = counts["year_2026"]
    active_pct = active_2026 / total_scanned * 100 if total_scanned > 0 else 0

    print(f"\n按最后净值日期(price_date)分布:")
    print(f"  2026年(活跃):      {counts['year_2026']:>8,}只  ({counts['year_2026']/total_scanned*100:.1f}%)")
    print(f"  2025年:            {counts['year_2025']:>8,}只  ({counts['year_2025']/total_scanned*100:.1f}%)")
    print(f"  2024年:            {counts['year_2024']:>8,}只  ({counts['year_2024']/total_scanned*100:.1f}%)")
    print(f"  2024年之前:        {counts['before_2024']:>8,}只  ({counts['before_2024']/total_scanned*100:.1f}%)")
    print(f"  无净值数据:        {counts['no_price_date']:>8,}只  ({counts['no_price_date']/total_scanned*100:.1f}%)")
    print(f"\n结论: 火富牛{remote_total:,}只基金中，{active_2026:,}只({active_pct:.1f}%)有2026年数据，可抓取入库")

    inactive = total_scanned - active_2026
    inactive_pct = inactive / total_scanned * 100 if total_scanned > 0 else 0
    print(f"       {inactive:,}只({inactive_pct:.1f}%)为空数据或已停止更新，不值得抓取")

    if strategy_counts:
        print(f"\n2026年活跃基金 — 策略分布 (Top 15):")
        sorted_strats = sorted(strategy_counts.items(), key=lambda x: -x[1])[:15]
        for strat, cnt in sorted_strats:
            print(f"  {strat:20s}: {cnt:>6,}只")

    # 保存survey结果
    survey_file = Path(__file__).parent / ".survey_result.json"
    survey_data = {
        "scan_date": str(date.today()),
        "remote_total": remote_total,
        "total_scanned": total_scanned,
        "counts": counts,
        "strategy_counts": strategy_counts,
        "active_pct": round(active_pct, 1),
        "elapsed_seconds": round(elapsed, 1),
    }
    survey_file.write_text(json.dumps(survey_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n结果已保存: {survey_file}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(description="并发抓取调度器")
    sub = parser.add_subparsers(dest="command")

    p_update = sub.add_parser("update", help="增量更新现有基金NAV")
    p_update.add_argument("--workers", type=int, default=2, help="worker数量")
    p_update.add_argument("--limit", type=int, default=0, help="最多更新几只")
    p_update.add_argument("--delay", type=float, default=1.0, help="基础请求延迟(秒)")

    p_expand = sub.add_parser("expand", help="扩展基金池")
    p_expand.add_argument("--workers", type=int, default=2, help="worker数量")
    p_expand.add_argument("--daily-limit", type=int, default=3000, help="每天上限")
    p_expand.add_argument("--start-page", type=int, default=0, help="起始页码")
    p_expand.add_argument("--delay", type=float, default=1.0, help="基础请求延迟(秒)")
    p_expand.add_argument("--no-quality-filter", action="store_true", help="不过滤2026年无数据")

    p_status = sub.add_parser("status", help="查看状态")

    p_cleanup = sub.add_parser("cleanup", help="清理火富牛设备绑定")
    p_cleanup.add_argument("--dry-run", action="store_true", default=True, help="预览模式(默认)")
    p_cleanup.add_argument("--force", dest="dry_run", action="store_false", help="执行实际删除")

    p_survey = sub.add_parser("survey", help="扫描火富牛全量基金，统计数据分布")
    p_survey.add_argument("--start-page", type=int, default=0, help="起始页码")
    p_survey.add_argument("--delay", type=float, default=0.3, help="请求间隔(秒)")

    args = parser.parse_args()

    if args.command == "update":
        await cmd_update(args)
    elif args.command == "expand":
        await cmd_expand(args)
    elif args.command == "status":
        await cmd_status(args)
    elif args.command == "cleanup":
        await cmd_cleanup(args)
    elif args.command == "survey":
        await cmd_survey(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
