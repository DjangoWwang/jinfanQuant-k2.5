"""定时任务调度器 — 基于APScheduler的独立进程调度。

功能:
1. 交易日18:30 — 日频基金净值增量更新
2. 每周六10:00 — 周频基金净值更新
3. 每周日02:00 — 数据质量评分重算
4. 每月1日03:00 — 全量爬取新基金(expand)

用法:
    cd backend
    python scripts/scheduler.py                # 启动调度器(前台)
    python scripts/scheduler.py --list         # 查看当前任务计划
    python scripts/scheduler.py --run-now daily # 立即执行某任务
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["DEBUG"] = "false"

from dotenv import load_dotenv
load_dotenv()

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scheduler")


# ---------------------------------------------------------------------------
# Job functions
# ---------------------------------------------------------------------------

async def job_daily_nav_update():
    """日频基金净值增量更新。"""
    logger.info("=== 开始: 日频净值更新 ===")
    try:
        from scripts.daily_update import get_funds_to_update, update_single_fund, is_trading_day
        from app.crawler.fof99.client import Fof99Client
        from app.crawler.fof99.nav_scraper import NavScraper

        today = date.today()
        if not await is_trading_day(today):
            logger.info("今天不是交易日, 跳过日频更新")
            return

        funds = await get_funds_to_update(force_all=False)
        if not funds:
            logger.info("无需更新的基金")
            return

        logger.info("需更新: %d 只基金", len(funds))
        client = Fof99Client()
        await client.login()
        scraper = NavScraper(client)

        success, failed, total_records = 0, 0, 0
        try:
            for i, (fund_id, fof99_id, fund_name, freq, latest_date) in enumerate(funds):
                try:
                    count = await update_single_fund(scraper, fund_id, fof99_id, fund_name, latest_date)
                    if count > 0:
                        success += 1
                        total_records += count
                except Exception as e:
                    failed += 1
                    logger.warning("更新失败 %s: %s", fund_name[:20], e)

                if (i + 1) % 100 == 0:
                    logger.info("进度: %d/%d", i + 1, len(funds))
                await asyncio.sleep(0.8)
        finally:
            await client.close()

        logger.info("日频更新完成: 更新=%d 失败=%d 新记录=%d", success, failed, total_records)

    except Exception as e:
        logger.error("日频更新异常: %s", e, exc_info=True)


async def job_weekly_nav_update():
    """周频基金净值更新（每周六执行）。"""
    logger.info("=== 开始: 周频净值更新 ===")
    try:
        from scripts.daily_update import get_funds_to_update, update_single_fund
        from app.crawler.fof99.client import Fof99Client
        from app.crawler.fof99.nav_scraper import NavScraper

        funds = await get_funds_to_update(force_all=False)
        if not funds:
            logger.info("无需更新的基金")
            return

        logger.info("需更新: %d 只基金", len(funds))
        client = Fof99Client()
        await client.login()
        scraper = NavScraper(client)

        success, failed = 0, 0
        try:
            for fund_id, fof99_id, fund_name, freq, latest_date in funds:
                try:
                    count = await update_single_fund(scraper, fund_id, fof99_id, fund_name, latest_date)
                    if count > 0:
                        success += 1
                except Exception as e:
                    failed += 1
                    logger.warning("更新失败 %s: %s", fund_name[:20], e)
                await asyncio.sleep(1.0)
        finally:
            await client.close()

        logger.info("周频更新完成: 更新=%d 失败=%d", success, failed)

    except Exception as e:
        logger.error("周频更新异常: %s", e, exc_info=True)


async def job_quality_score_update():
    """重算数据质量评分。"""
    logger.info("=== 开始: 数据质量评分重算 ===")
    try:
        from scripts.daily_update import update_quality_scores
        await update_quality_scores()
        logger.info("数据质量评分重算完成")
    except Exception as e:
        logger.error("质量评分异常: %s", e, exc_info=True)


async def job_index_update():
    """每日指数数据更新（市场指数+私募指数）。"""
    logger.info("=== 开始: 指数数据更新 ===")
    try:
        import subprocess
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "import_all_indices.py")
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=600,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        if result.returncode == 0:
            logger.info("指数更新完成")
        else:
            logger.warning("指数更新退出码=%d", result.returncode)
    except subprocess.TimeoutExpired:
        logger.warning("指数更新超时(10min)")
    except Exception as e:
        logger.error("指数更新异常: %s", e, exc_info=True)


async def job_expand_crawl():
    """月度全量爬取新基金。"""
    logger.info("=== 开始: 月度全量爬取 ===")
    try:
        import subprocess
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crawl_concurrent.py")
        result = subprocess.run(
            [sys.executable, script_path, "expand", "--limit", "2000", "--workers", "3"],
            capture_output=True,
            text=True,
            timeout=7200,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        if result.returncode == 0:
            logger.info("月度爬取完成")
        else:
            logger.warning("月度爬取退出码=%d, stderr=%s", result.returncode, result.stderr[-500:] if result.stderr else "")
    except subprocess.TimeoutExpired:
        logger.warning("月度爬取超时(2h)")
    except Exception as e:
        logger.error("月度爬取异常: %s", e, exc_info=True)


# ---------------------------------------------------------------------------
# Job registry
# ---------------------------------------------------------------------------

JOBS = {
    "daily": {
        "func": job_daily_nav_update,
        "trigger": CronTrigger(hour=18, minute=30, day_of_week="mon-fri"),
        "description": "日频净值更新 (交易日18:30)",
    },
    "weekly": {
        "func": job_weekly_nav_update,
        "trigger": CronTrigger(hour=10, minute=0, day_of_week="sat"),
        "description": "周频净值更新 (周六10:00)",
    },
    "quality": {
        "func": job_quality_score_update,
        "trigger": CronTrigger(hour=2, minute=0, day_of_week="sun"),
        "description": "数据质量评分 (周日02:00)",
    },
    "index": {
        "func": job_index_update,
        "trigger": CronTrigger(hour=19, minute=0, day_of_week="mon-fri"),
        "description": "指数数据更新 (交易日19:00)",
    },
    "expand": {
        "func": job_expand_crawl,
        "trigger": CronTrigger(day=1, hour=3, minute=0),
        "description": "月度全量爬取 (每月1日03:00)",
    },
}


def list_jobs():
    """打印所有计划任务。"""
    print("\n定时任务列表:")
    print("-" * 60)
    for job_id, job in JOBS.items():
        trigger = job["trigger"]
        print(f"  {job_id:10s}  {job['description']}")
        print(f"{'':12s}  cron: {trigger}")
    print("-" * 60)
    print(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()


async def run_now(job_name: str):
    """立即执行指定任务。"""
    if job_name not in JOBS:
        print(f"未知任务: {job_name}")
        print(f"可选: {', '.join(JOBS.keys())}")
        return
    logger.info("立即执行: %s", job_name)
    await JOBS[job_name]["func"]()


def main():
    parser = argparse.ArgumentParser(description="晋帆投研定时任务调度器")
    parser.add_argument("--list", action="store_true", help="查看任务列表")
    parser.add_argument("--run-now", type=str, help="立即执行某任务 (daily/weekly/quality/expand)")
    args = parser.parse_args()

    if args.list:
        list_jobs()
        return

    if args.run_now:
        asyncio.run(run_now(args.run_now))
        return

    # Start scheduler
    scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
    for job_id, job in JOBS.items():
        scheduler.add_job(
            job["func"],
            trigger=job["trigger"],
            id=job_id,
            name=job["description"],
            misfire_grace_time=3600,
            coalesce=True,
        )

    scheduler.start()
    logger.info("调度器已启动, %d 个任务已注册", len(JOBS))
    list_jobs()

    # Keep running
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def shutdown(sig, frame):
        logger.info("收到信号 %s, 正在关闭...", sig)
        scheduler.shutdown(wait=False)
        loop.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        scheduler.shutdown(wait=False)
        logger.info("调度器已关闭")


if __name__ == "__main__":
    main()
