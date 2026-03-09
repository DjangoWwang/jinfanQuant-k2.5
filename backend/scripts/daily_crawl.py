"""每日自动化爬虫脚本 — 清理设备 + 检查新基金 + 更新NAV。

用法:
    cd backend
    python scripts/daily_crawl.py                  # 完整流程
    python scripts/daily_crawl.py --skip-expand     # 跳过新基金检查
    python scripts/daily_crawl.py --skip-update     # 跳过NAV更新

建议配合 Windows 任务计划程序或 cron 每日凌晨运行:
    # Windows Task Scheduler (每天 02:00)
    schtasks /create /tn "FOF-DailyCrawl" /tr "python D:\\AI\\Claude code\\FOF平台开发\\fof-platform\\backend\\scripts\\daily_crawl.py" /sc daily /st 02:00

    # Linux cron
    0 2 * * * cd /path/to/backend && python scripts/daily_crawl.py >> logs/daily.log 2>&1
"""

import argparse
import asyncio
import logging
import os
import sys
import time
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["DEBUG"] = "false"

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("daily_crawl")


async def run_step(name: str, args_list: list[str]):
    """运行 crawl_concurrent.py 的一个子命令。"""
    cmd = [sys.executable, "scripts/crawl_concurrent.py"] + args_list
    logger.info("=" * 50)
    logger.info("步骤: %s", name)
    logger.info("命令: %s", " ".join(cmd))

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await proc.communicate()
    output = stdout.decode("utf-8", errors="replace")

    for line in output.strip().split("\n"):
        logger.info("  %s", line)

    if proc.returncode != 0:
        logger.error("步骤 %s 失败 (exit=%d)", name, proc.returncode)
        return False
    logger.info("步骤 %s 完成", name)
    return True


async def main():
    parser = argparse.ArgumentParser(description="每日自动化爬虫")
    parser.add_argument("--skip-expand", action="store_true", help="跳过新基金检查")
    parser.add_argument("--skip-update", action="store_true", help="跳过NAV更新")
    parser.add_argument("--expand-limit", type=int, default=5000, help="新基金检查上限")
    parser.add_argument("--update-workers", type=int, default=5, help="NAV更新worker数")
    args = parser.parse_args()

    start = time.time()
    today = date.today()
    logger.info("每日爬虫启动: %s", today)

    # Step 1: 清理设备
    await run_step("清理设备", ["cleanup", "--force"])

    # Step 2: 扩展新基金（检查是否有新上架的基金）
    if not args.skip_expand:
        await run_step("检查新基金", [
            "expand",
            "--workers", "10",
            "--daily-limit", str(args.expand_limit),
            "--start-page", "1",  # 每次从头开始，price_date DESC确保活跃基金在前
            "--delay", "0.5",
        ])

    # Step 3: 增量更新已有基金NAV
    if not args.skip_update:
        await run_step("更新NAV", [
            "update",
            "--workers", str(args.update_workers),
            "--delay", "0.8",
        ])

    elapsed = time.time() - start
    logger.info("=" * 50)
    logger.info("每日爬虫完成! 总耗时: %.1f分钟", elapsed / 60)


if __name__ == "__main__":
    asyncio.run(main())
