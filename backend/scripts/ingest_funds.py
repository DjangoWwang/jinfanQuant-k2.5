"""爬取火富牛数据并入库。

用法:
    cd backend
    # 爬取基金列表（小批量测试，仅1页=300只）
    python scripts/ingest_funds.py --fund-list --max-pages 1

    # 爬取净值（前10只基金）
    python scripts/ingest_funds.py --nav --limit 10

    # 爬取指数列表
    python scripts/ingest_funds.py --index-list

    # 爬取指数净值（前5个指数）
    python scripts/ingest_funds.py --index-nav --limit 5

    # 全量爬取（基金列表+净值+指数）
    python scripts/ingest_funds.py --all
"""

import argparse
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from app.crawler.fof99.client import Fof99Client
from app.database import async_session
from app.services.ingestion_service import IngestionService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ingest")


async def main():
    parser = argparse.ArgumentParser(description="火富牛数据爬取入库")
    parser.add_argument("--fund-list", action="store_true", help="爬取基金列表")
    parser.add_argument("--nav", action="store_true", help="爬取基金净值")
    parser.add_argument("--index-list", action="store_true", help="爬取指数列表")
    parser.add_argument("--index-nav", action="store_true", help="爬取指数净值")
    parser.add_argument("--all", action="store_true", help="全量爬取")
    parser.add_argument("--fund-id", type=int, help="指定基金ID")
    parser.add_argument("--limit", type=int, help="限制处理数量")
    parser.add_argument("--max-pages", type=int, default=100, help="基金列表最大翻页数")
    parser.add_argument("--delay", type=float, default=0.5, help="请求间延迟(秒)")
    parser.add_argument("--full", action="store_true", help="全量爬取净值(非增量)")
    args = parser.parse_args()

    if not any([args.fund_list, args.nav, args.index_list, args.index_nav, args.all]):
        parser.print_help()
        return

    # 登录火富牛
    client = Fof99Client()
    logger.info("正在登录火富牛...")
    await client.login()
    logger.info("登录成功")

    try:
        svc = IngestionService(client)

        async with async_session() as db:
            if args.all or args.fund_list:
                logger.info("=== 爬取基金列表 ===")
                result = await svc.ingest_fund_list(db, max_pages=args.max_pages)
                logger.info("基金列表结果: %s", result)
                await db.commit()

            if args.all or args.nav:
                logger.info("=== 爬取基金净值 ===")
                result = await svc.ingest_fund_nav(
                    db,
                    fund_id=args.fund_id,
                    limit=args.limit,
                    incremental=not args.full,
                    delay=args.delay,
                )
                logger.info("基金净值结果: %s", result)
                await db.commit()

            if args.all or args.index_list:
                logger.info("=== 爬取指数列表 ===")
                result = await svc.ingest_index_list(db)
                logger.info("指数列表结果: %s", result)
                await db.commit()

            if args.all or args.index_nav:
                logger.info("=== 爬取指数净值 ===")
                result = await svc.ingest_index_nav(
                    db,
                    limit=args.limit,
                    delay=args.delay,
                )
                logger.info("指数净值结果: %s", result)
                await db.commit()

        logger.info("全部完成!")

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
