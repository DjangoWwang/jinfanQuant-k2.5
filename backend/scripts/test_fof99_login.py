"""快速测试火富牛登录和API调用。

用法: python -m scripts.test_fof99_login
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from app.crawler.fof99.client import Fof99Client


async def main():
    async with Fof99Client() as client:
        print(f"设备ID: {client.device_id}")
        print(f"用户名: {os.getenv('FOF99_USERNAME')}")

        # 如果设备ID未保存到.env，提示保存
        if not os.getenv("FOF99_DEVICE_ID"):
            print(f"\n⚠️  请将以下行添加到 .env 文件以复用设备绑定:")
            print(f"FOF99_DEVICE_ID={client.device_id}")

        try:
            await client.login()
            print("\n✅ 登录成功!")

            # 测试基金搜索
            from app.crawler.fof99.fund_scraper import FundScraper
            scraper = FundScraper(client)
            data = await scraper.search_funds(keyword="明河", pagesize=5)
            print(f"\n搜索'明河': {data}")

            # 测试指数列表
            from app.crawler.fof99.index_scraper import IndexScraper
            idx_scraper = IndexScraper(client)
            indices = await idx_scraper.fetch_index_list()
            print(f"\n指数列表({len(indices)}只): {indices[:3]}")

        except Exception as e:
            print(f"\n❌ 错误: {e}")


if __name__ == "__main__":
    asyncio.run(main())
