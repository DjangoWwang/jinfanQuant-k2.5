"""NAV数据校验脚本 — 随机抽样对比API原始数据与数据库数据。

用法:
    cd backend
    python scripts/verify_nav_data.py           # 默认抽10只
    python scripts/verify_nav_data.py --sample 20
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ["DEBUG"] = "false"

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text
from app.crawler.fof99.client import Fof99Client
from app.crawler.fof99.nav_scraper import NavScraper
from app.database import async_session


async def main():
    parser = argparse.ArgumentParser(description="NAV数据校验")
    parser.add_argument("--sample", type=int, default=10, help="抽样基金数量")
    args = parser.parse_args()

    # 1. 从数据库随机抽样有NAV数据的基金
    async with async_session() as db:
        result = await db.execute(text(f"""
            SELECT f.id, f.fof99_fund_id, f.fund_name, f.nav_frequency,
                   count(n.id) as db_count,
                   min(n.nav_date) as db_min_date,
                   max(n.nav_date) as db_max_date
            FROM funds f
            JOIN nav_history n ON n.fund_id = f.id
            WHERE f.fof99_fund_id IS NOT NULL
            GROUP BY f.id, f.fof99_fund_id, f.fund_name, f.nav_frequency
            ORDER BY random()
            LIMIT {args.sample}
        """))
        samples = result.all()

    if not samples:
        print("ERROR: 数据库中无NAV数据")
        return

    print(f"\n{'='*70}")
    print(f"NAV数据校验 — 随机抽样 {len(samples)} 只基金")
    print(f"{'='*70}\n")

    # 2. 登录火富牛
    client = Fof99Client()
    await client.login()
    scraper = NavScraper(client)

    total_checked = 0
    total_match = 0
    total_mismatch = 0
    issues = []

    try:
        for fund_id, fof99_id, fund_name, freq, db_count, db_min, db_max in samples:
            print(f"基金 {fund_id}: {fund_name} ({freq})")
            print(f"  数据库: {db_count}条, {db_min} ~ {db_max}")

            # 从API重新获取
            api_records = await scraper.fetch_nav_history(fof99_id, use_v2=True)
            print(f"  API v2: {len(api_records)}条")

            if not api_records:
                print(f"  WARN: API无数据，跳过")
                issues.append((fund_id, fund_name, "API无数据"))
                continue

            # 从数据库获取全部NAV
            async with async_session() as db:
                result = await db.execute(text("""
                    SELECT nav_date, unit_nav, cumulative_nav
                    FROM nav_history
                    WHERE fund_id = :fid
                    ORDER BY nav_date
                """), {"fid": fund_id})
                db_rows = {str(r[0]): (float(r[1]) if r[1] else None, float(r[2]) if r[2] else None) for r in result.all()}

            # 逐条比对
            api_map = {r["nav_date"]: (r["unit_nav"], r["cumulative_nav"]) for r in api_records}

            matched, mismatched, missing_in_db = 0, 0, 0
            mismatch_details = []

            for date_str, (api_nav, api_cum) in api_map.items():
                if date_str not in db_rows:
                    missing_in_db += 1
                    continue
                db_nav, db_cum = db_rows[date_str]
                if db_nav is not None and api_nav is not None:
                    if abs(db_nav - api_nav) > 0.0001:
                        mismatched += 1
                        mismatch_details.append(f"    {date_str}: DB={db_nav} vs API={api_nav}")
                    else:
                        matched += 1
                else:
                    matched += 1

            total_checked += matched + mismatched
            total_match += matched
            total_mismatch += mismatched

            status = "PASS" if mismatched == 0 else "FAIL"
            print(f"  {status} 匹配={matched}, 不匹配={mismatched}, DB缺失={missing_in_db}")
            if mismatch_details:
                for d in mismatch_details[:5]:
                    print(d)
                if len(mismatch_details) > 5:
                    print(f"    ... 还有 {len(mismatch_details)-5} 条不匹配")
                issues.append((fund_id, fund_name, f"{mismatched}条不匹配"))
            print()

            await asyncio.sleep(0.5)

    finally:
        await client.close()

    # 汇总
    print(f"{'='*70}")
    print(f"校验汇总")
    print(f"{'='*70}")
    print(f"抽样基金: {len(samples)} 只")
    print(f"总比对: {total_checked} 条")
    print(f"匹配: {total_match} 条")
    print(f"不匹配: {total_mismatch} 条")
    accuracy = total_match / total_checked * 100 if total_checked > 0 else 0
    print(f"准确率: {accuracy:.2f}%")

    if issues:
        print(f"\n问题列表:")
        for fid, name, issue in issues:
            print(f"  基金 {fid} ({name}): {issue}")
    else:
        print(f"\nPASS: 全部校验通过!")


if __name__ == "__main__":
    asyncio.run(main())
