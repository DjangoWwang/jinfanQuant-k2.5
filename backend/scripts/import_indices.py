"""Import common indices from FOF99 into benchmarks + index_nav tables.

Uses:
  1. GET /newgoapi/index/indexList -> index metadata
  2. GET /pyapi/fund/viewv2?fid=<id> -> index NAV history
"""

import asyncio
import sys
import io
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.crawler.fof99.client import Fof99Client
from app.crawler.fof99.nav_scraper import NavScraper
from app.config import settings

# Which indices to import (will match by name keyword from indexList)
TARGET_INDICES = [
    "沪深300",
    "中证500",
    "中证1000",
    "南华商品指数",
    "中债综合",
    "万得全A",
    "创业板指",
    "上证指数",
]


async def main():
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    client = Fof99Client()
    await client.login()
    print(f"[OK] FOF99 login (device={client.device_id})")

    nav_scraper = NavScraper(client)

    # Step 1: Fetch index list from FOF99
    print("\nFetching indexList...")
    data = await client.api_get("/newgoapi/index/indexList")
    if not data:
        print("[FAIL] No index list returned")
        return

    index_list = data if isinstance(data, list) else data.get("list", data.get("data", []))
    print(f"  Total indices: {len(index_list)}")

    # Find target indices
    targets = []
    for idx in index_list:
        name = idx.get("name", "")
        for target in TARGET_INDICES:
            if target in name:
                targets.append(idx)
                print(f"  Found: {name} (id={idx.get('id')}, code={idx.get('code')})")
                break

    print(f"\nMatched {len(targets)} target indices")

    async with async_session() as db:
        imported = 0
        for idx in targets:
            idx_id = idx.get("id", "")
            idx_code = idx.get("code", "")
            idx_name = idx.get("name", "")
            category = idx.get("category", "")

            print(f"\n--- {idx_name} (code={idx_code}, fid={idx_id})")

            # Check if already in benchmarks table
            existing = await db.execute(
                text("SELECT id FROM benchmarks WHERE index_code = :code"),
                {"code": idx_code}
            )
            bench = existing.scalar_one_or_none()

            if not bench:
                await db.execute(
                    text("""INSERT INTO benchmarks (index_code, index_name, category, is_public)
                            VALUES (:code, :name, :cat, true)"""),
                    {"code": idx_code, "name": idx_name, "cat": category}
                )
                print(f"    Created benchmark: {idx_code}")
            else:
                print(f"    Benchmark already exists")

            # Fetch NAV history using viewv2 (index uses id as fid)
            print(f"    Fetching NAV history...")
            try:
                nav_records = await nav_scraper.fetch_nav_history(idx_id)
            except Exception as e:
                print(f"    [FAIL] NAV fetch error: {e}")
                continue

            if not nav_records:
                print(f"    [FAIL] No NAV data")
                continue

            # Get existing dates
            existing_dates_result = await db.execute(
                text("SELECT nav_date FROM index_nav WHERE index_code = :code"),
                {"code": idx_code}
            )
            existing_dates = set(row[0] for row in existing_dates_result.fetchall())

            new_count = 0
            for rec in nav_records:
                nav_date_str = rec.get("nav_date", "")
                if not nav_date_str:
                    continue
                try:
                    nav_dt = datetime.strptime(nav_date_str, "%Y-%m-%d").date()
                except ValueError:
                    continue

                if nav_dt in existing_dates:
                    continue

                await db.execute(
                    text("""INSERT INTO index_nav (index_code, nav_date, nav_value)
                            VALUES (:code, :date, :nav)"""),
                    {"code": idx_code, "date": nav_dt, "nav": rec["unit_nav"]}
                )
                new_count += 1

            print(f"    [OK] total={len(nav_records)}, new={new_count}, "
                  f"range={nav_records[0]['nav_date']}~{nav_records[-1]['nav_date']}")
            imported += 1

        await db.commit()
        print(f"\n{'='*60}")
        print(f"Import done: {imported}/{len(targets)} indices")

        # Summary
        result = await db.execute(text("""
            SELECT b.index_code, b.index_name, count(n.id) as nav_count,
                   min(n.nav_date) as start_date, max(n.nav_date) as end_date
            FROM benchmarks b
            LEFT JOIN index_nav n ON n.index_code = b.index_code
            GROUP BY b.index_code, b.index_name
            ORDER BY nav_count DESC
        """))
        rows = result.fetchall()
        print(f"\nBenchmarks in DB ({len(rows)}):")
        for r in rows:
            print(f"  {r[0]:15s} | {r[1]:20s} | nav={r[2]:6d} | {r[3]}~{r[4]}")

    await client.close()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
