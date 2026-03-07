"""Import NAV data for 6 commodity/bond indices that are missing data.

These indices have benchmark records but no NAV data:
- CBA00221 (中债综合1-3年)
- CBA00211 (中债综合1年以下)
- T_NH (南华10年期国债期货)
- NHAI (南华农产品)
- NHCI (南华商品)
- NHII (南华工业品)

Uses encode_id from global/index/configs to fetch via viewv2.
Has longer delays to avoid rate limiting when crawler is running.
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

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.crawler.fof99.client import Fof99Client
from app.crawler.fof99.nav_scraper import NavScraper
from app.config import settings


MISSING_INDICES = {
    "CBA00221": "cdcb96a30f8c16d863051694a438de58",
    "CBA00211": "cdcb96a30f8c16d8028ec0a686321daa",
    "T_NH":     "76ad03602b4f2fe1a85ef1bfbe9da76e",
    "NHAI":     "06314224409c5b49b6d0a1a73b4d5f1e",
    "NHCI":     "06314224409c5b490110e9c939d41053",
    "NHII":     "06314224409c5b4949328c82473bcd6d",
}


async def main():
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    client = Fof99Client()
    await client.login()
    print(f"[OK] Logged in")

    nav_scraper = NavScraper(client)

    async with async_session() as db:
        for code, encode_id in MISSING_INDICES.items():
            print(f"\n--- {code} (encode_id={encode_id[:16]}...) ---")

            # Extra delay to avoid rate limits when crawler is running
            await asyncio.sleep(5)

            try:
                records = await nav_scraper.fetch_nav_history(encode_id)
            except Exception as e:
                print(f"  [FAIL] {e}")
                # Wait longer and retry once
                print(f"  Retrying after 30s...")
                await asyncio.sleep(30)
                try:
                    records = await nav_scraper.fetch_nav_history(encode_id)
                except Exception as e2:
                    print(f"  [FAIL retry] {e2}")
                    continue

            if not records:
                print(f"  [FAIL] No data returned")
                continue

            print(f"  Got {len(records)} records")

            # Get existing dates
            existing = await db.execute(
                text("SELECT nav_date FROM index_nav WHERE index_code = :code"),
                {"code": code}
            )
            existing_dates = set(r[0] for r in existing.fetchall())

            new_count = 0
            for rec in records:
                nav_date_str = rec.get("nav_date", "")
                nav_value = rec.get("unit_nav")
                if not nav_date_str or nav_value is None:
                    continue
                try:
                    nav_dt = datetime.strptime(nav_date_str, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if nav_dt in existing_dates:
                    continue
                await db.execute(
                    text("INSERT INTO index_nav (index_code, nav_date, nav_value) VALUES (:code, :date, :nav)"),
                    {"code": code, "date": nav_dt, "nav": float(nav_value)}
                )
                new_count += 1

            await db.commit()
            print(f"  [OK] Inserted {new_count} new records")
            if records:
                print(f"  Range: {records[0]['nav_date']} ~ {records[-1]['nav_date']}")

    # Summary
    async with async_session() as db:
        result = await db.execute(text("""
            SELECT b.index_code, b.index_name, count(n.id) as nav_count
            FROM benchmarks b
            LEFT JOIN index_nav n ON n.index_code = b.index_code
            WHERE b.index_code IN :codes
            GROUP BY b.index_code, b.index_name
        """), {"codes": tuple(MISSING_INDICES.keys())})
        print("\n=== Summary ===")
        for r in result.fetchall():
            status = "OK" if r[2] > 0 else "MISSING"
            print(f"  [{status}] {r[0]}: {r[1]} ({r[2]} records)")

    await client.close()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
