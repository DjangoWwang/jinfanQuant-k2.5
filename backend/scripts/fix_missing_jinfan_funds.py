"""Fix the 3 failed Jinfan sub-fund imports.

Issues:
1. "箦安" was OCR error, correct name is "箐安期权稳健一号"
2. "量道1000指增保护1号" and "榕树海鲲鹏2号" had no NAV — try A/B/C class variants
3. "盛冠达泽众进取6号" had only 1 NAV record — try fetching with broader date range

Strategy: When a fund has no NAV data, search for class variants (A类/B类/C类)
and use whichever has the most NAV data.
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from sqlalchemy import select, func as sa_func, delete
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.crawler.fof99.client import Fof99Client
from app.crawler.fof99.fund_scraper import FundScraper
from app.crawler.fof99.nav_scraper import NavScraper
from app.models.fund import Fund, NavHistory
from app.config import settings

# Funds to fix: (keyword, correct_name_hint)
FUNDS_TO_FIX = [
    "箐安期权稳健一号",        # was "箦安" (OCR error)
    "量道1000指增保护1号",      # had no NAV, try A类
    "榕树海鲲鹏2号",            # had no NAV, try variants
    "盛冠达泽众进取6号",        # only 1 record, re-fetch
]

# Class suffixes to try when main fund has no NAV
CLASS_SUFFIXES = ["A类", "B类", "C类", "D类", "1号A", "1号B"]


async def search_with_variants(scraper, nav_scraper, keyword):
    """Search for a fund, try class variants if main fund has no NAV."""

    result = await scraper.advanced_search(keyword=keyword, pagesize=20)
    fund_list = result.get("list", [])

    if not fund_list:
        print(f"    [WARN] No search results for '{keyword}'")
        return None, None, None

    # Try each result to find one with NAV data
    best_candidate = None
    best_nav = []
    best_freq = "irregular"

    for i, raw in enumerate(fund_list):
        norm = scraper._normalize_advanced(raw)
        name = norm["fund_name"]
        eid = norm["encode_id"]
        print(f"    [{i+1}] {name} (encode_id={eid})")

        try:
            nav_records, freq = await nav_scraper.fetch_nav_with_frequency(eid)
        except Exception as e:
            print(f"        NAV fetch error: {e}")
            continue

        print(f"        NAV count: {len(nav_records)}, freq: {freq}")

        if len(nav_records) > len(best_nav):
            best_candidate = norm
            best_nav = nav_records
            best_freq = freq

        # If we found a good one (>5 records), stop searching
        if len(nav_records) > 5:
            break

    if not best_candidate:
        return None, None, None

    return best_candidate, best_nav, best_freq


async def upsert_fund(db, norm, nav_records, freq):
    """Create or update fund and its NAV data."""
    fund_name = norm["fund_name"]
    encode_id = norm["encode_id"]
    filing_number = norm["filing_number"]

    # Check existing by filing_number or fund_name
    existing = await db.execute(
        select(Fund).where(
            (Fund.filing_number == filing_number) | (Fund.fund_name == fund_name)
        )
    )
    fund_db = existing.scalar_one_or_none()

    if fund_db:
        print(f"    Updating existing fund ID={fund_db.id} ({fund_db.fund_name})")
        # Update name/encode_id if different
        if fund_db.fund_name != fund_name:
            print(f"    Name change: {fund_db.fund_name} -> {fund_name}")
            fund_db.fund_name = fund_name
        if fund_db.fof99_fund_id != encode_id:
            fund_db.fof99_fund_id = encode_id
    else:
        inception = None
        if norm.get("inception_date"):
            try:
                inception = datetime.strptime(norm["inception_date"], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                pass

        fund_db = Fund(
            fund_name=fund_name,
            filing_number=filing_number,
            manager_name=norm.get("advisor", ""),
            inception_date=inception,
            strategy_type=norm.get("strategy_type", ""),
            strategy_sub=norm.get("strategy_sub", ""),
            latest_nav=norm.get("latest_nav"),
            nav_frequency="unknown",
            data_source="fof99",
            is_private=True,
            fof99_fund_id=encode_id,
        )
        db.add(fund_db)
        await db.flush()
        print(f"    Created fund ID={fund_db.id}")

    # Update frequency
    if freq in ("daily", "weekly"):
        fund_db.nav_frequency = freq

    # Get existing NAV dates
    existing_nav = await db.execute(
        select(NavHistory.nav_date).where(NavHistory.fund_id == fund_db.id)
    )
    existing_dates = set(row[0] for row in existing_nav.fetchall())

    # Insert new NAV records
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

        db.add(NavHistory(
            fund_id=fund_db.id,
            nav_date=nav_dt,
            unit_nav=rec["unit_nav"],
            cumulative_nav=rec.get("cumulative_nav") or rec["unit_nav"],
            daily_return=rec.get("change_pct"),
            data_source="fof99",
        ))
        new_count += 1

    # Update latest NAV
    if nav_records:
        latest = nav_records[-1]
        fund_db.latest_nav = latest["unit_nav"]
        try:
            fund_db.latest_nav_date = datetime.strptime(
                latest["nav_date"], "%Y-%m-%d"
            ).date()
        except (ValueError, TypeError):
            pass

    return fund_db, new_count


async def main():
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    client = Fof99Client()
    await client.login()
    print(f"[OK] FOF99 login success (device={client.device_id})")

    scraper = FundScraper(client)
    nav_scraper = NavScraper(client)

    async with async_session() as db:
        for keyword in FUNDS_TO_FIX:
            print(f"\n{'='*60}")
            print(f"--- Fixing: {keyword}")
            print(f"{'='*60}")

            candidate, nav_records, freq = await search_with_variants(
                scraper, nav_scraper, keyword
            )

            if not candidate:
                print(f"    [FAIL] No candidate with NAV data found")
                continue

            if not nav_records:
                print(f"    [FAIL] No NAV data for best candidate")
                continue

            print(f"\n    Best match: {candidate['fund_name']}")
            print(f"    Filing: {candidate['filing_number']}")
            print(f"    NAV records: {len(nav_records)}, freq: {freq}")
            print(f"    Range: {nav_records[0]['nav_date']} ~ {nav_records[-1]['nav_date']}")

            fund_db, new_count = await upsert_fund(db, candidate, nav_records, freq)
            print(f"    [OK] Fund ID={fund_db.id}, new NAV={new_count}")

        await db.commit()

    # Summary
    async with async_session() as db:
        result = await db.execute(
            select(Fund).where(Fund.is_private == True).order_by(Fund.id)
        )
        private_funds = list(result.scalars().all())
        if private_funds:
            print(f"\nPrivate funds in DB ({len(private_funds)}):")
            for f in private_funds:
                nav_count = await db.scalar(
                    select(sa_func.count(NavHistory.id))
                    .where(NavHistory.fund_id == f.id)
                )
                print(f"  ID={f.id:4d} | {(f.fund_name or '')[:40]:<40s} | "
                      f"freq={f.nav_frequency or '?':7s} | "
                      f"nav_count={nav_count or 0:5d} | "
                      f"latest={float(f.latest_nav or 0):.4f}")

    await client.close()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
