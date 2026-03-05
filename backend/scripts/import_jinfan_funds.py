"""Import Ludam Jinfan FOF sub-funds from FOF99 into the database.

Search by keyword, fetch NAV history, and save to funds + nav_history tables.
Also stores the portfolio allocation weights from the valuation report.
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from sqlalchemy import select, func as sa_func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.crawler.fof99.client import Fof99Client
from app.crawler.fof99.fund_scraper import FundScraper
from app.crawler.fof99.nav_scraper import NavScraper
from app.models.fund import Fund, NavHistory
from app.config import settings

# 12 sub-funds from the valuation report (keyword for search)
# Weight will be determined after import from the valuation table
JINFAN_SUBFUNDS = [
    "哲萌龙泉进取2号",
    "米纳中证央企指数增强1号",
    "榕树海鲲鹏2号",
    "汉鸿雪融1号",
    "量道1000指增保护1号",
    "量智数益CTA一号",
    "善流钱塘一号",
    "博益安盈精英1号",
    "鸣瑜机器人二号",
    "宣夜投资星图一号",
    "箐安期权稳健一号",
    "盛冠达泽众进取6号",
]


async def main():
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    client = Fof99Client()
    await client.login()
    print(f"[OK] FOF99 login success (device={client.device_id})")

    scraper = FundScraper(client)
    nav_scraper = NavScraper(client)

    async with async_session() as db:
        imported = 0
        failed = 0

        for keyword in JINFAN_SUBFUNDS:
            print(f"\n--- Search: {keyword}")

            # Search via advancedList
            try:
                result = await scraper.advanced_search(keyword=keyword, pagesize=10)
            except Exception as e:
                print(f"    [FAIL] Search error: {e}")
                failed += 1
                continue

            fund_list = result.get("list", [])
            if not fund_list:
                print(f"    [FAIL] No results")
                failed += 1
                continue

            # Take the first match
            raw = fund_list[0]
            norm = scraper._normalize_advanced(raw)
            fund_name = norm["fund_name"]
            encode_id = norm["encode_id"]
            filing_number = norm["filing_number"]

            print(f"    Found: {fund_name}")
            print(f"    Filing: {filing_number} | encode_id: {encode_id}")

            # Check if already in DB
            existing = await db.execute(
                select(Fund).where(
                    (Fund.filing_number == filing_number) | (Fund.fund_name == fund_name)
                )
            )
            fund_db = existing.scalar_one_or_none()

            if fund_db:
                print(f"    Already in DB (ID={fund_db.id})")
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

            # Fetch NAV history
            print(f"    Fetching NAV...")
            try:
                nav_records, freq = await nav_scraper.fetch_nav_with_frequency(encode_id)
            except Exception as e:
                print(f"    [FAIL] NAV fetch error: {e}")
                failed += 1
                continue

            # If no NAV data, try other search results (A/B/C class variants)
            if not nav_records and len(fund_list) > 1:
                print(f"    No NAV for main fund, trying class variants...")
                for alt_raw in fund_list[1:]:
                    alt_norm = scraper._normalize_advanced(alt_raw)
                    alt_name = alt_norm["fund_name"]
                    alt_eid = alt_norm["encode_id"]
                    print(f"    Trying: {alt_name} ({alt_eid})")
                    try:
                        nav_records, freq = await nav_scraper.fetch_nav_with_frequency(alt_eid)
                    except Exception:
                        continue
                    if nav_records:
                        # Use this variant instead
                        fund_name = alt_name
                        encode_id = alt_eid
                        filing_number = alt_norm["filing_number"]
                        fund_db.fund_name = fund_name
                        fund_db.filing_number = filing_number
                        fund_db.fof99_fund_id = encode_id
                        print(f"    [OK] Found NAV via variant: {alt_name}")
                        break

            if not nav_records:
                print(f"    [FAIL] No NAV data (including variants)")
                failed += 1
                continue

            # Update frequency
            if freq in ("daily", "weekly"):
                fund_db.nav_frequency = freq

            # Store fof99_fund_id
            if not fund_db.fof99_fund_id:
                fund_db.fof99_fund_id = encode_id

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

            print(f"    [OK] NAV: total={len(nav_records)}, new={new_count}, "
                  f"freq={freq}, "
                  f"range={nav_records[0]['nav_date']}~{nav_records[-1]['nav_date']}")
            imported += 1

        await db.commit()
        print(f"\n{'='*60}")
        print(f"Import done: success={imported}, failed={failed}")
        print(f"{'='*60}")

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
                print(f"  ID={f.id:4d} | {(f.fund_name or '')[:35]:<35s} | "
                      f"freq={f.nav_frequency or '?':7s} | "
                      f"nav_count={nav_count or 0:5d} | "
                      f"latest={float(f.latest_nav or 0):.4f}")

    await client.close()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
