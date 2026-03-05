"""Import valuation table and create portfolio with allocations.

Parses the valuation Excel, matches sub-funds to existing funds in DB
by filing_number, and creates a portfolio with allocation weights.
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

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.importer.valuation_parser import ValuationParser
from app.models.fund import Fund
from app.models.portfolio import Portfolio, PortfolioAllocation
from app.config import settings


VALUATION_FILES = [
    r"D:\AI\Claude code\FOF平台开发\估值表日报-GF1077-博孚利鹭岛晋帆私募证券投资基金-4-20260213.xlsx",
    r"D:\AI\Claude code\FOF平台开发\估值表日报-GF1077-博孚利鹭岛晋帆私募证券投资基金-4-20260227.xlsx",
]


async def main():
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    parser = ValuationParser()

    async with async_session() as db:
        # Load all private funds for matching
        result = await db.execute(
            select(Fund).where(Fund.is_private == True)
        )
        all_funds = list(result.scalars().all())

        # Build filing_number -> fund_id lookup
        filing_map: dict[str, Fund] = {}
        for f in all_funds:
            if f.filing_number:
                filing_map[f.filing_number.upper()] = f

        print(f"Loaded {len(filing_map)} private funds for matching")
        print()

        # Check if portfolio already exists
        existing_portfolio = await db.execute(
            select(Portfolio).where(Portfolio.name == "博孚利鹭岛晋帆FOF")
        )
        portfolio = existing_portfolio.scalar_one_or_none()

        if not portfolio:
            portfolio = Portfolio(
                name="博孚利鹭岛晋帆FOF",
                description="鹭岛晋帆私募证券投资基金 - 实盘FOF组合",
                portfolio_type="live",
                allocation_model="custom_weight",
                rebalance_freq="monthly",
                is_active=True,
            )
            db.add(portfolio)
            await db.flush()
            print(f"Created portfolio: ID={portfolio.id}, name={portfolio.name}")
        else:
            print(f"Portfolio already exists: ID={portfolio.id}")

        # Process each valuation file
        for file_path in VALUATION_FILES:
            print(f"\n{'='*70}")
            print(f"Processing: {Path(file_path).name}")
            print(f"{'='*70}")

            result = parser.parse(file_path)
            val_date = result["valuation_date"]
            print(f"  Date: {val_date}")
            print(f"  Unit NAV: {result['unit_nav']}")
            print(f"  Product: {result['product_name']}")
            print(f"  Sub-funds: {len(result['sub_fund_allocations'])}")

            if not val_date:
                print("  [SKIP] No valuation date found")
                continue

            effective_date = datetime.strptime(val_date, "%Y-%m-%d").date()

            # Update portfolio start_date if earlier
            if portfolio.start_date is None or effective_date < portfolio.start_date:
                portfolio.start_date = effective_date

            matched = 0
            unmatched = 0

            for sf in result["sub_fund_allocations"]:
                filing = sf["filing_number"].upper()
                weight_pct = sf["weight_pct"] or 0.0
                # Convert percentage to decimal (e.g. 6.1163% -> 0.061163)
                weight_decimal = weight_pct / 100.0

                # Try exact match first
                fund = filing_map.get(filing)

                # Try partial match (filing number might be a suffix)
                if not fund:
                    for fn_key, fn_fund in filing_map.items():
                        if fn_key.endswith(filing) or filing.endswith(fn_key):
                            fund = fn_fund
                            break

                # Try by name similarity
                if not fund:
                    sf_name = sf["fund_name"]
                    for fn_fund in all_funds:
                        if fn_fund.fund_name and (
                            sf_name in fn_fund.fund_name or fn_fund.fund_name in sf_name
                        ):
                            fund = fn_fund
                            break

                if fund:
                    # Check if allocation already exists
                    existing_alloc = await db.execute(
                        select(PortfolioAllocation).where(
                            PortfolioAllocation.portfolio_id == portfolio.id,
                            PortfolioAllocation.fund_id == fund.id,
                            PortfolioAllocation.effective_date == effective_date,
                        )
                    )
                    if existing_alloc.scalar_one_or_none():
                        print(f"  [SKIP] {filing:10s} -> Fund ID={fund.id:4d} "
                              f"(already exists for {val_date})")
                    else:
                        db.add(PortfolioAllocation(
                            portfolio_id=portfolio.id,
                            fund_id=fund.id,
                            target_weight=weight_decimal,
                            effective_date=effective_date,
                        ))
                        print(f"  [OK]   {filing:10s} -> Fund ID={fund.id:4d} | "
                              f"{sf['fund_name'][:30]:30s} | weight={weight_pct:.4f}%")
                    matched += 1
                else:
                    print(f"  [MISS] {filing:10s} | {sf['fund_name'][:30]:30s} | "
                          f"weight={weight_pct:.4f}% (no matching fund in DB)")
                    unmatched += 1

            print(f"\n  Summary: matched={matched}, unmatched={unmatched}")

        await db.commit()
        print(f"\n{'='*70}")
        print("Done! Portfolio allocations saved.")

        # Show final portfolio state
        allocs = await db.execute(
            select(PortfolioAllocation, Fund.fund_name)
            .join(Fund, PortfolioAllocation.fund_id == Fund.id)
            .where(PortfolioAllocation.portfolio_id == portfolio.id)
            .order_by(PortfolioAllocation.effective_date, PortfolioAllocation.fund_id)
        )
        rows = allocs.fetchall()
        print(f"\nPortfolio '{portfolio.name}' allocations ({len(rows)} total):")
        current_date = None
        for alloc, fund_name in rows:
            if alloc.effective_date != current_date:
                current_date = alloc.effective_date
                print(f"\n  --- {current_date} ---")
            print(f"  Fund ID={alloc.fund_id:4d} | {fund_name[:35]:35s} | "
                  f"weight={float(alloc.target_weight)*100:.4f}%")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
