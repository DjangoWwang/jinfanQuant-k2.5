"""NAV calculation service: compute product daily NAV from sub-fund holdings.

Algorithm:
1. Load all valuation snapshots for the product (ordered by date).
2. For each snapshot, extract sub-fund positions (items with linked_fund_id)
   and non-fund assets (total_nav - sum of sub-fund market values).
3. Between consecutive snapshots, interpolate daily NAV using sub-fund
   daily unit_nav * shares held. Non-fund assets held constant.
4. On valuation dates, use actual snapshot values directly.
5. Store results via PostgreSQL upsert (ON CONFLICT DO UPDATE).
"""

from __future__ import annotations

import logging
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Sequence

from sqlalchemy import select, delete, func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fund import NavHistory
from app.models.product import Product, ValuationSnapshot, ValuationItem, ProductNav

logger = logging.getLogger(__name__)

# Decimal precision constants
_NAV6 = Decimal("0.000001")
_AMT2 = Decimal("0.01")
_SHARE4 = Decimal("0.0001")
_ZERO = Decimal("0")

# Safety limit: max calendar days to calculate per snapshot interval
MAX_CALCULATION_DAYS = 365 * 5

# Advisory lock namespace for NAV calculations (separate from auth module's 10001)
_NAV_CALC_LOCK_NS = 20001


def _q6(v: Decimal | None) -> Decimal | None:
    """Quantize to 6 decimal places (NAV precision)."""
    return v.quantize(_NAV6, rounding=ROUND_HALF_UP) if v is not None else None


def _q2(v: Decimal | None) -> Decimal | None:
    """Quantize to 2 decimal places (amount precision)."""
    return v.quantize(_AMT2, rounding=ROUND_HALF_UP) if v is not None else None


def _to_decimal(val, default: Decimal = _ZERO) -> Decimal:
    """Convert a value to Decimal safely."""
    if val is None:
        return default
    if isinstance(val, Decimal):
        return val
    return Decimal(str(val))


@dataclass(slots=True)
class SubFundPosition:
    """A sub-fund holding extracted from a valuation snapshot."""
    fund_id: int
    fund_name: str
    shares: Decimal
    market_value: Decimal
    market_price: Decimal | None = None


class NavCalcResult:
    """Result of a NAV calculation run."""

    def __init__(self) -> None:
        self.total_days: int = 0
        self.calculated_days: int = 0
        self.snapshot_days: int = 0
        self.skipped_days: int = 0
        self.date_range: tuple[date | None, date | None] = (None, None)
        self.warnings: list[str] = []


async def calculate_product_nav(
    db: AsyncSession,
    product_id: int,
    *,
    recalculate: bool = False,
) -> NavCalcResult:
    """Calculate daily NAV for a product based on sub-fund holdings.

    Uses PostgreSQL advisory lock (double-key) per product to prevent
    concurrent calculations. Uses upsert for idempotent writes.
    """
    result = NavCalcResult()

    # Acquire advisory lock: namespace=20001, key=product_id
    await db.execute(
        text("SELECT pg_advisory_xact_lock(:ns, :pid)"),
        {"ns": _NAV_CALC_LOCK_NS, "pid": product_id},
    )

    # Load product
    product = await db.get(Product, product_id)
    if not product:
        raise ValueError(f"产品不存在: {product_id}")

    # Load all snapshots ordered by date
    snap_result = await db.execute(
        select(ValuationSnapshot)
        .where(ValuationSnapshot.product_id == product_id)
        .order_by(ValuationSnapshot.valuation_date)
    )
    snapshots = list(snap_result.scalars().all())

    if not snapshots:
        result.warnings.append("该产品没有估值快照数据")
        return result

    logger.info("产品 %d: 发现 %d 个快照, 开始NAV计算", product_id, len(snapshots))

    if recalculate:
        await db.execute(
            delete(ProductNav).where(
                ProductNav.product_id == product_id,
                ProductNav.source.in_(["calculated", "valuation"]),
            )
        )

    # Batch load all positions across all snapshots (fixes N+1)
    snapshot_ids = [s.id for s in snapshots]
    positions_by_snapshot = await _batch_extract_positions(db, snapshot_ids)

    # Collect all fund_ids across all snapshots
    all_fund_ids: set[int] = set()
    for positions in positions_by_snapshot.values():
        for pos in positions:
            all_fund_ids.add(pos.fund_id)

    # Determine date range for NAV loading
    first_date = snapshots[0].valuation_date
    last_snap_date = snapshots[-1].valuation_date

    # End date: limited to MIN of per-fund latest NAV dates (conservative)
    # This ensures we don't generate NAV with stale prices from lagging funds
    if all_fund_ids:
        last_positions = positions_by_snapshot.get(snapshots[-1].id, [])
        last_fund_ids = [p.fund_id for p in last_positions]
        if last_fund_ids:
            end_limit = await _get_common_latest_nav_date(db, last_fund_ids) or last_snap_date
        else:
            end_limit = last_snap_date
        logger.info("产品 %d: NAV计算截止日期 %s (子基金共同最新NAV)", product_id, end_limit)
    else:
        end_limit = last_snap_date

    # Batch load all fund NAVs for the entire range at once
    all_nav_map: dict[int, dict[date, Decimal]] = {}
    nav_index: dict[int, tuple[list[date], list[Decimal]]] = {}
    if all_fund_ids:
        all_nav_map = await _batch_load_fund_navs(
            db, list(all_fund_ids), first_date, end_limit,
        )
        # Build sorted indexes for O(log n) forward-fill lookups
        for fid, series in all_nav_map.items():
            sorted_dates = sorted(series.keys())
            sorted_values = [series[d] for d in sorted_dates]
            nav_index[fid] = (sorted_dates, sorted_values)

    # Process each snapshot interval
    nav_rows: list[dict] = []

    for i, snapshot in enumerate(snapshots):
        positions = positions_by_snapshot.get(snapshot.id, [])
        total_shares = _to_decimal(snapshot.total_shares or product.total_shares)
        snapshot_total_nav = _to_decimal(snapshot.total_nav) if snapshot.total_nav is not None else None
        snapshot_unit_nav = _to_decimal(snapshot.unit_nav) if snapshot.unit_nav is not None else None

        if not positions:
            if snapshot_total_nav is not None:
                nav_rows.append(_make_nav_row(
                    product_id=product_id,
                    nav_date=snapshot.valuation_date,
                    unit_nav=_q6(snapshot_unit_nav),
                    cumulative_nav=None,
                    total_nav=_q2(snapshot_total_nav),
                    total_shares=total_shares.quantize(_SHARE4) if total_shares else None,
                    fund_assets=None,
                    non_fund_assets=None,
                    source="valuation",
                    snapshot_id=snapshot.id,
                ))
                result.snapshot_days += 1
            else:
                result.skipped_days += 1
                result.warnings.append(
                    f"快照 {snapshot.valuation_date} 缺少 total_nav，已跳过"
                )
            continue

        # Calculate fund assets and non-fund assets on snapshot date
        fund_mv = sum(p.market_value for p in positions)

        if snapshot_total_nav is None:
            result.skipped_days += 1
            result.warnings.append(
                f"快照 {snapshot.valuation_date} 缺少 total_nav，已跳过插值"
            )
            continue

        non_fund_assets = snapshot_total_nav - fund_mv

        # Record the snapshot date itself
        nav_rows.append(_make_nav_row(
            product_id=product_id,
            nav_date=snapshot.valuation_date,
            unit_nav=_q6(snapshot_unit_nav),
            cumulative_nav=None,
            total_nav=_q2(snapshot_total_nav),
            total_shares=total_shares.quantize(_SHARE4) if total_shares else None,
            fund_assets=_q2(fund_mv),
            non_fund_assets=_q2(non_fund_assets),
            source="valuation",
            snapshot_id=snapshot.id,
        ))
        result.snapshot_days += 1

        # Determine the date range to interpolate
        start = snapshot.valuation_date + timedelta(days=1)
        if i + 1 < len(snapshots):
            end = snapshots[i + 1].valuation_date - timedelta(days=1)
        else:
            end = end_limit

        if start > end:
            continue

        # Safety check: prevent excessively large date ranges
        days_count = (end - start).days + 1
        if days_count > MAX_CALCULATION_DAYS:
            result.warnings.append(
                f"快照 {snapshot.valuation_date} 后日期范围过大({days_count}天)，截断至{MAX_CALCULATION_DAYS}天"
            )
            end = start + timedelta(days=MAX_CALCULATION_DAYS - 1)

        # Generate daily NAV for each calendar day in range
        cur = start
        while cur <= end:
            daily_fund_assets = _ZERO
            missing_funds = 0

            for pos in positions:
                fund_series = all_nav_map.get(pos.fund_id, {})
                fund_nav = fund_series.get(cur)

                if fund_nav is None:
                    # Forward-fill using bisect O(log n)
                    idx_data = nav_index.get(pos.fund_id)
                    if idx_data:
                        fund_nav = _find_last_nav_indexed(idx_data[0], idx_data[1], cur)

                if fund_nav is not None:
                    daily_fund_assets += pos.shares * fund_nav
                else:
                    # Fallback: use snapshot market value
                    daily_fund_assets += pos.market_value
                    missing_funds += 1

            if missing_funds > 0:
                result.warnings.append(
                    f"{cur}: {missing_funds}只子基金NAV缺失，使用快照市值替代"
                )

            total_assets = daily_fund_assets + non_fund_assets
            daily_unit_nav = (total_assets / total_shares) if total_shares > 0 else None

            nav_rows.append(_make_nav_row(
                product_id=product_id,
                nav_date=cur,
                unit_nav=_q6(daily_unit_nav),
                cumulative_nav=None,
                total_nav=_q2(total_assets),
                total_shares=total_shares.quantize(_SHARE4) if total_shares else None,
                fund_assets=_q2(daily_fund_assets),
                non_fund_assets=_q2(non_fund_assets),
                source="calculated",
                snapshot_id=snapshot.id,
            ))
            result.calculated_days += 1
            cur += timedelta(days=1)

    # Deduplicate by date (later entries win)
    by_date: dict[date, dict] = {}
    for row in nav_rows:
        by_date[row["nav_date"]] = row

    sorted_rows = sorted(by_date.values(), key=lambda r: r["nav_date"])

    # Batch upsert using PostgreSQL ON CONFLICT
    if sorted_rows:
        BATCH_SIZE = 500
        for i in range(0, len(sorted_rows), BATCH_SIZE):
            batch = sorted_rows[i:i + BATCH_SIZE]
            stmt = insert(ProductNav).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["product_id", "nav_date"],
                set_={
                    "unit_nav": stmt.excluded.unit_nav,
                    "cumulative_nav": stmt.excluded.cumulative_nav,
                    "total_nav": stmt.excluded.total_nav,
                    "total_shares": stmt.excluded.total_shares,
                    "fund_assets": stmt.excluded.fund_assets,
                    "non_fund_assets": stmt.excluded.non_fund_assets,
                    "source": stmt.excluded.source,
                    "snapshot_id": stmt.excluded.snapshot_id,
                    "updated_at": func.now(),
                },
            )
            await db.execute(stmt)

        result.total_days = len(sorted_rows)
        result.date_range = (
            sorted_rows[0]["nav_date"],
            sorted_rows[-1]["nav_date"],
        )

    logger.info(
        "产品 %d NAV计算完成: 总%d天 (快照%d + 计算%d + 跳过%d)",
        product_id, result.total_days, result.snapshot_days,
        result.calculated_days, result.skipped_days,
    )
    return result


async def get_calculated_nav_series(
    db: AsyncSession,
    product_id: int,
    start_date: date | None = None,
    end_date: date | None = None,
) -> Sequence[ProductNav]:
    """Get pre-calculated NAV series for a product."""
    query = (
        select(ProductNav)
        .where(ProductNav.product_id == product_id)
        .order_by(ProductNav.nav_date)
    )
    if start_date:
        query = query.where(ProductNav.nav_date >= start_date)
    if end_date:
        query = query.where(ProductNav.nav_date <= end_date)

    result = await db.execute(query)
    return result.scalars().all()


async def get_nav_stats(
    db: AsyncSession,
    product_id: int,
) -> dict:
    """Get NAV calculation statistics for a product."""
    total_q = select(func.count()).where(ProductNav.product_id == product_id)
    total = (await db.execute(total_q)).scalar() or 0

    if total == 0:
        return {"total_days": 0, "has_data": False}

    calc_count = (await db.execute(
        select(func.count()).where(
            ProductNav.product_id == product_id,
            ProductNav.source == "calculated",
        )
    )).scalar() or 0

    val_count = (await db.execute(
        select(func.count()).where(
            ProductNav.product_id == product_id,
            ProductNav.source == "valuation",
        )
    )).scalar() or 0

    date_range = (await db.execute(
        select(
            func.min(ProductNav.nav_date),
            func.max(ProductNav.nav_date),
        ).where(ProductNav.product_id == product_id)
    )).one()

    latest = (await db.execute(
        select(ProductNav)
        .where(ProductNav.product_id == product_id)
        .order_by(ProductNav.nav_date.desc())
        .limit(1)
    )).scalar_one_or_none()

    return {
        "total_days": total,
        "calculated_days": calc_count,
        "valuation_days": val_count,
        "has_data": True,
        "date_range": {
            "start": date_range[0].isoformat() if date_range[0] else None,
            "end": date_range[1].isoformat() if date_range[1] else None,
        },
        "latest_nav": float(latest.unit_nav) if latest and latest.unit_nav is not None else None,
        "latest_date": latest.nav_date.isoformat() if latest else None,
        "latest_total_nav": float(latest.total_nav) if latest and latest.total_nav is not None else None,
    }


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _make_nav_row(**kwargs) -> dict:
    """Create a dict suitable for bulk insert."""
    return kwargs


async def _batch_extract_positions(
    db: AsyncSession, snapshot_ids: list[int]
) -> dict[int, list[SubFundPosition]]:
    """Extract sub-fund positions from multiple snapshots in a single query."""
    if not snapshot_ids:
        return {}

    result = await db.execute(
        select(ValuationItem)
        .where(
            ValuationItem.snapshot_id.in_(snapshot_ids),
            ValuationItem.linked_fund_id.is_not(None),
        )
    )
    items = result.scalars().all()

    positions_by_snapshot: dict[int, list[SubFundPosition]] = defaultdict(list)
    for item in items:
        shares = _to_decimal(item.quantity)
        mv = _to_decimal(item.market_value)
        mp = _to_decimal(item.market_price) if item.market_price is not None else None

        if shares <= 0 and mv <= 0:
            continue

        positions_by_snapshot[item.snapshot_id].append(SubFundPosition(
            fund_id=item.linked_fund_id,
            fund_name=item.item_name or "",
            shares=shares,
            market_value=mv,
            market_price=mp,
        ))

    return dict(positions_by_snapshot)


async def _batch_load_fund_navs(
    db: AsyncSession,
    fund_ids: list[int],
    start_date: date,
    end_date: date,
) -> dict[int, dict[date, Decimal]]:
    """Batch load NAV history for multiple funds in a date range.

    Returns: {fund_id: {nav_date: unit_nav as Decimal}}
    Uses unit_nav consistently for position valuation.
    """
    if not fund_ids:
        return {}

    nav_map: dict[int, dict[date, Decimal]] = {}

    # Batch fund_ids to stay within PostgreSQL IN clause limits
    BATCH_SIZE = 1000
    for i in range(0, len(fund_ids), BATCH_SIZE):
        batch_ids = fund_ids[i:i + BATCH_SIZE]
        result = await db.execute(
            select(
                NavHistory.fund_id,
                NavHistory.nav_date,
                NavHistory.unit_nav,
            )
            .where(
                NavHistory.fund_id.in_(batch_ids),
                NavHistory.nav_date >= start_date,
                NavHistory.nav_date <= end_date,
            )
            .order_by(NavHistory.nav_date)
        )

        for row in result.all():
            nav_value = _to_decimal(row.unit_nav) if row.unit_nav is not None else None
            if nav_value is not None and nav_value > 0:
                nav_map.setdefault(row.fund_id, {})[row.nav_date] = nav_value

    return nav_map


async def _get_common_latest_nav_date(
    db: AsyncSession, fund_ids: list[int]
) -> date | None:
    """Get the MIN of per-fund latest NAV dates (conservative bound).

    This ensures all sub-funds have data up to the returned date,
    preventing stale-price interpolation for lagging funds.
    """
    if not fund_ids:
        return None

    rows = (await db.execute(
        select(
            NavHistory.fund_id,
            func.max(NavHistory.nav_date).label("latest_date"),
        )
        .where(NavHistory.fund_id.in_(fund_ids))
        .group_by(NavHistory.fund_id)
    )).all()

    latest_dates = [row.latest_date for row in rows if row.latest_date is not None]
    return min(latest_dates) if latest_dates else None


def _find_last_nav_indexed(
    sorted_dates: list[date],
    sorted_values: list[Decimal],
    target_date: date,
) -> Decimal | None:
    """Find the most recent NAV before target_date using bisect O(log n)."""
    idx = bisect_right(sorted_dates, target_date) - 1
    return sorted_values[idx] if idx >= 0 else None
