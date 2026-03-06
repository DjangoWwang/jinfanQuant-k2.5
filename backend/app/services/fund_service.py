"""Business logic layer for fund management."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Sequence

import pandas as pd
from sqlalchemy import select, func, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fund import Fund, NavHistory
from app.schemas.fund import FundCreate, FundResponse, FundUpdate, FundListParams

logger = logging.getLogger(__name__)


class FundService:
    """Fund CRUD and NAV operations."""

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def create_fund(self, db: AsyncSession, payload: FundCreate) -> Fund:
        fund = Fund(**payload.model_dump())
        db.add(fund)
        await db.flush()
        await db.refresh(fund)
        return fund

    async def get_fund(self, db: AsyncSession, fund_id: int) -> Fund | None:
        result = await db.execute(select(Fund).where(Fund.id == fund_id))
        return result.scalar_one_or_none()

    async def list_funds(
        self, db: AsyncSession, params: FundListParams
    ) -> tuple[list[Fund], int]:
        query = select(Fund).where(Fund.status == "active")

        if params.strategy_types:
            query = query.where(Fund.strategy_type.in_(params.strategy_types))
        if params.strategy_subs:
            query = query.where(Fund.strategy_sub.in_(params.strategy_subs))
        if params.nav_frequency:
            query = query.where(Fund.nav_frequency == params.nav_frequency)
        if params.search:
            pattern = f"%{params.search}%"
            query = query.where(
                or_(
                    Fund.fund_name.ilike(pattern),
                    Fund.filing_number.ilike(pattern),
                    Fund.manager_name.ilike(pattern),
                )
            )

        # Count
        count_q = select(func.count()).select_from(query.subquery())
        total = (await db.execute(count_q)).scalar() or 0

        # Paginate
        query = query.order_by(Fund.id).offset(
            (params.page - 1) * params.page_size
        ).limit(params.page_size)

        result = await db.execute(query)
        return list(result.scalars().all()), total

    async def update_fund(
        self, db: AsyncSession, fund_id: int, payload: FundUpdate
    ) -> Fund | None:
        fund = await self.get_fund(db, fund_id)
        if not fund:
            return None
        for field, value in payload.model_dump(exclude_unset=True).items():
            setattr(fund, field, value)
        await db.flush()
        await db.refresh(fund)
        return fund

    async def delete_fund(self, db: AsyncSession, fund_id: int) -> bool:
        fund = await self.get_fund(db, fund_id)
        if not fund:
            return False
        await db.delete(fund)
        await db.flush()
        return True

    # ------------------------------------------------------------------
    # NAV operations
    # ------------------------------------------------------------------

    async def upsert_nav_records(
        self,
        db: AsyncSession,
        fund_id: int,
        records: list[dict[str, Any]],
    ) -> int:
        """Bulk upsert NAV records using INSERT ... ON CONFLICT DO UPDATE."""
        if not records:
            return 0

        rows = []
        for r in records:
            rows.append({
                "fund_id": fund_id,
                "nav_date": r["nav_date"],
                "unit_nav": r.get("unit_nav"),
                "cumulative_nav": r.get("cumulative_nav"),
                "adjusted_nav": r.get("adjusted_nav"),
                "daily_return": r.get("daily_return"),
                "data_source": r.get("data_source", "fof99"),
            })

        stmt = pg_insert(NavHistory).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_nav_history_fund_date",
            set_={
                "unit_nav": stmt.excluded.unit_nav,
                "cumulative_nav": stmt.excluded.cumulative_nav,
                "adjusted_nav": stmt.excluded.adjusted_nav,
                "daily_return": stmt.excluded.daily_return,
            },
        )
        await db.execute(stmt)
        await db.flush()

        # Update fund's latest_nav
        await self._update_latest_nav(db, fund_id)

        return len(rows)

    async def get_nav_history(
        self,
        db: AsyncSession,
        fund_id: int,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[NavHistory]:
        query = (
            select(NavHistory)
            .where(NavHistory.fund_id == fund_id)
            .order_by(NavHistory.nav_date)
        )
        if start_date:
            query = query.where(NavHistory.nav_date >= start_date)
        if end_date:
            query = query.where(NavHistory.nav_date <= end_date)

        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_nav_series(
        self,
        db: AsyncSession,
        fund_id: int,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.Series:
        """Get NAV as a pandas Series indexed by date (for metrics calculation).

        处理逻辑：
        1. 优先使用 cumulative_nav（消除分红跳变）
        2. 检测交替模式（unit_nav与cumulative_nav交替不一致）
           若存在，只保留 unit_nav==cumulative_nav 的记录
        3. 回退到 unit_nav
        """
        records = await self.get_nav_history(db, fund_id, start_date, end_date)
        if not records:
            return pd.Series(dtype=float)

        # 检测交替模式：统计 unit_nav != cumulative_nav 的比例
        same_count = 0
        diff_count = 0
        for r in records:
            if r.unit_nav is not None and r.cumulative_nav is not None:
                if abs(float(r.unit_nav) - float(r.cumulative_nav)) < 0.01:
                    same_count += 1
                else:
                    diff_count += 1

        # 若 same 和 diff 各占 20%-80%，判定为交替模式
        total = same_count + diff_count
        is_interleaved = (
            total > 20
            and same_count > total * 0.15
            and diff_count > total * 0.15
        )

        pairs = []
        for r in records:
            if is_interleaved:
                # 交替模式：只用 unit_nav == cumulative_nav 的行
                if (r.unit_nav is not None and r.cumulative_nav is not None
                        and abs(float(r.unit_nav) - float(r.cumulative_nav)) < 0.01):
                    pairs.append((r.nav_date, float(r.cumulative_nav)))
            else:
                # 正常模式：优先 cumulative_nav
                nav = r.cumulative_nav if r.cumulative_nav is not None else r.unit_nav
                if nav is not None:
                    pairs.append((r.nav_date, float(nav)))

        if not pairs:
            return pd.Series(dtype=float)
        dates, navs = zip(*pairs)
        return pd.Series(navs, index=pd.DatetimeIndex(dates), name=f"fund_{fund_id}")

    async def _update_latest_nav(self, db: AsyncSession, fund_id: int) -> None:
        """Update the fund's latest_nav and latest_nav_date from nav_history."""
        result = await db.execute(
            select(NavHistory.unit_nav, NavHistory.nav_date)
            .where(NavHistory.fund_id == fund_id)
            .order_by(NavHistory.nav_date.desc())
            .limit(1)
        )
        row = result.first()
        if row:
            fund = await self.get_fund(db, fund_id)
            if fund:
                fund.latest_nav = row[0]
                fund.latest_nav_date = row[1]


fund_service = FundService()
