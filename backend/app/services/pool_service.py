"""Business logic for fund pool management."""

from __future__ import annotations

import logging
from typing import Sequence

from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fund import Fund
from app.models.pool import FundPool

logger = logging.getLogger(__name__)

VALID_POOL_TYPES = ("basic", "watch", "investment")


class PoolService:

    async def list_pool_funds(
        self,
        db: AsyncSession,
        pool_type: str,
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[list[dict], int]:
        """List funds in a pool with fund metadata joined."""
        if pool_type not in VALID_POOL_TYPES:
            raise ValueError(f"Invalid pool_type: {pool_type}")

        query = (
            select(FundPool, Fund)
            .join(Fund, FundPool.fund_id == Fund.id)
            .where(FundPool.pool_type == pool_type)
            .order_by(FundPool.added_at.desc())
        )

        count_q = select(func.count()).select_from(query.subquery())
        total = (await db.execute(count_q)).scalar() or 0

        query = query.offset((page - 1) * page_size).limit(page_size)
        result = await db.execute(query)

        items = []
        for pool, fund in result.all():
            items.append({
                "id": pool.id,
                "pool_type": pool.pool_type,
                "fund_id": fund.id,
                "fund_name": fund.fund_name,
                "strategy_type": fund.strategy_type,
                "nav_frequency": fund.nav_frequency,
                "latest_nav": float(fund.latest_nav) if fund.latest_nav else None,
                "latest_nav_date": fund.latest_nav_date.isoformat() if fund.latest_nav_date else None,
                "added_by": pool.added_by,
                "notes": pool.notes,
                "added_at": pool.added_at.isoformat() if pool.added_at else None,
            })
        return items, total

    async def add_fund_to_pool(
        self,
        db: AsyncSession,
        pool_type: str,
        fund_id: int,
        notes: str | None = None,
        added_by: str = "user",
    ) -> FundPool:
        if pool_type not in VALID_POOL_TYPES:
            raise ValueError(f"Invalid pool_type: {pool_type}")

        # Check fund exists
        fund = await db.get(Fund, fund_id)
        if not fund:
            raise ValueError(f"Fund {fund_id} not found")

        # Check not already in pool
        existing = await db.execute(
            select(FundPool).where(
                FundPool.pool_type == pool_type,
                FundPool.fund_id == fund_id,
            )
        )
        if existing.scalar_one_or_none():
            raise ValueError(f"Fund {fund_id} already in {pool_type} pool")

        pool_entry = FundPool(
            pool_type=pool_type,
            fund_id=fund_id,
            notes=notes,
            added_by=added_by,
        )
        db.add(pool_entry)
        await db.flush()
        await db.refresh(pool_entry)
        return pool_entry

    async def remove_fund_from_pool(
        self,
        db: AsyncSession,
        pool_type: str,
        fund_id: int,
    ) -> bool:
        result = await db.execute(
            delete(FundPool).where(
                FundPool.pool_type == pool_type,
                FundPool.fund_id == fund_id,
            )
        )
        await db.flush()
        return result.rowcount > 0

    async def get_pool_counts(self, db: AsyncSession) -> dict[str, int]:
        """Get fund count for each pool type."""
        result = await db.execute(
            select(FundPool.pool_type, func.count(FundPool.id))
            .group_by(FundPool.pool_type)
        )
        return {row[0]: row[1] for row in result.all()}


pool_service = PoolService()
