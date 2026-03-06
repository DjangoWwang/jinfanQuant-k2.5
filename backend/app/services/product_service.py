"""Business logic layer for FOF product management."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.importer.valuation_parser import ValuationParser
from app.models.fund import Fund
from app.models.product import Product, ValuationSnapshot, ValuationItem
from app.schemas.product import (
    ProductCreate,
    ProductResponse,
    ProductUpdate,
    ValuationUploadResponse,
    ValuationSnapshotResponse,
    ValuationHolding,
    SubFundAllocation,
    NavSeriesPoint,
)

logger = logging.getLogger(__name__)

# Sub-fund item code prefix
_SUBFUND_PREFIX = "11090601"


class ProductService:
    """Service for FOF product CRUD and valuation table processing."""

    def __init__(self) -> None:
        self._valuation_parser = ValuationParser()

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def create_product(
        self, db: AsyncSession, payload: ProductCreate
    ) -> Product:
        product = Product(**payload.model_dump())
        db.add(product)
        await db.flush()
        await db.refresh(product)
        return product

    async def get_product(
        self, db: AsyncSession, product_id: int
    ) -> Product | None:
        result = await db.execute(select(Product).where(Product.id == product_id))
        return result.scalar_one_or_none()

    async def list_products(
        self,
        db: AsyncSession,
        product_type: str | None = None,
        skip: int = 0,
        limit: int = 50,
    ) -> tuple[list[Product], int]:
        query = select(Product).where(Product.is_active == True)
        if product_type:
            query = query.where(Product.product_type == product_type)

        count_q = select(func.count()).select_from(query.subquery())
        total = (await db.execute(count_q)).scalar() or 0

        query = query.order_by(Product.created_at.desc()).offset(skip).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all()), total

    async def update_product(
        self, db: AsyncSession, product_id: int, payload: ProductUpdate
    ) -> Product | None:
        product = await self.get_product(db, product_id)
        if not product:
            return None
        for field, value in payload.model_dump(exclude_unset=True).items():
            setattr(product, field, value)
        await db.flush()
        await db.refresh(product)
        return product

    async def delete_product(self, db: AsyncSession, product_id: int) -> bool:
        product = await self.get_product(db, product_id)
        if not product:
            return False
        # Cascade delete snapshots & items via FK
        await db.execute(
            delete(ValuationSnapshot).where(ValuationSnapshot.product_id == product_id)
        )
        await db.delete(product)
        await db.flush()
        return True

    # ------------------------------------------------------------------
    # Product response enrichment
    # ------------------------------------------------------------------

    async def to_response(self, db: AsyncSession, product: Product) -> ProductResponse:
        """Convert a Product ORM to a response with latest snapshot info."""
        latest = await self._get_latest_snapshot(db, product.id)
        snap_count = await self._count_snapshots(db, product.id)

        return ProductResponse(
            id=product.id,
            product_name=product.product_name,
            product_code=product.product_code,
            custodian=product.custodian,
            administrator=product.administrator,
            product_type=product.product_type or "live",
            inception_date=product.inception_date,
            total_shares=float(product.total_shares) if product.total_shares else None,
            management_fee_rate=float(product.management_fee_rate or 0),
            performance_fee_rate=float(product.performance_fee_rate or 0),
            high_watermark=float(product.high_watermark) if product.high_watermark else None,
            linked_portfolio_id=product.linked_portfolio_id,
            notes=product.notes,
            is_active=product.is_active,
            created_at=product.created_at,
            latest_nav=float(latest.unit_nav) if latest and latest.unit_nav else None,
            latest_total_nav=float(latest.total_nav) if latest and latest.total_nav else None,
            latest_valuation_date=latest.valuation_date if latest else None,
            snapshot_count=snap_count,
        )

    async def _get_latest_snapshot(
        self, db: AsyncSession, product_id: int
    ) -> ValuationSnapshot | None:
        result = await db.execute(
            select(ValuationSnapshot)
            .where(ValuationSnapshot.product_id == product_id)
            .order_by(ValuationSnapshot.valuation_date.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def _count_snapshots(self, db: AsyncSession, product_id: int) -> int:
        result = await db.execute(
            select(func.count())
            .where(ValuationSnapshot.product_id == product_id)
        )
        return result.scalar() or 0

    # ------------------------------------------------------------------
    # Valuation table operations
    # ------------------------------------------------------------------

    async def process_valuation_upload(
        self,
        db: AsyncSession,
        product_id: int,
        file_path: str | Path,
    ) -> ValuationUploadResponse:
        """Parse an uploaded valuation table and persist snapshot + items."""
        parsed = self._valuation_parser.parse(file_path)

        warnings: list[str] = []
        if parsed["total_nav"] is None:
            warnings.append("无法从估值表中识别资产净值")
        if parsed["valuation_date"] is None:
            warnings.append("无法识别估值日期，请手动设置")

        # Create or update snapshot
        val_date = date.fromisoformat(parsed["valuation_date"]) if parsed["valuation_date"] else date.today()

        existing = await db.execute(
            select(ValuationSnapshot).where(
                ValuationSnapshot.product_id == product_id,
                ValuationSnapshot.valuation_date == val_date,
            )
        )
        snapshot = existing.scalar_one_or_none()

        if snapshot:
            # Update existing: remove old items
            await db.execute(
                delete(ValuationItem).where(ValuationItem.snapshot_id == snapshot.id)
            )
            snapshot.total_nav = parsed["total_nav"]
            snapshot.unit_nav = parsed["unit_nav"]
            snapshot.source_file = parsed["file_name"]
            warnings.append(f"已覆盖 {val_date} 的旧估值快照")
        else:
            snapshot = ValuationSnapshot(
                product_id=product_id,
                valuation_date=val_date,
                total_nav=parsed["total_nav"],
                unit_nav=parsed["unit_nav"],
                source_file=parsed["file_name"],
            )
            db.add(snapshot)
            await db.flush()

        # Insert holdings as ValuationItem rows
        sub_funds_linked = 0
        for h in parsed["holdings"]:
            item_code = h["item_code"]
            level = h.get("level", 1)

            # Determine parent_code from item_code hierarchy
            parent_code = None
            if level == 2:
                parent_code = item_code[:4]
            elif level == 3:
                parent_code = item_code[:6]
            elif level == 4:
                parent_code = item_code[:8]

            # Auto-link sub-fund to funds table by filing number
            linked_fund_id = None
            if item_code.startswith(_SUBFUND_PREFIX) and len(item_code) > 8:
                filing_suffix = item_code[8:]
                linked_fund_id = await self._find_fund_by_filing(db, filing_suffix)
                if linked_fund_id:
                    sub_funds_linked += 1

            item = ValuationItem(
                snapshot_id=snapshot.id,
                item_code=item_code,
                item_name=h.get("item_name", ""),
                level=level,
                parent_code=parent_code,
                quantity=h.get("quantity"),
                unit_cost=h.get("unit_cost"),
                cost_amount=h.get("total_cost"),
                cost_pct_nav=h.get("cost_pct"),
                market_price=h.get("market_price"),
                market_value=h.get("market_value"),
                value_pct_nav=h.get("mv_pct"),
                value_diff=h.get("valuation_appreciation"),
                linked_fund_id=linked_fund_id,
            )
            db.add(item)

        await db.flush()

        return ValuationUploadResponse(
            snapshot_id=snapshot.id,
            product_id=product_id,
            file_name=parsed["file_name"],
            valuation_date=parsed["valuation_date"],
            unit_nav=parsed["unit_nav"],
            total_nav=parsed["total_nav"],
            holdings_count=len(parsed["holdings"]),
            sub_funds_count=len(parsed.get("sub_fund_allocations", [])),
            sub_funds_linked=sub_funds_linked,
            warnings=warnings,
        )

    async def _find_fund_by_filing(
        self, db: AsyncSession, filing_suffix: str
    ) -> int | None:
        """Find a fund by filing number suffix (case-insensitive)."""
        result = await db.execute(
            select(Fund.id).where(
                func.upper(Fund.filing_number).like(f"%{filing_suffix.upper()}")
            ).limit(1)
        )
        row = result.scalar_one_or_none()
        return row

    # ------------------------------------------------------------------
    # Valuation snapshot queries
    # ------------------------------------------------------------------

    async def get_valuation_snapshot(
        self, db: AsyncSession, snapshot_id: int
    ) -> ValuationSnapshotResponse | None:
        result = await db.execute(
            select(ValuationSnapshot).where(ValuationSnapshot.id == snapshot_id)
        )
        snapshot = result.scalar_one_or_none()
        if not snapshot:
            return None
        return await self._snapshot_to_response(db, snapshot)

    async def list_valuations(
        self,
        db: AsyncSession,
        product_id: int,
        skip: int = 0,
        limit: int = 50,
    ) -> tuple[list[ValuationSnapshotResponse], int]:
        count_q = select(func.count()).where(
            ValuationSnapshot.product_id == product_id
        )
        total = (await db.execute(count_q)).scalar() or 0

        result = await db.execute(
            select(ValuationSnapshot)
            .where(ValuationSnapshot.product_id == product_id)
            .order_by(ValuationSnapshot.valuation_date.desc())
            .offset(skip)
            .limit(limit)
        )
        snapshots = list(result.scalars().all())
        responses = []
        for s in snapshots:
            responses.append(await self._snapshot_to_response(db, s))
        return responses, total

    async def get_latest_valuation(
        self, db: AsyncSession, product_id: int
    ) -> ValuationSnapshotResponse | None:
        snapshot = await self._get_latest_snapshot(db, product_id)
        if not snapshot:
            return None
        return await self._snapshot_to_response(db, snapshot)

    async def _snapshot_to_response(
        self, db: AsyncSession, snapshot: ValuationSnapshot
    ) -> ValuationSnapshotResponse:
        """Convert a snapshot ORM with items to a response."""
        # Fetch items
        result = await db.execute(
            select(ValuationItem)
            .where(ValuationItem.snapshot_id == snapshot.id)
            .order_by(ValuationItem.item_code)
        )
        items = list(result.scalars().all())

        holdings = []
        sub_fund_allocations = []
        for item in items:
            # Get linked fund name if exists
            linked_fund_name = None
            if item.linked_fund_id:
                fund_result = await db.execute(
                    select(Fund.fund_name).where(Fund.id == item.linked_fund_id)
                )
                linked_fund_name = fund_result.scalar_one_or_none()

            holdings.append(ValuationHolding(
                item_code=item.item_code or "",
                item_name=item.item_name or "",
                level=item.level or 1,
                parent_code=item.parent_code,
                quantity=float(item.quantity) if item.quantity else None,
                unit_cost=float(item.unit_cost) if item.unit_cost else None,
                cost_amount=float(item.cost_amount) if item.cost_amount else None,
                cost_pct_nav=float(item.cost_pct_nav) if item.cost_pct_nav else None,
                market_price=float(item.market_price) if item.market_price else None,
                market_value=float(item.market_value) if item.market_value else None,
                value_pct_nav=float(item.value_pct_nav) if item.value_pct_nav else None,
                value_diff=float(item.value_diff) if item.value_diff else None,
                linked_fund_id=item.linked_fund_id,
                linked_fund_name=linked_fund_name,
            ))

            # Sub-fund allocations
            code = item.item_code or ""
            if code.startswith(_SUBFUND_PREFIX) and len(code) > 8:
                filing_number = code[8:]
                sub_fund_allocations.append(SubFundAllocation(
                    filing_number=filing_number,
                    fund_name=item.item_name or "",
                    quantity=float(item.quantity) if item.quantity else None,
                    unit_cost=float(item.unit_cost) if item.unit_cost else None,
                    cost=float(item.cost_amount) if item.cost_amount else None,
                    cost_weight_pct=float(item.cost_pct_nav) if item.cost_pct_nav else None,
                    market_price=float(item.market_price) if item.market_price else None,
                    market_value=float(item.market_value) if item.market_value else None,
                    weight_pct=float(item.value_pct_nav) if item.value_pct_nav else None,
                    appreciation=float(item.value_diff) if item.value_diff else None,
                    linked_fund_id=item.linked_fund_id,
                ))

        return ValuationSnapshotResponse(
            id=snapshot.id,
            product_id=snapshot.product_id,
            valuation_date=snapshot.valuation_date,
            unit_nav=float(snapshot.unit_nav) if snapshot.unit_nav else None,
            cumulative_nav=float(snapshot.cumulative_nav) if snapshot.cumulative_nav else None,
            total_nav=float(snapshot.total_nav) if snapshot.total_nav else None,
            total_shares=float(snapshot.total_shares) if snapshot.total_shares else None,
            source_file=snapshot.source_file,
            imported_at=snapshot.imported_at,
            items=holdings,
            sub_fund_allocations=sub_fund_allocations,
        )

    # ------------------------------------------------------------------
    # NAV series from valuation snapshots
    # ------------------------------------------------------------------

    async def get_nav_series(
        self,
        db: AsyncSession,
        product_id: int,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[NavSeriesPoint]:
        """Get product NAV series from valuation snapshots."""
        query = (
            select(
                ValuationSnapshot.valuation_date,
                ValuationSnapshot.unit_nav,
                ValuationSnapshot.total_nav,
            )
            .where(ValuationSnapshot.product_id == product_id)
            .order_by(ValuationSnapshot.valuation_date)
        )
        if start_date:
            query = query.where(ValuationSnapshot.valuation_date >= start_date)
        if end_date:
            query = query.where(ValuationSnapshot.valuation_date <= end_date)

        result = await db.execute(query)
        rows = result.all()
        return [
            NavSeriesPoint(
                date=row.valuation_date,
                unit_nav=float(row.unit_nav) if row.unit_nav else None,
                total_nav=float(row.total_nav) if row.total_nav else None,
            )
            for row in rows
        ]


product_service = ProductService()
