"""Mobile dashboard API — lightweight endpoints for mobile app."""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.product import Product, ValuationSnapshot

router = APIRouter(prefix="/mobile", tags=["mobile"])


class ProductSummary(BaseModel):
    product_id: int
    product_name: str
    product_type: str = "live"
    unit_nav: float | None = None
    total_nav: float | None = None
    valuation_date: date | None = None
    daily_return_pct: float | None = None


class MobileDashboardResponse(BaseModel):
    date: date
    live_products: list[ProductSummary] = Field(default_factory=list)
    simulation_products: list[ProductSummary] = Field(default_factory=list)


@router.get("/dashboard", response_model=MobileDashboardResponse)
async def mobile_dashboard(
    db: AsyncSession = Depends(get_db),
):
    """Return compact summary of all active products for mobile display."""
    result = await db.execute(
        select(Product).where(Product.is_active == True).order_by(Product.product_name)
    )
    products = list(result.scalars().all())

    live_products = []
    sim_products = []

    for p in products:
        # Get latest two snapshots for daily return calculation
        snap_result = await db.execute(
            select(ValuationSnapshot)
            .where(ValuationSnapshot.product_id == p.id)
            .order_by(ValuationSnapshot.valuation_date.desc())
            .limit(2)
        )
        snaps = list(snap_result.scalars().all())

        unit_nav = None
        total_nav = None
        val_date = None
        daily_return = None

        if snaps:
            latest = snaps[0]
            unit_nav = float(latest.unit_nav) if latest.unit_nav else None
            total_nav = float(latest.total_nav) if latest.total_nav else None
            val_date = latest.valuation_date

            if len(snaps) >= 2 and snaps[1].unit_nav and latest.unit_nav:
                prev_nav = float(snaps[1].unit_nav)
                if prev_nav > 0:
                    daily_return = round(
                        (float(latest.unit_nav) - prev_nav) / prev_nav * 100, 4
                    )

        summary = ProductSummary(
            product_id=p.id,
            product_name=p.product_name,
            product_type=p.product_type or "live",
            unit_nav=unit_nav,
            total_nav=total_nav,
            valuation_date=val_date,
            daily_return_pct=daily_return,
        )

        if p.product_type == "simulation":
            sim_products.append(summary)
        else:
            live_products.append(summary)

    return MobileDashboardResponse(
        date=date.today(),
        live_products=live_products,
        simulation_products=sim_products,
    )
