"""Pydantic schemas for FOF product management."""

from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, Field


# ------------------------------------------------------------------
# Request schemas
# ------------------------------------------------------------------

class ProductCreate(BaseModel):
    """Schema for registering a new FOF product."""

    product_name: str = Field(..., min_length=1, max_length=200)
    product_code: str | None = Field(None, max_length=50, description="Internal product code")
    custodian: str | None = Field(None, max_length=100, description="Custodian bank name")
    administrator: str | None = Field(None, max_length=100, description="Fund administrator")
    inception_date: date | None = None
    total_shares: float | None = Field(None, ge=0, description="Total outstanding shares")
    management_fee_rate: float = Field(0.0, ge=0.0, description="Annual management fee (%)")
    performance_fee_rate: float = Field(0.0, ge=0.0, description="Performance fee (%)")
    high_watermark: float | None = Field(None, description="High-water mark NAV for perf fee")
    notes: str | None = Field(None, max_length=2000)


class ProductUpdate(BaseModel):
    """Schema for partial product updates."""

    product_name: str | None = Field(None, min_length=1, max_length=200)
    custodian: str | None = None
    administrator: str | None = None
    total_shares: float | None = None
    management_fee_rate: float | None = None
    performance_fee_rate: float | None = None
    high_watermark: float | None = None
    notes: str | None = None


# ------------------------------------------------------------------
# Response schemas
# ------------------------------------------------------------------

class ProductResponse(BaseModel):
    """Response schema for product metadata."""

    id: int
    product_name: str
    product_code: str | None = None
    custodian: str | None = None
    administrator: str | None = None
    inception_date: date | None = None
    total_shares: float | None = None
    management_fee_rate: float = 0.0
    performance_fee_rate: float = 0.0
    high_watermark: float | None = None
    notes: str | None = None

    model_config = {"from_attributes": True}


class ValuationHolding(BaseModel):
    """A single parsed holding from a valuation table."""

    item_code: str
    item_name: str
    level: int = Field(..., ge=1, le=4, description="Hierarchy level (1-4)")
    quantity: float | None = None
    unit_cost: float | None = None
    total_cost: float | None = None
    market_price: float | None = None
    market_value: float | None = None
    valuation_appreciation: float | None = None
    proportion: float | None = None


class ValuationUploadResponse(BaseModel):
    """Response returned after uploading and parsing a valuation table."""

    product_id: int | None = None
    file_name: str
    valuation_date: str | None = None
    total_nav: float | None = None
    holdings_count: int = 0
    holdings: list[ValuationHolding] = Field(default_factory=list)
    warnings: list[str] = Field(
        default_factory=list,
        description="Non-fatal warnings encountered during parsing",
    )
