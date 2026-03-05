"""Pydantic schemas for fund-related API requests and responses."""

from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field


# ------------------------------------------------------------------
# Request schemas
# ------------------------------------------------------------------

class FundCreate(BaseModel):
    """Schema for creating or registering a new fund."""

    fund_name: str = Field(..., min_length=1, max_length=200, description="Fund display name")
    fund_code: str | None = Field(None, max_length=50, description="External fund code (e.g. FOF99 ID)")
    manager_name: str | None = Field(None, max_length=100, description="Fund manager name")
    strategy_type: str | None = Field(
        None,
        max_length=50,
        description="Strategy category (stock_long, cta, macro, ...)",
    )
    inception_date: date | None = Field(None, description="Fund inception date")
    benchmark_index: str | None = Field(None, max_length=50, description="Benchmark index code")
    data_source: str = Field(
        "manual",
        description="How NAV data is obtained: 'manual', 'crawler', 'api'",
    )
    notes: str | None = Field(None, max_length=1000)


class FundUpdate(BaseModel):
    """Schema for partial fund updates."""

    fund_name: str | None = Field(None, min_length=1, max_length=200)
    manager_name: str | None = None
    strategy_type: str | None = None
    benchmark_index: str | None = None
    data_source: str | None = None
    notes: str | None = None


# ------------------------------------------------------------------
# Response schemas
# ------------------------------------------------------------------

class FundResponse(BaseModel):
    """Schema returned when reading fund metadata."""

    id: int
    fund_name: str
    fund_code: str | None = None
    manager_name: str | None = None
    strategy_type: str | None = None
    inception_date: date | None = None
    benchmark_index: str | None = None
    data_source: str = "manual"
    notes: str | None = None

    model_config = {"from_attributes": True}


class NavRecord(BaseModel):
    """A single NAV data point."""

    nav_date: date
    unit_nav: float
    cumulative_nav: float | None = None
    change_pct: float | None = Field(None, description="Period return (%)")


class NavHistoryResponse(BaseModel):
    """Response wrapper for a fund's NAV history."""

    fund_id: int
    fund_name: str
    frequency: str | None = Field(None, description="daily / weekly / monthly / irregular")
    records: list[NavRecord] = Field(default_factory=list)
    total_count: int = 0
