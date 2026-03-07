"""Pydantic schemas for FOF product management."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


# ------------------------------------------------------------------
# Request schemas
# ------------------------------------------------------------------

class ProductCreate(BaseModel):
    product_name: str = Field(..., min_length=1, max_length=200)
    product_code: str | None = Field(None, max_length=50)
    custodian: str | None = Field(None, max_length=100)
    administrator: str | None = Field(None, max_length=100)
    product_type: str = Field("live", pattern=r"^(live|simulation)$")
    inception_date: date | None = None
    total_shares: float | None = Field(None, ge=0)
    management_fee_rate: float = Field(0.0, ge=0.0)
    performance_fee_rate: float = Field(0.0, ge=0.0)
    high_watermark: float | None = None
    linked_portfolio_id: int | None = None
    benchmark_id: int | None = None
    notes: str | None = Field(None, max_length=2000)


class ProductUpdate(BaseModel):
    product_name: str | None = Field(None, min_length=1, max_length=200)
    product_code: str | None = None
    custodian: str | None = None
    administrator: str | None = None
    product_type: str | None = Field(None, pattern=r"^(live|simulation)$")
    inception_date: date | None = None
    total_shares: float | None = None
    management_fee_rate: float | None = None
    performance_fee_rate: float | None = None
    high_watermark: float | None = None
    linked_portfolio_id: int | None = None
    benchmark_id: int | None = None
    notes: str | None = None


# ------------------------------------------------------------------
# Response schemas
# ------------------------------------------------------------------

class ProductResponse(BaseModel):
    id: int
    product_name: str
    product_code: str | None = None
    custodian: str | None = None
    administrator: str | None = None
    product_type: str = "live"
    inception_date: date | None = None
    total_shares: float | None = None
    management_fee_rate: float = 0.0
    performance_fee_rate: float = 0.0
    high_watermark: float | None = None
    linked_portfolio_id: int | None = None
    benchmark_id: int | None = None
    notes: str | None = None
    is_active: bool = True
    created_at: datetime | None = None
    # computed fields from latest snapshot
    latest_nav: float | None = None
    latest_total_nav: float | None = None
    latest_valuation_date: date | None = None
    snapshot_count: int = 0

    model_config = {"from_attributes": True}


class ProductListResponse(BaseModel):
    items: list[ProductResponse]
    total: int


# ------------------------------------------------------------------
# Valuation schemas
# ------------------------------------------------------------------

class ValuationHolding(BaseModel):
    item_code: str
    item_name: str
    level: int = Field(..., ge=1, le=4)
    parent_code: str | None = None
    quantity: float | None = None
    unit_cost: float | None = None
    cost_amount: float | None = None
    cost_pct_nav: float | None = None
    market_price: float | None = None
    market_value: float | None = None
    value_pct_nav: float | None = None
    value_diff: float | None = None
    linked_fund_id: int | None = None
    linked_fund_name: str | None = None

    model_config = {"from_attributes": True}


class SubFundAllocation(BaseModel):
    filing_number: str
    fund_name: str
    quantity: float | None = None
    unit_cost: float | None = None
    cost: float | None = None
    cost_weight_pct: float | None = None
    market_price: float | None = None
    market_value: float | None = None
    weight_pct: float | None = None
    appreciation: float | None = None
    linked_fund_id: int | None = None


class ValuationSnapshotResponse(BaseModel):
    id: int
    product_id: int
    valuation_date: date
    unit_nav: float | None = None
    cumulative_nav: float | None = None
    total_nav: float | None = None
    total_shares: float | None = None
    source_file: str | None = None
    imported_at: datetime | None = None
    items: list[ValuationHolding] = Field(default_factory=list)
    sub_fund_allocations: list[SubFundAllocation] = Field(default_factory=list)

    model_config = {"from_attributes": True}


class ValuationUploadResponse(BaseModel):
    snapshot_id: int | None = None
    product_id: int
    file_name: str
    valuation_date: str | None = None
    unit_nav: float | None = None
    total_nav: float | None = None
    holdings_count: int = 0
    sub_funds_count: int = 0
    sub_funds_linked: int = 0
    warnings: list[str] = Field(default_factory=list)


class NavSeriesPoint(BaseModel):
    date: date
    unit_nav: float | None = None
    total_nav: float | None = None


class NavCalcPoint(BaseModel):
    date: date
    unit_nav: float | None = None
    cumulative_nav: float | None = None
    total_nav: float | None = None
    total_shares: float | None = None
    fund_assets: float | None = None
    non_fund_assets: float | None = None
    source: str = "calculated"


class ProductNavResponse(BaseModel):
    product_id: int
    product_name: str
    nav_series: list[NavSeriesPoint]


class ProductNavCalcResponse(BaseModel):
    product_id: int
    product_name: str
    nav_series: list[NavCalcPoint]


class NavCalcResultResponse(BaseModel):
    product_id: int
    total_days: int = 0
    calculated_days: int = 0
    snapshot_days: int = 0
    skipped_days: int = 0
    date_range_start: str | None = None
    date_range_end: str | None = None
    warnings: list[str] = Field(default_factory=list)


class NavStatsResponse(BaseModel):
    product_id: int
    total_days: int = 0
    calculated_days: int = 0
    valuation_days: int = 0
    has_data: bool = False
    date_range: dict[str, str | None] = Field(default_factory=dict)
    latest_nav: float | None = None
    latest_date: str | None = None
    latest_total_nav: float | None = None


class ValuationListResponse(BaseModel):
    items: list[ValuationSnapshotResponse]
    total: int


# ------------------------------------------------------------------
# Strategy Attribution schemas
# ------------------------------------------------------------------

class StrategyFundDetail(BaseModel):
    fund_name: str
    fund_id: int | None = None
    strategy_type: str = ""
    strategy_sub: str = ""
    market_value: float = 0.0
    weight_pct: float = 0.0


class StrategyWeight(BaseModel):
    weight: float = 0.0
    weight_pct_nav: float = 0.0
    market_value: float = 0.0
    fund_count: int = 0
    funds: list[StrategyFundDetail] = Field(default_factory=list)


class SnapshotStrategyData(BaseModel):
    valuation_date: str
    unit_nav: float | None = None
    total_nav: float | None = None
    strategy_weights: dict[str, StrategyWeight] = Field(default_factory=dict)


class ReturnContribution(BaseModel):
    avg_weight: float = 0.0
    strategy_return: float | None = None
    contribution: float | None = None


class PeriodContribution(BaseModel):
    period_start: str
    period_end: str
    total_return: float = 0.0
    contributions: dict[str, ReturnContribution] = Field(default_factory=dict)


class StrategyAttributionResponse(BaseModel):
    snapshots: list[SnapshotStrategyData] = Field(default_factory=list)
    strategy_types: list[str] = Field(default_factory=list)
    return_contribution: list[PeriodContribution] = Field(default_factory=list)


# ------------------------------------------------------------------
# Factor Exposure schemas
# ------------------------------------------------------------------

class FactorBeta(BaseModel):
    beta: float | None = None
    t_stat: float | None = None
    se: float | None = None


class StaticRegression(BaseModel):
    alpha: float | None = None
    alpha_annualized: float | None = None
    alpha_t_stat: float | None = None
    r_squared: float | None = None
    betas: dict[str, FactorBeta] = Field(default_factory=dict)


class RollingPoint(BaseModel):
    date: str
    r_squared: float | None = None
    betas: dict[str, FactorBeta] = Field(default_factory=dict)


class FactorExposureResponse(BaseModel):
    product_id: int
    data_points: int = 0
    date_range: dict[str, str] = Field(default_factory=dict)
    factors: dict[str, str] = Field(default_factory=dict)
    static: StaticRegression | None = None
    rolling: list[RollingPoint] = Field(default_factory=list)
    rolling_window: int = 60
    error: str | None = None
