"""Pydantic schemas for fund-related API requests and responses."""

from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field


# ------------------------------------------------------------------
# Request schemas
# ------------------------------------------------------------------

class FundCreate(BaseModel):
    fund_name: str = Field(..., min_length=1, max_length=200)
    filing_number: str | None = Field(None, max_length=50)
    manager_name: str = Field("", max_length=200)
    inception_date: date | None = None
    strategy_type: str | None = Field(None, max_length=50)
    strategy_sub: str | None = Field(None, max_length=50)
    nav_frequency: str = Field("daily", pattern="^(daily|weekly)$")
    data_source: str = Field("fof99", max_length=20)
    is_private: bool = True
    fof99_fund_id: str | None = Field(None, max_length=50)


class FundUpdate(BaseModel):
    fund_name: str | None = Field(None, min_length=1, max_length=200)
    manager_name: str | None = None
    strategy_type: str | None = None
    strategy_sub: str | None = None
    nav_frequency: str | None = Field(None, pattern="^(daily|weekly)$")
    status: str | None = None


class FundListParams(BaseModel):
    strategy_types: list[str] | None = None  # 多选一级策略
    strategy_subs: list[str] | None = None   # 多选二级策略
    nav_frequency: str | None = None
    search: str | None = None
    page: int = Field(1, ge=1)
    page_size: int = Field(50, ge=1, le=200)


# ------------------------------------------------------------------
# Response schemas
# ------------------------------------------------------------------

class FundResponse(BaseModel):
    id: int
    fund_name: str
    filing_number: str | None = None
    manager_name: str | None = None
    inception_date: date | None = None
    strategy_type: str | None = None
    strategy_sub: str | None = None
    latest_nav: float | None = None
    latest_nav_date: date | None = None
    nav_frequency: str = "daily"
    data_source: str = "fof99"
    is_private: bool = True
    status: str = "active"
    nav_status: str | None = "pending"
    data_quality_score: int | None = None
    data_quality_tags: str | None = None
    parent_fund_id: int | None = None
    share_class: str | None = None

    model_config = {"from_attributes": True}


class FundShareClassResponse(BaseModel):
    """份额关联信息"""
    parent_fund_id: int | None = None
    parent_fund_name: str | None = None
    share_classes: list[dict] = Field(default_factory=list)  # [{id, fund_name, share_class, filing_number}]


class FundListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[FundResponse]


class NavRecord(BaseModel):
    nav_date: date
    unit_nav: float
    cumulative_nav: float | None = None
    daily_return: float | None = None

    model_config = {"from_attributes": True}


class NavHistoryResponse(BaseModel):
    fund_id: int
    fund_name: str
    frequency: str | None = None
    records: list[NavRecord] = Field(default_factory=list)
    total_count: int = 0


class MetricsResponse(BaseModel):
    fund_id: int
    fund_name: str
    start_date: date | None = None
    end_date: date | None = None
    total_return: float | None = None
    annualized_return: float | None = None
    max_drawdown: float | None = None
    max_dd_peak: str | None = None
    max_dd_trough: str | None = None
    annualized_volatility: float | None = None
    sharpe_ratio: float | None = None
    sortino_ratio: float | None = None
    calmar_ratio: float | None = None
    monthly_win_rate: float | None = None
    quarterly_win_rate: float | None = None
    new_high_weeks: int | None = None
    return_drawdown_ratio: float | None = None


# ------------------------------------------------------------------
# Pool schemas
# ------------------------------------------------------------------

class PoolFundAdd(BaseModel):
    fund_id: int
    notes: str | None = None


class PoolFundResponse(BaseModel):
    id: int
    pool_type: str
    fund_id: int
    fund_name: str | None = None
    added_by: str = "system"
    notes: str | None = None
    added_at: str | None = None

    model_config = {"from_attributes": True}


# ------------------------------------------------------------------
# Comparison schemas
# ------------------------------------------------------------------

class CompareRequest(BaseModel):
    fund_ids: list[int] = Field(..., min_length=2, max_length=10)
    start_date: date | None = None
    end_date: date | None = None
    preset: str | None = Field(None, description="Interval preset: ytd, 1y, 3y, etc.")
    align_method: str = Field("downsample", pattern="^(downsample|interpolate)$")


class CompareSeriesItem(BaseModel):
    fund_id: int
    fund_name: str
    frequency: str | None = None
    nav_series: list[dict] = Field(default_factory=list, description="[{date, nav}]")


class CompareMetricsItem(BaseModel):
    fund_id: int
    fund_name: str
    total_return: float | None = None
    annualized_return: float | None = None
    max_drawdown: float | None = None
    annualized_volatility: float | None = None
    sharpe_ratio: float | None = None
    sortino_ratio: float | None = None
    calmar_ratio: float | None = None


class CompareResponse(BaseModel):
    start_date: date | None = None
    end_date: date | None = None
    alignment_method: str = "downsample"
    frequency_warning: str | None = None
    series: list[CompareSeriesItem] = Field(default_factory=list)
    metrics: list[CompareMetricsItem] = Field(default_factory=list)
