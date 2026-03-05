"""Pydantic schemas for portfolio and backtest API requests / responses."""

from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field


# ------------------------------------------------------------------
# Request schemas
# ------------------------------------------------------------------

class PortfolioWeight(BaseModel):
    """A single fund weight within a portfolio."""

    fund_id: int
    weight: float = Field(..., ge=0.0, le=1.0, description="Allocation weight (0-1)")


class PortfolioCreate(BaseModel):
    """Schema for creating a new portfolio (FOF combination)."""

    name: str = Field(..., min_length=1, max_length=200, description="Portfolio name")
    description: str | None = Field(None, max_length=1000)
    weights: list[PortfolioWeight] = Field(
        ...,
        min_length=1,
        description="Fund allocations; weights should sum to 1.0",
    )
    rebalance_frequency: str = Field(
        "monthly",
        description="Rebalancing frequency: daily / weekly / monthly / quarterly",
    )


class BacktestConfigSchema(BaseModel):
    """Configuration for running a portfolio backtest."""

    portfolio_id: int | None = Field(
        None,
        description="Existing portfolio ID. If None, use inline weights.",
    )
    weights: list[PortfolioWeight] | None = Field(
        None,
        description="Inline weights (used when portfolio_id is not set).",
    )
    start_date: date = Field(..., description="Backtest start date")
    end_date: date = Field(..., description="Backtest end date")
    initial_capital: float = Field(
        10_000_000.0, gt=0, description="Starting capital (CNY)"
    )
    rebalance_frequency: str = Field("monthly")
    transaction_cost_bps: float = Field(
        0.0, ge=0.0,
        description="One-way transaction cost in basis points (e.g. 10 = 0.10%)",
    )
    freq_align_method: Literal["downsample", "interpolate"] = Field(
        "downsample",
        description="Frequency alignment: 'downsample' (to weekly) or 'interpolate' (to daily)",
    )
    risk_free_rate: float = Field(
        0.02,
        description="Annual risk-free rate for Sharpe/Sortino (default 2%)",
    )
    history_mode: Literal["intersection", "dynamic_entry", "truncate"] = Field(
        "intersection",
        description=(
            "How to handle funds with different history lengths: "
            "'intersection' (strict common dates), "
            "'dynamic_entry' (funds enter when data available), "
            "'truncate' (exclude funds without enough data)"
        ),
    )
    benchmark_index: str | None = Field(
        None, description="Benchmark index code for comparison"
    )
    management_fee_rate: float = Field(0.0, ge=0.0, description="Annual management fee (%)")
    performance_fee_rate: float = Field(0.0, ge=0.0, description="Performance fee (%)")


# ------------------------------------------------------------------
# Response schemas
# ------------------------------------------------------------------

class PortfolioResponse(BaseModel):
    """Response schema for portfolio metadata."""

    id: int
    name: str
    description: str | None = None
    weights: list[PortfolioWeight] = Field(default_factory=list)
    rebalance_frequency: str = "monthly"
    created_at: str | None = None

    model_config = {"from_attributes": True}


class BacktestMetrics(BaseModel):
    """Key performance metrics from a backtest run."""

    total_return: float = Field(..., description="Cumulative return (%)")
    annualized_return: float = Field(..., description="CAGR (%)")
    max_drawdown: float = Field(..., description="Maximum drawdown (%)")
    sharpe_ratio: float | None = None
    sortino_ratio: float | None = None
    calmar_ratio: float | None = None
    volatility: float | None = Field(None, description="Annualised volatility (%)")
    win_rate: float | None = Field(None, description="Monthly win rate (%)")


class BacktestResultResponse(BaseModel):
    """Full backtest result returned to the frontend."""

    portfolio_id: int | None = None
    config: BacktestConfigSchema
    metrics: BacktestMetrics
    nav_series: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Daily NAV series [{date, nav, benchmark_nav}, ...]",
    )
    drawdown_series: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Drawdown series [{date, drawdown}, ...]",
    )
    monthly_returns: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Monthly return table [{year, month, return_pct}, ...]",
    )
