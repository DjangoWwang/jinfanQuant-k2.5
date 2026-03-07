"""Pydantic schemas for report generation and attribution analysis."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field


class ReportGenerateRequest(BaseModel):
    product_id: int
    report_type: str = Field("monthly", pattern=r"^(monthly|weekly)$")
    period_start: date
    period_end: date
    benchmark_id: int | None = None


class AsyncReportRequest(BaseModel):
    report_type: str = Field("monthly", pattern=r"^(monthly|weekly)$")
    period_start: date
    period_end: date
    benchmark_id: int | None = None
    format: str = Field("pdf", pattern=r"^(pdf|excel)$")


class AsyncReportResponse(BaseModel):
    task_id: str
    product_id: int
    format: str
    status: str = "queued"


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result: dict | None = None


class AttributionCategoryResponse(BaseModel):
    category: str
    category_name: str
    benchmark_weight: float = 0.0
    actual_weight: float = 0.0
    benchmark_return: float = 0.0
    actual_return: float = 0.0
    allocation_effect: float = 0.0
    selection_effect: float = 0.0
    interaction_effect: float = 0.0
    total_effect: float = 0.0


class AttributionPeriodResponse(BaseModel):
    period_start: date
    period_end: date
    benchmark_total_return: float = 0.0
    actual_total_return: float = 0.0
    excess_return: float = 0.0
    total_allocation: float = 0.0
    total_selection: float = 0.0
    total_interaction: float = 0.0
    categories: list[AttributionCategoryResponse] = Field(default_factory=list)


class AttributionResponse(BaseModel):
    product_id: int
    period_start: date
    period_end: date
    granularity: str = "monthly"
    periods: list[AttributionPeriodResponse] = Field(default_factory=list)
    cumulative_excess: float = 0.0
    cumulative_allocation: float = 0.0
    cumulative_selection: float = 0.0
    cumulative_interaction: float = 0.0
    aggregated_categories: list[AttributionCategoryResponse] = Field(default_factory=list)


class FundContributionItem(BaseModel):
    fund_id: int | None = None
    fund_name: str
    weight: float = 0.0
    return_: float | None = Field(None, alias="return")
    contribution: float | None = None

    model_config = {"populate_by_name": True, "serialize_by_alias": True}


class FundContributionResponse(BaseModel):
    product_id: int
    period_start: date
    period_end: date
    contributions: list[FundContributionItem] = Field(default_factory=list)


class CorrelationPeriod(BaseModel):
    start: str
    end: str


class CorrelationMatrixResponse(BaseModel):
    labels: list[str] = Field(default_factory=list)
    matrix: list[list[float]] = Field(default_factory=list)
    period: CorrelationPeriod
