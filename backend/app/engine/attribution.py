"""Brinson attribution analysis for FOF portfolios.

Implements the Brinson-Hood-Beebower (BHB) model for performance
attribution, decomposing excess return into allocation, selection,
and interaction effects.

Reference: 火富牛Brinson业绩归因分析算法说明2305
"""

from __future__ import annotations

import datetime
from typing import Sequence

import numpy as np
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Result models
# ---------------------------------------------------------------------------

class CategoryAttribution(BaseModel):
    """Brinson attribution for a single asset category."""
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


class PeriodAttribution(BaseModel):
    """Attribution result for a single period (e.g., one month)."""
    period_start: datetime.date
    period_end: datetime.date
    benchmark_total_return: float = 0.0   # Q1
    actual_total_return: float = 0.0      # Q4
    excess_return: float = 0.0            # Q4 - Q1
    total_allocation: float = 0.0         # AR = Q2 - Q1
    total_selection: float = 0.0          # SR = Q3 - Q1
    total_interaction: float = 0.0        # IR = Q4 - Q3 - Q2 + Q1
    categories: list[CategoryAttribution] = Field(default_factory=list)


class AttributionResult(BaseModel):
    """Full attribution analysis result (multi-period)."""
    product_id: int = 0
    period_start: datetime.date = datetime.date(2020, 1, 1)
    period_end: datetime.date = datetime.date(2020, 1, 1)
    granularity: str = "monthly"
    periods: list[PeriodAttribution] = Field(default_factory=list)
    cumulative_excess: float = 0.0
    cumulative_allocation: float = 0.0
    cumulative_selection: float = 0.0
    cumulative_interaction: float = 0.0
    aggregated_categories: list[CategoryAttribution] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def compute_single_period_brinson(
    actual_weights: dict[str, float],
    benchmark_weights: dict[str, float],
    actual_returns: dict[str, float],
    benchmark_returns: dict[str, float],
    category_names: dict[str, str] | None = None,
    period_start: datetime.date | None = None,
    period_end: datetime.date | None = None,
) -> PeriodAttribution:
    """Compute Brinson BHB attribution for a single period.

    Uses the BHB model:
    Q1 = sum(w_b_j * R_b_j)  -- benchmark portfolio return
    Q2 = sum(w_a_j * R_b_j)  -- timing portfolio return
    Q3 = sum(w_b_j * R_a_j)  -- selection portfolio return
    Q4 = sum(w_a_j * R_a_j)  -- actual portfolio return

    Per-category decomposition:
    AR_j = (w_a_j - w_b_j) * (R_b_j - R_b_total)
    SR_j = w_b_j * (R_a_j - R_b_j)
    IR_j = (w_a_j - w_b_j) * (R_a_j - R_b_j)
    """
    if category_names is None:
        category_names = {}

    all_cats = set(actual_weights) | set(benchmark_weights)

    # Q1..Q4
    q1 = sum(benchmark_weights.get(c, 0) * benchmark_returns.get(c, 0) for c in all_cats)
    q2 = sum(actual_weights.get(c, 0) * benchmark_returns.get(c, 0) for c in all_cats)
    q3 = sum(benchmark_weights.get(c, 0) * actual_returns.get(c, 0) for c in all_cats)
    q4 = sum(actual_weights.get(c, 0) * actual_returns.get(c, 0) for c in all_cats)

    total_ar = q2 - q1
    total_sr = q3 - q1
    total_ir = q4 - q3 - q2 + q1

    categories = []
    for cat in sorted(all_cats):
        w_a = actual_weights.get(cat, 0)
        w_b = benchmark_weights.get(cat, 0)
        r_a = actual_returns.get(cat, 0)
        r_b = benchmark_returns.get(cat, 0)

        ar_j = (w_a - w_b) * (r_b - q1)
        sr_j = w_b * (r_a - r_b)
        ir_j = (w_a - w_b) * (r_a - r_b)

        categories.append(CategoryAttribution(
            category=cat,
            category_name=category_names.get(cat, cat),
            benchmark_weight=w_b,
            actual_weight=w_a,
            benchmark_return=r_b,
            actual_return=r_a,
            allocation_effect=ar_j,
            selection_effect=sr_j,
            interaction_effect=ir_j,
            total_effect=ar_j + sr_j + ir_j,
        ))

    return PeriodAttribution(
        period_start=period_start or datetime.date.today(),
        period_end=period_end or datetime.date.today(),
        benchmark_total_return=q1,
        actual_total_return=q4,
        excess_return=q4 - q1,
        total_allocation=total_ar,
        total_selection=total_sr,
        total_interaction=total_ir,
        categories=categories,
    )


def compute_multi_period_brinson(
    period_data: Sequence[dict],
    product_id: int = 0,
    granularity: str = "monthly",
) -> AttributionResult:
    """Compute Brinson attribution over multiple periods and aggregate.

    Each item in period_data should have:
        period_start, period_end,
        actual_weights, benchmark_weights,
        actual_returns, benchmark_returns,
        category_names (optional)

    Uses arithmetic linking (sum) for aggregation.
    """
    if not period_data:
        return AttributionResult(product_id=product_id, granularity=granularity)

    periods: list[PeriodAttribution] = []
    for pd_item in period_data:
        pa = compute_single_period_brinson(
            actual_weights=pd_item["actual_weights"],
            benchmark_weights=pd_item["benchmark_weights"],
            actual_returns=pd_item["actual_returns"],
            benchmark_returns=pd_item["benchmark_returns"],
            category_names=pd_item.get("category_names"),
            period_start=pd_item["period_start"],
            period_end=pd_item["period_end"],
        )
        periods.append(pa)

    # Aggregate across periods
    cum_excess = sum(p.excess_return for p in periods)
    cum_alloc = sum(p.total_allocation for p in periods)
    cum_sel = sum(p.total_selection for p in periods)
    cum_inter = sum(p.total_interaction for p in periods)

    # Aggregate per category
    cat_agg: dict[str, dict] = {}
    for p in periods:
        for c in p.categories:
            if c.category not in cat_agg:
                cat_agg[c.category] = {
                    "category": c.category,
                    "category_name": c.category_name,
                    "benchmark_weight": 0.0,
                    "actual_weight": 0.0,
                    "benchmark_return": 0.0,
                    "actual_return": 0.0,
                    "allocation_effect": 0.0,
                    "selection_effect": 0.0,
                    "interaction_effect": 0.0,
                    "n": 0,
                }
            agg = cat_agg[c.category]
            agg["benchmark_weight"] += c.benchmark_weight
            agg["actual_weight"] += c.actual_weight
            agg["benchmark_return"] += c.benchmark_return
            agg["actual_return"] += c.actual_return
            agg["allocation_effect"] += c.allocation_effect
            agg["selection_effect"] += c.selection_effect
            agg["interaction_effect"] += c.interaction_effect
            agg["n"] += 1

    aggregated = []
    for agg in cat_agg.values():
        n = agg["n"] or 1
        aggregated.append(CategoryAttribution(
            category=agg["category"],
            category_name=agg["category_name"],
            benchmark_weight=agg["benchmark_weight"] / n,
            actual_weight=agg["actual_weight"] / n,
            benchmark_return=agg["benchmark_return"],
            actual_return=agg["actual_return"],
            allocation_effect=agg["allocation_effect"],
            selection_effect=agg["selection_effect"],
            interaction_effect=agg["interaction_effect"],
            total_effect=agg["allocation_effect"] + agg["selection_effect"] + agg["interaction_effect"],
        ))

    overall_start = periods[0].period_start
    overall_end = periods[-1].period_end

    return AttributionResult(
        product_id=product_id,
        period_start=overall_start,
        period_end=overall_end,
        granularity=granularity,
        periods=periods,
        cumulative_excess=cum_excess,
        cumulative_allocation=cum_alloc,
        cumulative_selection=cum_sel,
        cumulative_interaction=cum_inter,
        aggregated_categories=aggregated,
    )


# ---------------------------------------------------------------------------
# Helpers for extracting weights from valuation snapshots
# ---------------------------------------------------------------------------

# Common L1 asset category names
CATEGORY_NAMES = {
    "1101": "货币资金",
    "1102": "股票投资",
    "1103": "债券投资",
    "1104": "资产支持证券投资",
    "1105": "金融衍生品投资",
    "1106": "买入返售金融资产",
    "1107": "银行存款",
    "1108": "结算备付金",
    "1109": "基金投资",
    "1110": "其他资产",
    "1201": "银行存款",
    "1202": "结算备付金",
    "1203": "存出保证金",
}


def extract_l1_weights(items: list[dict]) -> dict[str, float]:
    """Extract L1 (level=1) asset category weights from valuation items.

    items: list of dicts with keys 'item_code', 'level', 'value_pct_nav'
    Returns: {category_code: weight_as_decimal}
    """
    weights = {}
    for item in items:
        if item.get("level") == 1 and item.get("value_pct_nav") is not None:
            code = item["item_code"]
            pct = float(item["value_pct_nav"])
            if pct != 0:
                weights[code] = pct / 100.0  # convert from percentage
    return weights


def get_category_names() -> dict[str, str]:
    """Return the standard L1 category name mapping."""
    return dict(CATEGORY_NAMES)
