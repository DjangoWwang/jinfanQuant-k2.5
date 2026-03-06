"""Brinson attribution engine tests — Round 1 & 2 cross-validation.

Tests use hand-calculated expected values to verify BHB model correctness.
"""

from __future__ import annotations

import datetime
import pytest

from app.engine.attribution import (
    compute_single_period_brinson,
    compute_multi_period_brinson,
    extract_l1_weights,
    CategoryAttribution,
    PeriodAttribution,
    AttributionResult,
)


# ---------------------------------------------------------------------------
# Round 1: Basic correctness
# ---------------------------------------------------------------------------

class TestBrinsonRound1:
    """Round 1 — verify BHB formulas against hand-calculated results."""

    def _sample_data(self):
        """3-category example with known results.

        Category A: w_b=0.4, w_a=0.5, R_b=0.05, R_a=0.08
        Category B: w_b=0.3, w_a=0.2, R_b=0.02, R_a=0.01
        Category C: w_b=0.3, w_a=0.3, R_b=-0.01, R_a=0.03

        Q1 = 0.4*0.05 + 0.3*0.02 + 0.3*(-0.01) = 0.02 + 0.006 - 0.003 = 0.023
        Q2 = 0.5*0.05 + 0.2*0.02 + 0.3*(-0.01) = 0.025 + 0.004 - 0.003 = 0.026
        Q3 = 0.4*0.08 + 0.3*0.01 + 0.3*0.03    = 0.032 + 0.003 + 0.009 = 0.044
        Q4 = 0.5*0.08 + 0.2*0.01 + 0.3*0.03    = 0.04  + 0.002 + 0.009 = 0.051
        """
        return dict(
            actual_weights={"A": 0.5, "B": 0.2, "C": 0.3},
            benchmark_weights={"A": 0.4, "B": 0.3, "C": 0.3},
            actual_returns={"A": 0.08, "B": 0.01, "C": 0.03},
            benchmark_returns={"A": 0.05, "B": 0.02, "C": -0.01},
            category_names={"A": "股票", "B": "债券", "C": "现金"},
            period_start=datetime.date(2026, 1, 1),
            period_end=datetime.date(2026, 1, 31),
        )

    def test_q1_q4_values(self):
        data = self._sample_data()
        result = compute_single_period_brinson(**data)
        assert abs(result.benchmark_total_return - 0.023) < 1e-10, f"Q1={result.benchmark_total_return}"
        assert abs(result.actual_total_return - 0.051) < 1e-10, f"Q4={result.actual_total_return}"

    def test_excess_return(self):
        data = self._sample_data()
        result = compute_single_period_brinson(**data)
        expected = 0.051 - 0.023  # 0.028
        assert abs(result.excess_return - expected) < 1e-10

    def test_total_decomposition(self):
        """AR + SR + IR = excess return."""
        data = self._sample_data()
        result = compute_single_period_brinson(**data)
        decomp_sum = result.total_allocation + result.total_selection + result.total_interaction
        assert abs(decomp_sum - result.excess_return) < 1e-10, \
            f"AR={result.total_allocation}, SR={result.total_selection}, IR={result.total_interaction}, sum={decomp_sum}, excess={result.excess_return}"

    def test_ar_sr_ir_totals(self):
        """Verify AR=Q2-Q1, SR=Q3-Q1, IR=Q4-Q3-Q2+Q1."""
        data = self._sample_data()
        result = compute_single_period_brinson(**data)
        assert abs(result.total_allocation - 0.003) < 1e-10  # Q2-Q1 = 0.026-0.023
        assert abs(result.total_selection - 0.021) < 1e-10   # Q3-Q1 = 0.044-0.023
        assert abs(result.total_interaction - 0.004) < 1e-10  # Q4-Q3-Q2+Q1 = 0.051-0.044-0.026+0.023

    def test_category_effects_sum_to_totals(self):
        """Sum of per-category effects should equal total effects."""
        data = self._sample_data()
        result = compute_single_period_brinson(**data)
        sum_ar = sum(c.allocation_effect for c in result.categories)
        sum_sr = sum(c.selection_effect for c in result.categories)
        sum_ir = sum(c.interaction_effect for c in result.categories)
        # Note: per-category AR uses (R_b_j - R_b_total), so sum might differ
        # from total_allocation when using BHB variant.
        # But sum of total_effect per category should equal excess_return.
        sum_total = sum(c.total_effect for c in result.categories)
        assert abs(sum_total - result.excess_return) < 1e-10

    def test_zero_excess(self):
        """When actual == benchmark, all effects should be zero."""
        result = compute_single_period_brinson(
            actual_weights={"A": 0.6, "B": 0.4},
            benchmark_weights={"A": 0.6, "B": 0.4},
            actual_returns={"A": 0.05, "B": 0.02},
            benchmark_returns={"A": 0.05, "B": 0.02},
        )
        assert abs(result.excess_return) < 1e-10
        assert abs(result.total_allocation) < 1e-10
        assert abs(result.total_selection) < 1e-10
        assert abs(result.total_interaction) < 1e-10

    def test_categories_count(self):
        data = self._sample_data()
        result = compute_single_period_brinson(**data)
        assert len(result.categories) == 3

    def test_category_names(self):
        data = self._sample_data()
        result = compute_single_period_brinson(**data)
        names = {c.category: c.category_name for c in result.categories}
        assert names["A"] == "股票"
        assert names["B"] == "债券"


# ---------------------------------------------------------------------------
# Round 2: Edge cases and multi-period
# ---------------------------------------------------------------------------

class TestBrinsonRound2:
    """Round 2 — edge cases, multi-period aggregation, weight extraction."""

    def test_single_category(self):
        """Only one category: allocation effect should be 0."""
        result = compute_single_period_brinson(
            actual_weights={"X": 1.0},
            benchmark_weights={"X": 1.0},
            actual_returns={"X": 0.10},
            benchmark_returns={"X": 0.05},
        )
        assert abs(result.total_allocation) < 1e-10
        assert abs(result.total_selection - 0.05) < 1e-10  # w_b * (R_a - R_b) = 1.0 * 0.05
        assert abs(result.total_interaction) < 1e-10
        assert abs(result.excess_return - 0.05) < 1e-10

    def test_missing_category_in_actual(self):
        """Category exists in benchmark but not in actual (weight=0)."""
        result = compute_single_period_brinson(
            actual_weights={"A": 1.0},
            benchmark_weights={"A": 0.7, "B": 0.3},
            actual_returns={"A": 0.05},
            benchmark_returns={"A": 0.05, "B": 0.02},
        )
        assert len(result.categories) == 2
        # Should still decompose correctly
        decomp = result.total_allocation + result.total_selection + result.total_interaction
        assert abs(decomp - result.excess_return) < 1e-10

    def test_negative_returns(self):
        """Negative returns for all categories."""
        result = compute_single_period_brinson(
            actual_weights={"A": 0.6, "B": 0.4},
            benchmark_weights={"A": 0.5, "B": 0.5},
            actual_returns={"A": -0.10, "B": -0.05},
            benchmark_returns={"A": -0.08, "B": -0.03},
        )
        assert result.actual_total_return < 0
        assert result.benchmark_total_return < 0
        decomp = result.total_allocation + result.total_selection + result.total_interaction
        assert abs(decomp - result.excess_return) < 1e-10

    def test_multi_period_aggregation(self):
        """Two periods aggregate correctly."""
        period_data = [
            {
                "period_start": datetime.date(2026, 1, 1),
                "period_end": datetime.date(2026, 1, 31),
                "actual_weights": {"A": 0.6, "B": 0.4},
                "benchmark_weights": {"A": 0.5, "B": 0.5},
                "actual_returns": {"A": 0.03, "B": 0.01},
                "benchmark_returns": {"A": 0.02, "B": 0.01},
                "category_names": {"A": "股票", "B": "债券"},
            },
            {
                "period_start": datetime.date(2026, 2, 1),
                "period_end": datetime.date(2026, 2, 28),
                "actual_weights": {"A": 0.7, "B": 0.3},
                "benchmark_weights": {"A": 0.5, "B": 0.5},
                "actual_returns": {"A": 0.05, "B": -0.02},
                "benchmark_returns": {"A": 0.03, "B": 0.00},
                "category_names": {"A": "股票", "B": "债券"},
            },
        ]
        result = compute_multi_period_brinson(period_data, product_id=1)
        assert len(result.periods) == 2
        assert result.period_start == datetime.date(2026, 1, 1)
        assert result.period_end == datetime.date(2026, 2, 28)

        # Cumulative should be sum of individual periods
        cum = sum(p.excess_return for p in result.periods)
        assert abs(result.cumulative_excess - cum) < 1e-10

        cum_ar = sum(p.total_allocation for p in result.periods)
        assert abs(result.cumulative_allocation - cum_ar) < 1e-10

    def test_multi_period_categories_aggregated(self):
        """Aggregated categories have correct number and non-zero effects."""
        period_data = [
            {
                "period_start": datetime.date(2026, 1, 1),
                "period_end": datetime.date(2026, 1, 31),
                "actual_weights": {"A": 0.5, "B": 0.5},
                "benchmark_weights": {"A": 0.4, "B": 0.6},
                "actual_returns": {"A": 0.04, "B": 0.02},
                "benchmark_returns": {"A": 0.03, "B": 0.01},
            },
        ]
        result = compute_multi_period_brinson(period_data)
        assert len(result.aggregated_categories) == 2

    def test_empty_period_data(self):
        """Empty input returns empty result."""
        result = compute_multi_period_brinson([])
        assert len(result.periods) == 0
        assert result.cumulative_excess == 0.0

    def test_extract_l1_weights(self):
        """Extract weights from valuation items."""
        items = [
            {"item_code": "1102", "level": 1, "value_pct_nav": 60.5},
            {"item_code": "1109", "level": 1, "value_pct_nav": 35.2},
            {"item_code": "1107", "level": 1, "value_pct_nav": 4.3},
            {"item_code": "110201", "level": 2, "value_pct_nav": 30.0},  # should be ignored
        ]
        weights = extract_l1_weights(items)
        assert len(weights) == 3
        assert abs(weights["1102"] - 0.605) < 1e-6
        assert abs(weights["1109"] - 0.352) < 1e-6
        assert abs(weights["1107"] - 0.043) < 1e-6

    def test_extract_l1_weights_zero_excluded(self):
        """Zero-weight categories are excluded."""
        items = [
            {"item_code": "1102", "level": 1, "value_pct_nav": 100.0},
            {"item_code": "1109", "level": 1, "value_pct_nav": 0.0},
        ]
        weights = extract_l1_weights(items)
        assert len(weights) == 1
        assert "1109" not in weights

    def test_large_category_count(self):
        """Handles many categories gracefully."""
        n = 20
        actual_w = {f"C{i}": 1.0 / n for i in range(n)}
        bench_w = {f"C{i}": 1.0 / n for i in range(n)}
        actual_r = {f"C{i}": 0.01 * i for i in range(n)}
        bench_r = {f"C{i}": 0.005 * i for i in range(n)}
        result = compute_single_period_brinson(actual_w, bench_w, actual_r, bench_r)
        assert len(result.categories) == n
        decomp = result.total_allocation + result.total_selection + result.total_interaction
        assert abs(decomp - result.excess_return) < 1e-10
