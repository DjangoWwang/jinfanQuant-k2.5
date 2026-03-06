"""Chart renderer + PDF builder tests — Round 1 & 2 cross-validation.

Round 1: Verify chart functions produce valid PNG bytes.
Round 2: Verify PDF builder produces valid PDF bytes with Chinese content.
"""

from __future__ import annotations

import datetime
import pytest
import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Round 1: Chart renderer validation
# ---------------------------------------------------------------------------

class TestChartRendererRound1:
    """Round 1 — each chart function returns valid PNG bytes."""

    def test_nav_chart(self):
        from app.services.chart_renderer import render_nav_chart
        dates = pd.date_range("2025-01-01", periods=60, freq="W")
        nav = pd.Series(np.cumprod(1 + np.random.normal(0.001, 0.02, 60)), index=dates)
        bench = pd.Series(np.cumprod(1 + np.random.normal(0.0005, 0.015, 60)), index=dates)
        png = render_nav_chart(nav, bench, product_name="测试产品", benchmark_name="沪深300")
        assert isinstance(png, bytes)
        assert len(png) > 1000
        assert png[:4] == b"\x89PNG"

    def test_nav_chart_without_benchmark(self):
        from app.services.chart_renderer import render_nav_chart
        dates = pd.date_range("2025-01-01", periods=30, freq="W")
        nav = pd.Series(np.cumprod(1 + np.random.normal(0.001, 0.02, 30)), index=dates)
        png = render_nav_chart(nav, None)
        assert png[:4] == b"\x89PNG"

    def test_drawdown_chart(self):
        from app.services.chart_renderer import render_drawdown_chart
        dates = pd.date_range("2025-01-01", periods=60, freq="W")
        nav = pd.Series(np.cumprod(1 + np.random.normal(0.001, 0.02, 60)), index=dates)
        png = render_drawdown_chart(nav)
        assert isinstance(png, bytes)
        assert png[:4] == b"\x89PNG"

    def test_attribution_bar(self):
        from app.services.chart_renderer import render_attribution_bar
        cats = [
            {"category_name": "股票投资", "allocation_effect": 0.005, "selection_effect": 0.01, "interaction_effect": -0.002},
            {"category_name": "债券投资", "allocation_effect": -0.001, "selection_effect": 0.003, "interaction_effect": 0.001},
            {"category_name": "基金投资", "allocation_effect": 0.003, "selection_effect": 0.008, "interaction_effect": 0.002},
        ]
        png = render_attribution_bar(cats)
        assert png[:4] == b"\x89PNG"

    def test_pie_chart(self):
        from app.services.chart_renderer import render_pie_chart
        data = [
            {"name": "股票", "value": 60.5},
            {"name": "债券", "value": 20.3},
            {"name": "基金", "value": 15.0},
            {"name": "现金", "value": 4.2},
        ]
        png = render_pie_chart(data)
        assert png[:4] == b"\x89PNG"

    def test_pie_chart_empty(self):
        from app.services.chart_renderer import render_pie_chart
        png = render_pie_chart([])
        assert png[:4] == b"\x89PNG"

    def test_weight_comparison(self):
        from app.services.chart_renderer import render_weight_comparison
        prev = {"1102": 0.6, "1109": 0.3, "1107": 0.1}
        curr = {"1102": 0.55, "1109": 0.35, "1107": 0.1}
        png = render_weight_comparison(prev, curr, {"1102": "股票", "1109": "基金", "1107": "现金"})
        assert png[:4] == b"\x89PNG"

    def test_monthly_heatmap(self):
        from app.services.chart_renderer import render_monthly_heatmap
        data = []
        for y in [2025, 2026]:
            for m in range(1, 13):
                data.append({"year": y, "month": m, "return_pct": np.random.uniform(-0.05, 0.08)})
        png = render_monthly_heatmap(data)
        assert png[:4] == b"\x89PNG"

    def test_monthly_heatmap_empty(self):
        from app.services.chart_renderer import render_monthly_heatmap
        png = render_monthly_heatmap([])
        assert png[:4] == b"\x89PNG"


# ---------------------------------------------------------------------------
# Round 2: PDF builder validation
# ---------------------------------------------------------------------------

class TestPDFBuilderRound2:
    """Round 2 — PDF builder produces valid PDF with all sections."""

    def test_empty_pdf(self):
        from app.services.pdf_builder import PDFReportBuilder
        builder = PDFReportBuilder(product_name="空报告", period="2026年3月")
        builder.add_header()
        pdf = builder.build()
        assert isinstance(pdf, bytes)
        assert pdf[:5] == b"%PDF-"
        assert len(pdf) > 100

    def test_pdf_with_metrics(self):
        from app.services.pdf_builder import PDFReportBuilder
        builder = PDFReportBuilder(product_name="指标测试", period="2026年2月")
        builder.add_header(product_code="TEST01", benchmark_name="沪深300", custodian="招商证券")
        builder.add_metrics_cards({
            "annualized_return": 0.12,
            "max_drawdown": -0.08,
            "sharpe_ratio": 1.35,
            "calmar_ratio": 1.5,
            "sortino_ratio": 2.1,
            "annualized_volatility": 0.09,
        })
        pdf = builder.build()
        assert pdf[:5] == b"%PDF-"

    def test_pdf_with_chart(self):
        from app.services.pdf_builder import PDFReportBuilder
        from app.services.chart_renderer import render_nav_chart
        dates = pd.date_range("2025-01-01", periods=30, freq="W")
        nav = pd.Series(np.cumprod(1 + np.random.normal(0.001, 0.02, 30)), index=dates)
        chart_png = render_nav_chart(nav)

        builder = PDFReportBuilder(product_name="图表测试", period="2026年2月")
        builder.add_header()
        builder.add_nav_chart(chart_png)
        pdf = builder.build()
        assert pdf[:5] == b"%PDF-"
        assert len(pdf) > 5000

    def test_pdf_with_attribution_table(self):
        from app.services.pdf_builder import PDFReportBuilder
        builder = PDFReportBuilder(product_name="归因测试", period="2026年1月")
        builder.add_header()
        builder.add_attribution_table([
            {"category_name": "股票", "benchmark_weight": 0.5, "actual_weight": 0.6,
             "benchmark_return": 0.03, "actual_return": 0.05,
             "allocation_effect": 0.003, "selection_effect": 0.01,
             "interaction_effect": 0.002, "total_effect": 0.015},
            {"category_name": "债券", "benchmark_weight": 0.3, "actual_weight": 0.2,
             "benchmark_return": 0.01, "actual_return": 0.005,
             "allocation_effect": -0.001, "selection_effect": -0.002,
             "interaction_effect": 0.001, "total_effect": -0.002},
        ])
        pdf = builder.build()
        assert pdf[:5] == b"%PDF-"

    def test_pdf_with_interval_table(self):
        from app.services.pdf_builder import PDFReportBuilder
        builder = PDFReportBuilder(product_name="区间测试", period="2026年1月")
        builder.add_header()
        builder.add_interval_metrics_table({
            "1m": {"total_return": 0.02, "annualized_return": 0.24, "max_drawdown": -0.01, "sharpe_ratio": 3.2},
            "3m": {"total_return": 0.05, "annualized_return": 0.20, "max_drawdown": -0.03, "sharpe_ratio": 2.1},
            "1y": None,
        })
        pdf = builder.build()
        assert pdf[:5] == b"%PDF-"

    def test_full_monthly_report(self):
        """Full 3-page monthly report with all sections."""
        from app.services.pdf_builder import PDFReportBuilder
        from app.services.chart_renderer import (
            render_nav_chart, render_attribution_bar,
            render_pie_chart, render_weight_comparison, render_monthly_heatmap,
        )

        dates = pd.date_range("2025-01-01", periods=50, freq="W")
        nav = pd.Series(np.cumprod(1 + np.random.normal(0.001, 0.02, 50)), index=dates)
        bench = pd.Series(np.cumprod(1 + np.random.normal(0.0005, 0.015, 50)), index=dates)

        cats = [
            {"category_name": "股票", "allocation_effect": 0.005, "selection_effect": 0.01, "interaction_effect": -0.002},
            {"category_name": "基金", "allocation_effect": 0.003, "selection_effect": 0.008, "interaction_effect": 0.002},
        ]
        pie_data = [{"name": "股票", "value": 60}, {"name": "基金", "value": 35}, {"name": "现金", "value": 5}]
        monthly = [{"year": 2025, "month": m, "return_pct": np.random.uniform(-0.03, 0.05)} for m in range(1, 13)]

        builder = PDFReportBuilder(product_name="完整月报测试", period="2026年2月", report_type="monthly")

        # Page 1
        builder.add_header(product_code="FOF001", benchmark_name="沪深300")
        builder.add_nav_chart(render_nav_chart(nav, bench))
        builder.add_metrics_cards({
            "annualized_return": 0.12, "max_drawdown": -0.08,
            "sharpe_ratio": 1.35, "calmar_ratio": 1.5,
            "sortino_ratio": 2.1, "annualized_volatility": 0.09,
        })
        builder.add_attribution_bar_chart(render_attribution_bar(cats))
        builder.add_monthly_heatmap(render_monthly_heatmap(monthly))

        # Page 2
        builder.add_page_break()
        builder.add_allocation_pie(render_pie_chart(pie_data))
        builder.add_weight_comparison(render_weight_comparison(
            {"1102": 0.6, "1109": 0.3}, {"1102": 0.55, "1109": 0.35},
            {"1102": "股票", "1109": "基金"},
        ))
        builder.add_interval_metrics_table({
            "1m": {"total_return": 0.02, "annualized_return": 0.24, "max_drawdown": -0.01, "sharpe_ratio": 3.2},
            "3m": {"total_return": 0.05, "annualized_return": 0.20, "max_drawdown": -0.03, "sharpe_ratio": 2.1},
        })

        # Page 3
        builder.add_page_break()
        builder.add_attribution_table([
            {"category_name": "股票", "benchmark_weight": 0.5, "actual_weight": 0.6,
             "benchmark_return": 0.03, "actual_return": 0.05,
             "allocation_effect": 0.003, "selection_effect": 0.01,
             "interaction_effect": 0.002, "total_effect": 0.015},
        ])

        pdf = builder.build()
        assert pdf[:5] == b"%PDF-"
        assert len(pdf) > 20000  # Full report should be substantial
