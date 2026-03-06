"""Report generation service orchestrating data, attribution, charts, and PDF."""

from __future__ import annotations

import calendar
import logging
from datetime import date, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.attribution import (
    AttributionResult,
    compute_single_period_brinson,
    compute_multi_period_brinson,
    extract_l1_weights,
    get_category_names,
)
from app.engine.metrics import (
    calc_all_metrics,
    calc_interval_metrics,
    calc_return,
    normalize_nav,
)
from app.models.benchmark import (
    CompositeBenchmark,
    CompositeBenchmarkItem,
    IndexNav,
)
from app.models.product import Product, ValuationSnapshot, ValuationItem
from app.services.chart_renderer import (
    render_attribution_bar,
    render_monthly_heatmap,
    render_nav_chart,
    render_pie_chart,
    render_weight_comparison,
)
from app.services.pdf_builder import PDFReportBuilder

logger = logging.getLogger(__name__)


class ReportService:
    """Orchestrates report data gathering, computation, and PDF generation."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def generate_report(
        self,
        db: AsyncSession,
        product_id: int,
        report_type: str,
        period_start: date,
        period_end: date,
        benchmark_id: int | None = None,
    ) -> bytes:
        """Generate a full PDF report and return its bytes."""
        product = await self._load_product(db, product_id)

        # Build NAV series
        product_nav = await self._build_product_nav(db, product_id)

        # Build benchmark NAV
        bm_id = benchmark_id or product.benchmark_id
        benchmark_nav, benchmark_name = await self._build_benchmark_nav(
            db, bm_id, period_start, period_end
        )

        # Normalize both for chart (rebase to 1.0)
        chart_product_nav = normalize_nav(product_nav) if not product_nav.empty else product_nav
        chart_bench_nav = normalize_nav(benchmark_nav) if benchmark_nav is not None and not benchmark_nav.empty else None

        # Calculate metrics
        metrics = calc_all_metrics(product_nav, start_date=period_start, end_date=period_end)
        interval_metrics = calc_interval_metrics(
            product_nav,
            ["1m", "3m", "6m", "1y", "2y", "3y", "inception"],
            reference_date=period_end,
        )

        # Attribution (only if we have benchmark and snapshots)
        attribution = None
        if bm_id:
            try:
                attribution = await self.compute_attribution(
                    db, product_id, period_start, period_end, bm_id, "monthly"
                )
            except Exception as e:
                logger.warning("Attribution computation failed: %s", e)

        # Get snapshots for holding weights
        curr_snapshot = await self._get_snapshot_near_date(db, product_id, period_end)
        prev_snapshot = await self._get_snapshot_near_date(db, product_id, period_start)

        # Monthly returns
        monthly_returns = self._calc_monthly_returns(product_nav)

        # Format period string
        period_str = f"{period_start.strftime('%Y年%m月')} — {period_end.strftime('%Y年%m月')}"

        # Build PDF
        builder = PDFReportBuilder(
            product_name=product.product_name,
            period=period_str,
            report_type=report_type,
        )

        # Page 1: Header + NAV chart + Metrics
        builder.add_header(
            product_code=product.product_code,
            benchmark_name=benchmark_name,
            custodian=product.custodian,
        )
        builder.add_nav_chart(render_nav_chart(
            chart_product_nav, chart_bench_nav,
            product_name=product.product_name,
            benchmark_name=benchmark_name or "基准",
        ))
        builder.add_metrics_cards(metrics)

        if attribution and attribution.aggregated_categories:
            attr_dicts = [c.model_dump() for c in attribution.aggregated_categories]
            builder.add_attribution_bar_chart(render_attribution_bar(attr_dicts))

        if monthly_returns:
            builder.add_monthly_heatmap(render_monthly_heatmap(monthly_returns))

        if report_type == "monthly":
            # Page 2: Allocation + Weight comparison + Interval table
            builder.add_page_break()

            if curr_snapshot:
                pie_data = self._snapshot_to_pie_data(curr_snapshot)
                if pie_data:
                    builder.add_allocation_pie(render_pie_chart(pie_data))

            if prev_snapshot and curr_snapshot:
                cat_names = get_category_names()
                prev_w = self._snapshot_to_l1_weights(prev_snapshot)
                curr_w = self._snapshot_to_l1_weights(curr_snapshot)
                if prev_w or curr_w:
                    builder.add_weight_comparison(
                        render_weight_comparison(prev_w, curr_w, cat_names)
                    )

            builder.add_interval_metrics_table(interval_metrics)

            # Page 3: Attribution detail table
            if attribution and attribution.aggregated_categories:
                builder.add_page_break()
                builder.add_attribution_table(
                    [c.model_dump() for c in attribution.aggregated_categories]
                )

        return builder.build()

    async def compute_attribution(
        self,
        db: AsyncSession,
        product_id: int,
        period_start: date,
        period_end: date,
        benchmark_id: int | None = None,
        granularity: str = "monthly",
    ) -> AttributionResult:
        """Compute Brinson attribution and return structured result."""
        product = await self._load_product(db, product_id)
        bm_id = benchmark_id or product.benchmark_id

        # Get all snapshots in range
        snapshots = await self._get_snapshots_in_range(db, product_id, period_start, period_end)
        if len(snapshots) < 1:
            return AttributionResult(
                product_id=product_id,
                period_start=period_start,
                period_end=period_end,
                granularity=granularity,
            )

        # Get benchmark weights
        benchmark_weights = await self._get_benchmark_weights(db, bm_id) if bm_id else {}

        # Get benchmark index returns
        benchmark_index_returns = await self._get_benchmark_index_returns(
            db, bm_id, period_start, period_end
        ) if bm_id else {}

        # Build period data for multi-period brinson
        # Split by month boundaries
        periods = self._split_into_periods(period_start, period_end, granularity)
        period_data = []

        cat_names = get_category_names()

        for p_start, p_end in periods:
            # Find snapshot closest to p_end for actual weights
            snap = self._find_closest_snapshot(snapshots, p_end)
            if not snap:
                continue

            actual_w = self._snapshot_to_l1_weights(snap)
            if not actual_w:
                continue

            # Calculate actual category returns from NAV changes
            # For simplicity, use product-level return distributed by weight change
            product_nav = await self._build_product_nav(db, product_id)
            period_return = calc_return(product_nav, p_start, p_end)

            # Estimate per-category actual returns using weight differences
            actual_returns = {}
            for cat in actual_w:
                # Use product return as proxy for each category
                actual_returns[cat] = period_return

            # Benchmark returns per category
            bench_returns = {}
            for cat in set(actual_w) | set(benchmark_weights):
                bench_returns[cat] = benchmark_index_returns.get(cat, 0)

            period_data.append({
                "period_start": p_start,
                "period_end": p_end,
                "actual_weights": actual_w,
                "benchmark_weights": benchmark_weights,
                "actual_returns": actual_returns,
                "benchmark_returns": bench_returns,
                "category_names": cat_names,
            })

        if not period_data:
            return AttributionResult(
                product_id=product_id,
                period_start=period_start,
                period_end=period_end,
                granularity=granularity,
            )

        return compute_multi_period_brinson(period_data, product_id, granularity)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _load_product(self, db: AsyncSession, product_id: int) -> Product:
        result = await db.execute(select(Product).where(Product.id == product_id))
        product = result.scalar_one_or_none()
        if not product:
            raise ValueError(f"Product {product_id} not found")
        return product

    async def _build_product_nav(self, db: AsyncSession, product_id: int) -> pd.Series:
        """Build a pandas Series of product NAV from snapshots."""
        result = await db.execute(
            select(ValuationSnapshot.valuation_date, ValuationSnapshot.unit_nav)
            .where(ValuationSnapshot.product_id == product_id)
            .where(ValuationSnapshot.unit_nav.isnot(None))
            .order_by(ValuationSnapshot.valuation_date)
        )
        rows = result.all()
        if not rows:
            return pd.Series(dtype=float)

        dates = [pd.Timestamp(r.valuation_date) for r in rows]
        navs = [float(r.unit_nav) for r in rows]
        return pd.Series(navs, index=pd.DatetimeIndex(dates), name=f"product_{product_id}")

    async def _build_benchmark_nav(
        self, db: AsyncSession, benchmark_id: int | None,
        start: date, end: date,
    ) -> tuple[pd.Series | None, str | None]:
        """Build benchmark NAV from composite benchmark."""
        if not benchmark_id:
            return None, None

        # Get composite benchmark
        bm_result = await db.execute(
            select(CompositeBenchmark).where(CompositeBenchmark.id == benchmark_id)
        )
        benchmark = bm_result.scalar_one_or_none()
        if not benchmark:
            return None, None

        # Get items
        items_result = await db.execute(
            select(CompositeBenchmarkItem).where(
                CompositeBenchmarkItem.composite_id == benchmark_id
            )
        )
        items = list(items_result.scalars().all())
        if not items:
            return None, benchmark.name

        # Build weighted nav
        nav_series: dict[str, pd.Series] = {}
        weights: dict[str, float] = {}
        for item in items:
            weights[item.index_code] = float(item.weight)
            nav_result = await db.execute(
                select(IndexNav.nav_date, IndexNav.nav_value)
                .where(IndexNav.index_code == item.index_code)
                .where(IndexNav.nav_date >= start)
                .where(IndexNav.nav_date <= end)
                .order_by(IndexNav.nav_date)
            )
            rows = nav_result.all()
            if rows:
                dates = [pd.Timestamp(r.nav_date) for r in rows]
                vals = [float(r.nav_value) for r in rows]
                nav_series[item.index_code] = pd.Series(vals, index=pd.DatetimeIndex(dates))

        if not nav_series:
            return None, benchmark.name

        # Combine: weighted sum of normalized navs
        combined = None
        for code, series in nav_series.items():
            w = weights.get(code, 0)
            normed = series / series.iloc[0] * w
            if combined is None:
                combined = normed
            else:
                combined = combined.add(normed, fill_value=0)

        return combined, benchmark.name

    async def _get_snapshot_near_date(
        self, db: AsyncSession, product_id: int, target: date,
    ) -> ValuationSnapshot | None:
        """Get the snapshot closest to target date (on or before)."""
        result = await db.execute(
            select(ValuationSnapshot)
            .where(ValuationSnapshot.product_id == product_id)
            .where(ValuationSnapshot.valuation_date <= target)
            .order_by(ValuationSnapshot.valuation_date.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def _get_snapshots_in_range(
        self, db: AsyncSession, product_id: int, start: date, end: date,
    ) -> list[ValuationSnapshot]:
        result = await db.execute(
            select(ValuationSnapshot)
            .where(ValuationSnapshot.product_id == product_id)
            .where(ValuationSnapshot.valuation_date >= start)
            .where(ValuationSnapshot.valuation_date <= end)
            .order_by(ValuationSnapshot.valuation_date)
        )
        return list(result.scalars().all())

    async def _get_benchmark_weights(
        self, db: AsyncSession, benchmark_id: int,
    ) -> dict[str, float]:
        """Get benchmark weights mapped to L1 category codes.

        Returns generic benchmark weights keyed by L1 category codes.
        """
        items_result = await db.execute(
            select(CompositeBenchmarkItem).where(
                CompositeBenchmarkItem.composite_id == benchmark_id
            )
        )
        items = list(items_result.scalars().all())
        # Map index codes to category codes: treat each index as a category
        weights = {}
        for item in items:
            weights[item.index_code] = float(item.weight)
        return weights

    async def _get_benchmark_index_returns(
        self, db: AsyncSession, benchmark_id: int | None,
        start: date, end: date,
    ) -> dict[str, float]:
        """Get per-index returns for the benchmark over the period."""
        if not benchmark_id:
            return {}

        items_result = await db.execute(
            select(CompositeBenchmarkItem).where(
                CompositeBenchmarkItem.composite_id == benchmark_id
            )
        )
        items = list(items_result.scalars().all())

        returns = {}
        for item in items:
            nav_result = await db.execute(
                select(IndexNav.nav_date, IndexNav.nav_value)
                .where(IndexNav.index_code == item.index_code)
                .where(IndexNav.nav_date >= start)
                .where(IndexNav.nav_date <= end)
                .order_by(IndexNav.nav_date)
            )
            rows = nav_result.all()
            if len(rows) >= 2:
                first_val = float(rows[0].nav_value)
                last_val = float(rows[-1].nav_value)
                returns[item.index_code] = (last_val / first_val - 1) if first_val else 0
        return returns

    def _snapshot_to_l1_weights(self, snapshot: ValuationSnapshot) -> dict[str, float]:
        """Extract L1 (level=1) weights from a snapshot's items."""
        items = []
        for item in (snapshot.items or []):
            items.append({
                "item_code": item.item_code or "",
                "level": item.level,
                "value_pct_nav": float(item.value_pct_nav) if item.value_pct_nav else None,
            })
        return extract_l1_weights(items)

    def _snapshot_to_pie_data(self, snapshot: ValuationSnapshot) -> list[dict]:
        """Convert L1 items to pie chart data."""
        cat_names = get_category_names()
        data = []
        for item in (snapshot.items or []):
            if item.level == 1 and item.value_pct_nav and float(item.value_pct_nav) > 0:
                code = item.item_code or ""
                data.append({
                    "name": cat_names.get(code, item.item_name or code),
                    "value": float(item.value_pct_nav),
                })
        return data

    def _calc_monthly_returns(self, nav: pd.Series) -> list[dict]:
        """Calculate monthly returns from NAV series."""
        if nav.empty or len(nav) < 2:
            return []

        monthly = nav.resample("ME").last().dropna()
        rets = monthly.pct_change().dropna()

        result = []
        for dt, ret in rets.items():
            result.append({
                "year": dt.year,
                "month": dt.month,
                "return_pct": float(ret),
            })
        return result

    def _split_into_periods(
        self, start: date, end: date, granularity: str,
    ) -> list[tuple[date, date]]:
        """Split a date range into monthly or weekly sub-periods."""
        periods = []
        current = start

        if granularity == "weekly":
            while current <= end:
                p_end = min(current + timedelta(days=6), end)
                periods.append((current, p_end))
                current = p_end + timedelta(days=1)
        else:  # monthly
            while current <= end:
                year, month = current.year, current.month
                last_day = calendar.monthrange(year, month)[1]
                p_end = min(date(year, month, last_day), end)
                periods.append((current, p_end))
                # Next month
                if month == 12:
                    current = date(year + 1, 1, 1)
                else:
                    current = date(year, month + 1, 1)

        return periods

    @staticmethod
    def _find_closest_snapshot(
        snapshots: list[ValuationSnapshot], target: date,
    ) -> ValuationSnapshot | None:
        """Find snapshot closest to (on or before) the target date."""
        candidates = [s for s in snapshots if s.valuation_date <= target]
        if not candidates:
            return snapshots[0] if snapshots else None
        return max(candidates, key=lambda s: s.valuation_date)


report_service = ReportService()
