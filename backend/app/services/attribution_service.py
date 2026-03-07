"""FOF strategy-level attribution and factor exposure analysis."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fund import Fund, NavHistory
from app.models.product import Product, ValuationSnapshot, ValuationItem

logger = logging.getLogger(__name__)

# Sub-fund item code prefix in valuation tables
_SUBFUND_PREFIX = "11090601"

# Factor proxy index mapping: strategy_type -> index_code
# Priority: 招商私募指数 (daily, 2465+ pts) > 火富牛精选指数 (weekly)
# Core factors: minimal set to avoid multicollinearity
# Each represents a distinct risk source in FOF allocation
FACTOR_PROXIES = {
    "股票多头": "ZSGPDT",    # 招商股票多头私募指数 - equity beta
    "CTA":     "ZSCTA",     # 招商CTA私募指数 - managed futures
    "市场中性": "ZSSCZX",    # 招商股票市场中性私募指数 - alpha/neutral
    "套利策略": "NNTLZS",    # 火富牛套利策略精选指数 - arb
    "期权策略": "NNQQZS",    # 火富牛期权策略精选指数 - vol selling
    "债券":    "000832",    # 中证可转换债券指数 - fixed income
}


class AttributionService:
    """Service for FOF strategy attribution and factor exposure analysis."""

    # ------------------------------------------------------------------
    # 1. Strategy-level attribution (based on valuation snapshots)
    # ------------------------------------------------------------------

    async def get_strategy_attribution(
        self,
        db: AsyncSession,
        product_id: int,
        group_by: str = "strategy_type",
    ) -> dict[str, Any]:
        """Compute strategy-level weight and return attribution from valuation snapshots.

        For each snapshot, groups sub-fund holdings by strategy_type (or strategy_sub),
        computes weights, and calculates return contribution between periods.

        group_by: "strategy_type" (大类) or "strategy_sub" (子类)
        """
        # Get all snapshots ordered by date
        snap_result = await db.execute(
            select(ValuationSnapshot)
            .where(ValuationSnapshot.product_id == product_id)
            .order_by(ValuationSnapshot.valuation_date)
        )
        snapshots = list(snap_result.scalars().all())
        if not snapshots:
            return {"snapshots": [], "strategy_types": [], "return_contribution": []}

        strategy_types_set: set[str] = set()
        snapshot_data = []

        for snap in snapshots:
            # Get sub-fund items for this snapshot
            items_result = await db.execute(
                select(
                    ValuationItem.item_code,
                    ValuationItem.item_name,
                    ValuationItem.market_value,
                    ValuationItem.value_pct_nav,
                    ValuationItem.linked_fund_id,
                    Fund.strategy_type,
                    Fund.strategy_sub,
                    Fund.fund_name,
                )
                .outerjoin(Fund, Fund.id == ValuationItem.linked_fund_id)
                .where(
                    ValuationItem.snapshot_id == snap.id,
                    ValuationItem.item_code.like(f"{_SUBFUND_PREFIX}%"),
                    func.length(ValuationItem.item_code) > 8,
                )
                .order_by(ValuationItem.market_value.desc())
            )
            items = items_result.all()

            # Group by strategy label
            strategy_groups: dict[str, list[dict]] = {}
            total_subfund_mv = 0.0

            for item in items:
                strategy_type = item.strategy_type or "未分类"
                strategy_sub = item.strategy_sub or ""
                # Determine group key based on group_by parameter
                if group_by == "strategy_sub" and strategy_sub:
                    group_key = strategy_sub
                else:
                    group_key = strategy_type
                mv = float(item.market_value) if item.market_value else 0.0
                pct = float(item.value_pct_nav) if item.value_pct_nav else 0.0
                total_subfund_mv += mv

                if group_key not in strategy_groups:
                    strategy_groups[group_key] = []
                strategy_groups[group_key].append({
                    "fund_name": item.item_name or item.fund_name or "",
                    "fund_id": item.linked_fund_id,
                    "strategy_sub": strategy_sub,
                    "strategy_type": strategy_type,
                    "market_value": mv,
                    "weight_pct": pct,
                })
                strategy_types_set.add(group_key)

            # Compute strategy-level weights
            strategy_weights = {}
            for st, funds in strategy_groups.items():
                st_mv = sum(f["market_value"] for f in funds)
                # Weight relative to total sub-fund allocation (not total NAV)
                st_weight = st_mv / total_subfund_mv if total_subfund_mv > 0 else 0.0
                # Weight relative to total NAV (from value_pct_nav)
                st_pct_nav = sum(f["weight_pct"] for f in funds)
                strategy_weights[st] = {
                    "weight": round(st_weight, 6),
                    "weight_pct_nav": round(st_pct_nav, 4),
                    "market_value": round(st_mv, 2),
                    "fund_count": len(funds),
                    "funds": funds,
                }

            snapshot_data.append({
                "valuation_date": snap.valuation_date.isoformat(),
                "unit_nav": float(snap.unit_nav) if snap.unit_nav else None,
                "total_nav": float(snap.total_nav) if snap.total_nav else None,
                "strategy_weights": strategy_weights,
            })

        # Compute return contribution between consecutive snapshots
        return_contributions = []
        for i in range(1, len(snapshot_data)):
            prev = snapshot_data[i - 1]
            curr = snapshot_data[i]
            prev_nav = prev["unit_nav"]
            curr_nav = curr["unit_nav"]
            if not prev_nav or not curr_nav:
                continue

            period_return = (curr_nav - prev_nav) / prev_nav
            contributions = {}

            for st in strategy_types_set:
                prev_w = prev["strategy_weights"].get(st, {})
                curr_w = curr["strategy_weights"].get(st, {})
                # Use average weight * strategy return estimate
                # Since we don't have daily sub-fund returns between snapshots,
                # we estimate from linked fund NAV changes
                prev_weight = prev_w.get("weight", 0)
                curr_weight = curr_w.get("weight", 0)
                avg_weight = (prev_weight + curr_weight) / 2

                # Get sub-fund returns for this strategy in the period
                strategy_return = await self._estimate_strategy_return(
                    db,
                    prev["valuation_date"],
                    curr["valuation_date"],
                    prev_w.get("funds", []),
                    curr_w.get("funds", []),
                )
                contribution = avg_weight * strategy_return if strategy_return is not None else None
                contributions[st] = {
                    "avg_weight": round(avg_weight, 6),
                    "strategy_return": round(strategy_return, 6) if strategy_return is not None else None,
                    "contribution": round(contribution, 6) if contribution is not None else None,
                }

            return_contributions.append({
                "period_start": prev["valuation_date"],
                "period_end": curr["valuation_date"],
                "total_return": round(period_return, 6),
                "contributions": contributions,
            })

        # Build ordered strategy list
        strategy_types = sorted(strategy_types_set)

        return {
            "snapshots": snapshot_data,
            "strategy_types": strategy_types,
            "return_contribution": return_contributions,
        }

    async def _estimate_strategy_return(
        self,
        db: AsyncSession,
        start_date_str: str,
        end_date_str: str,
        prev_funds: list[dict],
        curr_funds: list[dict],
    ) -> float | None:
        """Estimate the return of a strategy group between two dates.

        Uses the NAV history of linked funds, weighted by their allocation.
        """
        start_d = date.fromisoformat(start_date_str)
        end_d = date.fromisoformat(end_date_str)

        # Combine fund IDs from both periods
        fund_ids = set()
        fund_weights: dict[int, float] = {}
        for f in prev_funds + curr_funds:
            fid = f.get("fund_id")
            if fid:
                fund_ids.add(fid)
                # Use latest weight for this fund
                fund_weights[fid] = f.get("market_value", 0)

        if not fund_ids:
            return None

        total_w = sum(fund_weights.values())
        returns = []
        weights = []

        for fid in fund_ids:
            # Get NAV at start and end
            nav_start = await self._get_nearest_nav(db, fid, start_d, direction="after")
            nav_end = await self._get_nearest_nav(db, fid, end_d, direction="before")

            if nav_start and nav_end and nav_start > 0:
                ret = (nav_end - nav_start) / nav_start
                w = fund_weights.get(fid, 0) / total_w if total_w > 0 else 0
                returns.append(ret)
                weights.append(w)

        if not returns:
            return None

        # Weighted average return
        total_w2 = sum(weights)
        if total_w2 > 0:
            return sum(r * w / total_w2 for r, w in zip(returns, weights))
        return float(np.mean(returns))

    async def _get_nearest_nav(
        self, db: AsyncSession, fund_id: int, target_date: date, direction: str = "before"
    ) -> float | None:
        """Get the nearest NAV to a target date."""
        if direction == "before":
            result = await db.execute(
                select(NavHistory.unit_nav)
                .where(NavHistory.fund_id == fund_id, NavHistory.nav_date <= target_date)
                .order_by(NavHistory.nav_date.desc())
                .limit(1)
            )
        else:
            result = await db.execute(
                select(NavHistory.unit_nav)
                .where(NavHistory.fund_id == fund_id, NavHistory.nav_date >= target_date)
                .order_by(NavHistory.nav_date.asc())
                .limit(1)
            )
        val = result.scalar_one_or_none()
        return float(val) if val else None

    # ------------------------------------------------------------------
    # 2. Factor exposure analysis (regression-based)
    # ------------------------------------------------------------------

    async def get_factor_exposure(
        self,
        db: AsyncSession,
        product_id: int,
        window: int = 60,
    ) -> dict[str, Any]:
        """Run multi-factor regression to estimate FOF factor exposures.

        Uses strategy-type proxy indices as factors.
        Returns both static (full-period) and rolling exposures.
        """
        # 1. Get FOF daily NAV series
        product = await db.execute(
            select(Product).where(Product.id == product_id)
        )
        product = product.scalar_one_or_none()
        if not product:
            return {"error": "Product not found"}

        fof_nav = await self._get_product_nav_series(db, product)
        if fof_nav is None or len(fof_nav) < 10:
            return {"error": "Insufficient FOF NAV data"}

        # 2. Get factor index NAV series
        factors = {}
        for strategy_type, index_code in FACTOR_PROXIES.items():
            factor_nav = await self._get_index_nav_series(db, index_code)
            if factor_nav is not None and len(factor_nav) > 10:
                factors[strategy_type] = factor_nav

        if not factors:
            return {"error": "No factor data available"}

        # 3. Align all series to common dates
        all_series = {"FOF": fof_nav}
        all_series.update(factors)
        df = pd.DataFrame(all_series)
        df = df.dropna()

        if len(df) < 10:
            return {"error": "Insufficient overlapping data"}

        # 4. Compute returns
        returns = df.pct_change().dropna()
        if len(returns) < 10:
            return {"error": "Insufficient return data"}

        fof_ret = returns["FOF"]
        factor_names = [k for k in factors.keys() if k in returns.columns]
        X = returns[factor_names]

        # 5. Static regression (full period OLS)
        static_result = self._run_ols(fof_ret, X, factor_names)

        # 6. Rolling regression
        rolling_results = []
        if len(returns) >= window + 10:
            for end_idx in range(window, len(returns)):
                start_idx = end_idx - window
                y_win = fof_ret.iloc[start_idx:end_idx]
                X_win = X.iloc[start_idx:end_idx]
                dt = returns.index[end_idx]
                r = self._run_ols(y_win, X_win, factor_names)
                rolling_results.append({
                    "date": dt.isoformat() if hasattr(dt, "isoformat") else str(dt),
                    "r_squared": r["r_squared"],
                    "betas": r["betas"],
                })

        return {
            "product_id": product_id,
            "data_points": len(returns),
            "date_range": {
                "start": returns.index[0].isoformat() if hasattr(returns.index[0], "isoformat") else str(returns.index[0]),
                "end": returns.index[-1].isoformat() if hasattr(returns.index[-1], "isoformat") else str(returns.index[-1]),
            },
            "factors": {
                name: FACTOR_PROXIES.get(name, "")
                for name in factor_names
            },
            "static": static_result,
            "rolling": rolling_results,
            "rolling_window": window,
        }

    def _run_ols(
        self, y: pd.Series, X: pd.DataFrame, factor_names: list[str]
    ) -> dict[str, Any]:
        """Run OLS regression: y = alpha + sum(beta_i * X_i) + epsilon."""
        # Add constant (intercept)
        X_with_const = X.copy()
        X_with_const.insert(0, "const", 1.0)

        try:
            # OLS: beta = (X'X)^-1 X'y
            Xm = X_with_const.values
            ym = y.values
            XtX = Xm.T @ Xm
            Xty = Xm.T @ ym
            betas = np.linalg.solve(XtX, Xty)

            # Fitted values and R²
            y_hat = Xm @ betas
            ss_res = np.sum((ym - y_hat) ** 2)
            ss_tot = np.sum((ym - np.mean(ym)) ** 2)
            r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

            # Standard errors and t-statistics
            n = len(ym)
            k = len(betas)
            if n > k:
                mse = ss_res / (n - k)
                var_betas = mse * np.linalg.inv(XtX)
                se = np.sqrt(np.diag(var_betas))
                t_stats = betas / se
            else:
                se = np.zeros(k)
                t_stats = np.zeros(k)

            alpha = float(betas[0])
            beta_dict = {}
            for i, name in enumerate(factor_names):
                beta_dict[name] = {
                    "beta": round(float(betas[i + 1]), 6),
                    "t_stat": round(float(t_stats[i + 1]), 4),
                    "se": round(float(se[i + 1]), 6),
                }

            return {
                "alpha": round(alpha, 8),
                "alpha_annualized": round(alpha * 252, 6),
                "alpha_t_stat": round(float(t_stats[0]), 4),
                "r_squared": round(float(r_squared), 6),
                "betas": beta_dict,
            }

        except np.linalg.LinAlgError:
            return {
                "alpha": None,
                "alpha_annualized": None,
                "alpha_t_stat": None,
                "r_squared": None,
                "betas": {name: {"beta": None, "t_stat": None, "se": None} for name in factor_names},
            }

    async def _get_product_nav_series(
        self, db: AsyncSession, product: Product
    ) -> pd.Series | None:
        """Get product NAV as a pandas Series indexed by date."""
        # Try linked fund first (same logic as product_service.get_nav_series)
        fund_id = None
        if product.product_code:
            r = await db.execute(
                select(Fund.id).where(
                    Fund.status == "active",
                    Fund.filing_number == product.product_code,
                )
            )
            fund_id = r.scalar_one_or_none()

        if fund_id:
            result = await db.execute(
                select(NavHistory.nav_date, NavHistory.unit_nav)
                .where(NavHistory.fund_id == fund_id)
                .order_by(NavHistory.nav_date)
            )
            rows = result.all()
            if rows:
                dates = [r[0] for r in rows]
                values = [float(r[1]) for r in rows]
                return pd.Series(values, index=pd.DatetimeIndex(dates), name="FOF")

        # Fallback: valuation snapshots
        result = await db.execute(
            select(ValuationSnapshot.valuation_date, ValuationSnapshot.unit_nav)
            .where(ValuationSnapshot.product_id == product.id)
            .order_by(ValuationSnapshot.valuation_date)
        )
        rows = result.all()
        if rows:
            dates = [r[0] for r in rows]
            values = [float(r[1]) for r in rows]
            return pd.Series(values, index=pd.DatetimeIndex(dates), name="FOF")
        return None

    async def _get_index_nav_series(
        self, db: AsyncSession, index_code: str
    ) -> pd.Series | None:
        """Get benchmark index NAV as a pandas Series."""
        result = await db.execute(
            text("SELECT nav_date, nav_value FROM index_nav WHERE index_code = :code ORDER BY nav_date"),
            {"code": index_code},
        )
        rows = result.all()
        if not rows:
            return None
        dates = [r[0] for r in rows]
        values = [float(r[1]) for r in rows]
        return pd.Series(values, index=pd.DatetimeIndex(dates), name=index_code)


    # ------------------------------------------------------------------
    # 3. Fund-level contribution analysis
    # ------------------------------------------------------------------

    async def get_fund_contribution(
        self,
        db: AsyncSession,
        product_id: int,
        period_start: date,
        period_end: date,
    ) -> list[dict[str, Any]]:
        """计算产品中每只子基金对组合收益的贡献度。

        通过估值快照获取产品持仓的子基金，计算每只基金在期间内的
        权重 * 收益 = 贡献，按贡献绝对值降序排列。

        Returns:
            [{fund_id, fund_name, weight, return, contribution}, ...]
        """
        # 获取期间内最近的两个估值快照（期初、期末）
        snap_start_result = await db.execute(
            select(ValuationSnapshot)
            .where(
                ValuationSnapshot.product_id == product_id,
                ValuationSnapshot.valuation_date <= period_start,
            )
            .order_by(ValuationSnapshot.valuation_date.desc())
            .limit(1)
        )
        snap_start = snap_start_result.scalar_one_or_none()

        # 如果期初没有快照，取期间内最早的
        if not snap_start:
            snap_start_result = await db.execute(
                select(ValuationSnapshot)
                .where(
                    ValuationSnapshot.product_id == product_id,
                    ValuationSnapshot.valuation_date >= period_start,
                )
                .order_by(ValuationSnapshot.valuation_date.asc())
                .limit(1)
            )
            snap_start = snap_start_result.scalar_one_or_none()

        snap_end_result = await db.execute(
            select(ValuationSnapshot)
            .where(
                ValuationSnapshot.product_id == product_id,
                ValuationSnapshot.valuation_date <= period_end,
            )
            .order_by(ValuationSnapshot.valuation_date.desc())
            .limit(1)
        )
        snap_end = snap_end_result.scalar_one_or_none()

        if not snap_start or not snap_end:
            return []

        # 从期末快照获取子基金持仓信息
        items_result = await db.execute(
            select(
                ValuationItem.linked_fund_id,
                ValuationItem.item_name,
                ValuationItem.market_value,
                ValuationItem.value_pct_nav,
                Fund.fund_name,
            )
            .outerjoin(Fund, Fund.id == ValuationItem.linked_fund_id)
            .where(
                ValuationItem.snapshot_id == snap_end.id,
                ValuationItem.item_code.like(f"{_SUBFUND_PREFIX}%"),
                func.length(ValuationItem.item_code) > 8,
            )
        )
        items = items_result.all()

        if not items:
            return []

        # 计算总市值用于权重
        total_mv = sum(float(item.market_value) if item.market_value else 0.0 for item in items)
        if total_mv <= 0:
            return []

        # Batch query NAV for all fund_ids to avoid N+1
        linked_fund_ids = [item.linked_fund_id for item in items if item.linked_fund_id]
        nav_lookup: dict[int, dict[str, float | None]] = {}
        if linked_fund_ids:
            nav_result = await db.execute(
                select(NavHistory.fund_id, NavHistory.nav_date, NavHistory.unit_nav)
                .where(
                    NavHistory.fund_id.in_(linked_fund_ids),
                    NavHistory.nav_date >= period_start,
                    NavHistory.nav_date <= period_end,
                )
                .order_by(NavHistory.fund_id, NavHistory.nav_date)
            )
            from collections import defaultdict
            fund_nav_series: dict[int, list[tuple]] = defaultdict(list)
            for row in nav_result.all():
                fund_nav_series[row.fund_id].append((row.nav_date, float(row.unit_nav)))
            for fid, series in fund_nav_series.items():
                if len(series) >= 2:
                    nav_lookup[fid] = {"start": series[0][1], "end": series[-1][1]}

        contributions = []
        for item in items:
            fund_id = item.linked_fund_id
            fund_name = item.fund_name or item.item_name or "未知基金"
            mv = float(item.market_value) if item.market_value else 0.0
            weight = mv / total_mv

            fund_return = None
            if fund_id and fund_id in nav_lookup:
                nav_start = nav_lookup[fund_id]["start"]
                nav_end = nav_lookup[fund_id]["end"]
                if nav_start and nav_end and nav_start > 0:
                    fund_return = (nav_end - nav_start) / nav_start

            contribution = weight * fund_return if fund_return is not None else None

            contributions.append({
                "fund_id": fund_id,
                "fund_name": fund_name,
                "weight": round(weight, 6),
                "return": round(fund_return, 6) if fund_return is not None else None,
                "contribution": round(contribution, 6) if contribution is not None else None,
            })

        # 按贡献绝对值降序排列
        contributions.sort(
            key=lambda x: abs(x["contribution"]) if x["contribution"] is not None else 0,
            reverse=True,
        )
        return contributions

    # ------------------------------------------------------------------
    # 4. Correlation matrix computation
    # ------------------------------------------------------------------

    async def compute_correlation_matrix(
        self,
        db: AsyncSession,
        fund_ids: list[int],
        period_start: date,
        period_end: date,
    ) -> dict[str, Any]:
        """计算多只基金之间日收益率的相关系数矩阵。

        加载每只基金在指定期间内的净值序列，对齐日期后计算
        pairwise Pearson相关系数矩阵。

        Returns:
            {labels: [fund_name, ...], matrix: [[1.0, 0.8, ...], ...],
             period: {start, end}}
        """
        # Batch query: fund names
        fund_result = await db.execute(
            select(Fund.id, Fund.fund_name).where(Fund.id.in_(fund_ids))
        )
        fund_names = {r.id: r.fund_name for r in fund_result.all()}

        # Batch query: all NAV data in one query
        nav_result = await db.execute(
            select(NavHistory.fund_id, NavHistory.nav_date, NavHistory.unit_nav)
            .where(
                NavHistory.fund_id.in_(fund_ids),
                NavHistory.nav_date >= period_start,
                NavHistory.nav_date <= period_end,
            )
            .order_by(NavHistory.fund_id, NavHistory.nav_date)
        )
        all_rows = nav_result.all()

        # Group by fund_id in memory
        from collections import defaultdict
        fund_navs: dict[int, list[tuple]] = defaultdict(list)
        for row in all_rows:
            fund_navs[row.fund_id].append((row.nav_date, float(row.unit_nav)))

        labels: list[str] = []
        nav_series_dict: dict[str, pd.Series] = {}

        for fund_id in fund_ids:
            fname = fund_names.get(fund_id)
            if not fname:
                continue
            rows = fund_navs.get(fund_id, [])
            if len(rows) < 2:
                continue
            dates = [r[0] for r in rows]
            values = [r[1] for r in rows]
            # Use "name(id)" to avoid duplicate name collision
            label = f"{fname}({fund_id})" if list(fund_names.values()).count(fname) > 1 else fname
            series = pd.Series(values, index=pd.DatetimeIndex(dates), name=label)
            labels.append(label)
            nav_series_dict[label] = series

        if len(labels) < 2:
            return {
                "labels": labels,
                "matrix": [[1.0]] if labels else [],
                "period": {
                    "start": period_start.isoformat(),
                    "end": period_end.isoformat(),
                },
            }

        # 合并为DataFrame并对齐日期（内连接）
        df = pd.DataFrame(nav_series_dict)
        df = df.dropna()

        if len(df) < 2:
            # 数据不足，返回单位矩阵
            n = len(labels)
            identity = np.eye(n).tolist()
            return {
                "labels": labels,
                "matrix": identity,
                "period": {
                    "start": period_start.isoformat(),
                    "end": period_end.isoformat(),
                },
            }

        # 计算日收益率
        returns = df.pct_change().dropna()

        if len(returns) < 2:
            n = len(labels)
            identity = np.eye(n).tolist()
            return {
                "labels": labels,
                "matrix": identity,
                "period": {
                    "start": period_start.isoformat(),
                    "end": period_end.isoformat(),
                },
            }

        # 计算相关系数矩阵
        corr = returns[labels].corr()
        # 转为嵌套列表，保留6位小数
        matrix = [
            [round(float(corr.iloc[i, j]), 6) for j in range(len(labels))]
            for i in range(len(labels))
        ]

        return {
            "labels": labels,
            "matrix": matrix,
            "period": {
                "start": period_start.isoformat(),
                "end": period_end.isoformat(),
            },
        }


attribution_service = AttributionService()
