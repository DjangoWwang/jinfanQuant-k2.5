"""Business logic layer for portfolio management and backtesting."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.portfolio import Portfolio, PortfolioAllocation
from app.models.portfolio import BacktestResult as BacktestResultModel
from app.models.fund import Fund
from app.models.benchmark import Benchmark, IndexNav
from app.schemas.portfolio import (
    BacktestConfigSchema,
    PortfolioCreate,
    PortfolioWeight,
)
from app.engine.backtest import BacktestEngine, BacktestConfig
from app.engine.calendar import get_trading_days
from app.services.fund_service import fund_service

logger = logging.getLogger(__name__)


class PortfolioService:
    """Service for portfolio CRUD and backtest orchestration."""

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def create_portfolio(
        self, db: AsyncSession, payload: PortfolioCreate
    ) -> dict:
        """Create a new portfolio with its fund/index weight allocations.

        Validates that weights sum to ~1.0, that all referenced fund_ids
        and index_codes exist, then creates the Portfolio row and
        PortfolioAllocation rows inside a single transaction.
        """
        # --- validate weights sum ---
        total_weight = sum(w.weight for w in payload.weights)
        if abs(total_weight - 1.0) > 0.01:
            raise ValueError(f"权重之和应为1.0，当前为{total_weight:.4f}")

        # --- validate fund_ids exist ---
        fund_ids = [w.fund_id for w in payload.weights if w.fund_id is not None]
        if fund_ids:
            result = await db.execute(select(Fund.id).where(Fund.id.in_(fund_ids)))
            existing_ids = {row[0] for row in result.all()}
            missing = set(fund_ids) - existing_ids
            if missing:
                raise ValueError(f"以下基金ID不存在: {missing}")

        # --- validate index_codes exist ---
        index_codes = [w.index_code for w in payload.weights if w.index_code is not None]
        if index_codes:
            result = await db.execute(
                select(Benchmark.index_code).where(Benchmark.index_code.in_(index_codes))
            )
            existing_codes = {row[0] for row in result.all()}
            missing = set(index_codes) - existing_codes
            if missing:
                raise ValueError(f"以下指数代码不存在: {missing}")

        # --- create portfolio row ---
        portfolio = Portfolio(
            name=payload.name,
            description=payload.description,
            portfolio_type="simulation",
            allocation_model="custom",
            rebalance_freq=payload.rebalance_frequency,
        )
        db.add(portfolio)
        await db.flush()

        # --- create allocation rows ---
        today = date.today()
        for w in payload.weights:
            db.add(PortfolioAllocation(
                portfolio_id=portfolio.id,
                fund_id=w.fund_id,
                index_code=w.index_code,
                target_weight=w.weight,
                effective_date=today,
            ))

        await db.commit()
        await db.refresh(portfolio)

        logger.info("Portfolio created: id=%d, name=%s", portfolio.id, portfolio.name)

        return {
            "id": portfolio.id,
            "name": portfolio.name,
            "description": portfolio.description,
            "portfolio_type": portfolio.portfolio_type,
            "allocation_model": portfolio.allocation_model,
            "rebalance_freq": portfolio.rebalance_freq,
            "created_at": portfolio.created_at.isoformat() if portfolio.created_at else None,
            "weights": [
                {
                    "fund_id": w.fund_id,
                    "index_code": w.index_code,
                    "weight": w.weight,
                }
                for w in payload.weights
            ],
        }

    async def get_portfolio(
        self, db: AsyncSession, portfolio_id: int
    ) -> dict | None:
        """Retrieve a portfolio by ID, including its weight breakdown
        with resolved fund names and index names."""
        result = await db.execute(select(Portfolio).where(Portfolio.id == portfolio_id))
        portfolio = result.scalar_one_or_none()
        if not portfolio:
            return None

        # Load allocations with fund details via outer join
        alloc_result = await db.execute(
            select(PortfolioAllocation, Fund.fund_name, Fund.strategy_type, Fund.nav_frequency)
            .join(Fund, PortfolioAllocation.fund_id == Fund.id, isouter=True)
            .where(PortfolioAllocation.portfolio_id == portfolio_id)
            .order_by(PortfolioAllocation.target_weight.desc())
        )
        allocs = alloc_result.all()

        # Resolve index names for any index-based allocations
        index_codes = [
            a.PortfolioAllocation.index_code
            for a in allocs
            if a.PortfolioAllocation.index_code
        ]
        idx_names: dict[str, str] = {}
        if index_codes:
            idx_result = await db.execute(
                select(Benchmark.index_code, Benchmark.index_name)
                .where(Benchmark.index_code.in_(index_codes))
            )
            idx_names = {row[0]: row[1] for row in idx_result.all()}

        return {
            "id": portfolio.id,
            "name": portfolio.name,
            "description": portfolio.description,
            "portfolio_type": portfolio.portfolio_type,
            "allocation_model": portfolio.allocation_model,
            "rebalance_freq": portfolio.rebalance_freq,
            "created_at": portfolio.created_at.isoformat() if portfolio.created_at else None,
            "weights": [
                {
                    "fund_id": a.PortfolioAllocation.fund_id,
                    "index_code": a.PortfolioAllocation.index_code,
                    "fund_name": (
                        a.fund_name
                        or idx_names.get(
                            a.PortfolioAllocation.index_code or "",
                            a.PortfolioAllocation.index_code or "",
                        )
                    ),
                    "strategy_type": a.strategy_type,
                    "nav_frequency": a.nav_frequency,
                    "weight": float(a.PortfolioAllocation.target_weight),
                }
                for a in allocs
            ],
        }

    async def list_portfolios(
        self,
        db: AsyncSession,
        skip: int = 0,
        limit: int = 50,
        portfolio_type: str | None = None,
    ) -> tuple[list[dict], int]:
        """Paginated list of active portfolios with total count.

        Returns (items, total_count).
        """
        query = select(Portfolio).where(Portfolio.is_active.is_(True))
        count_query = select(func.count(Portfolio.id)).where(Portfolio.is_active.is_(True))

        if portfolio_type:
            query = query.where(Portfolio.portfolio_type == portfolio_type)
            count_query = count_query.where(Portfolio.portfolio_type == portfolio_type)

        query = query.order_by(Portfolio.created_at.desc())
        query = query.offset(skip).limit(limit)

        result = await db.execute(query)
        portfolios = list(result.scalars().all())

        total_result = await db.execute(count_query)
        total = total_result.scalar() or 0

        # Collect index codes across all portfolios for batch lookup
        all_index_codes: set[str] = set()
        items_with_allocs: list[tuple[Portfolio, list]] = []

        for p in portfolios:
            alloc_result = await db.execute(
                select(PortfolioAllocation, Fund.fund_name)
                .join(Fund, PortfolioAllocation.fund_id == Fund.id, isouter=True)
                .where(PortfolioAllocation.portfolio_id == p.id)
                .order_by(PortfolioAllocation.target_weight.desc())
            )
            allocs = alloc_result.all()
            for a in allocs:
                if a.PortfolioAllocation.index_code:
                    all_index_codes.add(a.PortfolioAllocation.index_code)
            items_with_allocs.append((p, allocs))

        # Batch-resolve index names
        idx_names: dict[str, str] = {}
        if all_index_codes:
            idx_result = await db.execute(
                select(Benchmark.index_code, Benchmark.index_name)
                .where(Benchmark.index_code.in_(all_index_codes))
            )
            idx_names = {row[0]: row[1] for row in idx_result.all()}

        items = []
        for p, allocs in items_with_allocs:
            items.append({
                "id": p.id,
                "name": p.name,
                "description": p.description,
                "portfolio_type": p.portfolio_type,
                "allocation_model": p.allocation_model,
                "rebalance_freq": p.rebalance_freq,
                "fund_count": len(allocs),
                "weights": [
                    {
                        "fund_id": a.PortfolioAllocation.fund_id,
                        "index_code": a.PortfolioAllocation.index_code,
                        "fund_name": (
                            a.fund_name
                            or idx_names.get(
                                a.PortfolioAllocation.index_code or "",
                                a.PortfolioAllocation.index_code or "",
                            )
                        ),
                        "weight": float(a.PortfolioAllocation.target_weight),
                    }
                    for a in allocs
                ],
                "created_at": p.created_at.isoformat() if p.created_at else None,
            })

        return items, total

    async def update_weights(
        self,
        db: AsyncSession,
        portfolio_id: int,
        weights: list[PortfolioWeight],
    ) -> dict | None:
        """Replace the weight allocations for an existing portfolio.

        Validates weight sum, deletes old allocations, inserts new ones.
        Returns the updated portfolio dict or None if not found.
        """
        result = await db.execute(select(Portfolio).where(Portfolio.id == portfolio_id))
        portfolio = result.scalar_one_or_none()
        if not portfolio:
            return None

        # Validate weights sum
        total_weight = sum(w.weight for w in weights)
        if abs(total_weight - 1.0) > 0.01:
            raise ValueError(f"权重之和应为1.0，当前为{total_weight:.4f}")

        # Delete old allocations
        await db.execute(
            delete(PortfolioAllocation).where(
                PortfolioAllocation.portfolio_id == portfolio_id
            )
        )

        # Insert new allocations
        today = date.today()
        for w in weights:
            db.add(PortfolioAllocation(
                portfolio_id=portfolio_id,
                fund_id=w.fund_id,
                index_code=w.index_code,
                target_weight=w.weight,
                effective_date=today,
            ))

        await db.commit()
        logger.info("Portfolio %d weights updated (%d allocations)", portfolio_id, len(weights))

        # Return the updated portfolio
        return await self.get_portfolio(db, portfolio_id)

    async def delete_portfolio(self, db: AsyncSession, portfolio_id: int) -> bool:
        """Soft-delete a portfolio by setting is_active = False.

        Returns True if the portfolio existed and was deactivated.
        """
        result = await db.execute(select(Portfolio).where(Portfolio.id == portfolio_id))
        portfolio = result.scalar_one_or_none()
        if not portfolio:
            return False

        portfolio.is_active = False
        await db.commit()
        logger.info("Portfolio %d soft-deleted", portfolio_id)
        return True

    # ------------------------------------------------------------------
    # Backtesting
    # ------------------------------------------------------------------

    async def run_backtest(
        self, db: AsyncSession, config: BacktestConfigSchema
    ) -> dict:
        """Execute a historical backtest for the given configuration.

        High-level flow:
            1. Resolve fund weights (from portfolio_id or inline).
            2. Load NAV histories for all constituent assets (funds + indices).
            3. Get trading calendar for the date range.
            4. Run BacktestEngine.
            5. Optionally save result if portfolio_id is provided.
            6. Return full result dict with metrics, nav series, etc.
        """
        # --- 1. Resolve weights ---
        weights_map: dict[str, float] = {}

        if config.portfolio_id:
            alloc_result = await db.execute(
                select(PortfolioAllocation)
                .where(PortfolioAllocation.portfolio_id == config.portfolio_id)
            )
            allocs = list(alloc_result.scalars().all())
            if not allocs:
                raise ValueError("组合不存在或无权重配置")
            for a in allocs:
                if a.index_code:
                    weights_map[f"idx_{a.index_code}"] = float(a.target_weight)
                elif a.fund_id:
                    weights_map[f"fund_{a.fund_id}"] = float(a.target_weight)
        elif config.weights:
            for w in config.weights:
                if w.fund_id is not None:
                    weights_map[f"fund_{w.fund_id}"] = w.weight
                elif w.index_code is not None:
                    weights_map[f"idx_{w.index_code}"] = w.weight
                else:
                    raise ValueError("每个权重项必须指定 fund_id 或 index_code")
        else:
            raise ValueError("需要提供 portfolio_id 或 weights")

        if not weights_map:
            raise ValueError("权重为空")

        # --- 2. Get trading calendar ---
        trading_days = await get_trading_days(db, config.start_date, config.end_date)
        if not trading_days:
            raise ValueError("所选日期范围内无交易日")

        # --- 3. Load NAV series for all assets ---
        nav_dict: dict[str, pd.Series] = {}
        fund_names: dict[str, str] = {}
        missing_assets: list[str] = []

        for asset_key in weights_map:
            if asset_key.startswith("fund_"):
                fund_id = int(asset_key.split("_", 1)[1])
                series = await fund_service.get_nav_series(
                    db, fund_id, config.start_date, config.end_date
                )
                if series.empty:
                    fund_result = await db.execute(
                        select(Fund.fund_name).where(Fund.id == fund_id)
                    )
                    name = fund_result.scalar_one_or_none() or f"#{fund_id}"
                    missing_assets.append(name)
                    continue
                nav_dict[asset_key] = series
                fund_result = await db.execute(
                    select(Fund.fund_name).where(Fund.id == fund_id)
                )
                fund_names[asset_key] = fund_result.scalar_one_or_none() or f"#{fund_id}"

            elif asset_key.startswith("idx_"):
                index_code = asset_key.split("_", 1)[1]
                series = await self._get_index_nav_series(
                    db, index_code, config.start_date, config.end_date
                )
                if series.empty:
                    bm_result = await db.execute(
                        select(Benchmark.index_name).where(
                            Benchmark.index_code == index_code
                        )
                    )
                    name = bm_result.scalar_one_or_none() or index_code
                    missing_assets.append(name)
                    continue
                nav_dict[asset_key] = series
                bm_result = await db.execute(
                    select(Benchmark.index_name).where(
                        Benchmark.index_code == index_code
                    )
                )
                fund_names[asset_key] = bm_result.scalar_one_or_none() or index_code

        if missing_assets and config.history_mode == "intersection":
            raise ValueError(
                f"以下资产在回测区间内无净值数据: {', '.join(missing_assets)}"
            )

        # --- 4. Build engine config and run ---
        engine_config = BacktestConfig(
            start_date=config.start_date,
            end_date=config.end_date,
            rebalance_freq=config.rebalance_frequency,
            transaction_cost_bps=config.transaction_cost_bps,
            freq_align_method=config.freq_align_method,
            risk_free_rate=config.risk_free_rate,
            history_mode=config.history_mode,
        )

        engine = BacktestEngine()
        result = await engine.run(engine_config, weights_map, nav_dict, trading_days)

        # --- 5. Build response data ---
        nav_list, drawdown_list = self._build_nav_and_drawdown(result.nav_series)
        monthly_returns = self._calc_monthly_returns(result.nav_series)

        # --- 6. Save to DB if portfolio_id provided ---
        bt_id = None
        if config.portfolio_id:
            bt_record = BacktestResultModel(
                portfolio_id=config.portfolio_id,
                config_json=config.model_dump(mode="json"),
                metrics_json=result.metrics,
                nav_series_json=result.nav_series,
                status="completed",
            )
            db.add(bt_record)
            await db.commit()
            bt_id = bt_record.id

        metrics = result.metrics
        return {
            "backtest_id": bt_id,
            "portfolio_id": config.portfolio_id,
            "config": config.model_dump(mode="json"),
            "history_mode": config.history_mode,
            "metrics": {
                "total_return": metrics.get("total_return"),
                "annualized_return": metrics.get("annualized_return"),
                "max_drawdown": metrics.get("max_drawdown"),
                "sharpe_ratio": metrics.get("sharpe_ratio"),
                "sortino_ratio": metrics.get("sortino_ratio"),
                "calmar_ratio": metrics.get("calmar_ratio"),
                "volatility": metrics.get("annualized_volatility"),
            },
            "nav_series": nav_list,
            "drawdown_series": drawdown_list,
            "monthly_returns": monthly_returns,
            "fund_names": fund_names,
            "excluded_funds": result.excluded_funds,
            "entry_log": result.entry_log,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _get_index_nav_series(
        db: AsyncSession,
        index_code: str,
        start_date: date,
        end_date: date,
    ) -> pd.Series:
        """Load index NAV series from the database."""
        result = await db.execute(
            select(IndexNav.nav_date, IndexNav.nav_value)
            .where(IndexNav.index_code == index_code)
            .where(IndexNav.nav_date >= start_date)
            .where(IndexNav.nav_date <= end_date)
            .order_by(IndexNav.nav_date)
        )
        rows = result.all()
        if not rows:
            return pd.Series(dtype=float)
        # Filter out rows with None nav_value
        pairs = [(r[0], float(r[1])) for r in rows if r[1] is not None]
        if not pairs:
            return pd.Series(dtype=float)
        dates, vals = zip(*pairs)
        return pd.Series(vals, index=pd.DatetimeIndex(dates), name=f"idx_{index_code}")

    @staticmethod
    def _build_nav_and_drawdown(
        nav_series: dict[str, float],
    ) -> tuple[list[dict], list[dict]]:
        """Build nav_list and drawdown_list from a nav_series dict."""
        nav_list = []
        drawdown_list = []
        peak = 0.0
        for date_str, nav in sorted(nav_series.items()):
            nav_list.append({"date": date_str, "nav": round(nav, 6)})
            peak = max(peak, nav)
            dd = (nav - peak) / peak if peak > 0 else 0.0
            drawdown_list.append({"date": date_str, "drawdown": round(dd, 6)})
        return nav_list, drawdown_list

    @staticmethod
    def _calc_monthly_returns(nav_series: dict[str, float]) -> list[dict]:
        """Compute monthly returns from the NAV series."""
        if not nav_series:
            return []

        sorted_items = sorted(nav_series.items())
        monthly: dict[tuple[int, int], list[tuple[str, float]]] = {}
        for date_str, nav in sorted_items:
            parts = date_str.split("-")
            year, month = int(parts[0]), int(parts[1])
            monthly.setdefault((year, month), []).append((date_str, nav))

        result = []
        prev_end_nav = None
        for (year, month), entries in sorted(monthly.items()):
            start_nav = entries[0][1]
            end_nav = entries[-1][1]
            if prev_end_nav is not None and prev_end_nav > 0:
                ret = (end_nav - prev_end_nav) / prev_end_nav
            elif prev_end_nav is None and start_nav > 0 and len(entries) > 1:
                ret = (end_nav - start_nav) / start_nav
            else:
                prev_end_nav = end_nav
                continue
            result.append({
                "year": year,
                "month": month,
                "return_pct": round(ret * 100, 4),
            })
            prev_end_nav = end_nav

        return result


portfolio_service = PortfolioService()
