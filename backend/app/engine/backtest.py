"""Back-test engine for FOF portfolio simulation.

Produces a synthetic NAV series given a set of fund weights, their
historical NAV data, and a rebalancing schedule.
"""

from __future__ import annotations

import datetime
import logging
from typing import Optional, Sequence

import pandas as pd
from pydantic import BaseModel, Field

from app.engine.metrics import calc_all_metrics
from app.engine.freq_align import align_frequencies

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration & result models
# ---------------------------------------------------------------------------

class BacktestConfig(BaseModel):
    """Parameters that control a single back-test run."""

    start_date: datetime.date
    end_date: datetime.date
    rebalance_freq: str = Field(
        default="monthly",
        description="Rebalancing cadence: 'daily', 'weekly', 'monthly', 'quarterly'.",
    )
    transaction_cost_bps: float = Field(
        default=0.0,
        description="One-way transaction cost in basis points (e.g. 10 = 0.10 %).",
    )
    freq_align_method: str = Field(
        default="downsample",
        description="How to align mixed-frequency NAV series: 'downsample' or 'interpolate'.",
    )
    risk_free_rate: float = Field(
        default=0.02,
        description="Annual risk-free rate for Sharpe/Sortino (default 2%).",
    )
    history_mode: str = Field(
        default="intersection",
        description=(
            "How to handle funds with different history lengths: "
            "'intersection' (strict common dates), "
            "'dynamic_entry' (funds enter when data available), "
            "'truncate' (exclude funds without enough data)."
        ),
    )


class BacktestResult(BaseModel):
    """Output of a single back-test run."""

    nav_series: dict[str, float] = Field(
        default_factory=dict,
        description="Mapping of ISO-date string to portfolio NAV.",
    )
    metrics: dict = Field(default_factory=dict)
    config: BacktestConfig
    excluded_funds: list[str] = Field(
        default_factory=list,
        description="Funds excluded due to insufficient data (truncate mode).",
    )
    entry_log: list[dict] = Field(
        default_factory=list,
        description="Dynamic entry log: when each fund entered the portfolio.",
    )

    model_config = {"arbitrary_types_allowed": True}


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class BacktestEngine:
    """Simulate a portfolio's historical performance.

    Usage::

        engine = BacktestEngine()
        result = await engine.run(config, weights, nav_dict, trading_days)
    """

    # ----- rebalance schedule helpers -----

    @staticmethod
    def _build_rebalance_dates(
        dates: Sequence[datetime.date],
        freq: str,
    ) -> set[datetime.date]:
        """Return the set of dates on which the portfolio should rebalance."""
        if freq == "daily":
            # Exclude first day — initial portfolio construction, not rebalance.
            return set(dates[1:]) if len(dates) > 1 else set()

        rebalance: set[datetime.date] = set()
        if not dates:
            return rebalance

        # NOTE: Do NOT add dates[0] — the first day is initial portfolio
        # construction, not a rebalance. Transaction costs should not apply.
        prev = dates[0]

        for d in dates[1:]:
            trigger = False
            if freq == "weekly":
                trigger = d.isocalendar()[1] != prev.isocalendar()[1]
            elif freq == "monthly":
                trigger = d.month != prev.month
            elif freq == "quarterly":
                trigger = (d.month - 1) // 3 != (prev.month - 1) // 3
            if trigger:
                rebalance.add(d)
            prev = d

        return rebalance

    # ----- main entry point -----

    async def run(
        self,
        config: BacktestConfig,
        weights: dict[str, float],
        nav_dict: dict[str, pd.Series],
        trading_days: Sequence[datetime.date],
    ) -> BacktestResult:
        """Execute the back-test and return a :class:`BacktestResult`.

        Parameters
        ----------
        config:
            :class:`BacktestConfig` controlling the simulation.
        weights:
            Target allocation weights keyed by fund identifier.  Values
            should sum to 1.0.
        nav_dict:
            Raw NAV series for each fund (same keys as *weights*).
        trading_days:
            Sorted trading days from the calendar.
        """
        mode = config.history_mode
        logger.info(
            "Backtest start: %s → %s, mode=%s, funds=%d, rebalance=%s, cost=%.1fbps",
            config.start_date, config.end_date, mode,
            len(weights), config.rebalance_freq, config.transaction_cost_bps,
        )

        if mode == "dynamic_entry":
            return await self._run_dynamic_entry(config, weights, nav_dict, trading_days)

        # --- intersection / truncate modes ---

        # 1. Frequency-align all NAV series
        aligned = align_frequencies(nav_dict, trading_days, method=config.freq_align_method)

        if mode == "truncate":
            # Exclude funds whose data doesn't cover start_date
            aligned = self._truncate_filter(aligned, config.start_date)
            # Re-normalise weights for remaining funds
            remaining = set(aligned.keys())
            weights = {k: v for k, v in weights.items() if k in remaining}
            if not weights:
                return BacktestResult(
                    nav_series={}, metrics={}, config=config,
                    excluded_funds=list(set(nav_dict.keys()) - remaining),
                )
            total_w = sum(weights.values())
            weights = {k: v / total_w for k, v in weights.items()}

        # 2. Build a returns DataFrame from aligned NAV
        returns_df = pd.DataFrame({k: v.pct_change() for k, v in aligned.items()}).dropna()

        # Ensure index is DatetimeIndex for reliable comparison
        if not isinstance(returns_df.index, pd.DatetimeIndex):
            returns_df.index = pd.DatetimeIndex(returns_df.index)

        # 3. Filter to the back-test window
        start = pd.Timestamp(config.start_date)
        end = pd.Timestamp(config.end_date)
        returns_df = returns_df[(returns_df.index >= start) & (returns_df.index <= end)]

        if returns_df.empty:
            return BacktestResult(nav_series={}, metrics={}, config=config)

        # 4-8. Run the core simulation
        return self._simulate(config, weights, returns_df)

    # ----- dynamic entry mode -----

    async def _run_dynamic_entry(
        self,
        config: BacktestConfig,
        weights: dict[str, float],
        nav_dict: dict[str, pd.Series],
        trading_days: Sequence[datetime.date],
    ) -> BacktestResult:
        """Dynamic entry: funds join the portfolio when their data becomes available."""
        aligned = align_frequencies(nav_dict, trading_days, method=config.freq_align_method)

        # Build per-fund returns and track each fund's first available date
        fund_returns: dict[str, pd.Series] = {}
        fund_start_dates: dict[str, datetime.date] = {}
        for key, series in aligned.items():
            ret = series.pct_change().dropna()
            if ret.empty:
                continue
            idx = [d.date() if isinstance(d, pd.Timestamp) else d for d in ret.index]
            ret.index = pd.DatetimeIndex(idx)
            fund_returns[key] = ret
            fund_start_dates[key] = idx[0]

        if not fund_returns:
            return BacktestResult(nav_series={}, metrics={}, config=config)

        # Collect all dates within backtest window
        start = pd.Timestamp(config.start_date)
        end = pd.Timestamp(config.end_date)
        all_dates = sorted(set().union(*(set(r.index) for r in fund_returns.values())))
        sim_dates = [d for d in all_dates if start <= d <= end]

        if not sim_dates:
            return BacktestResult(nav_series={}, metrics={}, config=config)

        raw_dates = [d.date() if isinstance(d, pd.Timestamp) else d for d in sim_dates]
        rebalance_dates = self._build_rebalance_dates(raw_dates, config.rebalance_freq)
        cost_rate = config.transaction_cost_bps / 10_000

        portfolio_nav = [1.0]
        current_weights: dict[str, float] = {}
        active_funds: set[str] = set()
        entry_log: list[dict] = []
        is_first_day = True  # First entry is portfolio construction, no cost

        for ts in sim_dates:
            d = ts.date() if isinstance(ts, pd.Timestamp) else ts

            # Check for new funds entering
            # Use d > fund_start (not >=) to avoid point-in-time bias:
            # on fund_start day we only learn the fund exists; we can trade
            # into it starting from the next observation.
            newly_entered = []
            for fund_key in fund_returns:
                if fund_key not in active_funds:
                    fund_start = fund_start_dates[fund_key]
                    if d > fund_start:
                        newly_entered.append(fund_key)
                        active_funds.add(fund_key)

            # If new funds entered, rebalance to target weights (normalised to active set)
            entry_cost_turnover = 0.0
            if newly_entered:
                old_weights = dict(current_weights)
                active_w = {k: weights.get(k, 0.0) for k in active_funds}
                total_w = sum(active_w.values()) or 1.0
                current_weights = {k: v / total_w for k, v in active_w.items()}
                if is_first_day:
                    is_first_day = False  # no cost on initial build
                elif cost_rate > 0:
                    entry_cost_turnover = sum(
                        abs(old_weights.get(k, 0) - current_weights.get(k, 0))
                        for k in set(old_weights) | set(current_weights)
                    )
                entry_log.append({"date": d.isoformat(), "funds_entered": newly_entered})

            if not current_weights:
                portfolio_nav.append(portfolio_nav[-1])
                continue

            # Calculate portfolio return
            new_weights = {}
            port_return = 0.0
            for fund, w in current_weights.items():
                r = 0.0
                if fund in fund_returns and ts in fund_returns[fund].index:
                    r = fund_returns[fund].loc[ts]
                port_return += w * r
                new_weights[fund] = w * (1 + r)

            nav_today = portfolio_nav[-1] * (1 + port_return)
            # Apply entry cost on nav_today (consistent with scheduled rebalance)
            if entry_cost_turnover > 0:
                nav_today *= (1 - entry_cost_turnover * cost_rate)

            # Rebalance if scheduled (and no new entry already triggered rebalance)
            if d in rebalance_dates and not newly_entered:
                active_w = {k: weights.get(k, 0.0) for k in active_funds}
                total_w = sum(active_w.values()) or 1.0
                target = {k: v / total_w for k, v in active_w.items()}

                total_drift = sum(new_weights.values()) or 1.0
                drift_weights = {k: v / total_drift for k, v in new_weights.items()}
                turnover = sum(
                    abs(drift_weights.get(k, 0) - target.get(k, 0))
                    for k in set(drift_weights) | set(target)
                )
                nav_today *= (1 - turnover * cost_rate)
                current_weights = target
            else:
                total_w = sum(new_weights.values()) or 1.0
                current_weights = {k: v / total_w for k, v in new_weights.items()}

            portfolio_nav.append(nav_today)

        # Build result
        nav_index = [d.date() if isinstance(d, pd.Timestamp) else d for d in sim_dates]
        nav_s = pd.Series(portfolio_nav[1:], index=pd.DatetimeIndex(nav_index))
        metrics = calc_all_metrics(nav_s, risk_free_rate=config.risk_free_rate)
        nav_out = {d.isoformat(): float(v) for d, v in zip(nav_index, portfolio_nav[1:])}

        return BacktestResult(
            nav_series=nav_out, metrics=metrics, config=config,
            entry_log=entry_log,
        )

    # ----- truncate filter -----

    @staticmethod
    def _truncate_filter(
        aligned: dict[str, pd.Series],
        start_date: datetime.date,
    ) -> dict[str, pd.Series]:
        """Keep only funds whose data starts at or near start_date.

        Allow up to 7 calendar days of slack to handle weekends and holidays.
        """
        cutoff = start_date + datetime.timedelta(days=7)
        result = {}
        for key, series in aligned.items():
            s = series.dropna().sort_index()
            if s.empty:
                continue
            first = s.index[0]
            if isinstance(first, pd.Timestamp):
                first = first.date()
            if first <= cutoff:
                result[key] = series
        return result

    # ----- core simulation (shared by intersection & truncate) -----

    def _simulate(
        self,
        config: BacktestConfig,
        weights: dict[str, float],
        returns_df: pd.DataFrame,
    ) -> BacktestResult:
        """Core simulation loop used by intersection and truncate modes."""
        sim_dates = sorted(returns_df.index)
        raw_dates = [
            d.date() if isinstance(d, pd.Timestamp) else d for d in sim_dates
        ]
        rebalance_dates = self._build_rebalance_dates(raw_dates, config.rebalance_freq)

        cost_rate = config.transaction_cost_bps / 10_000
        current_weights = {k: weights.get(k, 0.0) for k in returns_df.columns}
        portfolio_nav = [1.0]

        for ts in sim_dates:
            d = ts.date() if isinstance(ts, pd.Timestamp) else ts
            day_returns = returns_df.loc[ts]

            new_weights = {}
            port_return = 0.0
            for fund, w in current_weights.items():
                r = day_returns.get(fund, 0.0)
                port_return += w * r
                new_weights[fund] = w * (1 + r)

            nav_today = portfolio_nav[-1] * (1 + port_return)

            if d in rebalance_dates:
                total_weight = sum(new_weights.values()) or 1.0
                drift_weights = {k: v / total_weight for k, v in new_weights.items()}
                # sum(|drift - target|) = double-sided turnover (buy + sell
                # mirror each other). Multiply by one-way cost_rate to get
                # total cost: each unit of turnover incurs cost on one side.
                turnover = sum(
                    abs(drift_weights.get(k, 0) - weights.get(k, 0))
                    for k in set(drift_weights) | set(weights)
                )
                nav_today *= (1 - turnover * cost_rate)
                current_weights = {k: weights.get(k, 0.0) for k in returns_df.columns}
            else:
                total_weight = sum(new_weights.values()) or 1.0
                current_weights = {k: v / total_weight for k, v in new_weights.items()}

            portfolio_nav.append(nav_today)

        nav_index = [
            d.date() if isinstance(d, pd.Timestamp) else d for d in sim_dates
        ]
        nav_s = pd.Series(portfolio_nav[1:], index=pd.DatetimeIndex(nav_index))
        metrics = calc_all_metrics(nav_s, risk_free_rate=config.risk_free_rate)
        nav_out = {d.isoformat(): float(v) for d, v in zip(nav_index, portfolio_nav[1:])}

        logger.info(
            "Backtest done: %d periods, final_nav=%.6f, return=%.4f%%, mdd=%.4f%%",
            len(nav_out), portfolio_nav[-1],
            metrics.get("total_return", 0) * 100,
            metrics.get("max_drawdown", 0) * 100,
        )
        return BacktestResult(nav_series=nav_out, metrics=metrics, config=config)
