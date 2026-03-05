"""Back-test engine for FOF portfolio simulation.

Produces a synthetic NAV series given a set of fund weights, their
historical NAV data, and a rebalancing schedule.
"""

from __future__ import annotations

import datetime
from typing import Optional, Sequence

import pandas as pd
from pydantic import BaseModel, Field

from app.engine.metrics import calc_all_metrics
from app.engine.freq_align import align_frequencies


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


class BacktestResult(BaseModel):
    """Output of a single back-test run."""

    nav_series: dict[str, float] = Field(
        default_factory=dict,
        description="Mapping of ISO-date string to portfolio NAV.",
    )
    metrics: dict = Field(default_factory=dict)
    config: BacktestConfig

    class Config:
        arbitrary_types_allowed = True


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
            return set(dates)

        rebalance: set[datetime.date] = set()
        if not dates:
            return rebalance

        prev = dates[0]
        rebalance.add(prev)

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
        # 1. Frequency-align all NAV series
        aligned = align_frequencies(nav_dict, trading_days, method=config.freq_align_method)

        # 2. Build a returns DataFrame from aligned NAV
        returns_df = pd.DataFrame({k: v.pct_change() for k, v in aligned.items()}).dropna()

        # 3. Filter to the back-test window
        start = pd.Timestamp(config.start_date)
        end = pd.Timestamp(config.end_date)
        returns_df = returns_df[(returns_df.index >= start) & (returns_df.index <= end)]

        if returns_df.empty:
            return BacktestResult(nav_series={}, metrics={}, config=config)

        # 4. Determine rebalance schedule
        sim_dates = sorted(returns_df.index)
        raw_dates = [
            d.date() if isinstance(d, pd.Timestamp) else d for d in sim_dates
        ]
        rebalance_dates = self._build_rebalance_dates(raw_dates, config.rebalance_freq)

        # 5. Simulate
        cost_rate = config.transaction_cost_bps / 10_000
        current_weights = {k: weights.get(k, 0.0) for k in returns_df.columns}
        portfolio_nav = [1.0]

        for ts in sim_dates:
            d = ts.date() if isinstance(ts, pd.Timestamp) else ts
            day_returns = returns_df.loc[ts]

            # Grow each position by its daily return
            new_weights = {}
            port_return = 0.0
            for fund, w in current_weights.items():
                r = day_returns.get(fund, 0.0)
                port_return += w * r
                new_weights[fund] = w * (1 + r)

            nav_today = portfolio_nav[-1] * (1 + port_return)

            # Rebalance if scheduled
            if d in rebalance_dates:
                # Transaction cost = sum of absolute weight changes * cost_rate
                total_weight = sum(new_weights.values()) or 1.0
                drift_weights = {k: v / total_weight for k, v in new_weights.items()}
                turnover = sum(
                    abs(drift_weights.get(k, 0) - weights.get(k, 0))
                    for k in set(drift_weights) | set(weights)
                )
                nav_today *= (1 - turnover * cost_rate)
                current_weights = {k: weights.get(k, 0.0) for k in returns_df.columns}
            else:
                # Drift weights naturally
                total_weight = sum(new_weights.values()) or 1.0
                current_weights = {k: v / total_weight for k, v in new_weights.items()}

            portfolio_nav.append(nav_today)

        # 6. Build NAV series (skip the seed value at index 0)
        nav_index = [
            d.date() if isinstance(d, pd.Timestamp) else d for d in sim_dates
        ]
        nav_s = pd.Series(portfolio_nav[1:], index=pd.DatetimeIndex(nav_index))

        # 7. Compute metrics
        metrics = calc_all_metrics(nav_s)

        # 8. Pack result
        nav_out = {d.isoformat(): float(v) for d, v in zip(nav_index, portfolio_nav[1:])}

        return BacktestResult(nav_series=nav_out, metrics=metrics, config=config)
