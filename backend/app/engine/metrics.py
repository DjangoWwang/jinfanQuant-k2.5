"""Financial metrics calculation for NAV series.

All public functions accept a ``pandas.Series`` whose index is
``datetime.date`` (or ``DatetimeIndex``) and whose values are net-asset
values (NAV).
"""

from __future__ import annotations

import datetime
from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _daily_returns(nav_series: pd.Series) -> pd.Series:
    """Compute simple daily returns from a NAV series."""
    return nav_series.pct_change().dropna()


def _slice(
    nav_series: pd.Series,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
) -> pd.Series:
    """Slice the NAV series by optional start/end dates."""
    s = nav_series.sort_index()
    if start_date is not None:
        s = s[s.index >= pd.Timestamp(start_date)]
    if end_date is not None:
        s = s[s.index <= pd.Timestamp(end_date)]
    return s


# ---------------------------------------------------------------------------
# Core metrics
# ---------------------------------------------------------------------------

def calc_return(
    nav_series: pd.Series,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
) -> float:
    """Cumulative return over the (optionally sliced) period.

    ``return = nav_end / nav_start - 1``
    """
    s = _slice(nav_series, start_date, end_date)
    if len(s) < 2:
        return 0.0
    return float(s.iloc[-1] / s.iloc[0] - 1)


def calc_annualized_return(nav_series: pd.Series) -> float:
    """Annualised return assuming 252 trading days per year.

    ``annualized = (1 + total_return) ^ (252 / n_days) - 1``
    """
    s = nav_series.sort_index().dropna()
    if len(s) < 2:
        return 0.0
    total_return = s.iloc[-1] / s.iloc[0] - 1
    n_days = len(s) - 1  # number of return observations
    if n_days == 0:
        return 0.0
    ann = (1 + total_return) ** (252 / n_days) - 1
    return float(ann)


def calc_max_drawdown(
    nav_series: pd.Series,
) -> tuple[float, datetime.date, datetime.date]:
    """Maximum drawdown and the peak / trough dates.

    Returns ``(drawdown, peak_date, trough_date)`` where *drawdown* is a
    **negative** float (e.g. -0.15 for a 15 % drawdown).  If the series
    has fewer than 2 points, returns ``(0.0, first_date, first_date)``.
    """
    s = nav_series.sort_index().dropna()
    if len(s) < 2:
        d = s.index[0] if len(s) else datetime.date.today()
        if isinstance(d, pd.Timestamp):
            d = d.date()
        return (0.0, d, d)

    cummax = s.cummax()
    drawdown = (s - cummax) / cummax

    trough_idx = drawdown.idxmin()
    peak_idx = s.loc[:trough_idx].idxmax()

    peak_date = peak_idx.date() if isinstance(peak_idx, pd.Timestamp) else peak_idx
    trough_date = trough_idx.date() if isinstance(trough_idx, pd.Timestamp) else trough_idx

    return (float(drawdown.min()), peak_date, trough_date)


def calc_annualized_volatility(nav_series: pd.Series) -> float:
    """Annualised volatility (standard deviation of daily returns * sqrt(252))."""
    rets = _daily_returns(nav_series.sort_index().dropna())
    if len(rets) < 2:
        return 0.0
    return float(rets.std(ddof=1) * np.sqrt(252))


def calc_sharpe_ratio(
    nav_series: pd.Series, risk_free_rate: float = 0.025
) -> float:
    """Annualised Sharpe ratio.

    ``sharpe = (ann_return - risk_free_rate) / ann_volatility``
    """
    ann_ret = calc_annualized_return(nav_series)
    ann_vol = calc_annualized_volatility(nav_series)
    if ann_vol == 0.0:
        return 0.0
    return float((ann_ret - risk_free_rate) / ann_vol)


# ---------------------------------------------------------------------------
# Convenience: all-in-one
# ---------------------------------------------------------------------------

def calc_all_metrics(
    nav_series: pd.Series, risk_free_rate: float = 0.025
) -> dict:
    """Return a dict with all key performance metrics.

    Keys: ``total_return``, ``annualized_return``, ``max_drawdown``,
    ``max_dd_peak``, ``max_dd_trough``, ``annualized_volatility``,
    ``sharpe_ratio``.
    """
    total_ret = calc_return(nav_series)
    ann_ret = calc_annualized_return(nav_series)
    mdd, peak, trough = calc_max_drawdown(nav_series)
    ann_vol = calc_annualized_volatility(nav_series)
    sharpe = calc_sharpe_ratio(nav_series, risk_free_rate)

    return {
        "total_return": total_ret,
        "annualized_return": ann_ret,
        "max_drawdown": mdd,
        "max_dd_peak": peak.isoformat() if isinstance(peak, datetime.date) else str(peak),
        "max_dd_trough": trough.isoformat() if isinstance(trough, datetime.date) else str(trough),
        "annualized_volatility": ann_vol,
        "sharpe_ratio": sharpe,
    }
