"""Financial metrics calculation for NAV series.

All public functions accept a ``pandas.Series`` whose index is
``datetime.date`` (or ``DatetimeIndex``) and whose values are net-asset
values (NAV).
"""

from __future__ import annotations

import calendar
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


def _annualization_factor(nav_series: pd.Series) -> float:
    """Compute annualization factor using 365 / avg_calendar_gap.

    This follows the 火富牛 convention: 365 / average calendar-day gap
    between observations.  Falls back to 252 if too few data points.
    """
    s = nav_series.sort_index().dropna()
    if len(s) < 2:
        return 365.0  # fallback: assume daily with ~1 day gap
    deltas = pd.Series(s.index).diff().dropna().dt.days
    avg_gap = float(deltas.mean())
    if avg_gap <= 0:
        return 365.0
    return 365.0 / avg_gap


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


def calc_annualized_return(
    nav_series: pd.Series,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
) -> float:
    """Annualised return, auto-detecting frequency.

    ``annualized = (1 + total_return) ^ (ann_factor / n_periods) - 1``
    """
    s = _slice(nav_series, start_date, end_date).sort_index().dropna()
    if len(s) < 2:
        return 0.0
    total_return = s.iloc[-1] / s.iloc[0] - 1
    n_periods = len(s) - 1
    if n_periods == 0:
        return 0.0
    ann_factor = _annualization_factor(s)
    base = 1 + total_return
    exp = ann_factor / n_periods
    # Guard against negative base (total loss > 100%), which would make
    # fractional exponentiation produce complex numbers.
    if base <= 0:
        return -1.0
    # Guard against blow-up: when very few data points (e.g. 2-3 NAVs),
    # exp can be huge (365/1=365), making 1.05^365 astronomical.
    # Cap the annualized return at +/- 9999% (ratio ±99.99).
    ann = base ** exp - 1
    ann = max(min(ann, 99.99), -0.9999)
    return float(ann)


def calc_max_drawdown(
    nav_series: pd.Series,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
) -> tuple[float, datetime.date, datetime.date]:
    """Maximum drawdown and the peak / trough dates.

    Returns ``(drawdown, peak_date, trough_date)`` where *drawdown* is a
    **negative** float (e.g. -0.15 for a 15% drawdown).
    """
    s = _slice(nav_series, start_date, end_date).sort_index().dropna()
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


def calc_drawdown_series(nav_series: pd.Series) -> pd.Series:
    """Compute the full drawdown series (values <= 0)."""
    s = nav_series.sort_index().dropna()
    if len(s) < 2:
        return pd.Series(dtype=float)
    cummax = s.cummax()
    return (s - cummax) / cummax


def calc_annualized_volatility(
    nav_series: pd.Series,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
) -> float:
    """Annualised volatility (std of returns * sqrt(ann_factor))."""
    s = _slice(nav_series, start_date, end_date).sort_index().dropna()
    rets = _daily_returns(s)
    if len(rets) < 2:
        return 0.0
    ann_factor = _annualization_factor(s)
    return float(rets.std(ddof=1) * np.sqrt(ann_factor))


def calc_downside_deviation(
    nav_series: pd.Series,
    target_return: float = 0.0,
) -> float:
    """Annualised downside deviation following 火富牛 convention.

    Uses ``min(Xi - rf_period, 0)`` clipping on ALL observations,
    denominator is ``n - 1`` (full sample size, not just negative count).

    ``DD = sqrt( sum(min(Xi-rf,0)^2) / (n-1) ) * sqrt(ann_factor)``
    """
    s = nav_series.sort_index().dropna()
    rets = _daily_returns(s)
    n = len(rets)
    if n < 2:
        return 0.0
    ann_factor = _annualization_factor(s)
    daily_target = (1 + target_return) ** (1 / ann_factor) - 1
    # Clip: min(Xi - rf, 0) — zeros for returns above target
    clipped = np.minimum(rets - daily_target, 0.0)
    return float(np.sqrt((clipped**2).sum() / (n - 1)) * np.sqrt(ann_factor))


def calc_sharpe_ratio(
    nav_series: pd.Series,
    risk_free_rate: float = 0.02,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
) -> float:
    """Annualised Sharpe ratio.

    ``sharpe = (ann_return - risk_free_rate) / ann_volatility``
    """
    ann_ret = calc_annualized_return(nav_series, start_date, end_date)
    ann_vol = calc_annualized_volatility(nav_series, start_date, end_date)
    if ann_vol == 0.0:
        return 0.0
    return float((ann_ret - risk_free_rate) / ann_vol)


def calc_sortino_ratio(
    nav_series: pd.Series,
    risk_free_rate: float = 0.02,
) -> float:
    """Annualised Sortino ratio.

    ``sortino = (ann_return - risk_free_rate) / downside_deviation``

    Returns 0 if insufficient data, or a capped large value when
    downside deviation is zero (all returns above target).
    """
    ann_ret = calc_annualized_return(nav_series)
    dd = calc_downside_deviation(nav_series, target_return=risk_free_rate)
    if dd == 0.0:
        if ann_ret > risk_free_rate:
            return 99.99  # No downside risk, positive excess return
        return 0.0
    return float((ann_ret - risk_free_rate) / dd)


def calc_calmar_ratio(nav_series: pd.Series) -> float:
    """Calmar ratio = annualized_return / abs(max_drawdown).

    Returns 0 if max drawdown is zero.
    """
    ann_ret = calc_annualized_return(nav_series)
    mdd, _, _ = calc_max_drawdown(nav_series)
    if mdd == 0.0:
        return 0.0
    return float(ann_ret / abs(mdd))


# ---------------------------------------------------------------------------
# Normalized NAV (for comparison charts)
# ---------------------------------------------------------------------------

def normalize_nav(
    nav_series: pd.Series,
    base: float = 1.0,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
) -> pd.Series:
    """Rebase a NAV series so the first value equals *base*.

    Useful for multi-fund comparison charts where each series
    starts at 1.0 on the same date.
    """
    s = _slice(nav_series, start_date, end_date).sort_index().dropna()
    if s.empty:
        return s
    return s / s.iloc[0] * base


# ---------------------------------------------------------------------------
# Interval helpers (for preset date ranges)
# ---------------------------------------------------------------------------

def interval_dates(
    preset: str,
    reference_date: Optional[datetime.date] = None,
) -> tuple[datetime.date, datetime.date]:
    """Compute (start_date, end_date) for common presets.

    Presets: "wtd" (week-to-date), "mtd" (month-to-date),
    "qtd" (quarter-to-date), "ytd" (year-to-date),
    "1m", "3m", "6m", "1y", "2y", "3y", "5y", "inception".
    """
    today = reference_date or datetime.date.today()

    if preset == "wtd":
        start = today - datetime.timedelta(days=today.weekday())
        return (start, today)
    elif preset == "mtd":
        return (today.replace(day=1), today)
    elif preset == "qtd":
        q_month = ((today.month - 1) // 3) * 3 + 1
        return (today.replace(month=q_month, day=1), today)
    elif preset == "ytd":
        return (today.replace(month=1, day=1), today)

    months_map = {"1m": 1, "3m": 3, "6m": 6, "1y": 12, "2y": 24, "3y": 36, "5y": 60}
    if preset in months_map:
        months = months_map[preset]
        y = today.year
        m = today.month - months
        while m <= 0:
            m += 12
            y -= 1
        max_day = calendar.monthrange(y, m)[1]
        d = min(today.day, max_day)
        return (datetime.date(y, m, d), today)

    if preset == "inception":
        return (datetime.date(1970, 1, 1), today)

    raise ValueError(f"Unknown preset: {preset}")


# ---------------------------------------------------------------------------
# Win rate & new-high weeks
# ---------------------------------------------------------------------------

def calc_win_rate(nav_series: pd.Series, freq: str = "M") -> float:
    """Win rate: percentage of positive-return periods.

    *freq*: ``"M"`` for monthly, ``"Q"`` for quarterly.
    """
    if freq not in ("M", "Q"):
        raise ValueError("freq must be 'M' (monthly) or 'Q' (quarterly)")
    s = nav_series.sort_index().dropna()
    if len(s) < 2:
        return 0.0
    rule = "ME" if freq == "M" else "QE"
    resampled = s.resample(rule).last().dropna()
    if len(resampled) < 2:
        return 0.0
    rets = resampled.pct_change().dropna()
    if len(rets) == 0:
        return 0.0
    return float((rets > 0).sum() / len(rets))


def calc_new_high_weeks(nav_series: pd.Series) -> int:
    """Count weeks where NAV reached a new all-time high (excluding first week)."""
    s = nav_series.sort_index().dropna()
    if len(s) < 2:
        return 0
    weekly = s.resample("W").last().dropna()
    if len(weekly) < 2:
        return 0
    prev_cummax = weekly.cummax().shift(1)
    # A week is "new high" if its value strictly exceeds previous cumulative max
    return int((weekly > prev_cummax).sum())


# ---------------------------------------------------------------------------
# Convenience: all-in-one
# ---------------------------------------------------------------------------

def calc_all_metrics(
    nav_series: pd.Series,
    risk_free_rate: float = 0.02,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
) -> dict:
    """Return a dict with all key performance metrics.

    Supports optional date slicing for interval calculations.
    """
    s = _slice(nav_series, start_date, end_date)
    total_ret = calc_return(s)
    ann_ret = calc_annualized_return(s)
    mdd, peak, trough = calc_max_drawdown(s)
    ann_vol = calc_annualized_volatility(s)
    sharpe = calc_sharpe_ratio(s, risk_free_rate)
    sortino = calc_sortino_ratio(s, risk_free_rate)
    calmar = calc_calmar_ratio(s)

    monthly_win = calc_win_rate(s, "M")
    quarterly_win = calc_win_rate(s, "Q")
    new_highs = calc_new_high_weeks(s)
    ret_dd_ratio = float(abs(ann_ret / mdd)) if mdd != 0 else (99.99 if ann_ret > 0 else 0.0)

    return {
        "total_return": total_ret,
        "annualized_return": ann_ret,
        "max_drawdown": mdd,
        "max_dd_peak": peak.isoformat() if isinstance(peak, datetime.date) else str(peak),
        "max_dd_trough": trough.isoformat() if isinstance(trough, datetime.date) else str(trough),
        "annualized_volatility": ann_vol,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "calmar_ratio": calmar,
        "monthly_win_rate": monthly_win,
        "quarterly_win_rate": quarterly_win,
        "new_high_weeks": new_highs,
        "return_drawdown_ratio": ret_dd_ratio,
    }


def calc_interval_metrics(
    nav_series: pd.Series,
    presets: list[str],
    risk_free_rate: float = 0.02,
    reference_date: Optional[datetime.date] = None,
) -> dict[str, dict]:
    """Compute metrics for multiple interval presets at once.

    Returns a dict mapping preset name to its metrics dict.
    """
    result = {}
    for preset in presets:
        try:
            sd, ed = interval_dates(preset, reference_date)
            s = _slice(nav_series, sd, ed)
            if len(s) >= 2:
                result[preset] = calc_all_metrics(s, risk_free_rate)
            else:
                result[preset] = None
        except ValueError:
            result[preset] = None
    return result
