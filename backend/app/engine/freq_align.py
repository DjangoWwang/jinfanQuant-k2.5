"""Daily / weekly frequency alignment utilities.

All functions work with ``pandas.Series`` objects whose index is
``datetime.date`` (or ``DatetimeIndex``) and whose values are NAV.
``trading_days`` is always a sorted list of ``datetime.date``.
"""

from __future__ import annotations

import datetime
from typing import Sequence

import pandas as pd


# ---------------------------------------------------------------------------
# Frequency detection
# ---------------------------------------------------------------------------

def detect_frequency(
    nav_series: pd.Series,
    trading_days: Sequence[datetime.date],
) -> str:
    """Heuristic frequency detection – returns ``"daily"`` or ``"weekly"``.

    If the median gap between consecutive observations (measured in
    trading days) is <= 2, the series is considered daily; otherwise
    weekly.
    """
    if len(nav_series) < 3:
        return "weekly"

    dates = sorted(nav_series.dropna().index)
    td_set = set(trading_days)

    gaps: list[int] = []
    for i in range(1, len(dates)):
        prev = dates[i - 1]
        curr = dates[i]
        if isinstance(prev, pd.Timestamp):
            prev = prev.date()
        if isinstance(curr, pd.Timestamp):
            curr = curr.date()
        # Count trading days between the two observation dates
        count = sum(
            1 for d in trading_days
            if prev < d <= curr and d in td_set
        )
        gaps.append(count)

    if not gaps:
        return "weekly"

    median_gap = sorted(gaps)[len(gaps) // 2]
    return "daily" if median_gap <= 2 else "weekly"


# ---------------------------------------------------------------------------
# Down-sample to weekly
# ---------------------------------------------------------------------------

def downsample_to_weekly(
    nav_series: pd.Series,
    trading_days: Sequence[datetime.date],
) -> pd.Series:
    """Down-sample a daily NAV series to weekly by keeping the last
    trading day of each ISO week.

    Non-trading-day observations are dropped first.
    """
    td_set = set(trading_days)

    # Normalise index to datetime.date
    idx = [
        d.date() if isinstance(d, pd.Timestamp) else d
        for d in nav_series.index
    ]
    s = pd.Series(nav_series.values, index=idx, name=nav_series.name)

    # Keep only trading-day observations
    s = s[s.index.map(lambda d: d in td_set)]

    if s.empty:
        return s

    # Group by (iso_year, iso_week) and take the last date in each group
    def _week_key(d: datetime.date) -> tuple[int, int]:
        iso = d.isocalendar()
        return (iso[0], iso[1])

    groups: dict[tuple[int, int], list[datetime.date]] = {}
    for d in sorted(s.index):
        key = _week_key(d)
        groups.setdefault(key, []).append(d)

    weekly_dates = [dates[-1] for dates in groups.values()]
    return s.loc[weekly_dates].sort_index()


# ---------------------------------------------------------------------------
# Interpolate to daily
# ---------------------------------------------------------------------------

def interpolate_to_daily(
    nav_series: pd.Series,
    trading_days: Sequence[datetime.date],
) -> pd.Series:
    """Linearly interpolate a (possibly weekly) NAV series onto a daily
    trading-day grid.
    """
    if nav_series.empty:
        return nav_series

    # Normalise index to datetime.date
    idx = [
        d.date() if isinstance(d, pd.Timestamp) else d
        for d in nav_series.index
    ]
    s = pd.Series(nav_series.values, index=pd.DatetimeIndex(idx), name=nav_series.name)

    # Build target daily grid limited to the series' range
    start, end = s.index.min(), s.index.max()
    daily_idx = pd.DatetimeIndex(
        [d for d in trading_days if start.date() <= d <= end.date()]
    )

    # Re-index and interpolate
    s = s.reindex(s.index.union(daily_idx)).sort_index()
    s = s.interpolate(method="index")
    # Keep only the trading day grid
    s = s.reindex(daily_idx)
    return s


# ---------------------------------------------------------------------------
# Align multiple fund series
# ---------------------------------------------------------------------------

def align_frequencies(
    nav_dict: dict[str, pd.Series],
    trading_days: Sequence[datetime.date],
    method: str = "downsample",
) -> dict[str, pd.Series]:
    """Align multiple fund NAV series to a common frequency.

    Parameters
    ----------
    nav_dict:
        Mapping of fund identifier to NAV ``Series``.
    trading_days:
        Sorted list of trading days from the calendar.
    method:
        ``"downsample"`` – convert all to weekly (lowest common freq).
        ``"interpolate"`` – convert all to daily via linear interpolation.

    Returns
    -------
    A new dict with the same keys, each value aligned to the chosen
    frequency.
    """
    result: dict[str, pd.Series] = {}

    if method == "interpolate":
        for key, series in nav_dict.items():
            freq = detect_frequency(series, trading_days)
            if freq == "weekly":
                result[key] = interpolate_to_daily(series, trading_days)
            else:
                result[key] = series
    else:
        # Default: downsample everything to weekly
        for key, series in nav_dict.items():
            freq = detect_frequency(series, trading_days)
            if freq == "daily":
                result[key] = downsample_to_weekly(series, trading_days)
            else:
                result[key] = series

    return result
