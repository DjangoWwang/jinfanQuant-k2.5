"""Daily / weekly frequency alignment utilities.

All functions work with ``pandas.Series`` objects whose index is
``datetime.date`` (or ``DatetimeIndex``) and whose values are NAV.
``trading_days`` is always a sorted list of ``datetime.date``.
"""

from __future__ import annotations

import bisect
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
    # Ensure trading_days is a sorted list for bisect
    td_list = sorted(trading_days)

    gaps: list[int] = []
    for i in range(1, len(dates)):
        prev = dates[i - 1]
        curr = dates[i]
        if isinstance(prev, pd.Timestamp):
            prev = prev.date()
        if isinstance(curr, pd.Timestamp):
            curr = curr.date()
        # O(log n) count of trading days in (prev, curr] using bisect
        lo = bisect.bisect_right(td_list, prev)
        hi = bisect.bisect_right(td_list, curr)
        gaps.append(hi - lo)

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
    """Down-sample a NAV series to weekly, mapping each observation to
    the **last trading day of its ISO week** (from the trading calendar).

    This ensures that all funds—regardless of their original observation
    day—land on the same weekly grid, enabling apples-to-apples comparison.

    For a given ISO week the fund's NAV is the last observation within
    that week (forward-carried to the week's canonical end date).
    """
    td_set = set(trading_days)

    # Normalise index to datetime.date
    idx = [
        d.date() if isinstance(d, pd.Timestamp) else d
        for d in nav_series.index
    ]
    s = pd.Series(nav_series.values, index=idx, name=nav_series.name)

    if s.empty:
        return s

    def _week_key(d: datetime.date) -> tuple[int, int]:
        iso = d.isocalendar()
        return (iso[0], iso[1])

    # Build a mapping: ISO week → last trading day (from calendar)
    td_by_week: dict[tuple[int, int], datetime.date] = {}
    for d in trading_days:
        key = _week_key(d)
        if key not in td_by_week or d > td_by_week[key]:
            td_by_week[key] = d

    # Group the fund's observations by ISO week, take the last value
    obs_by_week: dict[tuple[int, int], float] = {}
    for d in sorted(s.index):
        key = _week_key(d)
        obs_by_week[key] = s[d]  # last write wins (sorted order)

    # Map each observation to its week's canonical last trading day
    result_dates = []
    result_values = []
    for week_key in sorted(obs_by_week):
        if week_key in td_by_week:
            result_dates.append(td_by_week[week_key])
            result_values.append(obs_by_week[week_key])

    if not result_dates:
        return pd.Series(dtype=float)

    return pd.Series(result_values, index=result_dates, name=nav_series.name)


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
    freqs = {
        key: detect_frequency(series, trading_days)
        for key, series in nav_dict.items()
    }
    has_mixed = len(set(freqs.values())) > 1

    result: dict[str, pd.Series] = {}

    if method == "interpolate":
        for key, series in nav_dict.items():
            if freqs[key] == "weekly":
                result[key] = interpolate_to_daily(series, trading_days)
            else:
                result[key] = series
    else:
        # Default: downsample everything to weekly if any is weekly.
        # Also normalise pure-weekly funds so all land on the same
        # "last trading day of the ISO week" grid (different weekly funds
        # may report on different weekdays).
        any_weekly = "weekly" in freqs.values()
        need_downsample = any_weekly or method == "downsample_force"
        for key, series in nav_dict.items():
            if need_downsample:
                result[key] = downsample_to_weekly(series, trading_days)
            else:
                result[key] = series

    return result


def align_to_common_dates(
    nav_dict: dict[str, pd.Series],
) -> dict[str, pd.Series]:
    """Align multiple NAV series to their common date intersection.

    All series are trimmed to the date range where all funds have data,
    then only dates present in ALL series are kept.
    This is essential for apples-to-apples comparison.
    """
    if not nav_dict:
        return {}

    # Find common date range (max of starts, min of ends)
    starts = []
    ends = []
    for s in nav_dict.values():
        s_clean = s.sort_index().dropna()
        if not s_clean.empty:
            starts.append(s_clean.index[0])
            ends.append(s_clean.index[-1])

    if not starts:
        return {k: pd.Series(dtype=float) for k in nav_dict}

    common_start = max(starts)
    common_end = min(ends)

    if common_start > common_end:
        return {k: pd.Series(dtype=float) for k in nav_dict}

    # Trim to common range
    trimmed = {}
    for key, series in nav_dict.items():
        s = series.sort_index().dropna()
        s = s[(s.index >= common_start) & (s.index <= common_end)]
        trimmed[key] = s

    # Find dates present in ALL series
    date_sets = [set(s.index) for s in trimmed.values()]
    common_dates = sorted(set.intersection(*date_sets))

    if not common_dates:
        return {k: pd.Series(dtype=float) for k in nav_dict}

    return {key: series.loc[common_dates] for key, series in trimmed.items()}


def detect_mixed_frequencies(
    nav_dict: dict[str, pd.Series],
    trading_days: Sequence[datetime.date],
) -> dict[str, str]:
    """Detect frequency of each series and return a mapping.

    Returns dict like {"fund_a": "daily", "fund_b": "weekly"}.
    """
    return {
        key: detect_frequency(series, trading_days)
        for key, series in nav_dict.items()
    }
