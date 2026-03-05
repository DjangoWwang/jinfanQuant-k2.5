"""Trading calendar utilities for A-share market (XSHG).

Uses the exchange_calendars package to populate and query
the trading calendar stored in the TradingCalendar model.
"""

from __future__ import annotations

import datetime
from typing import Sequence

import exchange_calendars as xcals
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.calendar import TradingCalendar


def _get_xshg_sessions(
    start_year: int = 2016, end_year: int = 2027
) -> list[datetime.date]:
    """Return all XSHG trading sessions between *start_year* and *end_year*."""
    cal = xcals.get_calendar("XSHG")
    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)
    # exchange_calendars uses pandas Timestamps internally
    sessions = cal.sessions_in_range(
        start.isoformat(), end.isoformat()
    )
    return [s.date() for s in sessions]


async def fill_trading_calendar(
    session: AsyncSession,
    start_year: int = 2016,
    end_year: int = 2027,
) -> int:
    """Populate the ``trading_calendar`` table with XSHG sessions.

    All dates in the range are inserted: trading days get
    ``is_trading_day=True``, the rest get ``is_trading_day=False``.

    Returns the total number of rows inserted.
    """
    trading_dates = set(_get_xshg_sessions(start_year, end_year))

    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)

    # Build the full date range
    all_dates: list[datetime.date] = []
    current = start
    while current <= end:
        all_dates.append(current)
        current += datetime.timedelta(days=1)

    # Clear existing rows in range to allow idempotent re-runs
    await session.execute(
        delete(TradingCalendar).where(
            TradingCalendar.cal_date >= start,
            TradingCalendar.cal_date <= end,
        )
    )

    # Bulk insert
    rows = [
        TradingCalendar(
            cal_date=d,
            is_trading_day=(d in trading_dates),
            market="CN",
        )
        for d in all_dates
    ]
    session.add_all(rows)
    await session.flush()
    return len(rows)


async def is_trading_day(
    session: AsyncSession, date: datetime.date
) -> bool:
    """Check whether *date* is a trading day in the calendar."""
    result = await session.execute(
        select(TradingCalendar.is_trading_day).where(
            TradingCalendar.cal_date == date
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        # Fallback: use exchange_calendars directly
        cal = xcals.get_calendar("XSHG")
        return cal.is_session(date.isoformat())
    return bool(row)


async def get_trading_days(
    session: AsyncSession,
    start_date: datetime.date,
    end_date: datetime.date,
) -> Sequence[datetime.date]:
    """Return a sorted list of trading days between *start_date* and *end_date* (inclusive)."""
    result = await session.execute(
        select(TradingCalendar.cal_date)
        .where(
            TradingCalendar.cal_date >= start_date,
            TradingCalendar.cal_date <= end_date,
            TradingCalendar.is_trading_day.is_(True),
        )
        .order_by(TradingCalendar.cal_date)
    )
    return [row[0] for row in result.all()]
