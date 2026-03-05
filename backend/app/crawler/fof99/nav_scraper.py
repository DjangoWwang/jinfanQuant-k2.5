"""FOF99 NAV (net asset value) history scraper."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from app.crawler.base import BaseCrawler
from app.crawler.fof99.client import Fof99Client

logger = logging.getLogger(__name__)


def detect_frequency(nav_records: list[dict[str, Any]]) -> str:
    """Heuristically detect the reporting frequency of NAV records.

    Args:
        nav_records: Sorted list of NAV dicts, each containing a ``nav_date``
            key with an ISO-format date string (YYYY-MM-DD).

    Returns:
        One of "daily", "weekly", "monthly", or "irregular".
    """
    if len(nav_records) < 3:
        return "irregular"

    deltas: list[int] = []
    for i in range(1, min(len(nav_records), 20)):
        d1 = date.fromisoformat(nav_records[i - 1]["nav_date"])
        d2 = date.fromisoformat(nav_records[i]["nav_date"])
        deltas.append(abs((d2 - d1).days))

    if not deltas:
        return "irregular"

    avg_delta = sum(deltas) / len(deltas)

    if avg_delta <= 2:
        return "daily"
    elif avg_delta <= 8:
        return "weekly"
    elif avg_delta <= 35:
        return "monthly"
    else:
        return "irregular"


class NavScraper(BaseCrawler):
    """Scrapes historical NAV data for individual funds from FOF99."""

    def __init__(self, client: Fof99Client) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # BaseCrawler interface
    # ------------------------------------------------------------------

    async def login(self) -> bool:
        return self._client.is_authenticated

    async def fetch_fund_list(
        self, strategy_type: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        raise NotImplementedError("Use FundScraper for fund list retrieval.")

    async def fetch_nav_history(
        self, fund_id: str, start_date: str | None = None
    ) -> list[dict[str, Any]]:
        """Fetch the NAV history of a single fund.

        Args:
            fund_id: The platform-specific fund identifier.
            start_date: Optional ISO date to limit how far back to fetch.

        Returns:
            List of dicts ordered by date ascending, each containing:
                - nav_date (str): ISO date
                - unit_nav (float): Unit net asset value
                - cumulative_nav (float): Cumulative NAV
                - change_pct (float | None): Period return percentage
        """
        params: dict[str, Any] = {"fund_id": fund_id}
        if start_date is not None:
            params["start_date"] = start_date

        # TODO: Implement actual scraping logic.
        #   1. Request the NAV page / API endpoint for the given fund.
        #   2. Parse the HTML table or JSON response.
        #   3. Normalize rows into the dict structure above.
        #   4. Handle pagination if the platform splits history into pages.
        logger.info(
            "NavScraper.fetch_nav_history called fund_id=%s start=%s (stub)",
            fund_id,
            start_date,
        )
        return []

    async def fetch_index_data(
        self, index_code: str, start_date: str | None = None
    ) -> list[dict[str, Any]]:
        raise NotImplementedError("Use IndexScraper for index data retrieval.")
