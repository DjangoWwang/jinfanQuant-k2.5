"""FOF99 fund list scraper."""

from __future__ import annotations

import logging
from typing import Any

from app.crawler.base import BaseCrawler
from app.crawler.fof99.client import Fof99Client

logger = logging.getLogger(__name__)

# Mapping of canonical strategy names to FOF99 platform category IDs.
STRATEGY_TYPE_MAP: dict[str, str] = {
    "stock_long": "1",
    "stock_long_short": "2",
    "market_neutral": "3",
    "event_driven": "4",
    "macro": "5",
    "cta": "6",
    "relative_value": "7",
    "fixed_income": "8",
    "composite": "9",
    "other": "10",
}


class FundScraper(BaseCrawler):
    """Scrapes fund list and related data from the FOF99 platform."""

    def __init__(self, client: Fof99Client) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # BaseCrawler interface
    # ------------------------------------------------------------------

    async def login(self) -> bool:
        """Delegate login to the underlying HTTP client.

        Callers should normally authenticate via ``Fof99Client.login()``
        before constructing a ``FundScraper``.
        """
        return self._client.is_authenticated

    async def fetch_fund_list(
        self, strategy_type: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Fetch a paginated list of funds for the given strategy.

        Args:
            strategy_type: One of the keys in ``STRATEGY_TYPE_MAP``.
            limit: Maximum number of funds to retrieve.

        Returns:
            List of dicts, each containing at minimum:
                - fund_id (str)
                - fund_name (str)
                - manager_name (str)
                - strategy (str)
                - inception_date (str | None)
        """
        category_id = STRATEGY_TYPE_MAP.get(strategy_type)
        if category_id is None:
            raise ValueError(
                f"Unknown strategy_type '{strategy_type}'. "
                f"Valid options: {list(STRATEGY_TYPE_MAP.keys())}"
            )

        # TODO: Implement actual scraping logic.
        #   1. Call self._client.get("/api/fund/list", params={...})
        #      or parse the HTML page at /fund/list?category=...
        #   2. Extract fund rows from the response / HTML table.
        #   3. Normalize into the dict structure described above.
        logger.info(
            "FundScraper.fetch_fund_list called strategy=%s limit=%d (stub)",
            strategy_type,
            limit,
        )
        return []

    async def fetch_nav_history(
        self, fund_id: str, start_date: str | None = None
    ) -> list[dict[str, Any]]:
        """Not the primary concern of FundScraper; delegates to NavScraper."""
        raise NotImplementedError(
            "Use NavScraper for NAV history retrieval."
        )

    async def fetch_index_data(
        self, index_code: str, start_date: str | None = None
    ) -> list[dict[str, Any]]:
        """Not the primary concern of FundScraper; delegates to IndexScraper."""
        raise NotImplementedError(
            "Use IndexScraper for index data retrieval."
        )
