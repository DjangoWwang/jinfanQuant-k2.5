"""FOF99 market index data scraper."""

from __future__ import annotations

import logging
from typing import Any

from app.crawler.base import BaseCrawler
from app.crawler.fof99.client import Fof99Client

logger = logging.getLogger(__name__)

# Well-known index codes supported by the platform.
KNOWN_INDICES: dict[str, str] = {
    "hs300": "沪深300",
    "csi500": "中证500",
    "csi1000": "中证1000",
    "gem": "创业板指",
    "sse50": "上证50",
    "bond_total": "中债综合指数",
    "south_china_cta": "南华商品指数",
}


class IndexScraper(BaseCrawler):
    """Scrapes benchmark / market index data from FOF99."""

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
        raise NotImplementedError("Use NavScraper for fund NAV history.")

    async def fetch_index_data(
        self, index_code: str, start_date: str | None = None
    ) -> list[dict[str, Any]]:
        """Fetch historical closing data for a market index.

        Args:
            index_code: One of the keys in ``KNOWN_INDICES``
                (e.g. "hs300", "csi500").
            start_date: Optional ISO date (YYYY-MM-DD) to limit history.

        Returns:
            List of dicts ordered by date ascending, each containing:
                - date (str): ISO date
                - close (float): Closing value
                - change_pct (float | None): Daily change percentage
        """
        if index_code not in KNOWN_INDICES:
            logger.warning(
                "Index code '%s' is not in KNOWN_INDICES; proceeding anyway.",
                index_code,
            )

        params: dict[str, Any] = {"index_code": index_code}
        if start_date is not None:
            params["start_date"] = start_date

        # TODO: Implement actual scraping logic.
        #   1. Request the index data page / API endpoint.
        #   2. Parse JSON or HTML table rows.
        #   3. Normalize into the dict structure above.
        logger.info(
            "IndexScraper.fetch_index_data called index=%s start=%s (stub)",
            index_code,
            start_date,
        )
        return []
