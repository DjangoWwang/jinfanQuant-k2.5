from abc import ABC, abstractmethod


class BaseCrawler(ABC):
    """Abstract base class for all data crawlers."""

    @abstractmethod
    async def login(self) -> bool:
        """Authenticate with the data source.

        Returns:
            True if login succeeded, False otherwise.
        """
        ...

    @abstractmethod
    async def fetch_fund_list(
        self, strategy_type: str, limit: int = 50
    ) -> list[dict]:
        """Fetch a list of funds filtered by strategy type.

        Args:
            strategy_type: Strategy category (e.g. "stock_long", "macro", "cta").
            limit: Maximum number of results to return.

        Returns:
            List of dicts with fund metadata.
        """
        ...

    @abstractmethod
    async def fetch_nav_history(
        self, fund_id: str, start_date: str | None = None
    ) -> list[dict]:
        """Fetch historical NAV data for a specific fund.

        Args:
            fund_id: Unique identifier of the fund.
            start_date: Optional ISO-format date string (YYYY-MM-DD) to limit history.

        Returns:
            List of dicts with keys like nav_date, unit_nav, cumulative_nav.
        """
        ...

    @abstractmethod
    async def fetch_index_data(
        self, index_code: str, start_date: str | None = None
    ) -> list[dict]:
        """Fetch historical index data.

        Args:
            index_code: Index identifier (e.g. "hs300", "csi500").
            start_date: Optional ISO-format start date.

        Returns:
            List of dicts with keys like date, close, change_pct.
        """
        ...
