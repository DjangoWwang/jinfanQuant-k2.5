"""Business logic layer for portfolio management and backtesting."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.portfolio import (
    BacktestConfigSchema,
    BacktestResultResponse,
    PortfolioCreate,
    PortfolioResponse,
)

logger = logging.getLogger(__name__)


class PortfolioService:
    """Service for portfolio CRUD and backtest orchestration."""

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def create_portfolio(
        self, db: AsyncSession, payload: PortfolioCreate
    ) -> PortfolioResponse:
        """Create a new portfolio with its fund weight allocations.

        Args:
            db: Active database session.
            payload: Validated portfolio data including weights.

        Returns:
            The newly created portfolio.
        """
        # TODO: Insert portfolio row, then insert weight rows in a transaction.
        #   portfolio = Portfolio(name=payload.name, ...)
        #   db.add(portfolio)
        #   await db.flush()
        #   for w in payload.weights:
        #       db.add(PortfolioWeight(portfolio_id=portfolio.id, ...))
        #   await db.commit()
        raise NotImplementedError

    async def get_portfolio(
        self, db: AsyncSession, portfolio_id: int
    ) -> PortfolioResponse | None:
        """Retrieve a portfolio by ID, including its weight breakdown."""
        # TODO: Join portfolio + weights tables.
        raise NotImplementedError

    async def list_portfolios(
        self, db: AsyncSession, skip: int = 0, limit: int = 50
    ) -> list[PortfolioResponse]:
        """List all portfolios with pagination."""
        raise NotImplementedError

    async def update_weights(
        self,
        db: AsyncSession,
        portfolio_id: int,
        weights: list[dict[str, Any]],
    ) -> PortfolioResponse | None:
        """Replace the weight allocations for an existing portfolio.

        Returns the updated portfolio or None if not found.
        """
        # TODO: Delete old weights, insert new ones, commit.
        raise NotImplementedError

    async def delete_portfolio(self, db: AsyncSession, portfolio_id: int) -> bool:
        """Delete a portfolio and its weights.

        Returns True if the portfolio existed and was deleted.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Backtesting
    # ------------------------------------------------------------------

    async def run_backtest(
        self, db: AsyncSession, config: BacktestConfigSchema
    ) -> BacktestResultResponse:
        """Execute a historical backtest for the given configuration.

        High-level flow:
            1. Resolve fund weights (from portfolio_id or inline).
            2. Load NAV histories for all constituent funds.
            3. Align dates and compute weighted portfolio returns.
            4. Apply rebalancing at the specified frequency.
            5. Calculate performance metrics.
            6. Optionally load benchmark index data for comparison.

        Args:
            db: Database session for loading NAV data.
            config: Backtest parameters.

        Returns:
            Full backtest result including metrics and time series.
        """
        # TODO: Delegate heavy computation to app.engine.backtest module.
        raise NotImplementedError
