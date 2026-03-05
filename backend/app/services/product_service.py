"""Business logic layer for FOF product management."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.importer.valuation_parser import ValuationParser
from app.schemas.product import (
    ProductCreate,
    ProductResponse,
    ProductUpdate,
    ValuationUploadResponse,
)

logger = logging.getLogger(__name__)


class ProductService:
    """Service for FOF product CRUD and valuation table processing."""

    def __init__(self) -> None:
        self._valuation_parser = ValuationParser()

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def create_product(
        self, db: AsyncSession, payload: ProductCreate
    ) -> ProductResponse:
        """Register a new FOF product.

        Args:
            db: Active database session.
            payload: Validated product data.

        Returns:
            The newly created product.
        """
        # TODO: Insert into products table.
        #   product = Product(**payload.model_dump())
        #   db.add(product)
        #   await db.commit()
        #   await db.refresh(product)
        #   return ProductResponse.model_validate(product)
        raise NotImplementedError

    async def get_product(
        self, db: AsyncSession, product_id: int
    ) -> ProductResponse | None:
        """Retrieve a single product by ID."""
        # TODO: select(Product).where(Product.id == product_id)
        raise NotImplementedError

    async def list_products(
        self, db: AsyncSession, skip: int = 0, limit: int = 50
    ) -> list[ProductResponse]:
        """List products with pagination."""
        raise NotImplementedError

    async def update_product(
        self, db: AsyncSession, product_id: int, payload: ProductUpdate
    ) -> ProductResponse | None:
        """Partially update a product."""
        raise NotImplementedError

    async def delete_product(self, db: AsyncSession, product_id: int) -> bool:
        """Delete a product and its associated valuation records."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Valuation table operations
    # ------------------------------------------------------------------

    async def process_valuation_upload(
        self,
        db: AsyncSession,
        product_id: int,
        file_path: str | Path,
    ) -> ValuationUploadResponse:
        """Parse an uploaded valuation table and persist the holdings.

        Args:
            db: Database session.
            product_id: The product this valuation belongs to.
            file_path: Path to the saved Excel file on disk.

        Returns:
            Parsed valuation summary with holding details.
        """
        parsed = self._valuation_parser.parse(file_path)

        warnings: list[str] = []
        if parsed["total_nav"] is None:
            warnings.append("Could not detect total NAV from the valuation table.")
        if parsed["valuation_date"] is None:
            warnings.append("Could not detect valuation date; please set manually.")

        # TODO: Persist holdings to the database.
        #   for holding in parsed["holdings"]:
        #       db.add(ValuationHolding(product_id=product_id, **holding))
        #   await db.commit()

        return ValuationUploadResponse(
            product_id=product_id,
            file_name=parsed["file_name"],
            valuation_date=parsed["valuation_date"],
            total_nav=parsed["total_nav"],
            holdings_count=len(parsed["holdings"]),
            holdings=parsed["holdings"],
            warnings=warnings,
        )

    async def get_latest_valuation(
        self, db: AsyncSession, product_id: int
    ) -> ValuationUploadResponse | None:
        """Retrieve the most recent valuation snapshot for a product."""
        # TODO: Query valuation_holdings table grouped by latest valuation_date.
        raise NotImplementedError
