"""Valuation routes — consolidated into products.py.

This module is kept empty to avoid import errors in router.py.
All valuation endpoints are now under /products/{product_id}/valuation*.
"""

from fastapi import APIRouter

router = APIRouter(tags=["valuation"])
