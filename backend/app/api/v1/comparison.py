"""Fund comparison API endpoints."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.fund import CompareRequest
from app.services.comparison_service import comparison_service

router = APIRouter(prefix="/comparison", tags=["comparison"])


@router.post("/")
async def compare_funds(
    req: CompareRequest,
    db: AsyncSession = Depends(get_db),
):
    """Compare multiple funds with aligned NAV series and metrics.

    Supports date range via start_date/end_date or preset (ytd, 1y, 3y, etc.).
    Handles mixed frequency (daily/weekly) funds with configurable alignment.
    """
    result = await comparison_service.compare_funds(
        db=db,
        fund_ids=req.fund_ids,
        start_date=req.start_date,
        end_date=req.end_date,
        preset=req.preset,
        align_method=req.align_method,
    )

    if "error" in result:
        raise HTTPException(400, result["error"])

    return result
