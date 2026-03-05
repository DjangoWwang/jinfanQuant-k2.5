"""Fund pool management API endpoints."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.fund import PoolFundAdd
from app.services.pool_service import pool_service

router = APIRouter(prefix="/pools", tags=["pools"])


@router.get("/counts")
async def pool_counts(db: AsyncSession = Depends(get_db)):
    """Get fund count for each pool type."""
    counts = await pool_service.get_pool_counts(db)
    return {
        "basic": counts.get("basic", 0),
        "watch": counts.get("watch", 0),
        "investment": counts.get("investment", 0),
    }


@router.get("/{pool_type}")
async def list_pool_funds(
    pool_type: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    try:
        items, total = await pool_service.list_pool_funds(db, pool_type, page, page_size)
    except ValueError as e:
        raise HTTPException(400, str(e))

    return {"pool_type": pool_type, "total": total, "page": page, "page_size": page_size, "items": items}


@router.post("/{pool_type}/funds", status_code=201)
async def add_to_pool(
    pool_type: str,
    payload: PoolFundAdd,
    db: AsyncSession = Depends(get_db),
):
    try:
        entry = await pool_service.add_fund_to_pool(
            db, pool_type, payload.fund_id, payload.notes
        )
        await db.commit()
        return {"message": "已添加", "pool_type": pool_type, "fund_id": payload.fund_id}
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.delete("/{pool_type}/funds/{fund_id}", status_code=204)
async def remove_from_pool(
    pool_type: str,
    fund_id: int,
    db: AsyncSession = Depends(get_db),
):
    removed = await pool_service.remove_fund_from_pool(db, pool_type, fund_id)
    if not removed:
        raise HTTPException(404, "该基金不在此基金池中")
    await db.commit()
