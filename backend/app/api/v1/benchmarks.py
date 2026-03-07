"""Benchmark index API endpoints."""

from typing import Optional
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.benchmark import Benchmark, IndexNav

router = APIRouter(prefix="/benchmarks", tags=["benchmarks"])


@router.get("/")
async def list_benchmarks(db: AsyncSession = Depends(get_db)):
    """List all available benchmark indices."""
    result = await db.execute(
        select(Benchmark.id, Benchmark.index_code, Benchmark.index_name, Benchmark.category)
        .where(Benchmark.is_active == True)
        .order_by(Benchmark.id)
    )
    return [
        {"id": r[0], "index_code": r[1], "index_name": r[2], "category": r[3]}
        for r in result.all()
    ]


@router.get("/{code}/nav")
async def benchmark_nav(
    code: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Get NAV history for a benchmark index."""
    # Verify benchmark exists
    bm = await db.execute(
        select(Benchmark).where(Benchmark.index_code == code)
    )
    if not bm.scalar_one_or_none():
        raise HTTPException(404, f"基准指数不存在: {code}")

    query = (
        select(IndexNav.nav_date, IndexNav.nav_value, IndexNav.daily_return)
        .where(IndexNav.index_code == code)
        .order_by(IndexNav.nav_date)
    )
    if start_date:
        query = query.where(IndexNav.nav_date >= date.fromisoformat(start_date))
    if end_date:
        query = query.where(IndexNav.nav_date <= date.fromisoformat(end_date))

    result = await db.execute(query)
    rows = result.all()

    return {
        "code": code,
        "total_count": len(rows),
        "records": [
            {
                "date": str(r[0]),
                "close": float(r[1]) if r[1] else None,
                "change_pct": float(r[2]) if r[2] else None,
            }
            for r in rows
        ],
    }
