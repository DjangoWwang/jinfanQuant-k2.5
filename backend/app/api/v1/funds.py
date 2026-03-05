"""Fund CRUD API endpoints."""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.fund import (
    FundCreate, FundUpdate, FundResponse, FundListResponse,
    FundListParams, NavHistoryResponse, NavRecord, MetricsResponse,
)
from app.services.fund_service import fund_service
from app.engine.metrics import calc_all_metrics

router = APIRouter(prefix="/funds", tags=["funds"])


@router.get("/", response_model=FundListResponse)
async def list_funds(
    strategy_type: Optional[str] = Query(None),
    strategy_sub: Optional[str] = Query(None),
    nav_frequency: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    params = FundListParams(
        strategy_type=strategy_type,
        strategy_sub=strategy_sub,
        nav_frequency=nav_frequency,
        search=search,
        page=page,
        page_size=page_size,
    )
    funds, total = await fund_service.list_funds(db, params)
    return FundListResponse(
        total=total,
        page=page,
        page_size=page_size,
        items=[FundResponse.model_validate(f) for f in funds],
    )


@router.get("/{fund_id}", response_model=FundResponse)
async def get_fund(fund_id: int, db: AsyncSession = Depends(get_db)):
    fund = await fund_service.get_fund(db, fund_id)
    if not fund:
        raise HTTPException(404, "基金不存在")
    return FundResponse.model_validate(fund)


@router.post("/", response_model=FundResponse, status_code=201)
async def create_fund(payload: FundCreate, db: AsyncSession = Depends(get_db)):
    fund = await fund_service.create_fund(db, payload)
    await db.commit()
    return FundResponse.model_validate(fund)


@router.patch("/{fund_id}", response_model=FundResponse)
async def update_fund(
    fund_id: int, payload: FundUpdate, db: AsyncSession = Depends(get_db)
):
    fund = await fund_service.update_fund(db, fund_id, payload)
    if not fund:
        raise HTTPException(404, "基金不存在")
    await db.commit()
    return FundResponse.model_validate(fund)


@router.delete("/{fund_id}", status_code=204)
async def delete_fund(fund_id: int, db: AsyncSession = Depends(get_db)):
    deleted = await fund_service.delete_fund(db, fund_id)
    if not deleted:
        raise HTTPException(404, "基金不存在")
    await db.commit()


@router.get("/{fund_id}/nav", response_model=NavHistoryResponse)
async def get_nav_history(
    fund_id: int,
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    fund = await fund_service.get_fund(db, fund_id)
    if not fund:
        raise HTTPException(404, "基金不存在")

    records = await fund_service.get_nav_history(db, fund_id, start_date, end_date)
    return NavHistoryResponse(
        fund_id=fund_id,
        fund_name=fund.fund_name,
        frequency=fund.nav_frequency,
        records=[NavRecord.model_validate(r) for r in records],
        total_count=len(records),
    )


@router.get("/{fund_id}/metrics", response_model=MetricsResponse)
async def get_fund_metrics(
    fund_id: int,
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    preset: Optional[str] = Query(None, description="ytd, 1y, 3y, inception..."),
    risk_free_rate: float = Query(0.025),
    db: AsyncSession = Depends(get_db),
):
    fund = await fund_service.get_fund(db, fund_id)
    if not fund:
        raise HTTPException(404, "基金不存在")

    if preset and not start_date:
        from app.engine.metrics import interval_dates
        start_date, end_date = interval_dates(preset)

    series = await fund_service.get_nav_series(db, fund_id, start_date, end_date)
    if series.empty:
        raise HTTPException(404, "该基金无净值数据")

    m = calc_all_metrics(series, risk_free_rate)
    first_date = series.index[0]
    last_date = series.index[-1]

    return MetricsResponse(
        fund_id=fund_id,
        fund_name=fund.fund_name,
        start_date=first_date.date() if hasattr(first_date, 'date') else first_date,
        end_date=last_date.date() if hasattr(last_date, 'date') else last_date,
        **{k: v for k, v in m.items() if k not in ("max_dd_peak", "max_dd_trough")},
        max_dd_peak=m.get("max_dd_peak"),
        max_dd_trough=m.get("max_dd_trough"),
    )
