"""Portfolio CRUD API endpoints."""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.portfolio import Portfolio, PortfolioAllocation
from app.models.fund import Fund
from app.models.benchmark import Benchmark
from app.schemas.portfolio import PortfolioCreate, PortfolioWeight

router = APIRouter(prefix="/portfolios", tags=["portfolios"])


@router.get("/")
async def list_portfolios(
    portfolio_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    query = select(Portfolio).where(Portfolio.is_active.is_(True))
    count_query = select(func.count(Portfolio.id)).where(Portfolio.is_active.is_(True))

    if portfolio_type:
        query = query.where(Portfolio.portfolio_type == portfolio_type)
        count_query = count_query.where(Portfolio.portfolio_type == portfolio_type)

    query = query.order_by(Portfolio.created_at.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await db.execute(query)
    portfolios = list(result.scalars().all())

    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Pre-fetch index names for display
    all_index_codes: set[str] = set()
    items = []
    for p in portfolios:
        alloc_result = await db.execute(
            select(PortfolioAllocation, Fund.fund_name)
            .join(Fund, PortfolioAllocation.fund_id == Fund.id, isouter=True)
            .where(PortfolioAllocation.portfolio_id == p.id)
            .order_by(PortfolioAllocation.target_weight.desc())
        )
        allocs = alloc_result.all()
        for a in allocs:
            if a.PortfolioAllocation.index_code:
                all_index_codes.add(a.PortfolioAllocation.index_code)
        items.append((p, allocs))

    # Resolve index names
    idx_names: dict[str, str] = {}
    if all_index_codes:
        idx_result = await db.execute(
            select(Benchmark.index_code, Benchmark.index_name)
            .where(Benchmark.index_code.in_(all_index_codes))
        )
        idx_names = {row[0]: row[1] for row in idx_result.all()}

    result_items = []
    for p, allocs in items:
        result_items.append({
            "id": p.id,
            "name": p.name,
            "description": p.description,
            "portfolio_type": p.portfolio_type,
            "allocation_model": p.allocation_model,
            "rebalance_freq": p.rebalance_freq,
            "fund_count": len(allocs),
            "weights": [
                {
                    "fund_id": a.PortfolioAllocation.fund_id,
                    "index_code": a.PortfolioAllocation.index_code,
                    "fund_name": a.fund_name or idx_names.get(a.PortfolioAllocation.index_code or "", a.PortfolioAllocation.index_code or ""),
                    "weight": float(a.PortfolioAllocation.target_weight),
                }
                for a in allocs
            ],
            "created_at": p.created_at.isoformat() if p.created_at else None,
        })
    items = result_items

    return {"total": total, "page": page, "page_size": page_size, "items": items}


@router.post("/", status_code=201)
async def create_portfolio(
    payload: PortfolioCreate,
    db: AsyncSession = Depends(get_db),
):
    total_weight = sum(w.weight for w in payload.weights)
    if abs(total_weight - 1.0) > 0.01:
        raise HTTPException(400, f"权重之和应为1.0，当前为{total_weight:.4f}")

    # Validate fund_ids exist
    fund_ids = [w.fund_id for w in payload.weights if w.fund_id is not None]
    if fund_ids:
        result = await db.execute(select(Fund.id).where(Fund.id.in_(fund_ids)))
        existing_ids = {row[0] for row in result.all()}
        missing = set(fund_ids) - existing_ids
        if missing:
            raise HTTPException(400, f"以下基金ID不存在: {missing}")

    # Validate index_codes exist
    index_codes = [w.index_code for w in payload.weights if w.index_code is not None]
    if index_codes:
        result = await db.execute(
            select(Benchmark.index_code).where(Benchmark.index_code.in_(index_codes))
        )
        existing_codes = {row[0] for row in result.all()}
        missing = set(index_codes) - existing_codes
        if missing:
            raise HTTPException(400, f"以下指数代码不存在: {missing}")

    portfolio = Portfolio(
        name=payload.name,
        description=payload.description,
        portfolio_type="simulation",
        allocation_model="custom",
        rebalance_freq=payload.rebalance_frequency,
    )
    db.add(portfolio)
    await db.flush()

    today = date.today()
    for w in payload.weights:
        db.add(PortfolioAllocation(
            portfolio_id=portfolio.id,
            fund_id=w.fund_id,
            index_code=w.index_code,
            target_weight=w.weight,
            effective_date=today,
        ))

    await db.commit()
    return {"id": portfolio.id, "name": portfolio.name, "message": "组合创建成功"}


@router.get("/{portfolio_id}")
async def get_portfolio(portfolio_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Portfolio).where(Portfolio.id == portfolio_id))
    portfolio = result.scalar_one_or_none()
    if not portfolio:
        raise HTTPException(404, "组合不存在")

    alloc_result = await db.execute(
        select(PortfolioAllocation, Fund.fund_name, Fund.strategy_type, Fund.nav_frequency)
        .join(Fund, PortfolioAllocation.fund_id == Fund.id, isouter=True)
        .where(PortfolioAllocation.portfolio_id == portfolio_id)
        .order_by(PortfolioAllocation.target_weight.desc())
    )
    allocs = alloc_result.all()

    return {
        "id": portfolio.id,
        "name": portfolio.name,
        "description": portfolio.description,
        "portfolio_type": portfolio.portfolio_type,
        "allocation_model": portfolio.allocation_model,
        "rebalance_freq": portfolio.rebalance_freq,
        "created_at": portfolio.created_at.isoformat() if portfolio.created_at else None,
        "weights": [
            {
                "fund_id": a.PortfolioAllocation.fund_id,
                "fund_name": a.fund_name,
                "strategy_type": a.strategy_type,
                "nav_frequency": a.nav_frequency,
                "weight": float(a.PortfolioAllocation.target_weight),
            }
            for a in allocs
        ],
    }


@router.put("/{portfolio_id}/weights")
async def update_weights(
    portfolio_id: int,
    weights: list[PortfolioWeight],
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Portfolio).where(Portfolio.id == portfolio_id))
    if not result.scalar_one_or_none():
        raise HTTPException(404, "组合不存在")

    total_weight = sum(w.weight for w in weights)
    if abs(total_weight - 1.0) > 0.01:
        raise HTTPException(400, f"权重之和应为1.0，当前为{total_weight:.4f}")

    await db.execute(
        delete(PortfolioAllocation).where(PortfolioAllocation.portfolio_id == portfolio_id)
    )
    today = date.today()
    for w in weights:
        db.add(PortfolioAllocation(
            portfolio_id=portfolio_id,
            fund_id=w.fund_id,
            target_weight=w.weight,
            effective_date=today,
        ))
    await db.commit()
    return {"message": "权重更新成功"}


@router.delete("/{portfolio_id}", status_code=204)
async def delete_portfolio(portfolio_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Portfolio).where(Portfolio.id == portfolio_id))
    portfolio = result.scalar_one_or_none()
    if not portfolio:
        raise HTTPException(404, "组合不存在")
    portfolio.is_active = False
    await db.commit()
