"""Fund CRUD API endpoints."""

import csv
import io
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.fund import Fund
from app.models.benchmark import Benchmark, IndexNav
from app.models.strategy import StrategyCategory
from app.schemas.fund import (
    FundCreate, FundUpdate, FundResponse, FundListResponse,
    FundListParams, NavHistoryResponse, NavRecord, MetricsResponse,
)
from app.services.fund_service import fund_service
from app.engine.metrics import calc_all_metrics

router = APIRouter(prefix="/funds", tags=["funds"])


# ------------------------------------------------------------------
# 策略分类
# ------------------------------------------------------------------

@router.get("/strategy-categories")
async def get_strategy_categories(db: AsyncSession = Depends(get_db)):
    """获取数据库中实际存在的策略分类及基金数量（从funds表聚合）。

    返回树状结构: [{strategy_type, total, subs: [{name, count}]}]
    """
    query = (
        select(
            Fund.strategy_type,
            Fund.strategy_sub,
            func.count(Fund.id).label("count"),
        )
        .where(Fund.status == "active")
        .group_by(Fund.strategy_type, Fund.strategy_sub)
        .order_by(func.count(Fund.id).desc())
    )
    result = await db.execute(query)
    rows = result.all()

    tree: dict = {}
    for row in rows:
        st = row.strategy_type or "未分类"
        ss = row.strategy_sub or ""
        cnt = row.count
        if st not in tree:
            tree[st] = {"subs": [], "total": 0}
        tree[st]["total"] += cnt
        if ss:
            tree[st]["subs"].append({"name": ss, "count": cnt})

    categories = []
    for st, info in sorted(tree.items(), key=lambda x: -x[1]["total"]):
        categories.append({
            "strategy_type": st,
            "total": info["total"],
            "subs": sorted(info["subs"], key=lambda x: -x["count"]),
        })
    return categories


@router.get("/strategy-tree")
async def get_strategy_tree(db: AsyncSession = Depends(get_db)):
    """获取自定义策略分类树（strategy_categories表，可编辑）。"""
    query = (
        select(StrategyCategory)
        .where(StrategyCategory.is_active.is_(True))
        .order_by(StrategyCategory.level, StrategyCategory.sort_order)
    )
    result = await db.execute(query)
    nodes = list(result.scalars().all())

    node_map = {n.id: {"id": n.id, "name": n.name, "level": n.level, "parent_id": n.parent_id, "children": []} for n in nodes}
    roots = []
    for n in nodes:
        item = node_map[n.id]
        if n.parent_id and n.parent_id in node_map:
            node_map[n.parent_id]["children"].append(item)
        else:
            roots.append(item)
    return roots


# ------------------------------------------------------------------
# 基金列表 (支持多选策略筛选)
# ------------------------------------------------------------------

@router.get("/", response_model=FundListResponse)
async def list_funds(
    strategy_type: Optional[str] = Query(None, description="逗号分隔多选，如: 股票类,期货策略"),
    strategy_sub: Optional[str] = Query(None, description="逗号分隔多选，如: 主观多头,量化期货"),
    nav_frequency: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    # 逗号分隔转列表
    strategy_types = [s.strip() for s in strategy_type.split(",") if s.strip()] if strategy_type else None
    strategy_subs = [s.strip() for s in strategy_sub.split(",") if s.strip()] if strategy_sub else None

    params = FundListParams(
        strategy_types=strategy_types,
        strategy_subs=strategy_subs,
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


# ------------------------------------------------------------------
# 导出 CSV
# ------------------------------------------------------------------

@router.get("/export")
async def export_funds_csv(
    strategy_type: Optional[str] = Query(None),
    strategy_sub: Optional[str] = Query(None),
    nav_frequency: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """导出基金列表为CSV（使用与列表页相同的筛选条件，无分页限制）。"""
    strategy_types = [s.strip() for s in strategy_type.split(",") if s.strip()] if strategy_type else None
    strategy_subs = [s.strip() for s in strategy_sub.split(",") if s.strip()] if strategy_sub else None

    params = FundListParams(
        strategy_types=strategy_types,
        strategy_subs=strategy_subs,
        nav_frequency=nav_frequency,
        search=search,
        page=1,
        page_size=200,  # max per query
    )

    # Collect all pages
    all_funds = []
    page_num = 1
    while True:
        params.page = page_num
        funds, total = await fund_service.list_funds(db, params)
        all_funds.extend(funds)
        if len(all_funds) >= total:
            break
        page_num += 1

    # Build CSV with BOM for Excel Chinese support
    buf = io.StringIO()
    buf.write("\ufeff")  # UTF-8 BOM
    writer = csv.writer(buf)
    writer.writerow([
        "ID", "基金名称", "备案号", "管理人", "一级策略", "二级策略",
        "最新净值", "净值日期", "成立日期", "频率", "数据状态", "质量评分", "质量标签",
    ])
    for f in all_funds:
        writer.writerow([
            f.id,
            f.fund_name,
            f.filing_number or "",
            f.manager_name or "",
            f.strategy_type or "",
            f.strategy_sub or "",
            f.latest_nav if f.latest_nav is not None else "",
            str(f.latest_nav_date) if f.latest_nav_date else "",
            str(f.inception_date) if f.inception_date else "",
            {"daily": "日频", "weekly": "周频"}.get(f.nav_frequency, f.nav_frequency or ""),
            f.nav_status or "",
            f.data_quality_score if f.data_quality_score is not None else "",
            f.data_quality_tags or "",
        ])

    buf.seek(0)
    filename = f"funds_export_{date.today().isoformat()}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ------------------------------------------------------------------
# 单只基金 CRUD
# ------------------------------------------------------------------

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


# ------------------------------------------------------------------
# 净值与指标
# ------------------------------------------------------------------

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
    risk_free_rate: float = Query(0.02),
    db: AsyncSession = Depends(get_db),
):
    fund = await fund_service.get_fund(db, fund_id)
    if not fund:
        raise HTTPException(404, "基金不存在")

    if preset and not start_date:
        from app.engine.metrics import interval_dates
        try:
            start_date, end_date = interval_dates(preset)
        except ValueError:
            raise HTTPException(400, f"无效的区间预设: {preset}")

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


# ------------------------------------------------------------------
# 业绩归因 (vs 基准指数)
# ------------------------------------------------------------------

class MonthlyReturn(BaseModel):
    period: str  # "2025-01"
    fund_return: float
    benchmark_return: float
    excess_return: float

class FundAttributionResponse(BaseModel):
    fund_id: int
    fund_name: str
    benchmark_code: str
    benchmark_name: str
    start_date: date
    end_date: date
    monthly_returns: list[MonthlyReturn] = Field(default_factory=list)
    cumulative_fund_return: float = 0.0
    cumulative_benchmark_return: float = 0.0
    cumulative_excess: float = 0.0
    annualized_fund_return: float | None = None
    annualized_benchmark_return: float | None = None
    annualized_excess: float | None = None
    tracking_error: float | None = None
    information_ratio: float | None = None
    win_rate: float | None = None  # % months with positive excess


@router.get("/{fund_id}/attribution", response_model=FundAttributionResponse)
async def get_fund_attribution(
    fund_id: int,
    benchmark_code: str = Query("000300", description="基准指数代码"),
    db: AsyncSession = Depends(get_db),
):
    """计算基金相对基准的月度业绩归因（自动从成立以来）。"""
    fund = await fund_service.get_fund(db, fund_id)
    if not fund:
        raise HTTPException(404, "基金不存在")

    # Get benchmark info
    bm_result = await db.execute(
        select(Benchmark).where(Benchmark.index_code == benchmark_code)
    )
    bm = bm_result.scalar_one_or_none()
    if not bm:
        raise HTTPException(404, f"基准指数不存在: {benchmark_code}")

    # Get fund NAV series (full history)
    fund_series = await fund_service.get_nav_series(db, fund_id)
    if fund_series.empty or len(fund_series) < 2:
        raise HTTPException(404, "基金净值数据不足")

    start_date = fund_series.index[0]
    end_date = fund_series.index[-1]
    start_d = start_date.date() if hasattr(start_date, 'date') else start_date
    end_d = end_date.date() if hasattr(end_date, 'date') else end_date

    # Get benchmark NAV series
    bm_result2 = await db.execute(
        select(IndexNav.nav_date, IndexNav.nav_value)
        .where(IndexNav.index_code == benchmark_code)
        .where(IndexNav.nav_date >= start_d)
        .where(IndexNav.nav_date <= end_d)
        .order_by(IndexNav.nav_date)
    )
    bm_rows = bm_result2.all()
    if len(bm_rows) < 2:
        raise HTTPException(404, f"基准 {benchmark_code} 在该期间无足够数据")

    bm_dates = [r[0] for r in bm_rows]
    bm_vals = [float(r[1]) for r in bm_rows]
    bm_series = pd.Series(bm_vals, index=pd.DatetimeIndex(bm_dates))

    # Align to common dates
    common_idx = fund_series.index.intersection(bm_series.index)
    if len(common_idx) < 2:
        raise HTTPException(404, "基金与基准在时间上无足够重叠数据")
    f_aligned = fund_series.loc[common_idx]
    b_aligned = bm_series.loc[common_idx]

    # Compute monthly returns
    f_monthly = f_aligned.resample("ME").last().dropna()
    b_monthly = b_aligned.resample("ME").last().dropna()
    common_months = f_monthly.index.intersection(b_monthly.index)
    if len(common_months) < 1:
        raise HTTPException(404, "无足够月度数据")

    f_m = f_monthly.loc[common_months]
    b_m = b_monthly.loc[common_months]

    f_returns = f_m.pct_change().dropna()
    b_returns = b_m.pct_change().dropna()
    common_ret = f_returns.index.intersection(b_returns.index)
    f_ret = f_returns.loc[common_ret]
    b_ret = b_returns.loc[common_ret]

    monthly_data = []
    for dt in common_ret:
        fr = float(f_ret[dt])
        br = float(b_ret[dt])
        monthly_data.append(MonthlyReturn(
            period=dt.strftime("%Y-%m"),
            fund_return=round(fr, 6),
            benchmark_return=round(br, 6),
            excess_return=round(fr - br, 6),
        ))

    # Cumulative returns
    cum_fund = float((1 + f_ret).prod() - 1) if len(f_ret) > 0 else 0.0
    cum_bm = float((1 + b_ret).prod() - 1) if len(b_ret) > 0 else 0.0
    cum_excess = cum_fund - cum_bm

    # Annualized
    n_months = len(f_ret)
    ann_fund = None
    ann_bm = None
    ann_excess = None
    if n_months >= 3:
        ann_fund = float((1 + cum_fund) ** (12 / n_months) - 1)
        ann_bm = float((1 + cum_bm) ** (12 / n_months) - 1)
        ann_excess = ann_fund - ann_bm

    # Tracking error & information ratio
    excess_arr = np.array([m.excess_return for m in monthly_data])
    te = float(np.std(excess_arr, ddof=1) * np.sqrt(12)) if len(excess_arr) > 1 else None
    ir = float(np.mean(excess_arr) * 12 / (np.std(excess_arr, ddof=1) * np.sqrt(12))) if te and te > 0 else None

    # Win rate
    positive_months = sum(1 for m in monthly_data if m.excess_return > 0)
    win_rate = positive_months / len(monthly_data) if monthly_data else None

    return FundAttributionResponse(
        fund_id=fund_id,
        fund_name=fund.fund_name,
        benchmark_code=benchmark_code,
        benchmark_name=bm.index_name,
        start_date=start_d,
        end_date=end_d,
        monthly_returns=monthly_data,
        cumulative_fund_return=round(cum_fund, 6),
        cumulative_benchmark_return=round(cum_bm, 6),
        cumulative_excess=round(cum_excess, 6),
        annualized_fund_return=round(ann_fund, 6) if ann_fund is not None else None,
        annualized_benchmark_return=round(ann_bm, 6) if ann_bm is not None else None,
        annualized_excess=round(ann_excess, 6) if ann_excess is not None else None,
        tracking_error=round(te, 6) if te is not None else None,
        information_ratio=round(ir, 4) if ir is not None else None,
        win_rate=round(win_rate, 4) if win_rate is not None else None,
    )
