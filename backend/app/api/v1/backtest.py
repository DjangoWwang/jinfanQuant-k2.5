"""Backtest API — run portfolio backtests using the engine."""

from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.fund import Fund, NavHistory
from app.models.benchmark import Benchmark, IndexNav
from app.models.portfolio import Portfolio, PortfolioAllocation
from app.models.portfolio import BacktestResult as BacktestResultModel
from app.schemas.portfolio import BacktestConfigSchema, PortfolioWeight
from app.engine.backtest import BacktestEngine, BacktestConfig
from app.engine.calendar import get_trading_days
from app.services.fund_service import fund_service

router = APIRouter(prefix="/backtest", tags=["backtest"])


def _build_nav_and_drawdown(nav_series: dict[str, float]):
    """Build nav_list and drawdown_list from a nav_series dict."""
    nav_list = []
    drawdown_list = []
    peak = 0.0
    for date_str, nav in sorted(nav_series.items()):
        nav_list.append({"date": date_str, "nav": round(nav, 6)})
        peak = max(peak, nav)
        dd = (nav - peak) / peak if peak > 0 else 0.0
        drawdown_list.append({"date": date_str, "drawdown": round(dd, 6)})
    return nav_list, drawdown_list


async def _get_fund_nav_series(
    db: AsyncSession,
    fund_id: int,
    start_date: date,
    end_date: date,
) -> pd.Series:
    """从数据库获取基金NAV序列。"""
    return await fund_service.get_nav_series(db, fund_id, start_date, end_date)


async def _get_index_nav_series(
    db: AsyncSession,
    index_code: str,
    start_date: date,
    end_date: date,
) -> pd.Series:
    """从数据库获取指数NAV序列。"""
    result = await db.execute(
        select(IndexNav.nav_date, IndexNav.nav_value)
        .where(IndexNav.index_code == index_code)
        .where(IndexNav.nav_date >= start_date)
        .where(IndexNav.nav_date <= end_date)
        .order_by(IndexNav.nav_date)
    )
    rows = result.all()
    if not rows:
        return pd.Series(dtype=float)
    dates = [r[0] for r in rows]
    vals = [float(r[1]) for r in rows if r[1] is not None]
    if len(vals) != len(dates):
        pairs = [(r[0], float(r[1])) for r in rows if r[1] is not None]
        if not pairs:
            return pd.Series(dtype=float)
        dates, vals = zip(*pairs)
    return pd.Series(vals, index=pd.DatetimeIndex(dates), name=f"idx_{index_code}")


@router.get("/search-assets")
async def search_assets(
    q: str = Query("", description="搜索关键词（名称/代码/管理人）"),
    asset_type: str = Query("", description="资产类型过滤: fund / index / 空=全部"),
    limit: int = Query(15, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
):
    """统一搜索可用于回测的资产（基金 + 指数/ETF）。

    返回统一格式，前端可直接展示和添加到组合。
    支持 asset_type 过滤，避免混合结果被截断。
    """
    from sqlalchemy import or_

    results = []

    # 搜索基金
    if asset_type != "index":
        fund_query = (
            select(Fund)
            .where(Fund.status == "active")
            .order_by(Fund.id)
            .limit(limit)
        )
        if q:
            pattern = f"%{q}%"
            fund_query = fund_query.where(
                or_(
                    Fund.fund_name.ilike(pattern),
                    Fund.filing_number.ilike(pattern),
                    Fund.manager_name.ilike(pattern),
                )
            )
        fund_result = await db.execute(fund_query)
        for f in fund_result.scalars().all():
            results.append({
                "asset_type": "fund",
                "asset_id": f"fund_{f.id}",
                "fund_id": f.id,
                "index_code": None,
                "name": f.fund_name,
                "sub_label": f.manager_name or "",
                "strategy": f"{f.strategy_type or ''}{('·' + f.strategy_sub) if f.strategy_sub else ''}",
                "frequency": f.nav_frequency or "daily",
                "latest_nav": float(f.latest_nav) if f.latest_nav else None,
            })

    # 搜索指数
    if asset_type != "fund":
        idx_query = (
            select(Benchmark)
            .where(Benchmark.is_active.is_not(False))
            .order_by(Benchmark.index_code)
            .limit(limit)
        )
        if q:
            pattern = f"%{q}%"
            idx_query = idx_query.where(
                or_(
                    Benchmark.index_name.ilike(pattern),
                    Benchmark.index_code.ilike(pattern),
                )
            )
        idx_result = await db.execute(idx_query)
        for b in idx_result.scalars().all():
            results.append({
                "asset_type": "index",
                "asset_id": f"idx_{b.index_code}",
                "fund_id": None,
                "index_code": b.index_code,
                "name": b.index_name,
                "sub_label": b.category or "",
                "strategy": "",
                "frequency": "daily",
                "latest_nav": None,
            })

    return results[:limit]


@router.post("/run")
async def run_backtest(
    req: BacktestConfigSchema,
    db: AsyncSession = Depends(get_db),
):
    """执行回测。

    支持两种模式:
    1. portfolio_id — 从已保存的组合获取权重
    2. inline weights — 直接传入权重（快速回测，不保存组合）
    """
    # 解析权重: 支持 fund_id(基金) 和 index_code(指数/ETF) 两种资产
    # key格式: "fund_123" 或 "idx_000300"
    weights_map: dict[str, float] = {}

    if req.portfolio_id:
        alloc_result = await db.execute(
            select(PortfolioAllocation)
            .where(PortfolioAllocation.portfolio_id == req.portfolio_id)
        )
        allocs = list(alloc_result.scalars().all())
        if not allocs:
            raise HTTPException(404, "组合不存在或无权重配置")
        for a in allocs:
            if a.index_code:
                weights_map[f"idx_{a.index_code}"] = float(a.target_weight)
            elif a.fund_id:
                weights_map[f"fund_{a.fund_id}"] = float(a.target_weight)
    elif req.weights:
        for w in req.weights:
            if w.fund_id is not None:
                weights_map[f"fund_{w.fund_id}"] = w.weight
            elif w.index_code is not None:
                weights_map[f"idx_{w.index_code}"] = w.weight
            else:
                raise HTTPException(400, "每个权重项必须指定 fund_id 或 index_code")
    else:
        raise HTTPException(400, "需要提供 portfolio_id 或 weights")

    if not weights_map:
        raise HTTPException(400, "权重为空")

    # 获取交易日历
    trading_days = await get_trading_days(db, req.start_date, req.end_date)
    if not trading_days:
        raise HTTPException(400, "所选日期范围内无交易日")

    # 获取各资产NAV序列 (基金 + 指数)
    nav_dict: dict[str, pd.Series] = {}
    fund_names: dict[str, str] = {}
    missing_funds = []

    for asset_key in weights_map:
        if asset_key.startswith("fund_"):
            fund_id = int(asset_key.split("_", 1)[1])
            series = await _get_fund_nav_series(db, fund_id, req.start_date, req.end_date)
            if series.empty:
                fund_result = await db.execute(select(Fund.fund_name).where(Fund.id == fund_id))
                name = fund_result.scalar_one_or_none() or f"#{fund_id}"
                missing_funds.append(name)
                continue
            nav_dict[asset_key] = series
            fund_result = await db.execute(select(Fund.fund_name).where(Fund.id == fund_id))
            fund_names[asset_key] = fund_result.scalar_one_or_none() or f"#{fund_id}"
        elif asset_key.startswith("idx_"):
            index_code = asset_key.split("_", 1)[1]
            series = await _get_index_nav_series(db, index_code, req.start_date, req.end_date)
            if series.empty:
                bm_result = await db.execute(select(Benchmark.index_name).where(Benchmark.index_code == index_code))
                name = bm_result.scalar_one_or_none() or index_code
                missing_funds.append(name)
                continue
            nav_dict[asset_key] = series
            bm_result = await db.execute(select(Benchmark.index_name).where(Benchmark.index_code == index_code))
            fund_names[asset_key] = bm_result.scalar_one_or_none() or index_code

    if missing_funds and req.history_mode == "intersection":
        raise HTTPException(
            400,
            f"以下资产在回测区间内无净值数据: {', '.join(missing_funds)}"
        )

    # 构造引擎配置 — use schema fields, not hardcoded values
    config = BacktestConfig(
        start_date=req.start_date,
        end_date=req.end_date,
        rebalance_freq=req.rebalance_frequency,
        transaction_cost_bps=req.transaction_cost_bps,
        freq_align_method=req.freq_align_method,
        risk_free_rate=req.risk_free_rate,
        history_mode=req.history_mode,
    )

    # weights_map keys are already strings like "fund_123" or "idx_000300"
    str_weights = weights_map

    # 执行回测
    engine = BacktestEngine()
    result = await engine.run(config, str_weights, nav_dict, trading_days)

    # 构造NAV序列 + 回撤序列
    nav_list, drawdown_list = _build_nav_and_drawdown(result.nav_series)

    # 构造月度收益表
    monthly_returns = _calc_monthly_returns(result.nav_series)

    # 保存回测结果到数据库（仅当有 portfolio_id 时）
    bt_id = None
    if req.portfolio_id:
        bt_record = BacktestResultModel(
            portfolio_id=req.portfolio_id,
            config_json=req.model_dump(mode="json"),
            metrics_json=result.metrics,
            nav_series_json=result.nav_series,
            status="completed",
        )
        db.add(bt_record)
        await db.commit()
        bt_id = bt_record.id

    metrics = result.metrics
    return {
        "backtest_id": bt_id,
        "portfolio_id": req.portfolio_id,
        "config": req.model_dump(mode="json"),
        "history_mode": req.history_mode,
        "metrics": {
            "total_return": metrics.get("total_return"),
            "annualized_return": metrics.get("annualized_return"),
            "max_drawdown": metrics.get("max_drawdown"),
            "sharpe_ratio": metrics.get("sharpe_ratio"),
            "sortino_ratio": metrics.get("sortino_ratio"),
            "calmar_ratio": metrics.get("calmar_ratio"),
            "volatility": metrics.get("annualized_volatility"),
        },
        "nav_series": nav_list,
        "drawdown_series": drawdown_list,
        "monthly_returns": monthly_returns,
        "fund_names": fund_names,
        "excluded_funds": result.excluded_funds,
        "entry_log": result.entry_log,
    }


@router.get("/results/{backtest_id}")
async def get_backtest_result(
    backtest_id: int,
    db: AsyncSession = Depends(get_db),
):
    """获取已保存的回测结果。"""
    result = await db.execute(
        select(BacktestResultModel).where(BacktestResultModel.id == backtest_id)
    )
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(404, "回测结果不存在")

    nav_series = record.nav_series_json or {}
    nav_list, drawdown_list = _build_nav_and_drawdown(nav_series)

    return {
        "backtest_id": record.id,
        "portfolio_id": record.portfolio_id,
        "status": record.status,
        "config": record.config_json,
        "metrics": record.metrics_json,
        "nav_series": nav_list,
        "drawdown_series": drawdown_list,
        "monthly_returns": _calc_monthly_returns(nav_series),
        "run_date": record.run_date.isoformat() if record.run_date else None,
    }


@router.get("/history/{portfolio_id}")
async def list_backtest_history(
    portfolio_id: int,
    db: AsyncSession = Depends(get_db),
):
    """列出某组合的历史回测记录。"""
    result = await db.execute(
        select(BacktestResultModel)
        .where(BacktestResultModel.portfolio_id == portfolio_id)
        .order_by(BacktestResultModel.run_date.desc())
        .limit(20)
    )
    records = list(result.scalars().all())

    return [
        {
            "backtest_id": r.id,
            "status": r.status,
            "metrics": r.metrics_json,
            "run_date": r.run_date.isoformat() if r.run_date else None,
        }
        for r in records
    ]


def _calc_monthly_returns(nav_series: dict[str, float]) -> list[dict]:
    """从NAV序列计算月度收益表。"""
    if not nav_series:
        return []

    sorted_items = sorted(nav_series.items())
    monthly: dict[tuple[int, int], list[tuple[str, float]]] = {}
    for date_str, nav in sorted_items:
        parts = date_str.split("-")
        year, month = int(parts[0]), int(parts[1])
        monthly.setdefault((year, month), []).append((date_str, nav))

    result = []
    prev_end_nav = None
    for (year, month), entries in sorted(monthly.items()):
        start_nav = entries[0][1]
        end_nav = entries[-1][1]
        if prev_end_nav is not None and prev_end_nav > 0:
            # Normal month: return from previous month-end to this month-end
            ret = (end_nav - prev_end_nav) / prev_end_nav
        elif prev_end_nav is None and start_nav > 0 and len(entries) > 1:
            # First month: return from first observation to month-end
            ret = (end_nav - start_nav) / start_nav
        else:
            prev_end_nav = end_nav
            continue
        result.append({
            "year": year,
            "month": month,
            "return_pct": round(ret * 100, 4),
        })
        prev_end_nav = end_nav

    return result
