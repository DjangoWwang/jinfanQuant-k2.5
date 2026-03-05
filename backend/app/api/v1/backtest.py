"""Backtest API — run portfolio backtests using the engine."""

from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.fund import Fund, NavHistory
from app.models.portfolio import Portfolio, PortfolioAllocation
from app.models.portfolio import BacktestResult as BacktestResultModel
from app.schemas.portfolio import BacktestConfigSchema, PortfolioWeight
from app.engine.backtest import BacktestEngine, BacktestConfig
from app.engine.calendar import get_trading_days
from app.services.fund_service import fund_service

router = APIRouter(prefix="/backtest", tags=["backtest"])


async def _get_nav_series(
    db: AsyncSession,
    fund_id: int,
    start_date: date,
    end_date: date,
) -> pd.Series:
    """从数据库获取基金NAV序列。"""
    series = await fund_service.get_nav_series(db, fund_id, start_date, end_date)
    return series


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
    # 解析权重
    weights_map: dict[int, float] = {}

    if req.portfolio_id:
        # 从组合获取权重
        alloc_result = await db.execute(
            select(PortfolioAllocation)
            .where(PortfolioAllocation.portfolio_id == req.portfolio_id)
        )
        allocs = list(alloc_result.scalars().all())
        if not allocs:
            raise HTTPException(404, "组合不存在或无权重配置")
        weights_map = {a.fund_id: float(a.target_weight) for a in allocs}
    elif req.weights:
        weights_map = {w.fund_id: w.weight for w in req.weights}
    else:
        raise HTTPException(400, "需要提供 portfolio_id 或 weights")

    if not weights_map:
        raise HTTPException(400, "权重为空")

    # 获取交易日历
    trading_days = await get_trading_days(db, req.start_date, req.end_date)
    if not trading_days:
        raise HTTPException(400, "所选日期范围内无交易日")

    # 获取各基金NAV序列
    nav_dict: dict[str, pd.Series] = {}
    fund_names: dict[str, str] = {}
    missing_funds = []

    for fund_id in weights_map:
        series = await _get_nav_series(db, fund_id, req.start_date, req.end_date)
        if series.empty:
            # 查基金名
            fund_result = await db.execute(select(Fund.fund_name).where(Fund.id == fund_id))
            name = fund_result.scalar_one_or_none() or f"#{fund_id}"
            missing_funds.append(name)
            continue
        key = str(fund_id)
        nav_dict[key] = series
        fund_result = await db.execute(select(Fund.fund_name).where(Fund.id == fund_id))
        fund_names[key] = fund_result.scalar_one_or_none() or f"#{fund_id}"

    if missing_funds and req.history_mode == "intersection":
        raise HTTPException(
            400,
            f"以下基金在回测区间内无净值数据: {', '.join(missing_funds)}"
        )

    # 构造引擎配置
    config = BacktestConfig(
        start_date=req.start_date,
        end_date=req.end_date,
        rebalance_freq=req.rebalance_frequency,
        transaction_cost_bps=0.0,
        freq_align_method="downsample",
        history_mode=req.history_mode,
    )

    # 转换权重key为字符串
    str_weights = {str(k): v for k, v in weights_map.items()}

    # 执行回测
    engine = BacktestEngine()
    result = await engine.run(config, str_weights, nav_dict, trading_days)

    # 构造NAV序列 + 回撤序列
    nav_list = []
    drawdown_list = []
    peak = 0.0
    for date_str, nav in sorted(result.nav_series.items()):
        nav_list.append({"date": date_str, "nav": round(nav, 6)})
        peak = max(peak, nav)
        dd = (nav - peak) / peak if peak > 0 else 0.0
        drawdown_list.append({"date": date_str, "drawdown": round(dd, 6)})

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
    nav_list = [{"date": d, "nav": round(v, 6)} for d, v in sorted(nav_series.items())]

    drawdown_list = []
    peak = 0.0
    for item in nav_list:
        peak = max(peak, item["nav"])
        dd = (item["nav"] - peak) / peak if peak > 0 else 0.0
        drawdown_list.append({"date": item["date"], "drawdown": round(dd, 6)})

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
        end_nav = entries[-1][1]
        if prev_end_nav is not None and prev_end_nav > 0:
            ret = (end_nav - prev_end_nav) / prev_end_nav
            result.append({
                "year": year,
                "month": month,
                "return_pct": round(ret * 100, 4),
            })
        prev_end_nav = end_nav

    return result
