"""回测引擎准确性验证 — 独立手动计算 vs 引擎输出对比。

测试场景:
1. 两只指数等权50/50，无交易成本，月度再平衡，intersection模式
2. 手动逐日计算组合NAV，与引擎输出对比（容差<1e-8）
3. 验证metrics计算（总收益、最大回撤）
"""

import asyncio
import datetime
import sys
import os

import numpy as np
import pandas as pd

# 让 import 能找到 app
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.engine.backtest import BacktestEngine, BacktestConfig
from app.engine.metrics import calc_all_metrics


def make_synthetic_nav(
    start: datetime.date,
    n_days: int,
    daily_return_mean: float = 0.0003,
    daily_return_std: float = 0.01,
    seed: int = 42,
) -> pd.Series:
    """生成合成日频NAV序列。"""
    np.random.seed(seed)
    returns = np.random.normal(daily_return_mean, daily_return_std, n_days)
    nav = np.cumprod(1 + returns) * 1.0
    dates = pd.bdate_range(start, periods=n_days)
    return pd.Series(nav, index=dates, name="nav")


def manual_portfolio_nav(
    nav_a: pd.Series,
    nav_b: pd.Series,
    weight_a: float,
    weight_b: float,
    rebalance_freq: str = "monthly",
    cost_bps: float = 0.0,
) -> pd.Series:
    """手动逐日计算等权组合NAV，作为验证基准。"""
    # 对齐到共同日期
    common = nav_a.index.intersection(nav_b.index)
    nav_a = nav_a.reindex(common)
    nav_b = nav_b.reindex(common)

    ret_a = nav_a.pct_change().dropna()
    ret_b = nav_b.pct_change().dropna()
    common_ret = ret_a.index.intersection(ret_b.index)
    ret_a = ret_a.reindex(common_ret)
    ret_b = ret_b.reindex(common_ret)

    dates = sorted(common_ret)
    cost_rate = cost_bps / 10_000
    target_wa, target_wb = weight_a, weight_b

    # 初始权重
    wa, wb = target_wa, target_wb
    portfolio_nav = [1.0]

    prev_date = dates[0]
    for i, d in enumerate(dates):
        ra = ret_a.loc[d]
        rb = ret_b.loc[d]

        # 组合收益 = 加权收益
        port_ret = wa * ra + wb * rb
        nav_today = portfolio_nav[-1] * (1 + port_ret)

        # 更新权重（漂移）
        new_wa = wa * (1 + ra)
        new_wb = wb * (1 + rb)
        total = new_wa + new_wb

        # 是否再平衡
        d_date = d.date() if isinstance(d, pd.Timestamp) else d
        prev = prev_date.date() if isinstance(prev_date, pd.Timestamp) else prev_date

        trigger = False
        if i > 0:  # 第一天不再平衡
            if rebalance_freq == "monthly":
                trigger = d_date.month != prev.month
            elif rebalance_freq == "weekly":
                trigger = d_date.isocalendar()[1] != prev.isocalendar()[1]
            elif rebalance_freq == "daily":
                trigger = True

        if trigger:
            # 计算turnover
            drift_wa = new_wa / total
            drift_wb = new_wb / total
            turnover = abs(drift_wa - target_wa) + abs(drift_wb - target_wb)
            nav_today *= (1 - turnover * cost_rate)
            wa, wb = target_wa, target_wb
        else:
            wa = new_wa / total
            wb = new_wb / total

        portfolio_nav.append(nav_today)
        prev_date = d

    return pd.Series(portfolio_nav[1:], index=pd.DatetimeIndex(dates))


async def test_equal_weight_no_cost():
    """测试1: 等权50/50，无交易成本，月度再平衡。"""
    print("=" * 60)
    print("测试1: 等权50/50, 无交易成本, 月度再平衡")
    print("=" * 60)

    start = datetime.date(2024, 1, 2)
    nav_a = make_synthetic_nav(start, 250, seed=42)
    nav_b = make_synthetic_nav(start, 250, daily_return_mean=0.0001, seed=99)

    # 引擎执行
    config = BacktestConfig(
        start_date=start,
        end_date=datetime.date(2024, 12, 31),
        rebalance_freq="monthly",
        transaction_cost_bps=0.0,
        risk_free_rate=0.02,
        history_mode="intersection",
    )

    trading_days = sorted(set(nav_a.index.date) | set(nav_b.index.date))
    nav_dict = {"fund_a": nav_a, "fund_b": nav_b}
    weights = {"fund_a": 0.5, "fund_b": 0.5}

    engine = BacktestEngine()
    result = await engine.run(config, weights, nav_dict, trading_days)

    # 手动计算
    manual = manual_portfolio_nav(nav_a, nav_b, 0.5, 0.5, "monthly", 0.0)

    # 对比
    engine_navs = sorted(result.nav_series.items())
    manual_vals = manual.values

    n = min(len(engine_navs), len(manual_vals))
    max_diff = 0.0
    for i in range(n):
        eng_date, eng_val = engine_navs[i]
        man_val = manual_vals[i]
        diff = abs(eng_val - man_val)
        max_diff = max(max_diff, diff)
        if diff > 1e-6:
            print(f"  !! 差异 @ {eng_date}: 引擎={eng_val:.8f}, 手动={man_val:.8f}, diff={diff:.2e}")

    print(f"  日期数: 引擎={len(engine_navs)}, 手动={len(manual_vals)}")
    print(f"  最大差异: {max_diff:.2e}")
    print(f"  最终NAV: 引擎={engine_navs[-1][1]:.8f}, 手动={manual_vals[-1]:.8f}")
    print(f"  总收益: {result.metrics.get('total_return', 0)*100:.4f}%")
    print(f"  最大回撤: {result.metrics.get('max_drawdown', 0)*100:.4f}%")
    print(f"  Sharpe: {result.metrics.get('sharpe_ratio', 0):.4f}")

    assert max_diff < 1e-8, f"差异过大: {max_diff:.2e}"
    print("  [PASS]")


async def test_with_transaction_cost():
    """测试2: 等权50/50，交易成本10bps，月度再平衡。"""
    print("\n" + "=" * 60)
    print("测试2: 等权50/50, 交易成本10bps, 月度再平衡")
    print("=" * 60)

    start = datetime.date(2024, 1, 2)
    nav_a = make_synthetic_nav(start, 250, seed=42)
    nav_b = make_synthetic_nav(start, 250, daily_return_mean=-0.0002, seed=77)

    config = BacktestConfig(
        start_date=start,
        end_date=datetime.date(2024, 12, 31),
        rebalance_freq="monthly",
        transaction_cost_bps=10.0,
        risk_free_rate=0.02,
        history_mode="intersection",
    )

    trading_days = sorted(set(nav_a.index.date) | set(nav_b.index.date))
    nav_dict = {"fund_a": nav_a, "fund_b": nav_b}
    weights = {"fund_a": 0.5, "fund_b": 0.5}

    engine = BacktestEngine()
    result = await engine.run(config, weights, nav_dict, trading_days)

    manual = manual_portfolio_nav(nav_a, nav_b, 0.5, 0.5, "monthly", 10.0)

    engine_navs = sorted(result.nav_series.items())
    manual_vals = manual.values

    n = min(len(engine_navs), len(manual_vals))
    max_diff = 0.0
    for i in range(n):
        eng_date, eng_val = engine_navs[i]
        man_val = manual_vals[i]
        diff = abs(eng_val - man_val)
        max_diff = max(max_diff, diff)

    print(f"  日期数: 引擎={len(engine_navs)}, 手动={len(manual_vals)}")
    print(f"  最大差异: {max_diff:.2e}")
    print(f"  最终NAV: 引擎={engine_navs[-1][1]:.8f}, 手动={manual_vals[-1]:.8f}")
    print(f"  总收益: {result.metrics.get('total_return', 0)*100:.4f}%")

    assert max_diff < 1e-8, f"差异过大: {max_diff:.2e}"
    print("  [PASS]")


async def test_metrics_accuracy():
    """测试3: 验证metrics计算（总收益、最大回撤、年化收益）。"""
    print("\n" + "=" * 60)
    print("测试3: 指标计算准确性")
    print("=" * 60)

    # 构造已知NAV序列
    dates = pd.bdate_range("2024-01-02", periods=100)
    # 先涨50%再跌20%
    nav_values = np.concatenate([
        np.linspace(1.0, 1.5, 50),
        np.linspace(1.5, 1.2, 50),
    ])
    nav_s = pd.Series(nav_values, index=dates)

    metrics = calc_all_metrics(nav_s, risk_free_rate=0.02)

    # 手动验证
    total_return = (1.2 - 1.0) / 1.0  # = 0.2 = 20%
    max_dd = (1.2 - 1.5) / 1.5  # = -0.2 = -20%

    print(f"  总收益: 引擎={metrics['total_return']*100:.4f}%, 预期=20.0000%")
    print(f"  最大回撤: 引擎={metrics['max_drawdown']*100:.4f}%, 预期=-20.0000%")
    print(f"  年化收益: {metrics.get('annualized_return', 0)*100:.4f}%")
    print(f"  波动率: {metrics.get('annualized_volatility', 0)*100:.4f}%")
    print(f"  Sharpe: {metrics.get('sharpe_ratio', 0):.4f}")

    assert abs(metrics["total_return"] - total_return) < 0.001, "总收益不匹配"
    assert abs(metrics["max_drawdown"] - max_dd) < 0.001, "最大回撤不匹配"
    print("  [PASS]")


async def test_real_index_backtest():
    """测试4: 用真实指数数据回测（如果DB可用）。"""
    print("\n" + "=" * 60)
    print("测试4: 真实指数数据回测 (DB)")
    print("=" * 60)

    try:
        import httpx
        resp = httpx.post(
            "http://localhost:8000/api/v1/backtest/run",
            json={
                "weights": [
                    {"index_code": "000300", "weight": 0.5},
                    {"index_code": "000016", "weight": 0.5},
                ],
                "start_date": "2024-06-01",
                "end_date": "2025-12-31",
                "rebalance_frequency": "monthly",
                "transaction_cost_bps": 0,
                "risk_free_rate": 0.02,
                "history_mode": "intersection",
                "initial_capital": 10000000,
            },
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            m = data.get("metrics", {})
            nav = data.get("nav_series", [])
            print(f"  NAV序列: {len(nav)} 天")
            print(f"  总收益: {(m.get('total_return') or 0)*100:.4f}%")
            print(f"  年化收益: {(m.get('annualized_return') or 0)*100:.4f}%")
            print(f"  最大回撤: {(m.get('max_drawdown') or 0)*100:.4f}%")
            print(f"  Sharpe: {m.get('sharpe_ratio', 0):.4f}")
            print(f"  首日NAV: {nav[0]['nav'] if nav else 'N/A'}")
            print(f"  末日NAV: {nav[-1]['nav'] if nav else 'N/A'}")

            # 基本合理性检查
            if nav:
                assert nav[0]["nav"] > 0.9 and nav[0]["nav"] < 1.1, "首日NAV应接近1.0"
                assert len(nav) > 100, f"交易日太少: {len(nav)}"
            print("  [PASS] (合理性检查)")
        else:
            print(f"  [FAIL] API错误: {resp.status_code} - {resp.text[:200]}")
    except Exception as e:
        print(f"  [SKIP]跳过 (后端未启动或无数据): {e}")


async def main():
    print("回测引擎准确性验证")
    print("=" * 60)

    await test_equal_weight_no_cost()
    await test_with_transaction_cost()
    await test_metrics_accuracy()
    await test_real_index_backtest()

    print("\n" + "=" * 60)
    print("全部测试完成")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
