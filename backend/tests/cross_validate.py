"""三方交叉验证: 平台引擎 vs 手动Python vs Codex独立实现(numpy矩阵)"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

import asyncio
import numpy as np
import pandas as pd
from datetime import date

from app.engine.backtest import BacktestEngine, BacktestConfig
from app.engine.metrics import calc_all_metrics, _annualization_factor


# ============================================================
# 确定性数据（三方可精确复现）
# ============================================================

trading_days = [
    # 1月: 22个交易日
    date(2025,1,2), date(2025,1,3), date(2025,1,6), date(2025,1,7), date(2025,1,8),
    date(2025,1,9), date(2025,1,10), date(2025,1,13), date(2025,1,14), date(2025,1,15),
    date(2025,1,16), date(2025,1,17), date(2025,1,20), date(2025,1,21), date(2025,1,22),
    date(2025,1,23), date(2025,1,24), date(2025,1,27), date(2025,1,28), date(2025,1,29),
    date(2025,1,30), date(2025,1,31),
    # 2月: 20个交易日
    date(2025,2,3), date(2025,2,4), date(2025,2,5), date(2025,2,6), date(2025,2,7),
    date(2025,2,10), date(2025,2,11), date(2025,2,12), date(2025,2,13), date(2025,2,14),
    date(2025,2,17), date(2025,2,18), date(2025,2,19), date(2025,2,20), date(2025,2,21),
    date(2025,2,24), date(2025,2,25), date(2025,2,26), date(2025,2,27), date(2025,2,28),
]

# 基金A: 稳健上涨 (每日+0.1%)
nav_a_vals = [1.0]
for i in range(41):
    nav_a_vals.append(nav_a_vals[-1] * 1.001)
nav_a = pd.Series(nav_a_vals[1:], index=trading_days[:41])

# 基金B: 震荡 (交替+0.5%/-0.3%)
nav_b_vals = [1.0]
for i in range(41):
    r = 0.005 if i % 2 == 0 else -0.003
    nav_b_vals.append(nav_b_vals[-1] * (1 + r))
nav_b = pd.Series(nav_b_vals[1:], index=trading_days[:41])

# 基金C: 先涨后跌 (前20天+0.2%, 后21天-0.15%)
nav_c_vals = [1.0]
for i in range(41):
    r = 0.002 if i < 20 else -0.0015
    nav_c_vals.append(nav_c_vals[-1] * (1 + r))
nav_c = pd.Series(nav_c_vals[1:], index=trading_days[:41])

weights = {"A": 1/3, "B": 1/3, "C": 1/3}
nav_dict = {"A": nav_a, "B": nav_b, "C": nav_c}
cost_rate = 10.0 / 10000  # 10bps


# ============================================================
# 公共: 收益序列 + 对齐日期 + 再平衡日
# ============================================================

# 将索引统一为 DatetimeIndex，确保 Timestamp 查找匹配
nav_a.index = pd.DatetimeIndex(nav_a.index)
nav_b.index = pd.DatetimeIndex(nav_b.index)
nav_c.index = pd.DatetimeIndex(nav_c.index)

rets_a = nav_a.pct_change().dropna()
rets_b = nav_b.pct_change().dropna()
rets_c = nav_c.pct_change().dropna()

common_dates = sorted(set(rets_a.index) & set(rets_b.index) & set(rets_c.index))
common_dates = [d for d in common_dates if pd.Timestamp(date(2025,1,2)) <= d <= pd.Timestamp(date(2025,2,28))]

rebalance_dates = set()
prev_d = common_dates[0]
for d in common_dates[1:]:
    d_date = d.date() if isinstance(d, pd.Timestamp) else d
    prev_date = prev_d.date() if isinstance(prev_d, pd.Timestamp) else prev_d
    if d_date.month != prev_date.month:
        rebalance_dates.add(d)
    prev_d = d


# ============================================================
# 方法1: 平台引擎
# ============================================================

async def run_engine():
    engine = BacktestEngine()
    config = BacktestConfig(
        start_date=date(2025, 1, 2),
        end_date=date(2025, 2, 28),
        rebalance_freq="monthly",
        transaction_cost_bps=10.0,
        risk_free_rate=0.02,
    )
    return await engine.run(config, weights, nav_dict, trading_days)

result = asyncio.run(run_engine())
engine_navs = {k: v for k, v in sorted(result.nav_series.items())}
engine_final = list(engine_navs.values())[-1]


# ============================================================
# 方法2: 手动Python逐日模拟
# ============================================================

w = {"A": 1/3, "B": 1/3, "C": 1/3}
target_w = {"A": 1/3, "B": 1/3, "C": 1/3}
manual_nav = [1.0]

for d in common_dates:
    ts = pd.Timestamp(d)
    r_a = float(rets_a.loc[ts]) if ts in rets_a.index else 0.0
    r_b = float(rets_b.loc[ts]) if ts in rets_b.index else 0.0
    r_c = float(rets_c.loc[ts]) if ts in rets_c.index else 0.0

    port_r = w["A"] * r_a + w["B"] * r_b + w["C"] * r_c
    nav_today = manual_nav[-1] * (1 + port_r)

    new_wa = w["A"] * (1 + r_a)
    new_wb = w["B"] * (1 + r_b)
    new_wc = w["C"] * (1 + r_c)
    total = new_wa + new_wb + new_wc

    if d in rebalance_dates:
        drift = {"A": new_wa/total, "B": new_wb/total, "C": new_wc/total}
        turnover = sum(abs(drift[k] - target_w[k]) for k in drift)
        nav_today *= (1 - turnover * cost_rate)
        w = dict(target_w)
    else:
        w = {"A": new_wa/total, "B": new_wb/total, "C": new_wc/total}

    manual_nav.append(nav_today)

manual_final = manual_nav[-1]
manual_total_ret = manual_final / manual_nav[0] - 1
manual_s = pd.Series(manual_nav[1:], index=pd.DatetimeIndex(common_dates))
manual_metrics = calc_all_metrics(manual_s, risk_free_rate=0.02)


# ============================================================
# 方法3: Codex独立实现 (numpy矩阵运算)
# ============================================================

n_days = len(common_dates)
R = np.zeros((n_days, 3))
for i, d in enumerate(common_dates):
    ts = pd.Timestamp(d)
    R[i, 0] = float(rets_a.loc[ts]) if ts in rets_a.index else 0.0
    R[i, 1] = float(rets_b.loc[ts]) if ts in rets_b.index else 0.0
    R[i, 2] = float(rets_c.loc[ts]) if ts in rets_c.index else 0.0

w_vec = np.array([1/3, 1/3, 1/3])
target_vec = np.array([1/3, 1/3, 1/3])
codex_nav = [1.0]

for i in range(n_days):
    port_r = float(np.dot(w_vec, R[i]))
    nav_t = codex_nav[-1] * (1 + port_r)

    new_w = w_vec * (1 + R[i])
    new_w = new_w / new_w.sum()

    if common_dates[i] in rebalance_dates:
        turnover = float(np.sum(np.abs(new_w - target_vec)))
        nav_t *= (1 - turnover * cost_rate)
        w_vec = target_vec.copy()
    else:
        w_vec = new_w

    codex_nav.append(nav_t)

codex_final = codex_nav[-1]
codex_total_ret = codex_final - 1.0
codex_s = pd.Series(codex_nav[1:], index=pd.DatetimeIndex(common_dates))
codex_metrics = calc_all_metrics(codex_s, risk_free_rate=0.02)


# ============================================================
# 输出 + 比较
# ============================================================

print("=" * 70)
print("三方交叉验证: 3基金等权 | 月度再平衡 | 10bps成本 | 42交易日")
print("=" * 70)

print(f"\n基金A最终NAV: {nav_a.iloc[-1]:.8f} (稳健上涨)")
print(f"基金B最终NAV: {nav_b.iloc[-1]:.8f} (震荡)")
print(f"基金C最终NAV: {nav_c.iloc[-1]:.8f} (先涨后跌)")

for label, final, metrics in [
    ("方法1: 平台引擎", engine_final, result.metrics),
    ("方法2: 手动Python", manual_final, manual_metrics),
    ("方法3: Codex/numpy", codex_final, codex_metrics),
]:
    print(f"\n--- {label} ---")
    print(f"  最终NAV:    {final:.10f}")
    print(f"  总收益:     {metrics.get('total_return', manual_total_ret if label.startswith('方法2') else codex_total_ret):.10f}")
    print(f"  年化收益:   {metrics['annualized_return']:.10f}")
    print(f"  最大回撤:   {metrics['max_drawdown']:.10f}")
    print(f"  年化波动率: {metrics['annualized_volatility']:.10f}")
    print(f"  Sharpe:     {metrics['sharpe_ratio']:.10f}")
    print(f"  Sortino:    {metrics['sortino_ratio']:.10f}")
    print(f"  Calmar:     {metrics['calmar_ratio']:.10f}")

print(f"\n{'=' * 70}")
print("三方数值比较 (容差 1e-10)")
print(f"{'=' * 70}")


def compare(name, v1, v2, v3, tol=1e-10):
    d12 = abs(v1 - v2)
    d13 = abs(v1 - v3)
    d23 = abs(v2 - v3)
    max_d = max(d12, d13, d23)
    status = "PASS" if max_d < tol else "FAIL"
    print(f"  {name:16s}: diff_max={max_d:.2e}  {status}")
    return status == "PASS"


all_pass = True
all_pass &= compare("最终NAV", engine_final, manual_final, codex_final)
all_pass &= compare("总收益", result.metrics["total_return"], manual_total_ret, codex_total_ret)
all_pass &= compare("年化收益", result.metrics["annualized_return"], manual_metrics["annualized_return"], codex_metrics["annualized_return"])
all_pass &= compare("最大回撤", result.metrics["max_drawdown"], manual_metrics["max_drawdown"], codex_metrics["max_drawdown"])
all_pass &= compare("年化波动率", result.metrics["annualized_volatility"], manual_metrics["annualized_volatility"], codex_metrics["annualized_volatility"])
all_pass &= compare("Sharpe比率", result.metrics["sharpe_ratio"], manual_metrics["sharpe_ratio"], codex_metrics["sharpe_ratio"])
all_pass &= compare("Sortino比率", result.metrics["sortino_ratio"], manual_metrics["sortino_ratio"], codex_metrics["sortino_ratio"])
all_pass &= compare("Calmar比率", result.metrics["calmar_ratio"], manual_metrics["calmar_ratio"], codex_metrics["calmar_ratio"])

print(f"\n{'=' * 70}")
if all_pass:
    print("总体结果: 全部 PASS  (三方一致)")
else:
    print("总体结果: 存在 FAIL  (三方不一致)")
print(f"再平衡日: {sorted(rebalance_dates)}")
print(f"回测交易日数: {len(common_dates)}")
print(f"年化因子: {_annualization_factor(manual_s):.4f}")
print(f"{'=' * 70}")
