"""真实数据三方交叉验证: 平台引擎 vs 手动Python vs Codex独立实现(numpy)

标的组合 (含真实混频):
  - 深圳德海国企混改价值1号 (ID=303, 日频, 股票多头)
  - 箐安期权稳健一号 (ID=1212, 日频, 期权策略)
  - 国民信托启航13号 (ID=301, 真·周频, 债券策略) ← 测试混频对齐
  - 沪深300指数 (ID=1216/000300, 日频, 当ETF用)

回测参数:
  - 区间: 2025-05-01 ~ 2025-12-31
  - 权重: 德海25%, 箐安20%, 启航25%, 沪深300 30%
  - 再平衡: 月度
  - 交易成本: 15bps
  - 频率对齐: downsample (日频→周频, 因含真·周频基金)
  - 无风险利率: 2%
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

import asyncio
import numpy as np
import pandas as pd
from datetime import date

from app.database import async_session
from sqlalchemy import text

from app.engine.backtest import BacktestEngine, BacktestConfig
from app.engine.metrics import calc_all_metrics, _annualization_factor
from app.engine.freq_align import (
    align_frequencies, detect_frequency, downsample_to_weekly,
)


# ============================================================
# 配置
# ============================================================

START_DATE = date(2025, 5, 1)
END_DATE = date(2025, 12, 31)
COST_BPS = 15.0
RISK_FREE = 0.02
REBALANCE_FREQ = "monthly"

# 权重: 德海25%, 箐安20%, 启航25%, 沪深300 30%
WEIGHTS = {
    "dehai_303": 0.25,
    "qingan_1212": 0.20,
    "qihang_301": 0.25,
    "hs300_1216": 0.30,
}

FUND_IDS = {
    "dehai_303": 303,
    "qingan_1212": 1212,
    "qihang_301": 301,
}
INDEX_CODE = "000300"
INDEX_KEY = "hs300_1216"


# ============================================================
# 第0步: 从数据库加载真实数据 + 交易日历
# ============================================================

async def load_data():
    """从数据库加载NAV数据和交易日历."""
    async with async_session() as s:
        # 交易日历 (需覆盖全部基金历史, 否则detect_frequency会误判)
        r = await s.execute(text(
            "SELECT cal_date FROM trading_calendar "
            "WHERE is_trading_day = true "
            "ORDER BY cal_date"
        ))
        trading_days = [row[0] for row in r.fetchall()]

        # 基金NAV
        nav_dict = {}
        for key, fid in FUND_IDS.items():
            r = await s.execute(text(
                "SELECT nav_date, cumulative_nav FROM nav_history "
                "WHERE fund_id = :fid ORDER BY nav_date"
            ), {"fid": fid})
            rows = r.fetchall()
            dates_raw = [row[0] for row in rows]
            vals = [float(row[1]) for row in rows]
            nav_dict[key] = pd.Series(vals, index=dates_raw, name=key)

        # 沪深300指数NAV
        r = await s.execute(text(
            "SELECT nav_date, nav_value FROM index_nav "
            "WHERE index_code = :code ORDER BY nav_date"
        ), {"code": INDEX_CODE})
        rows = r.fetchall()
        dates_raw = [row[0] for row in rows]
        vals = [float(row[1]) for row in rows]
        nav_dict[INDEX_KEY] = pd.Series(vals, index=dates_raw, name=INDEX_KEY)

    return trading_days, nav_dict


print("正在从数据库加载数据...")
trading_days, raw_nav_dict = asyncio.run(load_data())
print(f"交易日历: {len(trading_days)} 天 ({trading_days[0]} ~ {trading_days[-1]})")

for key, s in raw_nav_dict.items():
    freq_label = detect_frequency(s, trading_days)
    print(f"  {key}: {len(s)}条NAV, {s.index[0]} ~ {s.index[-1]}, 检测频率={freq_label}")


# ============================================================
# 第1步: 频率对齐 (三方共享同一份对齐数据)
# ============================================================

print("\n--- 频率对齐 (downsample to weekly) ---")
aligned = align_frequencies(raw_nav_dict, trading_days, method="downsample")

for key, s in aligned.items():
    print(f"  {key}: 对齐后 {len(s)} 条, {s.index[0]} ~ {s.index[-1]}")

# 构建收益矩阵 (三方共享)
returns_df = pd.DataFrame({k: v.pct_change() for k, v in aligned.items()}).dropna()
if not isinstance(returns_df.index, pd.DatetimeIndex):
    returns_df.index = pd.DatetimeIndex(returns_df.index)

start_ts = pd.Timestamp(START_DATE)
end_ts = pd.Timestamp(END_DATE)
returns_df = returns_df[(returns_df.index >= start_ts) & (returns_df.index <= end_ts)]

print(f"\n收益矩阵: {returns_df.shape[0]} 行 x {returns_df.shape[1]} 列")
print(f"  日期范围: {returns_df.index[0].date()} ~ {returns_df.index[-1].date()}")

fund_keys = list(WEIGHTS.keys())
sim_dates = sorted(returns_df.index)
raw_dates = [d.date() if isinstance(d, pd.Timestamp) else d for d in sim_dates]

# 再平衡日
rebalance_dates = set()
prev_d = raw_dates[0]
for d in raw_dates[1:]:
    if d.month != prev_d.month:
        rebalance_dates.add(d)
    prev_d = d
print(f"再平衡日: {sorted(rebalance_dates)}")

cost_rate = COST_BPS / 10_000


# ============================================================
# 方法1: 平台引擎
# ============================================================

print("\n=== 方法1: 平台引擎 ===")

async def run_engine():
    engine = BacktestEngine()
    config = BacktestConfig(
        start_date=START_DATE,
        end_date=END_DATE,
        rebalance_freq=REBALANCE_FREQ,
        transaction_cost_bps=COST_BPS,
        risk_free_rate=RISK_FREE,
        freq_align_method="downsample",
        history_mode="intersection",
    )
    return await engine.run(config, WEIGHTS, raw_nav_dict, trading_days)

result = asyncio.run(run_engine())
engine_navs = {k: v for k, v in sorted(result.nav_series.items())}
engine_final = list(engine_navs.values())[-1]
engine_metrics = result.metrics
print(f"  引擎NAV点数: {len(engine_navs)}")
print(f"  最终NAV: {engine_final:.10f}")


# ============================================================
# 方法2: 手动Python逐日模拟
# ============================================================

print("\n=== 方法2: 手动Python ===")

w = {k: WEIGHTS[k] for k in fund_keys}
target_w = dict(w)
manual_nav = [1.0]

for ts in sim_dates:
    d = ts.date() if isinstance(ts, pd.Timestamp) else ts
    day_rets = returns_df.loc[ts]

    # 计算组合日收益
    port_r = sum(w[k] * float(day_rets[k]) for k in fund_keys)
    nav_today = manual_nav[-1] * (1 + port_r)

    # 更新漂移后权重
    new_w = {k: w[k] * (1 + float(day_rets[k])) for k in fund_keys}
    total_w = sum(new_w.values())

    if d in rebalance_dates:
        drift_w = {k: new_w[k] / total_w for k in fund_keys}
        turnover = sum(abs(drift_w[k] - target_w[k]) for k in fund_keys)
        nav_today *= (1 - turnover * cost_rate)
        w = dict(target_w)
    else:
        w = {k: new_w[k] / total_w for k in fund_keys}

    manual_nav.append(nav_today)

manual_final = manual_nav[-1]
manual_total_ret = manual_final / manual_nav[0] - 1
manual_s = pd.Series(manual_nav[1:], index=pd.DatetimeIndex(raw_dates))
manual_metrics = calc_all_metrics(manual_s, risk_free_rate=RISK_FREE)
print(f"  最终NAV: {manual_final:.10f}")


# ============================================================
# 方法3: Codex独立实现 (numpy矩阵运算)
# ============================================================

print("\n=== 方法3: Codex/numpy ===")

n_days = len(sim_dates)
n_funds = len(fund_keys)

# 构建收益矩阵 R[T, N]
R = np.zeros((n_days, n_funds))
for j, key in enumerate(fund_keys):
    for i, ts in enumerate(sim_dates):
        R[i, j] = float(returns_df.loc[ts, key])

w_vec = np.array([WEIGHTS[k] for k in fund_keys])
target_vec = w_vec.copy()
codex_nav = [1.0]

# 预计算再平衡布尔数组
is_rebal = np.array([
    (d.date() if isinstance(d, pd.Timestamp) else d) in rebalance_dates
    for d in sim_dates
])

for i in range(n_days):
    # 组合收益
    port_r = float(np.dot(w_vec, R[i]))
    nav_t = codex_nav[-1] * (1 + port_r)

    # 漂移后权重
    new_w = w_vec * (1 + R[i])
    new_w = new_w / new_w.sum()

    if is_rebal[i]:
        turnover = float(np.sum(np.abs(new_w - target_vec)))
        nav_t *= (1 - turnover * cost_rate)
        w_vec = target_vec.copy()
    else:
        w_vec = new_w

    codex_nav.append(nav_t)

codex_final = codex_nav[-1]
codex_total_ret = codex_final - 1.0
codex_s = pd.Series(codex_nav[1:], index=pd.DatetimeIndex(raw_dates))
codex_metrics = calc_all_metrics(codex_s, risk_free_rate=RISK_FREE)
print(f"  最终NAV: {codex_final:.10f}")


# ============================================================
# 输出 + 比较
# ============================================================

print(f"\n{'=' * 70}")
print("真实数据三方交叉验证 | 4标的混频 | 月度再平衡 | 15bps成本")
print(f"{'=' * 70}")

print(f"\n标的信息:")
for key in fund_keys:
    s = aligned[key]
    print(f"  {key}: 对齐后{len(s)}条周频NAV")

for label, final, metrics in [
    ("方法1: 平台引擎", engine_final, engine_metrics),
    ("方法2: 手动Python", manual_final, manual_metrics),
    ("方法3: Codex/numpy", codex_final, codex_metrics),
]:
    print(f"\n--- {label} ---")
    print(f"  最终NAV:    {final:.10f}")
    print(f"  总收益:     {metrics['total_return']:.10f}")
    print(f"  年化收益:   {metrics['annualized_return']:.10f}")
    print(f"  最大回撤:   {metrics['max_drawdown']:.10f}")
    print(f"  年化波动率: {metrics['annualized_volatility']:.10f}")
    print(f"  Sharpe:     {metrics['sharpe_ratio']:.10f}")
    print(f"  Sortino:    {metrics['sortino_ratio']:.10f}")
    print(f"  Calmar:     {metrics['calmar_ratio']:.10f}")

print(f"\n{'=' * 70}")
print("三方数值比较 (容差 1e-8)")
print(f"{'=' * 70}")


def compare(name, v1, v2, v3, tol=1e-8):
    d12 = abs(v1 - v2)
    d13 = abs(v1 - v3)
    d23 = abs(v2 - v3)
    max_d = max(d12, d13, d23)
    status = "PASS" if max_d < tol else "FAIL"
    print(f"  {name:16s}: 引擎-手动={d12:.2e}  引擎-numpy={d13:.2e}  手动-numpy={d23:.2e}  {status}")
    return status == "PASS"


all_pass = True
all_pass &= compare("最终NAV", engine_final, manual_final, codex_final)
all_pass &= compare("总收益",
    engine_metrics["total_return"],
    manual_metrics["total_return"],
    codex_metrics["total_return"])
all_pass &= compare("年化收益",
    engine_metrics["annualized_return"],
    manual_metrics["annualized_return"],
    codex_metrics["annualized_return"])
all_pass &= compare("最大回撤",
    engine_metrics["max_drawdown"],
    manual_metrics["max_drawdown"],
    codex_metrics["max_drawdown"])
all_pass &= compare("年化波动率",
    engine_metrics["annualized_volatility"],
    manual_metrics["annualized_volatility"],
    codex_metrics["annualized_volatility"])
all_pass &= compare("Sharpe比率",
    engine_metrics["sharpe_ratio"],
    manual_metrics["sharpe_ratio"],
    codex_metrics["sharpe_ratio"])
all_pass &= compare("Sortino比率",
    engine_metrics["sortino_ratio"],
    manual_metrics["sortino_ratio"],
    codex_metrics["sortino_ratio"])
all_pass &= compare("Calmar比率",
    engine_metrics["calmar_ratio"],
    manual_metrics["calmar_ratio"],
    codex_metrics["calmar_ratio"])

# 逐日NAV对比: 方法2 vs 方法3 (检查模拟逻辑一致性)
nav_diffs = [abs(manual_nav[i+1] - codex_nav[i+1]) for i in range(n_days)]
max_nav_diff = max(nav_diffs)
print(f"\n  逐日NAV最大差异(手动vs numpy): {max_nav_diff:.2e}")

# 方法1 vs 方法2 逐日对比 (引擎NAV序列)
engine_nav_list = [engine_navs[d] for d in sorted(engine_navs.keys())]
if len(engine_nav_list) == len(manual_nav) - 1:
    engine_diffs = [abs(engine_nav_list[i] - manual_nav[i+1]) for i in range(len(engine_nav_list))]
    max_engine_diff = max(engine_diffs)
    print(f"  逐日NAV最大差异(引擎vs手动):  {max_engine_diff:.2e}")
else:
    print(f"  引擎NAV点数({len(engine_nav_list)}) != 手动NAV点数({len(manual_nav)-1}), 跳过逐日对比")

print(f"\n{'=' * 70}")
if all_pass:
    print("总体结果: 全部 PASS  (三方一致)")
else:
    print("总体结果: 存在 FAIL  (三方不一致)")
print(f"回测周期: {START_DATE} ~ {END_DATE}")
print(f"频率对齐后周数: {returns_df.shape[0]}")
print(f"再平衡日数: {len(rebalance_dates)}")
print(f"年化因子: {_annualization_factor(manual_s):.4f}")
print(f"{'=' * 70}")
