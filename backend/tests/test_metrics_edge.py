"""Metrics边界情况测试。

覆盖:
1. 单个数据点 → 所有指标返回0/安全值
2. 恒定NAV(零波动) → 波动率0, Sharpe 0
3. 全部亏损(NAV→0) → 年化收益-100%
4. 巨大收益 → 年化收益被cap到99.99
5. 周频数据的年化因子
6. NaN/inf在数据中
7. interval_dates边界
"""

import datetime
import sys
import os

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.engine.metrics import (
    calc_return,
    calc_annualized_return,
    calc_max_drawdown,
    calc_annualized_volatility,
    calc_sharpe_ratio,
    calc_sortino_ratio,
    calc_calmar_ratio,
    calc_all_metrics,
    normalize_nav,
    interval_dates,
    _annualization_factor,
)


def make_nav(values, start_date=datetime.date(2024, 1, 1), freq_days=1):
    """生成NAV Series。"""
    dates = [start_date + datetime.timedelta(days=i * freq_days) for i in range(len(values))]
    return pd.Series(values, index=pd.DatetimeIndex(dates))


class TestSingleDataPoint:
    """单个数据点。"""

    def test_return_single(self):
        nav = make_nav([1.0])
        assert calc_return(nav) == 0.0

    def test_annualized_return_single(self):
        nav = make_nav([1.0])
        assert calc_annualized_return(nav) == 0.0

    def test_max_drawdown_single(self):
        nav = make_nav([1.0])
        dd, peak, trough = calc_max_drawdown(nav)
        assert dd == 0.0

    def test_volatility_single(self):
        nav = make_nav([1.0])
        assert calc_annualized_volatility(nav) == 0.0

    def test_sharpe_single(self):
        nav = make_nav([1.0])
        assert calc_sharpe_ratio(nav) == 0.0


class TestConstantNav:
    """恒定NAV(零波动)。"""

    def test_return_zero(self):
        nav = make_nav([1.0] * 100)
        assert calc_return(nav) == 0.0

    def test_annualized_return_zero(self):
        nav = make_nav([1.0] * 100)
        assert calc_annualized_return(nav) == 0.0

    def test_volatility_zero(self):
        nav = make_nav([1.0] * 100)
        assert calc_annualized_volatility(nav) == 0.0

    def test_sharpe_zero_volatility(self):
        nav = make_nav([1.0] * 100)
        assert calc_sharpe_ratio(nav) == 0.0

    def test_max_drawdown_zero(self):
        nav = make_nav([1.0] * 100)
        dd, _, _ = calc_max_drawdown(nav)
        assert dd == 0.0


class TestTotalLoss:
    """全部亏损(NAV趋近0)。"""

    def test_return_near_minus_one(self):
        nav = make_nav([1.0, 0.5, 0.1, 0.01])
        ret = calc_return(nav)
        assert ret == pytest.approx(-0.99, abs=0.001)

    def test_annualized_return_capped(self):
        nav = make_nav([1.0, 0.001])
        ann = calc_annualized_return(nav)
        # 应被cap到-99.99%
        assert ann >= -0.9999

    def test_max_drawdown_severe(self):
        nav = make_nav([1.0, 0.5, 0.1, 0.01])
        dd, _, _ = calc_max_drawdown(nav)
        assert dd == pytest.approx(-0.99, abs=0.001)


class TestExtremeGains:
    """巨大收益。"""

    def test_annualized_return_capped_at_9999(self):
        # 2天内10倍
        nav = make_nav([1.0, 10.0])
        ann = calc_annualized_return(nav)
        assert ann <= 99.99


class TestWeeklyFrequency:
    """周频数据。"""

    def test_annualization_factor_weekly(self):
        # 52周数据, 每周间隔7天
        nav = make_nav([1.0 + i * 0.01 for i in range(52)], freq_days=7)
        factor = _annualization_factor(nav)
        # 365/7 ≈ 52.14
        assert factor == pytest.approx(52.14, abs=1.0)

    def test_weekly_volatility_reasonable(self):
        np.random.seed(42)
        values = [1.0]
        for _ in range(51):
            values.append(values[-1] * (1 + np.random.normal(0, 0.02)))
        nav = make_nav(values, freq_days=7)
        vol = calc_annualized_volatility(nav)
        # 周波动2% × sqrt(52) ≈ 14.4%
        assert 0.05 < vol < 0.30


class TestWithNaN:
    """含NaN的数据。"""

    def test_return_with_nan_in_middle(self):
        nav = make_nav([1.0, 1.01, float("nan"), 1.03, 1.04])
        # metrics应该处理NaN
        ret = calc_return(nav)
        assert np.isfinite(ret)

    def test_all_metrics_no_crash(self):
        nav = make_nav([1.0, 1.01, float("nan"), 1.03])
        metrics = calc_all_metrics(nav)
        for key, val in metrics.items():
            if isinstance(val, float):
                assert np.isfinite(val), f"{key} is not finite: {val}"


class TestNormalizeNav:
    """归一化。"""

    def test_normalize_base_1(self):
        nav = make_nav([2.0, 2.2, 2.1])
        n = normalize_nav(nav, base=1.0)
        assert n.iloc[0] == 1.0
        assert n.iloc[1] == pytest.approx(1.1, abs=0.001)

    def test_normalize_empty(self):
        nav = pd.Series(dtype=float)
        assert normalize_nav(nav).empty


class TestIntervalDates:
    """区间日期计算。"""

    def test_ytd(self):
        ref = datetime.date(2024, 6, 15)
        start, end = interval_dates("ytd", ref)
        assert start == datetime.date(2024, 1, 1)
        assert end == ref

    def test_mtd(self):
        ref = datetime.date(2024, 3, 20)
        start, end = interval_dates("mtd", ref)
        assert start == datetime.date(2024, 3, 1)
        assert end == ref

    def test_qtd(self):
        ref = datetime.date(2024, 5, 10)
        start, end = interval_dates("qtd", ref)
        assert start == datetime.date(2024, 4, 1)  # Q2
        assert end == ref

    def test_1y(self):
        ref = datetime.date(2024, 3, 15)
        start, end = interval_dates("1y", ref)
        assert start == datetime.date(2023, 3, 15)

    def test_unknown_preset_raises(self):
        with pytest.raises(ValueError):
            interval_dates("xyz")

    def test_1m_from_jan_31(self):
        """1月31日往前1月 → 12月31日（不应是12月32日）。"""
        ref = datetime.date(2024, 1, 31)
        start, end = interval_dates("1m", ref)
        assert start == datetime.date(2023, 12, 31)


class TestTwoDataPoints:
    """只有两个数据点（最小可计算情况）。"""

    def test_all_metrics_two_points(self):
        nav = make_nav([1.0, 1.05])
        metrics = calc_all_metrics(nav)
        assert metrics["total_return"] == pytest.approx(0.05, abs=0.001)
        assert metrics["max_drawdown"] == 0.0  # 单调上升
        assert metrics["annualized_volatility"] == 0.0  # 只有1个return, std(ddof=1)=NaN→0

    def test_two_points_loss(self):
        nav = make_nav([1.0, 0.9])
        metrics = calc_all_metrics(nav)
        assert metrics["total_return"] == pytest.approx(-0.10, abs=0.001)
        dd, _, _ = calc_max_drawdown(nav)
        assert dd == pytest.approx(-0.10, abs=0.001)
