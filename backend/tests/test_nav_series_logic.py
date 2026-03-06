"""测试 get_nav_series 的复权逻辑和交替模式检测。

场景:
1. 正常基金: unit_nav == cumulative_nav → 使用cumulative_nav
2. 分红基金: cumulative_nav存在且连续 → 使用cumulative_nav
3. 交替基金: unit_nav和cumulative_nav交替不一致 → 只保留same组
4. 边界情况: 空数据、单条记录、全部NULL
"""

import datetime
import sys
import os
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.fund_service import FundService


def make_nav_record(nav_date, unit_nav, cumulative_nav=None):
    """创建模拟的NavHistory记录。"""
    r = MagicMock()
    r.nav_date = nav_date
    r.unit_nav = Decimal(str(unit_nav)) if unit_nav is not None else None
    r.cumulative_nav = Decimal(str(cumulative_nav)) if cumulative_nav is not None else None
    return r


@pytest.mark.asyncio
async def test_normal_fund_uses_cumulative():
    """正常基金: unit_nav == cumulative_nav → 用cumulative_nav。"""
    service = FundService()
    records = [
        make_nav_record(datetime.date(2024, 1, 1), 1.0000, 1.0000),
        make_nav_record(datetime.date(2024, 1, 2), 1.0100, 1.0100),
        make_nav_record(datetime.date(2024, 1, 3), 1.0050, 1.0050),
    ]

    with patch.object(service, "get_nav_history", new_callable=AsyncMock, return_value=records):
        series = await service.get_nav_series(MagicMock(), fund_id=1)

    assert len(series) == 3
    assert series.iloc[0] == 1.0
    assert series.iloc[1] == pytest.approx(1.01, abs=1e-6)
    assert series.iloc[2] == pytest.approx(1.005, abs=1e-6)


@pytest.mark.asyncio
async def test_dividend_fund_uses_cumulative():
    """分红基金: cumulative_nav连续, unit_nav跳变 → 优先用cumulative_nav。"""
    service = FundService()
    records = [
        make_nav_record(datetime.date(2024, 1, 1), 1.5000, 1.5000),
        make_nav_record(datetime.date(2024, 1, 2), 1.5100, 1.5100),
        # 分红日: unit_nav下跌, cumulative_nav连续
        make_nav_record(datetime.date(2024, 1, 3), 1.2000, 1.5200),
        make_nav_record(datetime.date(2024, 1, 4), 1.2100, 1.5300),
    ]

    with patch.object(service, "get_nav_history", new_callable=AsyncMock, return_value=records):
        series = await service.get_nav_series(MagicMock(), fund_id=2)

    # 非交替模式(diff组只有2条, <15%), 应使用cumulative_nav
    assert len(series) == 4
    assert series.iloc[2] == pytest.approx(1.52, abs=1e-6)  # 用cumulative_nav
    assert series.iloc[3] == pytest.approx(1.53, abs=1e-6)
    # 确认返回率合理(无-20%跳变)
    returns = series.pct_change().dropna()
    assert all(abs(r) < 0.05 for r in returns)


@pytest.mark.asyncio
async def test_interleaved_fund_filters_same_group():
    """交替基金: unit_nav和cumulative_nav交替不一致 → 只保留same组。"""
    service = FundService()

    # 模拟fund 335的交替模式
    records = []
    base_date = datetime.date(2024, 1, 1)
    for i in range(40):
        d = base_date + datetime.timedelta(days=i)
        if i % 2 == 0:
            # same组: unit_nav == cumulative_nav
            records.append(make_nav_record(d, 1.5 + i * 0.002, 1.5 + i * 0.002))
        else:
            # diff组: unit_nav != cumulative_nav
            records.append(make_nav_record(d, 0.7 + i * 0.001, 0.93 + i * 0.001))

    with patch.object(service, "get_nav_history", new_callable=AsyncMock, return_value=records):
        series = await service.get_nav_series(MagicMock(), fund_id=335)

    # 应只保留same组(20条)
    assert len(series) == 20
    # 检查连续性: 返回率应合理
    returns = series.pct_change().dropna()
    assert all(abs(r) < 0.05 for r in returns)


@pytest.mark.asyncio
async def test_empty_records():
    """空数据 → 返回空Series。"""
    service = FundService()
    with patch.object(service, "get_nav_history", new_callable=AsyncMock, return_value=[]):
        series = await service.get_nav_series(MagicMock(), fund_id=999)

    assert len(series) == 0
    assert series.dtype == float


@pytest.mark.asyncio
async def test_single_record():
    """单条记录 → 返回长度1的Series。"""
    service = FundService()
    records = [make_nav_record(datetime.date(2024, 1, 1), 1.0, 1.0)]

    with patch.object(service, "get_nav_history", new_callable=AsyncMock, return_value=records):
        series = await service.get_nav_series(MagicMock(), fund_id=1)

    assert len(series) == 1


@pytest.mark.asyncio
async def test_null_cumulative_falls_back_to_unit():
    """cumulative_nav为NULL → 回退到unit_nav。"""
    service = FundService()
    records = [
        make_nav_record(datetime.date(2024, 1, 1), 1.0, None),
        make_nav_record(datetime.date(2024, 1, 2), 1.01, None),
    ]

    with patch.object(service, "get_nav_history", new_callable=AsyncMock, return_value=records):
        series = await service.get_nav_series(MagicMock(), fund_id=1)

    assert len(series) == 2
    assert series.iloc[0] == 1.0
    assert series.iloc[1] == pytest.approx(1.01, abs=1e-6)


@pytest.mark.asyncio
async def test_interleaved_detection_threshold():
    """不足15%的diff不触发交替模式检测。"""
    service = FundService()

    # 30条记录, 其中只有3条diff(10% < 15%)
    records = []
    base_date = datetime.date(2024, 1, 1)
    for i in range(30):
        d = base_date + datetime.timedelta(days=i)
        if i in (5, 15, 25):
            # diff组
            records.append(make_nav_record(d, 0.7, 0.93))
        else:
            # same组
            records.append(make_nav_record(d, 1.5 + i * 0.001, 1.5 + i * 0.001))

    with patch.object(service, "get_nav_history", new_callable=AsyncMock, return_value=records):
        series = await service.get_nav_series(MagicMock(), fund_id=1)

    # 不触发交替模式, 应保留全部30条
    assert len(series) == 30
