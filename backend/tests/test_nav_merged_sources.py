"""测试双数据源合并和区间收益计算。

覆盖:
1. 团队净值和平台净值合并逻辑
2. calc_period_return 区间收益计算
3. 鹭岛晋帆特殊日期处理 (2025.5.23开始)
"""

import datetime
import sys
import os

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.crawler.fof99.nav_scraper import merge_nav_sources
from app.engine.metrics import calc_period_return, calc_interval_metrics


def make_nav(values, start_date=datetime.date(2024, 1, 1), freq_days=1):
    """生成NAV Series。"""
    dates = [start_date + datetime.timedelta(days=i * freq_days) for i in range(len(values))]
    return pd.Series(values, index=pd.DatetimeIndex(dates))


class TestMergeNavSources:
    """测试双数据源合并。"""

    def test_merge_empty_sources(self):
        """测试两个空数据源合并。"""
        result = merge_nav_sources([], [], "fund1")
        assert result == []

    def test_merge_only_team_nav(self):
        """测试只有团队净值。"""
        team_nav = [
            {"nav_date": "2024-01-01", "unit_nav": 1.0, "cumulative_nav": 1.0},
            {"nav_date": "2024-01-02", "unit_nav": 1.01, "cumulative_nav": 1.01},
        ]
        result = merge_nav_sources(team_nav, [], "fund1")
        assert len(result) == 2
        assert result[0]["data_source"] == "team"
        assert result[1]["data_source"] == "team"

    def test_merge_only_platform_nav(self):
        """测试只有平台净值。"""
        platform_nav = [
            {"nav_date": "2024-01-01", "unit_nav": 1.0, "cumulative_nav": 1.0},
            {"nav_date": "2024-01-02", "unit_nav": 1.02, "cumulative_nav": 1.02},
        ]
        result = merge_nav_sources([], platform_nav, "fund1")
        assert len(result) == 2
        assert result[0]["data_source"] == "platform"
        assert result[1]["data_source"] == "platform"

    def test_merge_priority_team_over_platform(self):
        """测试同一天团队净值优先于平台净值。"""
        team_nav = [
            {"nav_date": "2024-01-02", "unit_nav": 1.01, "cumulative_nav": 1.01},
        ]
        platform_nav = [
            {"nav_date": "2024-01-01", "unit_nav": 1.0, "cumulative_nav": 1.0},
            {"nav_date": "2024-01-02", "unit_nav": 1.02, "cumulative_nav": 1.02},  # 同一天的另一个值
        ]
        result = merge_nav_sources(team_nav, platform_nav, "fund1")
        assert len(result) == 2
        
        # 01-01 应该使用平台净值
        jan_01 = next(r for r in result if r["nav_date"] == "2024-01-01")
        assert jan_01["data_source"] == "platform"
        assert jan_01["unit_nav"] == 1.0
        
        # 01-02 应该使用团队净值(优先)
        jan_02 = next(r for r in result if r["nav_date"] == "2024-01-02")
        assert jan_02["data_source"] == "team"
        assert jan_02["unit_nav"] == 1.01

    def test_merge_sorted_by_date(self):
        """测试合并结果按日期排序。"""
        team_nav = [
            {"nav_date": "2024-01-03", "unit_nav": 1.03, "cumulative_nav": 1.03},
            {"nav_date": "2024-01-01", "unit_nav": 1.01, "cumulative_nav": 1.01},
        ]
        platform_nav = [
            {"nav_date": "2024-01-02", "unit_nav": 1.02, "cumulative_nav": 1.02},
        ]
        result = merge_nav_sources(team_nav, platform_nav, "fund1")
        assert len(result) == 3
        assert result[0]["nav_date"] == "2024-01-01"
        assert result[1]["nav_date"] == "2024-01-02"
        assert result[2]["nav_date"] == "2024-01-03"


class TestCalcPeriodReturn:
    """测试区间收益计算。"""

    def test_basic_return_calculation(self):
        """测试基本收益率计算。"""
        nav = make_nav([1.0, 1.05, 1.10, 1.08])
        result = calc_period_return(nav)
        
        assert result["return"] == pytest.approx(0.08, abs=0.001)  # 1.08/1.0 - 1
        assert result["start_nav"] == 1.0
        assert result["end_nav"] == 1.08
        assert result["start_date"] == "2024-01-01"
        assert result["end_date"] == "2024-01-04"

    def test_with_inception_date_truncation(self):
        """测试成立日期截断。"""
        # NAV从2024-01-01开始
        nav = make_nav([1.0, 1.02, 1.05, 1.10], start_date=datetime.date(2024, 1, 1))
        
        # 但基金从2024-01-03成立(模拟鹭岛晋帆场景)
        inception_date = datetime.date(2024, 1, 3)
        result = calc_period_return(nav, inception_date=inception_date)
        
        # 应该从2024-01-03开始计算
        assert result["start_nav"] == 1.05  # 第三天的净值
        assert result["end_nav"] == 1.10
        assert result["return"] == pytest.approx(0.0476, abs=0.001)  # 1.10/1.05 - 1
        assert result["start_date"] == "2024-01-03"

    def test_single_point_return_zero(self):
        """测试单点数据收益率为0。"""
        nav = make_nav([1.05])
        result = calc_period_return(nav)
        
        assert result["return"] == 0.0
        assert result["start_nav"] == 1.05
        assert result["end_nav"] == 1.05

    def test_empty_series(self):
        """测试空序列。"""
        nav = pd.Series(dtype=float)
        result = calc_period_return(nav)
        
        assert result["return"] == 0.0
        assert result["start_nav"] == 0.0
        assert result["end_nav"] == 0.0
        assert result["start_date"] == ""

    def test_with_start_end_dates(self):
        """测试指定起止日期。"""
        nav = make_nav([1.0, 1.02, 1.05, 1.10, 1.12], start_date=datetime.date(2024, 1, 1))
        
        start_date = datetime.date(2024, 1, 2)
        end_date = datetime.date(2024, 1, 4)
        result = calc_period_return(nav, start_date=start_date, end_date=end_date)
        
        assert result["start_nav"] == 1.02
        assert result["end_nav"] == 1.10
        assert result["return"] == pytest.approx(0.0784, abs=0.001)


class TestLudaoJinFanScenario:
    """测试鹭岛晋帆场景 (2025.5.23开始运作)。"""

    def test_ludao_inception_date_handling(self):
        """模拟鹭岛晋帆从2025.5.23开始的场景。"""
        # 创建从2025-01-01开始的净值数据(但基金实际从5.23开始)
        dates = pd.date_range(start="2025-01-01", periods=150, freq="D")
        values = [1.0 + i * 0.001 for i in range(150)]  # 简单增长
        nav = pd.Series(values, index=dates)
        
        # 基金从2025-05-23开始
        inception_date = datetime.date(2025, 5, 23)
        
        result = calc_period_return(nav, inception_date=inception_date)
        
        # 验证起始日期被正确截断
        assert result["start_date"] == "2025-05-23"
        
        # 计算预期收益率
        start_idx = dates.get_loc("2025-05-23")
        expected_start_nav = values[start_idx]
        expected_return = values[-1] / expected_start_nav - 1
        
        assert result["start_nav"] == pytest.approx(expected_start_nav, abs=0.0001)
        assert result["return"] == pytest.approx(expected_return, abs=0.0001)

    def test_interval_metrics_with_inception(self):
        """测试区间指标计算带成立日期。"""
        dates = pd.date_range(start="2025-01-01", periods=100, freq="D")
        values = [1.0 + i * 0.001 for i in range(100)]
        nav = pd.Series(values, index=dates)
        
        inception_date = datetime.date(2025, 3, 1)  # 从3月1日开始
        
        result = calc_interval_metrics(
            nav,
            presets=["ytd", "1m"],
            reference_date=datetime.date(2025, 4, 10),
            inception_date=inception_date,
        )
        
        # 验证返回了结果
        assert "ytd" in result
        assert "1m" in result
        
        # YTD应该从inception_date开始计算,而不是年初
        if result["ytd"]:
            # 验证净值序列被截断
            detail = result["ytd"].get("period_return_detail", {})
            if detail:
                actual_start = datetime.datetime.strptime(detail["start_date"], "%Y-%m-%d").date()
                assert actual_start >= inception_date


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
