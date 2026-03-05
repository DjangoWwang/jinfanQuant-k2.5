"""基金列表API测试用例。

使用 SQLite 异步内存数据库 + httpx.AsyncClient，
覆盖分页、多选筛选、搜索、策略分类聚合等场景。
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base, get_db
from app.main import app
from app.models.fund import Fund

# ---------------------------------------------------------------------------
# 测试数据库配置（SQLite 异步内存库）
# ---------------------------------------------------------------------------

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

engine = create_async_engine(TEST_DATABASE_URL, echo=False)
TestSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _override_get_db():
    """替换 FastAPI 依赖，使用测试数据库 session。"""
    async with TestSessionLocal() as session:
        yield session


app.dependency_overrides[get_db] = _override_get_db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(autouse=True)
async def setup_database():
    """每个测试前建表、后销毁，保证隔离。"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def seed_funds():
    """插入测试基金数据，覆盖多种策略类型和频率。"""
    funds_data = [
        # 股票多头 - 日频
        Fund(fund_name="量化阿尔法1号", filing_number="SY0001", manager_name="量化资产",
             strategy_type="股票类", strategy_sub="主观多头", nav_frequency="daily", status="active"),
        Fund(fund_name="价值成长2号", filing_number="SY0002", manager_name="价值资本",
             strategy_type="股票类", strategy_sub="量化多头", nav_frequency="daily", status="active"),
        Fund(fund_name="股票精选3号", filing_number="SY0003", manager_name="精选投资",
             strategy_type="股票类", strategy_sub="主观多头", nav_frequency="weekly", status="active"),
        # 期货策略
        Fund(fund_name="CTA趋势跟踪", filing_number="SY0004", manager_name="期货管理",
             strategy_type="期货策略", strategy_sub="量化期货", nav_frequency="daily", status="active"),
        Fund(fund_name="商品套利基金", filing_number="SY0005", manager_name="套利资产",
             strategy_type="期货策略", strategy_sub="套利策略", nav_frequency="daily", status="active"),
        # 债券策略
        Fund(fund_name="固收增强1号", filing_number="SY0006", manager_name="固收资管",
             strategy_type="债券策略", strategy_sub="纯债策略", nav_frequency="weekly", status="active"),
        # 组合基金
        Fund(fund_name="FOF配置基金", filing_number="SY0007", manager_name="量化资产",
             strategy_type="组合基金", strategy_sub="FOF", nav_frequency="daily", status="active"),
        # 已清盘基金 (status != active，不应出现在列表中)
        Fund(fund_name="已清盘基金X", filing_number="SY9999", manager_name="退出管理",
             strategy_type="股票类", strategy_sub="主观多头", nav_frequency="daily", status="inactive"),
    ]
    async with TestSessionLocal() as session:
        session.add_all(funds_data)
        await session.commit()
    return funds_data


@pytest_asyncio.fixture
async def client():
    """异步 HTTP 测试客户端。"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ---------------------------------------------------------------------------
# 基金列表分页测试
# ---------------------------------------------------------------------------

class TestFundListPagination:
    """测试基金列表分页功能。"""

    @pytest.mark.asyncio
    async def test_default_pagination(self, client: AsyncClient, seed_funds):
        """默认分页参数应返回全部active基金。"""
        resp = await client.get("/api/v1/funds/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["page"] == 1
        assert data["page_size"] == 50
        # 8条数据中有7条active
        assert data["total"] == 7
        assert len(data["items"]) == 7

    @pytest.mark.asyncio
    async def test_custom_page_size(self, client: AsyncClient, seed_funds):
        """指定 page_size=3，第1页应返回3条记录。"""
        resp = await client.get("/api/v1/funds/", params={"page": 1, "page_size": 3})
        assert resp.status_code == 200
        data = resp.json()
        assert data["page"] == 1
        assert data["page_size"] == 3
        assert data["total"] == 7
        assert len(data["items"]) == 3

    @pytest.mark.asyncio
    async def test_second_page(self, client: AsyncClient, seed_funds):
        """第2页应返回剩余记录。"""
        resp = await client.get("/api/v1/funds/", params={"page": 2, "page_size": 3})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 3
        assert data["total"] == 7

    @pytest.mark.asyncio
    async def test_last_page_partial(self, client: AsyncClient, seed_funds):
        """最后一页可能不满 page_size。"""
        resp = await client.get("/api/v1/funds/", params={"page": 3, "page_size": 3})
        assert resp.status_code == 200
        data = resp.json()
        # 7条数据，第3页剩1条
        assert len(data["items"]) == 1

    @pytest.mark.asyncio
    async def test_page_out_of_range(self, client: AsyncClient, seed_funds):
        """超出范围的页码返回空 items 列表。"""
        resp = await client.get("/api/v1/funds/", params={"page": 100, "page_size": 10})
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []
        assert data["total"] == 7

    @pytest.mark.asyncio
    async def test_response_structure(self, client: AsyncClient, seed_funds):
        """验证返回的 JSON 结构包含必要字段。"""
        resp = await client.get("/api/v1/funds/", params={"page_size": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert "total" in data
        assert "page" in data
        assert "page_size" in data
        assert "items" in data
        item = data["items"][0]
        # 验证 FundResponse 中的关键字段
        for field in ["id", "fund_name", "strategy_type", "strategy_sub",
                       "nav_frequency", "status"]:
            assert field in item


# ---------------------------------------------------------------------------
# 策略多选筛选测试
# ---------------------------------------------------------------------------

class TestStrategyTypeFilter:
    """测试一级策略多选筛选。"""

    @pytest.mark.asyncio
    async def test_single_strategy_type(self, client: AsyncClient, seed_funds):
        """单选一级策略：只返回股票类基金。"""
        resp = await client.get("/api/v1/funds/", params={"strategy_type": "股票类"})
        assert resp.status_code == 200
        data = resp.json()
        # 3条 active 的股票类
        assert data["total"] == 3
        for item in data["items"]:
            assert item["strategy_type"] == "股票类"

    @pytest.mark.asyncio
    async def test_multi_strategy_type(self, client: AsyncClient, seed_funds):
        """多选一级策略（逗号分隔）：股票类 + 期货策略。"""
        resp = await client.get("/api/v1/funds/", params={"strategy_type": "股票类,期货策略"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 5
        types = {item["strategy_type"] for item in data["items"]}
        assert types == {"股票类", "期货策略"}

    @pytest.mark.asyncio
    async def test_nonexistent_strategy_type(self, client: AsyncClient, seed_funds):
        """不存在的策略类型应返回空结果。"""
        resp = await client.get("/api/v1/funds/", params={"strategy_type": "不存在的策略"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["items"] == []


# ---------------------------------------------------------------------------
# 二级策略多选测试
# ---------------------------------------------------------------------------

class TestStrategySubFilter:
    """测试二级策略多选筛选。"""

    @pytest.mark.asyncio
    async def test_single_strategy_sub(self, client: AsyncClient, seed_funds):
        """单选二级策略：主观多头。"""
        resp = await client.get("/api/v1/funds/", params={"strategy_sub": "主观多头"})
        assert resp.status_code == 200
        data = resp.json()
        # SY0001 和 SY0003 是 active 的主观多头
        assert data["total"] == 2
        for item in data["items"]:
            assert item["strategy_sub"] == "主观多头"

    @pytest.mark.asyncio
    async def test_multi_strategy_sub(self, client: AsyncClient, seed_funds):
        """多选二级策略：主观多头 + 量化期货。"""
        resp = await client.get("/api/v1/funds/", params={"strategy_sub": "主观多头,量化期货"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        subs = {item["strategy_sub"] for item in data["items"]}
        assert subs == {"主观多头", "量化期货"}


# ---------------------------------------------------------------------------
# 频率筛选测试
# ---------------------------------------------------------------------------

class TestNavFrequencyFilter:
    """测试净值频率筛选。"""

    @pytest.mark.asyncio
    async def test_daily_frequency(self, client: AsyncClient, seed_funds):
        """筛选日频基金。"""
        resp = await client.get("/api/v1/funds/", params={"nav_frequency": "daily"})
        assert resp.status_code == 200
        data = resp.json()
        # active 日频: SY0001, SY0002, SY0004, SY0005, SY0007 = 5条
        assert data["total"] == 5
        for item in data["items"]:
            assert item["nav_frequency"] == "daily"

    @pytest.mark.asyncio
    async def test_weekly_frequency(self, client: AsyncClient, seed_funds):
        """筛选周频基金。"""
        resp = await client.get("/api/v1/funds/", params={"nav_frequency": "weekly"})
        assert resp.status_code == 200
        data = resp.json()
        # active 周频: SY0003, SY0006 = 2条
        assert data["total"] == 2
        for item in data["items"]:
            assert item["nav_frequency"] == "weekly"


# ---------------------------------------------------------------------------
# 搜索测试
# ---------------------------------------------------------------------------

class TestSearchFilter:
    """测试模糊搜索（基金名称/备案号/管理人）。"""

    @pytest.mark.asyncio
    async def test_search_by_fund_name(self, client: AsyncClient, seed_funds):
        """按基金名称搜索"量化"。"""
        resp = await client.get("/api/v1/funds/", params={"search": "量化"})
        assert resp.status_code == 200
        data = resp.json()
        # "量化阿尔法1号" + "量化多头"(价值成长2号的strategy_sub，不匹配名称)
        # 实际名称包含"量化"的: "量化阿尔法1号"
        # manager_name包含"量化"的: "量化资产"(SY0001, SY0007)
        # 所以匹配: SY0001(名称+管理人), SY0007(管理人)
        assert data["total"] >= 1
        names_and_managers = []
        for item in data["items"]:
            names_and_managers.append(item["fund_name"])
            names_and_managers.append(item["manager_name"] or "")
        # 确保每条结果都包含"量化"（在名称或管理人中）
        for item in data["items"]:
            match = (
                "量化" in (item["fund_name"] or "")
                or "量化" in (item["filing_number"] or "")
                or "量化" in (item["manager_name"] or "")
            )
            assert match, f"结果不匹配搜索关键词: {item}"

    @pytest.mark.asyncio
    async def test_search_by_filing_number(self, client: AsyncClient, seed_funds):
        """按备案号搜索。"""
        resp = await client.get("/api/v1/funds/", params={"search": "SY0004"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["filing_number"] == "SY0004"

    @pytest.mark.asyncio
    async def test_search_by_manager(self, client: AsyncClient, seed_funds):
        """按管理人名称搜索。"""
        resp = await client.get("/api/v1/funds/", params={"search": "固收"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["manager_name"] == "固收资管"

    @pytest.mark.asyncio
    async def test_search_no_match(self, client: AsyncClient, seed_funds):
        """搜索不匹配任何基金。"""
        resp = await client.get("/api/v1/funds/", params={"search": "ZZZZZ不存在"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["items"] == []


# ---------------------------------------------------------------------------
# 组合筛选测试
# ---------------------------------------------------------------------------

class TestCombinedFilters:
    """测试多条件组合筛选。"""

    @pytest.mark.asyncio
    async def test_strategy_type_and_frequency(self, client: AsyncClient, seed_funds):
        """一级策略 + 频率筛选：股票类 + daily。"""
        resp = await client.get("/api/v1/funds/", params={
            "strategy_type": "股票类",
            "nav_frequency": "daily",
        })
        assert resp.status_code == 200
        data = resp.json()
        # 股票类 daily: SY0001, SY0002
        assert data["total"] == 2
        for item in data["items"]:
            assert item["strategy_type"] == "股票类"
            assert item["nav_frequency"] == "daily"

    @pytest.mark.asyncio
    async def test_strategy_type_and_search(self, client: AsyncClient, seed_funds):
        """一级策略 + 搜索：期货策略 + 搜索"套利"。"""
        resp = await client.get("/api/v1/funds/", params={
            "strategy_type": "期货策略",
            "search": "套利",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["fund_name"] == "商品套利基金"

    @pytest.mark.asyncio
    async def test_all_filters_combined(self, client: AsyncClient, seed_funds):
        """三条件组合：策略类型 + 频率 + 搜索。"""
        resp = await client.get("/api/v1/funds/", params={
            "strategy_type": "股票类",
            "nav_frequency": "daily",
            "search": "量化",
        })
        assert resp.status_code == 200
        data = resp.json()
        # 股票类 + daily + 名称或管理人含"量化": SY0001
        assert data["total"] == 1
        assert data["items"][0]["fund_name"] == "量化阿尔法1号"

    @pytest.mark.asyncio
    async def test_multi_strategy_with_frequency(self, client: AsyncClient, seed_funds):
        """多选策略 + 频率筛选：(股票类,期货策略) + weekly。"""
        resp = await client.get("/api/v1/funds/", params={
            "strategy_type": "股票类,期货策略",
            "nav_frequency": "weekly",
        })
        assert resp.status_code == 200
        data = resp.json()
        # 股票类 weekly: SY0003; 期货策略 weekly: 无
        assert data["total"] == 1
        assert data["items"][0]["fund_name"] == "股票精选3号"


# ---------------------------------------------------------------------------
# 策略分类聚合测试
# ---------------------------------------------------------------------------

class TestStrategyCategories:
    """测试策略分类聚合接口。"""

    @pytest.mark.asyncio
    async def test_strategy_categories_structure(self, client: AsyncClient, seed_funds):
        """策略分类聚合返回树状结构。"""
        resp = await client.get("/api/v1/funds/strategy-categories")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0

        # 每个节点应有 strategy_type, total, subs
        for cat in data:
            assert "strategy_type" in cat
            assert "total" in cat
            assert "subs" in cat
            assert isinstance(cat["subs"], list)

    @pytest.mark.asyncio
    async def test_strategy_categories_counts(self, client: AsyncClient, seed_funds):
        """验证策略分类的基金数量正确。"""
        resp = await client.get("/api/v1/funds/strategy-categories")
        data = resp.json()
        cat_map = {c["strategy_type"]: c for c in data}

        # 股票类: 3条 active (SY0001, SY0002, SY0003)
        assert cat_map["股票类"]["total"] == 3
        # 期货策略: 2条 active
        assert cat_map["期货策略"]["total"] == 2
        # 债券策略: 1条 active
        assert cat_map["债券策略"]["total"] == 1
        # 组合基金: 1条 active
        assert cat_map["组合基金"]["total"] == 1

    @pytest.mark.asyncio
    async def test_strategy_categories_subs(self, client: AsyncClient, seed_funds):
        """验证二级策略子分类正确。"""
        resp = await client.get("/api/v1/funds/strategy-categories")
        data = resp.json()
        cat_map = {c["strategy_type"]: c for c in data}

        stock_subs = {s["name"]: s["count"] for s in cat_map["股票类"]["subs"]}
        assert stock_subs["主观多头"] == 2  # SY0001, SY0003
        assert stock_subs["量化多头"] == 1  # SY0002

    @pytest.mark.asyncio
    async def test_strategy_categories_excludes_inactive(self, client: AsyncClient, seed_funds):
        """已清盘基金不计入策略分类统计。"""
        resp = await client.get("/api/v1/funds/strategy-categories")
        data = resp.json()
        # 总数应为7（不包含inactive的SY9999）
        total = sum(c["total"] for c in data)
        assert total == 7

    @pytest.mark.asyncio
    async def test_strategy_categories_sorted_by_count(self, client: AsyncClient, seed_funds):
        """策略分类应按基金数量降序排列。"""
        resp = await client.get("/api/v1/funds/strategy-categories")
        data = resp.json()
        totals = [c["total"] for c in data]
        assert totals == sorted(totals, reverse=True)


# ---------------------------------------------------------------------------
# 空结果与边界测试
# ---------------------------------------------------------------------------

class TestEmptyAndEdgeCases:
    """测试空结果和边界条件。"""

    @pytest.mark.asyncio
    async def test_empty_database(self, client: AsyncClient):
        """数据库无数据时返回空列表。"""
        resp = await client.get("/api/v1/funds/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["items"] == []

    @pytest.mark.asyncio
    async def test_inactive_funds_excluded(self, client: AsyncClient, seed_funds):
        """inactive 状态的基金不出现在列表中。"""
        resp = await client.get("/api/v1/funds/", params={"search": "已清盘"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0

    @pytest.mark.asyncio
    async def test_empty_strategy_categories(self, client: AsyncClient):
        """数据库无数据时策略分类返回空列表。"""
        resp = await client.get("/api/v1/funds/strategy-categories")
        assert resp.status_code == 200
        data = resp.json()
        assert data == []

    @pytest.mark.asyncio
    async def test_page_size_max_boundary(self, client: AsyncClient, seed_funds):
        """page_size 设为最大值 200 应正常工作。"""
        resp = await client.get("/api/v1/funds/", params={"page_size": 200})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 7

    @pytest.mark.asyncio
    async def test_page_size_exceeds_max(self, client: AsyncClient, seed_funds):
        """page_size 超过 200 应返回 422 校验错误。"""
        resp = await client.get("/api/v1/funds/", params={"page_size": 201})
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_page_zero_invalid(self, client: AsyncClient, seed_funds):
        """page=0 应返回 422 校验错误。"""
        resp = await client.get("/api/v1/funds/", params={"page": 0})
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_strategy_type_with_spaces(self, client: AsyncClient, seed_funds):
        """逗号分隔的策略类型带空格应正确解析。"""
        resp = await client.get("/api/v1/funds/", params={
            "strategy_type": " 股票类 , 期货策略 ",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 5
