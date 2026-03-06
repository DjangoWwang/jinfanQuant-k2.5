"""产品运营API测试用例。

覆盖：产品CRUD、估值表上传解析、NAV序列、子基金关联、移动端API。
使用 SQLite 异步内存数据库 + httpx.AsyncClient。
"""

from __future__ import annotations

import io
import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base, get_db
from app.main import app
from app.models.fund import Fund
from app.models.product import Product, ValuationSnapshot, ValuationItem

# ---------------------------------------------------------------------------
# Test database
# ---------------------------------------------------------------------------

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

engine = create_async_engine(TEST_DATABASE_URL, echo=False)
TestSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _override_get_db():
    async with TestSessionLocal() as session:
        yield session


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(autouse=True)
async def setup_database():
    app.dependency_overrides[get_db] = _override_get_db
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def seed_fund():
    """Create a test fund for sub-fund linking."""
    async with TestSessionLocal() as session:
        fund = Fund(
            fund_name="测试基金A",
            filing_number="AKE73A",
            manager_name="测试管理人",
            strategy_type="股票策略",
        )
        session.add(fund)
        await session.commit()
        await session.refresh(fund)
        return fund.id


# ---------------------------------------------------------------------------
# Product CRUD tests
# ---------------------------------------------------------------------------

class TestProductCRUD:
    @pytest.mark.asyncio
    async def test_create_product(self, client):
        resp = await client.post("/api/v1/products/", json={
            "product_name": "测试FOF产品",
            "product_code": "TEST001",
            "product_type": "live",
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["product_name"] == "测试FOF产品"
        assert data["product_code"] == "TEST001"
        assert data["product_type"] == "live"
        assert data["id"] > 0

    @pytest.mark.asyncio
    async def test_list_products_empty(self, client):
        resp = await client.get("/api/v1/products/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []
        assert data["total"] == 0

    @pytest.mark.asyncio
    async def test_list_products_with_data(self, client):
        await client.post("/api/v1/products/", json={
            "product_name": "实盘产品", "product_type": "live"
        })
        await client.post("/api/v1/products/", json={
            "product_name": "模拟产品", "product_type": "simulation"
        })

        # All
        resp = await client.get("/api/v1/products/")
        assert resp.json()["total"] == 2

        # Filter by type
        resp = await client.get("/api/v1/products/?product_type=live")
        assert resp.json()["total"] == 1
        assert resp.json()["items"][0]["product_type"] == "live"

    @pytest.mark.asyncio
    async def test_get_product(self, client):
        create = await client.post("/api/v1/products/", json={
            "product_name": "详情测试",
            "custodian": "国信证券",
            "inception_date": "2024-06-01",
        })
        pid = create.json()["id"]

        resp = await client.get(f"/api/v1/products/{pid}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["product_name"] == "详情测试"
        assert data["custodian"] == "国信证券"

    @pytest.mark.asyncio
    async def test_get_product_not_found(self, client):
        resp = await client.get("/api/v1/products/999")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_update_product(self, client):
        create = await client.post("/api/v1/products/", json={
            "product_name": "原始名称"
        })
        pid = create.json()["id"]

        resp = await client.patch(f"/api/v1/products/{pid}", json={
            "product_name": "新名称",
            "custodian": "招商证券"
        })
        assert resp.status_code == 200
        assert resp.json()["product_name"] == "新名称"
        assert resp.json()["custodian"] == "招商证券"

    @pytest.mark.asyncio
    async def test_delete_product(self, client):
        create = await client.post("/api/v1/products/", json={
            "product_name": "待删除"
        })
        pid = create.json()["id"]

        resp = await client.delete(f"/api/v1/products/{pid}")
        assert resp.status_code == 204

        resp = await client.get(f"/api/v1/products/{pid}")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# NAV series
# ---------------------------------------------------------------------------

class TestProductNAV:
    @pytest.mark.asyncio
    async def test_nav_empty(self, client):
        create = await client.post("/api/v1/products/", json={
            "product_name": "空产品"
        })
        pid = create.json()["id"]

        resp = await client.get(f"/api/v1/products/{pid}/nav")
        assert resp.status_code == 200
        assert resp.json()["nav_series"] == []

    @pytest.mark.asyncio
    async def test_valuations_empty(self, client):
        create = await client.post("/api/v1/products/", json={
            "product_name": "空产品"
        })
        pid = create.json()["id"]

        resp = await client.get(f"/api/v1/products/{pid}/valuations")
        assert resp.status_code == 200
        assert resp.json()["total"] == 0


# ---------------------------------------------------------------------------
# Mobile dashboard
# ---------------------------------------------------------------------------

class TestMobileDashboard:
    @pytest.mark.asyncio
    async def test_dashboard_empty(self, client):
        resp = await client.get("/api/v1/mobile/dashboard")
        assert resp.status_code == 200
        data = resp.json()
        assert data["live_products"] == []
        assert data["simulation_products"] == []

    @pytest.mark.asyncio
    async def test_dashboard_with_products(self, client):
        await client.post("/api/v1/products/", json={
            "product_name": "实盘1", "product_type": "live"
        })
        await client.post("/api/v1/products/", json={
            "product_name": "模拟1", "product_type": "simulation"
        })

        resp = await client.get("/api/v1/mobile/dashboard")
        data = resp.json()
        assert len(data["live_products"]) == 1
        assert len(data["simulation_products"]) == 1
        assert data["live_products"][0]["product_name"] == "实盘1"


# ---------------------------------------------------------------------------
# Valuation upload (requires real Excel file)
# ---------------------------------------------------------------------------

class TestValuationUpload:
    SAMPLE_0213 = "D:/AI/Claude code/FOF平台开发/估值表日报-GF1077-博孚利鹭岛晋帆私募证券投资基金-4-20260213.xlsx"
    SAMPLE_0227 = "D:/AI/Claude code/FOF平台开发/估值表日报-GF1077-博孚利鹭岛晋帆私募证券投资基金-4-20260227.xlsx"

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not os.path.exists(SAMPLE_0213),
        reason="Sample valuation file not available"
    )
    async def test_upload_valuation(self, client, seed_fund):
        create = await client.post("/api/v1/products/", json={
            "product_name": "上传测试"
        })
        pid = create.json()["id"]

        with open(self.SAMPLE_0213, "rb") as f:
            resp = await client.post(
                f"/api/v1/products/{pid}/valuation",
                files={"file": ("valuation_0213.xlsx", f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["valuation_date"] == "2026-02-13"
        assert data["unit_nav"] == 1.0923
        assert data["total_nav"] == 16343440.32
        assert data["holdings_count"] > 50
        assert data["sub_funds_count"] == 12
        assert data["sub_funds_linked"] >= 1  # At least AKE73A should link

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not os.path.exists(SAMPLE_0227),
        reason="Sample valuation file not available"
    )
    async def test_upload_two_valuations_and_nav(self, client, seed_fund):
        create = await client.post("/api/v1/products/", json={
            "product_name": "NAV测试"
        })
        pid = create.json()["id"]

        # Upload both
        for path in [self.SAMPLE_0213, self.SAMPLE_0227]:
            with open(path, "rb") as f:
                resp = await client.post(
                    f"/api/v1/products/{pid}/valuation",
                    files={"file": ("val.xlsx", f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
                )
                assert resp.status_code == 200

        # Check NAV series has 2 points
        resp = await client.get(f"/api/v1/products/{pid}/nav")
        assert resp.status_code == 200
        nav = resp.json()["nav_series"]
        assert len(nav) == 2
        assert nav[0]["date"] == "2026-02-13"
        assert nav[0]["unit_nav"] == 1.0923
        assert nav[1]["date"] == "2026-02-27"
        assert nav[1]["unit_nav"] == 1.105

        # Check product detail shows latest
        resp = await client.get(f"/api/v1/products/{pid}")
        detail = resp.json()
        assert detail["latest_nav"] == 1.105
        assert detail["latest_total_nav"] == 16533236.56
        assert detail["snapshot_count"] == 2

    @pytest.mark.asyncio
    async def test_upload_invalid_format(self, client):
        create = await client.post("/api/v1/products/", json={
            "product_name": "格式测试"
        })
        pid = create.json()["id"]

        resp = await client.post(
            f"/api/v1/products/{pid}/valuation",
            files={"file": ("test.txt", io.BytesIO(b"not excel"), "text/plain")}
        )
        assert resp.status_code == 400
