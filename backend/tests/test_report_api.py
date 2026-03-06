"""Report API integration tests — Round 1 & 2 cross-validation.

Round 1: API endpoint structure and error handling.
Round 2: Report generation with mock data + attribution computation.
"""

from __future__ import annotations

from datetime import date

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base, get_db
from app.main import app
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
async def product_with_snapshots():
    """Create a product with 2 valuation snapshots for testing."""
    async with TestSessionLocal() as session:
        product = Product(
            product_name="测试报告产品",
            product_code="RPT001",
            product_type="live",
        )
        session.add(product)
        await session.flush()

        # Snapshot 1
        snap1 = ValuationSnapshot(
            product_id=product.id,
            valuation_date=date(2026, 1, 15),
            unit_nav=1.05,
            total_nav=10500000,
        )
        session.add(snap1)
        await session.flush()

        # Add L1 items to snap1
        for code, name, pct in [
            ("1102", "股票投资", 60.0),
            ("1109", "基金投资", 30.0),
            ("1107", "银行存款", 10.0),
        ]:
            session.add(ValuationItem(
                snapshot_id=snap1.id,
                item_code=code,
                item_name=name,
                level=1,
                value_pct_nav=pct,
                market_value=pct * 100000,
            ))

        # Snapshot 2
        snap2 = ValuationSnapshot(
            product_id=product.id,
            valuation_date=date(2026, 2, 15),
            unit_nav=1.08,
            total_nav=10800000,
        )
        session.add(snap2)
        await session.flush()

        for code, name, pct in [
            ("1102", "股票投资", 55.0),
            ("1109", "基金投资", 35.0),
            ("1107", "银行存款", 10.0),
        ]:
            session.add(ValuationItem(
                snapshot_id=snap2.id,
                item_code=code,
                item_name=name,
                level=1,
                value_pct_nav=pct,
                market_value=pct * 100000,
            ))

        await session.commit()
        return product.id


# ---------------------------------------------------------------------------
# Round 1: API structure and error handling
# ---------------------------------------------------------------------------

class TestReportAPIRound1:
    """Round 1 — endpoint availability and error handling."""

    @pytest.mark.asyncio
    async def test_attribution_product_not_found(self, client):
        resp = await client.get(
            "/api/v1/reports/999/attribution",
            params={"period_start": "2026-01-01", "period_end": "2026-02-28"},
        )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_attribution_missing_params(self, client):
        resp = await client.get("/api/v1/reports/1/attribution")
        assert resp.status_code == 422  # missing required query params

    @pytest.mark.asyncio
    async def test_generate_report_product_not_found(self, client):
        resp = await client.post("/api/v1/reports/generate", json={
            "product_id": 999,
            "report_type": "monthly",
            "period_start": "2026-01-01",
            "period_end": "2026-02-28",
        })
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_generate_report_invalid_type(self, client):
        resp = await client.post("/api/v1/reports/generate", json={
            "product_id": 1,
            "report_type": "invalid_type",
            "period_start": "2026-01-01",
            "period_end": "2026-02-28",
        })
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_attribution_invalid_granularity(self, client, product_with_snapshots):
        pid = product_with_snapshots
        resp = await client.get(
            f"/api/v1/reports/{pid}/attribution",
            params={
                "period_start": "2026-01-01",
                "period_end": "2026-02-28",
                "granularity": "invalid",
            },
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Round 2: Report generation with real mock data
# ---------------------------------------------------------------------------

class TestReportAPIRound2:
    """Round 2 — report generation and attribution computation."""

    @pytest.mark.asyncio
    async def test_attribution_with_snapshots(self, client, product_with_snapshots):
        pid = product_with_snapshots
        resp = await client.get(
            f"/api/v1/reports/{pid}/attribution",
            params={
                "period_start": "2026-01-01",
                "period_end": "2026-02-28",
                "granularity": "monthly",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["product_id"] == pid
        assert data["granularity"] == "monthly"
        # Without benchmark, returns empty periods
        assert isinstance(data["periods"], list)

    @pytest.mark.asyncio
    async def test_generate_report_pdf(self, client, product_with_snapshots):
        """Generate a PDF report and verify it's a valid PDF."""
        pid = product_with_snapshots
        resp = await client.post("/api/v1/reports/generate", json={
            "product_id": pid,
            "report_type": "monthly",
            "period_start": "2026-01-01",
            "period_end": "2026-02-28",
        })
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/pdf"
        content = resp.content
        assert content[:5] == b"%PDF-"
        assert len(content) > 1000

    @pytest.mark.asyncio
    async def test_generate_weekly_report(self, client, product_with_snapshots):
        pid = product_with_snapshots
        resp = await client.post("/api/v1/reports/generate", json={
            "product_id": pid,
            "report_type": "weekly",
            "period_start": "2026-02-01",
            "period_end": "2026-02-15",
        })
        assert resp.status_code == 200
        assert resp.content[:5] == b"%PDF-"

    @pytest.mark.asyncio
    async def test_attribution_response_structure(self, client, product_with_snapshots):
        pid = product_with_snapshots
        resp = await client.get(
            f"/api/v1/reports/{pid}/attribution",
            params={"period_start": "2026-01-01", "period_end": "2026-02-28"},
        )
        data = resp.json()
        assert "product_id" in data
        assert "period_start" in data
        assert "period_end" in data
        assert "granularity" in data
        assert "periods" in data
        assert "cumulative_excess" in data
        assert "cumulative_allocation" in data
        assert "cumulative_selection" in data
        assert "cumulative_interaction" in data
        assert "aggregated_categories" in data

    @pytest.mark.asyncio
    async def test_report_content_disposition(self, client, product_with_snapshots):
        pid = product_with_snapshots
        resp = await client.post("/api/v1/reports/generate", json={
            "product_id": pid,
            "report_type": "monthly",
            "period_start": "2026-01-01",
            "period_end": "2026-02-28",
        })
        assert "content-disposition" in resp.headers
        assert "attachment" in resp.headers["content-disposition"]
