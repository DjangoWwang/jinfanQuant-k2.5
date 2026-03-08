"""P3-4 API安全加固测试: 安全头中间件、速率限制中间件、端点认证保护。

覆盖：
- SecurityHeadersMiddleware: CSP, X-Frame-Options, HSTS 条件判定, 安全头一致性
- RateLimitMiddleware: 阈值行为(login+register), per-endpoint key, 路径标准化,
  fail-open, IP 隔离(双向计数验证)
- 端点认证保护: products CRUD × 全角色矩阵(含anonymous), crawler trigger
- 中间件顺序: 所有错误响应(403/404/429)包含安全头
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import app.services.cache_service as cache_module
from app.database import Base, get_db
from app.main import app
from app.api.deps import get_current_user
from app.models.user import User

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
# Mock Redis — mirrors the Lua INCR+EXPIRE script behavior
# ---------------------------------------------------------------------------

# Login rate limit: 10 req / 60s
LOGIN_LIMIT = 10
LOGIN_WINDOW = 60
# Register rate limit: 5 req / 300s
REGISTER_LIMIT = 5
REGISTER_WINDOW = 300
# Default rate limit: 120 req / 60s
DEFAULT_LIMIT = 120
DEFAULT_WINDOW = 60


class MockRedis:
    """In-memory Redis mock that mirrors the Lua INCR+EXPIRE atomic script.

    The actual Lua script returns an integer (the current count after INCR).
    On first access (count==1), it also sets the EXPIRE/TTL.
    """

    def __init__(self):
        self._store: dict[str, dict] = {}

    async def eval(self, script: str, num_keys: int, key: str, window: int) -> int:
        """Simulate the Lua script: INCR key, EXPIRE on first access."""
        if key not in self._store:
            self._store[key] = {"count": 0, "ttl": int(window)}
        self._store[key]["count"] += 1
        return self._store[key]["count"]

    async def ttl(self, key: str) -> int:
        return self._store.get(key, {}).get("ttl", -1)

    def reset(self):
        self._store.clear()


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _make_user(role: str = "admin") -> User:
    user = MagicMock(spec=User)
    user.id = 1
    user.username = "testuser"
    user.role = role
    user.is_active = True
    return user


def _auth_override(role: str):
    """Create a dependency override returning a user with given role."""
    async def _get_user():
        return _make_user(role=role)
    return _get_user


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
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def client():
    """Unauthenticated async HTTP client with Redis disabled (fail-open).

    Yields an ``httpx.AsyncClient`` connected to the app via ASGITransport.
    Redis is replaced with a stub returning ``None`` so that rate-limiting
    is bypassed (fail-open), allowing non-rate-limit tests to execute freely.
    """
    _orig = cache_module.get_redis

    async def _no_redis():
        return None

    cache_module.get_redis = _no_redis
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    cache_module.get_redis = _orig


@pytest_asyncio.fixture
def mock_redis_factory():
    """Factory fixture that installs a fresh MockRedis per call and restores on cleanup.

    Returns a callable ``_create() -> MockRedis``.  Each invocation creates a
    **new, independent** MockRedis instance and patches ``cache_module.get_redis``
    to return it.  On fixture teardown the original ``get_redis`` is restored.

    Typically each test calls ``_create()`` exactly once so that it has its own
    isolated store; multiple calls within the same test are safe but will point
    ``get_redis`` at the most-recently-created instance.
    """
    _orig = cache_module.get_redis
    _instances: list[MockRedis] = []

    def _create() -> MockRedis:
        mr = MockRedis()
        _instances.append(mr)

        async def _get():
            return mr

        cache_module.get_redis = _get
        return mr

    yield _create
    cache_module.get_redis = _orig


def _assert_security_headers(resp):
    """Assert that the core security headers are present and correct on *resp*.

    Checked headers: X-Content-Type-Options, X-Frame-Options, Content-Security-Policy,
    Referrer-Policy.
    """
    assert resp.headers.get("x-content-type-options") == "nosniff"
    assert resp.headers.get("x-frame-options") == "DENY"
    assert "content-security-policy" in resp.headers
    assert "referrer-policy" in resp.headers


# ===========================================================================
# 1. SecurityHeadersMiddleware 测试
# ===========================================================================

class TestSecurityHeaders:
    """验证所有响应包含正确的安全头。"""

    @pytest.mark.asyncio
    async def test_basic_security_headers(self, client):
        """所有基础安全头应存在于健康检查响应中。"""
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.headers["x-content-type-options"] == "nosniff"
        assert resp.headers["x-frame-options"] == "DENY"
        assert resp.headers["referrer-policy"] == "strict-origin-when-cross-origin"
        assert "camera=()" in resp.headers["permissions-policy"]
        assert "microphone=()" in resp.headers["permissions-policy"]
        assert "geolocation=()" in resp.headers["permissions-policy"]

    @pytest.mark.asyncio
    async def test_csp_header_content(self, client):
        """CSP 头应包含 API 友好策略，且包含关键收口指令。"""
        resp = await client.get("/health")
        csp = resp.headers["content-security-policy"]
        assert "default-src 'none'" in csp
        assert "frame-ancestors 'none'" in csp
        assert "script-src 'self'" in csp
        assert "connect-src 'self'" in csp
        assert "img-src 'self'" in csp
        # 不应包含过于宽松的通配符
        assert "* " not in csp

    @pytest.mark.asyncio
    async def test_no_xss_protection_header(self, client):
        """已废弃的 X-XSS-Protection 头不应存在。"""
        resp = await client.get("/health")
        assert "x-xss-protection" not in resp.headers

    @pytest.mark.asyncio
    async def test_hsts_not_set_on_http(self, client):
        """HTTP 请求不应设置 HSTS 头。"""
        resp = await client.get("/health")
        assert "strict-transport-security" not in resp.headers

    @pytest.mark.asyncio
    @patch("app.middleware.security.settings")
    async def test_hsts_with_forwarded_proto_https(self, mock_settings, client):
        """通过代理的 HTTPS 请求应设置 HSTS 含 includeSubDomains。"""
        mock_settings.TRUST_PROXY_HEADERS = True
        resp = await client.get(
            "/health",
            headers={"x-forwarded-proto": "https"},
        )
        hsts = resp.headers["strict-transport-security"]
        assert hsts == "max-age=31536000; includeSubDomains"

    @pytest.mark.asyncio
    @patch("app.middleware.security.settings")
    async def test_hsts_not_set_when_proxy_untrusted(self, mock_settings, client):
        """TRUST_PROXY_HEADERS=False 时不解析代理头。"""
        mock_settings.TRUST_PROXY_HEADERS = False
        resp = await client.get(
            "/health",
            headers={"x-forwarded-proto": "https"},
        )
        assert "strict-transport-security" not in resp.headers

    @pytest.mark.asyncio
    @patch("app.middleware.security.settings")
    async def test_hsts_handles_comma_separated_proto(self, mock_settings, client):
        """代理链多值 x-forwarded-proto 应取首值。"""
        mock_settings.TRUST_PROXY_HEADERS = True
        resp = await client.get(
            "/health",
            headers={"x-forwarded-proto": "https, http"},
        )
        assert "strict-transport-security" in resp.headers

    @pytest.mark.asyncio
    async def test_security_headers_on_403(self, client):
        """403 禁止响应也应包含安全头。"""
        resp = await client.post(
            "/api/v1/products/",
            json={"product_name": "test", "product_type": "live"},
        )
        assert resp.status_code == 403
        _assert_security_headers(resp)

    @pytest.mark.asyncio
    async def test_security_headers_on_404(self, client):
        """404 未找到响应也应包含安全头。"""
        resp = await client.get("/nonexistent-path-404")
        assert resp.status_code == 404
        _assert_security_headers(resp)

    @pytest.mark.asyncio
    async def test_security_headers_on_422(self, client):
        """422 验证错误响应也应包含安全头。"""
        app.dependency_overrides[get_current_user] = _auth_override("admin")
        resp = await client.post(
            "/api/v1/products/",
            json={},  # 缺少必填字段
        )
        assert resp.status_code == 422
        _assert_security_headers(resp)


# ===========================================================================
# 2. RateLimitMiddleware 测试
# ===========================================================================

class TestRateLimit:
    """验证速率限制中间件行为。"""

    @pytest.mark.asyncio
    async def test_get_request_not_rate_limited(self, client):
        """GET 请求（非保护路径）不应被限流。"""
        for _ in range(5):
            resp = await client.get("/health")
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_rate_limit_fail_open_exception(self, client):
        """Redis 异常时应放行请求（fail-open）。"""
        _orig = cache_module.get_redis

        async def _broken():
            raise ConnectionError("Redis down")

        cache_module.get_redis = _broken
        try:
            app.dependency_overrides[get_current_user] = _auth_override("admin")
            resp = await client.post("/api/v1/crawler/trigger")
            assert resp.status_code == 200
        finally:
            cache_module.get_redis = _orig

    @pytest.mark.asyncio
    async def test_login_threshold_pass_then_reject(self, mock_redis_factory, client):
        """登录限流: 前 10 次放行，第 11 次返回 429。"""
        mock_redis = mock_redis_factory()

        # 前 LOGIN_LIMIT 次请求应该通过（或返回认证错误，但不是 429）
        for i in range(LOGIN_LIMIT):
            resp = await client.post(
                "/api/v1/auth/login",
                json={"username": "user", "password": "pass"},
            )
            assert resp.status_code != 429, f"Request {i+1}/{LOGIN_LIMIT} was rate limited"

        # 第 LOGIN_LIMIT+1 次应触发 429
        resp = await client.post(
            "/api/v1/auth/login",
            json={"username": "user", "password": "pass"},
        )
        assert resp.status_code == 429
        assert resp.headers.get("x-ratelimit-limit") == str(LOGIN_LIMIT)
        assert resp.headers.get("x-ratelimit-remaining") == "0"
        assert "retry-after" in resp.headers

    @pytest.mark.asyncio
    async def test_rate_limit_key_contains_ip(self, mock_redis_factory, client):
        """限流 key 应遵循 rl:{ip}:{path} 格式且 IP 为有效 IPv4 地址。"""
        mock_redis = mock_redis_factory()
        app.dependency_overrides[get_current_user] = _auth_override("admin")

        await client.post(
            "/api/v1/products/",
            json={"product_name": "test", "product_type": "live"},
        )

        keys = list(mock_redis._store.keys())
        assert len(keys) >= 1
        ipv4_re = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
        for key in keys:
            # 完整格式: rl:{ip}:{path}
            assert re.match(r"^rl:\d{1,3}(?:\.\d{1,3}){3}:/", key), (
                f"Key does not match rl:{{ip}}:{{path}} format: {key}"
            )
            ip_part = key.split(":")[1]
            assert ipv4_re.match(ip_part), f"Invalid IPv4 in key: {ip_part}"

    @pytest.mark.asyncio
    async def test_per_endpoint_key_isolation(self, mock_redis_factory, client):
        """不同端点应使用独立限流 key，计数互不干扰。"""
        mock_redis = mock_redis_factory()
        app.dependency_overrides[get_current_user] = _auth_override("admin")

        # 发 3 次产品创建请求
        for _ in range(3):
            await client.post(
                "/api/v1/products/",
                json={"product_name": "test", "product_type": "live"},
            )
        # 发 2 次爬虫触发请求
        for _ in range(2):
            await client.post("/api/v1/crawler/trigger")

        keys = list(mock_redis._store.keys())
        product_keys = [k for k in keys if "/products" in k]
        crawler_keys = [k for k in keys if "/crawler" in k]
        assert len(product_keys) >= 1
        assert len(crawler_keys) >= 1

        # 验证计数独立
        product_count = mock_redis._store[product_keys[0]]["count"]
        crawler_count = mock_redis._store[crawler_keys[0]]["count"]
        assert product_count == 3, f"Product count should be 3, got {product_count}"
        assert crawler_count == 2, f"Crawler count should be 2, got {crawler_count}"

        # 不应有 global key
        global_keys = [k for k in keys if "global" in k]
        assert len(global_keys) == 0

    @pytest.mark.asyncio
    async def test_path_normalization_same_key(self, mock_redis_factory, client):
        """带和不带尾部斜杠的请求应归一到同一 key。"""
        mock_redis = mock_redis_factory()
        app.dependency_overrides[get_current_user] = _auth_override("admin")

        # 带尾部斜杠
        await client.post(
            "/api/v1/products/",
            json={"product_name": "t1", "product_type": "live"},
        )
        keys_after_first = set(mock_redis._store.keys())

        # 不带尾部斜杠（FastAPI 可能会 redirect，但中间件先处理）
        await client.post(
            "/api/v1/products",
            json={"product_name": "t2", "product_type": "live"},
        )
        keys_after_second = set(mock_redis._store.keys())

        # 两次请求应使用同一个 key（key 集合不应增长）
        product_keys = [k for k in keys_after_second if "/products" in k]
        assert len(product_keys) == 1, f"Expected 1 product key, got: {product_keys}"
        # 计数应为 2，TTL 应在首次请求时设置
        entry = mock_redis._store[product_keys[0]]
        assert entry["count"] == 2
        assert entry["ttl"] == DEFAULT_WINDOW

    @pytest.mark.asyncio
    async def test_different_ips_separate_keys(self, mock_redis_factory):
        """不同客户端 IP 应使用独立限流 key，计数互不影响。"""
        mock_redis = mock_redis_factory()
        app.dependency_overrides[get_current_user] = _auth_override("admin")

        # 客户端 A (127.0.0.1) 发 3 次 POST
        transport1 = ASGITransport(app=app, client=("127.0.0.1", 0))
        async with AsyncClient(transport=transport1, base_url="http://test") as c1:
            for _ in range(3):
                await c1.post(
                    "/api/v1/products/",
                    json={"product_name": "a", "product_type": "live"},
                )

        # 客户端 B (10.0.0.1) 发 2 次 POST
        transport2 = ASGITransport(app=app, client=("10.0.0.1", 0))
        async with AsyncClient(transport=transport2, base_url="http://test") as c2:
            for _ in range(2):
                await c2.post(
                    "/api/v1/products/",
                    json={"product_name": "b", "product_type": "live"},
                )

        # 验证两个 IP 各有独立 key 和独立计数
        keys = list(mock_redis._store.keys())
        ip1_keys = [k for k in keys if "127.0.0.1" in k and "/products" in k]
        ip2_keys = [k for k in keys if "10.0.0.1" in k and "/products" in k]
        assert len(ip1_keys) == 1, f"Expected 1 key for 127.0.0.1, got {ip1_keys}"
        assert len(ip2_keys) == 1, f"Expected 1 key for 10.0.0.1, got {ip2_keys}"
        assert mock_redis._store[ip1_keys[0]]["count"] == 3
        assert mock_redis._store[ip2_keys[0]]["count"] == 2

    @pytest.mark.asyncio
    async def test_register_threshold_pass_then_reject(self, mock_redis_factory, client):
        """注册限流: 前 5 次放行，第 6 次返回 429。"""
        mock_redis = mock_redis_factory()

        for i in range(REGISTER_LIMIT):
            resp = await client.post(
                "/api/v1/auth/register",
                json={
                    "username": f"user{i}",
                    "password": "longpassword123",
                    "role": "viewer",
                },
            )
            assert resp.status_code != 429, (
                f"Request {i+1}/{REGISTER_LIMIT} was rate limited"
            )

        # 第 REGISTER_LIMIT+1 次应触发 429
        resp = await client.post(
            "/api/v1/auth/register",
            json={
                "username": "extra",
                "password": "longpassword123",
                "role": "viewer",
            },
        )
        assert resp.status_code == 429
        assert resp.headers.get("x-ratelimit-limit") == str(REGISTER_LIMIT)

    @pytest.mark.asyncio
    async def test_429_response_headers(self, mock_redis_factory, client):
        """429 响应应包含完整的限流头。"""
        mock_redis = mock_redis_factory()
        # 预填到限额边缘
        mock_redis._store["rl:127.0.0.1:/api/v1/auth/login"] = {
            "count": LOGIN_LIMIT, "ttl": LOGIN_WINDOW,
        }

        resp = await client.post(
            "/api/v1/auth/login",
            json={"username": "x", "password": "y"},
        )
        assert resp.status_code == 429
        assert resp.headers["x-ratelimit-limit"] == str(LOGIN_LIMIT)
        assert resp.headers["x-ratelimit-remaining"] == "0"
        retry_after = int(resp.headers["retry-after"])
        assert retry_after > 0


# ===========================================================================
# 3. 端点认证保护测试 — 完整角色矩阵
# ===========================================================================

class TestEndpointAuth:
    """验证写端点的认证和角色保护 (CRUD × admin/analyst/viewer/anonymous)。"""

    # --- CREATE ---

    @pytest.mark.asyncio
    async def test_create_product_anonymous_denied(self, client):
        """无认证不能创建产品。"""
        resp = await client.post(
            "/api/v1/products/",
            json={"product_name": "test", "product_type": "live"},
        )
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_create_product_admin_allowed(self, client):
        """admin 可以创建产品。"""
        app.dependency_overrides[get_current_user] = _auth_override("admin")
        resp = await client.post(
            "/api/v1/products/",
            json={"product_name": "admin产品", "product_type": "live"},
        )
        assert resp.status_code == 201

    @pytest.mark.asyncio
    async def test_create_product_analyst_allowed(self, client):
        """analyst 可以创建产品。"""
        app.dependency_overrides[get_current_user] = _auth_override("analyst")
        resp = await client.post(
            "/api/v1/products/",
            json={"product_name": "分析师产品", "product_type": "live"},
        )
        assert resp.status_code == 201

    @pytest.mark.asyncio
    async def test_create_product_viewer_denied(self, client):
        """viewer 不能创建产品。"""
        app.dependency_overrides[get_current_user] = _auth_override("viewer")
        resp = await client.post(
            "/api/v1/products/",
            json={"product_name": "test", "product_type": "live"},
        )
        assert resp.status_code == 403

    # --- UPDATE ---

    @pytest.mark.asyncio
    async def test_update_product_anonymous_denied(self, client):
        """无认证不能更新产品。"""
        resp = await client.patch(
            "/api/v1/products/1",
            json={"product_name": "updated"},
        )
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_update_product_admin_allowed(self, client):
        """admin 可以更新产品。"""
        app.dependency_overrides[get_current_user] = _auth_override("admin")
        create_resp = await client.post(
            "/api/v1/products/",
            json={"product_name": "原始", "product_type": "live"},
        )
        pid = create_resp.json()["id"]
        resp = await client.patch(
            f"/api/v1/products/{pid}",
            json={"product_name": "已更新"},
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_update_product_analyst_allowed(self, client):
        """analyst 可以更新产品。"""
        app.dependency_overrides[get_current_user] = _auth_override("admin")
        create_resp = await client.post(
            "/api/v1/products/",
            json={"product_name": "原始", "product_type": "live"},
        )
        pid = create_resp.json()["id"]
        app.dependency_overrides[get_current_user] = _auth_override("analyst")
        resp = await client.patch(
            f"/api/v1/products/{pid}",
            json={"product_name": "已更新"},
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_update_product_viewer_denied(self, client):
        """viewer 不能更新产品。"""
        app.dependency_overrides[get_current_user] = _auth_override("admin")
        create_resp = await client.post(
            "/api/v1/products/",
            json={"product_name": "原始", "product_type": "live"},
        )
        pid = create_resp.json()["id"]
        app.dependency_overrides[get_current_user] = _auth_override("viewer")
        resp = await client.patch(
            f"/api/v1/products/{pid}",
            json={"product_name": "已更新"},
        )
        assert resp.status_code == 403

    # --- DELETE ---

    @pytest.mark.asyncio
    async def test_delete_product_anonymous_denied(self, client):
        """无认证不能删除产品。"""
        resp = await client.delete("/api/v1/products/1")
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_product_admin_allowed(self, client):
        """admin 可以删除产品。"""
        app.dependency_overrides[get_current_user] = _auth_override("admin")
        create_resp = await client.post(
            "/api/v1/products/",
            json={"product_name": "待删除", "product_type": "live"},
        )
        pid = create_resp.json()["id"]
        resp = await client.delete(f"/api/v1/products/{pid}")
        assert resp.status_code == 204

    @pytest.mark.asyncio
    async def test_delete_product_analyst_denied(self, client):
        """analyst 不能删除产品。"""
        app.dependency_overrides[get_current_user] = _auth_override("admin")
        create_resp = await client.post(
            "/api/v1/products/",
            json={"product_name": "待删除", "product_type": "live"},
        )
        pid = create_resp.json()["id"]
        app.dependency_overrides[get_current_user] = _auth_override("analyst")
        resp = await client.delete(f"/api/v1/products/{pid}")
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_product_viewer_denied(self, client):
        """viewer 不能删除产品。"""
        app.dependency_overrides[get_current_user] = _auth_override("admin")
        create_resp = await client.post(
            "/api/v1/products/",
            json={"product_name": "待删除", "product_type": "live"},
        )
        pid = create_resp.json()["id"]
        app.dependency_overrides[get_current_user] = _auth_override("viewer")
        resp = await client.delete(f"/api/v1/products/{pid}")
        assert resp.status_code == 403

    # --- CRAWLER ---

    @pytest.mark.asyncio
    async def test_crawler_trigger_anonymous_denied(self, client):
        """无认证不能触发爬虫。"""
        resp = await client.post("/api/v1/crawler/trigger")
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_crawler_trigger_analyst_denied(self, client):
        """analyst 不能触发爬虫。"""
        app.dependency_overrides[get_current_user] = _auth_override("analyst")
        resp = await client.post("/api/v1/crawler/trigger")
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_crawler_trigger_viewer_denied(self, client):
        """viewer 不能触发爬虫。"""
        app.dependency_overrides[get_current_user] = _auth_override("viewer")
        resp = await client.post("/api/v1/crawler/trigger")
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_crawler_trigger_admin_allowed(self, client):
        """admin 可以触发爬虫。"""
        app.dependency_overrides[get_current_user] = _auth_override("admin")
        resp = await client.post("/api/v1/crawler/trigger")
        assert resp.status_code == 200

    # --- READ (public, no auth needed) ---

    @pytest.mark.asyncio
    async def test_list_products_no_auth_required(self, client):
        """产品列表是读操作，无需认证。"""
        resp = await client.get("/api/v1/products/")
        assert resp.status_code == 200


# ===========================================================================
# 4. 中间件顺序测试
# ===========================================================================

class TestMiddlewareOrder:
    """验证中间件执行顺序: SecurityHeaders 最外层，所有响应都有安全头。"""

    @pytest.mark.asyncio
    async def test_429_response_has_security_headers(self, mock_redis_factory, client):
        """429 限流响应应包含安全头。"""
        mock_redis = mock_redis_factory()
        mock_redis._store["rl:127.0.0.1:/api/v1/auth/login"] = {
            "count": LOGIN_LIMIT, "ttl": LOGIN_WINDOW,
        }
        resp = await client.post(
            "/api/v1/auth/login",
            json={"username": "x", "password": "y"},
        )
        assert resp.status_code == 429
        _assert_security_headers(resp)

    @pytest.mark.asyncio
    async def test_403_response_has_security_headers(self, client):
        """403 认证失败响应应包含安全头。"""
        resp = await client.post(
            "/api/v1/products/",
            json={"product_name": "test", "product_type": "live"},
        )
        assert resp.status_code == 403
        _assert_security_headers(resp)

    @pytest.mark.asyncio
    async def test_health_endpoint_always_accessible(self, client):
        """/health 端点无需认证即可访问，且包含安全头。"""
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        _assert_security_headers(resp)
