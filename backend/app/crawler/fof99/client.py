"""FOF99 (火富牛) HTTP client for authenticated data access.

API逆向分析结果:
- 前端: Vue SPA (mp.fof99.com, Aliyun OSS)
- API后端: https://api.huofuniu.com/
- Python API: https://pyapi.huofuniu.com/
- 认证: Access-Token header (登录后获取)
- 设备标识: X-DEVICE-ID header (MD5格式，限3台设备绑定)
- 密码加密: MD5(明文密码)
- 请求格式: JSON (Content-Type: application/json)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from binascii import unhexlify
from typing import Any

import httpx
from Crypto.Cipher import DES

logger = logging.getLogger(__name__)

API_BASE = "https://api.huofuniu.com"
PYAPI_BASE = "https://pyapi.huofuniu.com"

# 限流关键词识别
_RATE_LIMIT_KEYWORDS = ["同时访问人数太多", "稍后再试", "请求频繁", "访问过于频繁"]


class RateLimitError(Exception):
    """火富牛API限流错误，可重试。"""
    pass


class ApiBusinessError(Exception):
    """火富牛API业务错误，不可重试。"""
    pass

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


def md5(text: str) -> str:
    """Return MD5 hex digest of a string."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# 火富牛净值数据DES加密密钥 (从app.js逆向获取)
_PRICE_KEYS = [
    "ac68!3#1", "55%g7z!@", "(^g&vd+1", "FpV94n&3", "5*cKem&2",
    "&35%383@", "c@di*4#!", "(&j1k9Bv", "{[d8j*c6", "O~i2&8)8",
]


def decrypt_prices(encrypted_hex: str, key_index: int = 0) -> list[dict]:
    """解密火富牛加密的净值数据。

    火富牛使用DES-CBC加密净值序列，IV=key。
    解密后为JSON数组: [{pd, nav, cnw, cn, drawdown, pc}, ...]
    """
    if not encrypted_hex:
        return []
    key = _PRICE_KEYS[key_index].encode("utf-8")
    cipher = DES.new(key, DES.MODE_CBC, key)
    decrypted = cipher.decrypt(unhexlify(encrypted_hex))
    # PKCS7 unpadding
    pad_len = decrypted[-1]
    if 1 <= pad_len <= 8:
        decrypted = decrypted[:-pad_len]
    return json.loads(decrypted.decode("utf-8"))


class Fof99Client:
    """Async HTTP client for the FOF99 (火富牛) platform.

    持久化X-DEVICE-ID以避免浪费设备绑定配额（限3台）。
    """

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        device_id: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._username = username or os.getenv("FOF99_USERNAME", "")
        self._password = password or os.getenv("FOF99_PASSWORD", "")
        self._device_id = device_id or os.getenv("FOF99_DEVICE_ID", "")
        self._token: str = ""
        self._authenticated = False

        if not self._device_id:
            self._device_id = md5(DEFAULT_HEADERS["User-Agent"] + "jinfan_crawler_2026")
            logger.warning(
                "FOF99_DEVICE_ID 未配置，自动生成: %s  "
                "请将此ID保存到 .env 以复用设备绑定配额",
                self._device_id,
            )

        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
        )

        # 速率控制
        self._last_request_time: float = 0.0
        self._min_interval: float = 0.5  # 最小请求间隔(秒)
        self._consecutive_rate_limits: int = 0

    @property
    def device_id(self) -> str:
        return self._device_id

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> Fof99Client:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _build_headers(self) -> dict[str, str]:
        headers = {**DEFAULT_HEADERS, "X-DEVICE-ID": self._device_id}
        if self._token:
            headers["Access-Token"] = self._token
        return headers

    async def login(self) -> bool:
        """Authenticate and obtain Access-Token.

        Returns True on success. Raises on network error.
        """
        pwd_md5 = md5(self._password)
        payload = {"UserName": self._username, "Password": pwd_md5}

        logger.info("火富牛登录: user=%s, device=%s", self._username, self._device_id)

        resp = await self._client.post(
            f"{API_BASE}/newgoapi/login",
            json=payload,
            headers=self._build_headers(),
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("error_code") == 0 and data.get("data", {}).get("token"):
            self._token = data["data"]["token"]
            self._authenticated = True
            logger.info("火富牛登录成功，获取 token")
            return True

        error_msg = data.get("msg", "未知错误")
        logger.error("火富牛登录失败: %s", error_msg)
        raise RuntimeError(f"火富牛登录失败: {error_msg}")

    @property
    def is_authenticated(self) -> bool:
        return self._authenticated

    def set_token(self, token: str) -> None:
        """Manually set token (e.g. from browser session)."""
        self._token = token
        self._authenticated = True

    # ------------------------------------------------------------------
    # Device management
    # ------------------------------------------------------------------

    async def list_devices(self) -> list[dict]:
        """获取当前账号的已登录设备列表。"""
        data = await self.api_get("/newgoapi/login/loginManageList")
        return data.get("list", [])

    async def unbind_device(self, device_record_id: int) -> bool:
        """解绑指定设备 (传入 loginManageList 返回的 id 字段)。"""
        data = await self.api_post("/newgoapi/login/unbindBrowser", json_body={"id": device_record_id})
        return True

    # ------------------------------------------------------------------
    # Generic request helpers (with rate limiting and retry)
    # ------------------------------------------------------------------

    async def api_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET request to api.huofuniu.com with retry."""
        return await self._request_with_retry("GET", f"{API_BASE}{path}", params=params)

    async def api_post(self, path: str, json_body: dict[str, Any] | None = None) -> Any:
        """POST request to api.huofuniu.com with retry."""
        return await self._request_with_retry("POST", f"{API_BASE}{path}", json_body=json_body)

    async def pyapi_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET request to pyapi.huofuniu.com with retry."""
        return await self._request_with_retry("GET", f"{PYAPI_BASE}{path}", params=params)

    async def pyapi_post(self, path: str, json_body: dict[str, Any] | None = None) -> Any:
        """POST request to pyapi.huofuniu.com with retry."""
        return await self._request_with_retry("POST", f"{PYAPI_BASE}{path}", json_body=json_body)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_auth(self) -> None:
        if not self._authenticated:
            raise RuntimeError("Fof99Client 未认证，请先调用 login() 或 set_token()")

    async def _throttle(self) -> None:
        """Enforce minimum interval between requests."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request_time = time.monotonic()

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        max_retries: int = 3,
    ) -> Any:
        """Execute HTTP request with rate limit detection and adaptive retry."""
        self._ensure_auth()

        last_error: Exception | None = None
        for attempt in range(max_retries + 1):
            await self._throttle()
            try:
                if method == "GET":
                    resp = await self._client.get(url, params=params, headers=self._build_headers())
                else:
                    resp = await self._client.post(url, json=json_body, headers=self._build_headers())
                resp.raise_for_status()
                result = self._parse_response(resp.json())
                # 成功：重置限流计数，缓慢降低间隔
                if self._consecutive_rate_limits > 0:
                    self._consecutive_rate_limits = 0
                    self._min_interval = max(0.5, self._min_interval * 0.7)
                return result

            except RateLimitError as e:
                self._consecutive_rate_limits += 1
                # 自适应增加间隔
                self._min_interval = min(60.0, self._min_interval * 2)
                wait = 5 * (2 ** attempt)  # 5, 10, 20, 40...
                logger.warning(
                    "限流(第%d次连续, attempt %d/%d): 等%ds, 间隔调至%.1fs | %s",
                    self._consecutive_rate_limits, attempt + 1, max_retries + 1, wait, self._min_interval, url,
                )
                last_error = e
                if attempt < max_retries:
                    await asyncio.sleep(wait)

            except (httpx.TimeoutException, httpx.HTTPStatusError) as e:
                wait = 3 * (attempt + 1)
                logger.warning("网络错误(attempt %d/%d): 等%ds | %s: %s", attempt + 1, max_retries + 1, wait, url, e)
                last_error = e
                if attempt < max_retries:
                    await asyncio.sleep(wait)

        # 所有重试耗尽
        if isinstance(last_error, RateLimitError):
            raise last_error
        raise last_error or RuntimeError(f"请求失败: {url}")

    @staticmethod
    def _parse_response(data: dict[str, Any]) -> Any:
        if data.get("error_code") == 0:
            return data.get("data")
        msg = data.get("msg", "未知错误")
        # 检测限流
        if any(kw in msg for kw in _RATE_LIMIT_KEYWORDS):
            raise RateLimitError(msg)
        raise ApiBusinessError(f"火富牛API错误: {msg}")
