"""FOF99 (火富牛) HTTP client for authenticated data access."""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://www.fof99.com"

# Common request headers to mimic a browser session
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


class Fof99Client:
    """Async HTTP client for the FOF99 platform.

    Maintains a session with cookies so that subsequent requests
    after login are automatically authenticated.
    """

    def __init__(self, base_url: str = BASE_URL, timeout: float = 30.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers=DEFAULT_HEADERS,
            timeout=timeout,
            follow_redirects=True,
        )
        self._authenticated = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Close the underlying HTTP client and release resources."""
        await self._client.aclose()

    async def __aenter__(self) -> "Fof99Client":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    async def login(self, username: str, password: str) -> bool:
        """Authenticate against FOF99 and store session cookies.

        Args:
            username: FOF99 account username / phone number.
            password: Account password.

        Returns:
            True if login succeeded.

        Raises:
            httpx.HTTPStatusError: on non-2xx response.
        """
        # TODO: Implement the actual login flow.
        #   1. GET the login page to obtain any CSRF token / captcha.
        #   2. POST credentials to the login endpoint.
        #   3. Verify the response indicates success (e.g. redirect or JSON flag).
        #   4. Session cookies are stored automatically by httpx.AsyncClient.
        logger.info("Fof99Client.login called for user=%s (stub)", username)
        self._authenticated = True
        return True

    @property
    def is_authenticated(self) -> bool:
        return self._authenticated

    # ------------------------------------------------------------------
    # Generic request helpers
    # ------------------------------------------------------------------

    async def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Make an authenticated GET request and return parsed JSON.

        Args:
            path: URL path relative to base (e.g. "/api/fund/list").
            params: Optional query parameters.

        Returns:
            Parsed JSON response body.

        Raises:
            RuntimeError: if not authenticated.
            httpx.HTTPStatusError: on non-2xx status.
        """
        self._ensure_auth()
        response = await self._client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def post(
        self,
        path: str,
        data: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        """Make an authenticated POST request.

        Args:
            path: URL path relative to base.
            data: Form-encoded body (mutually exclusive with *json_body*).
            json_body: JSON body.

        Returns:
            Parsed JSON response body.
        """
        self._ensure_auth()
        response = await self._client.post(path, data=data, json=json_body)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_auth(self) -> None:
        if not self._authenticated:
            raise RuntimeError(
                "Fof99Client is not authenticated. Call login() first."
            )
