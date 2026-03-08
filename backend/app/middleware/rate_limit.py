"""Fixed-window counter rate limiting middleware backed by Redis."""

from __future__ import annotations

import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.config import settings

logger = logging.getLogger(__name__)

# Rate limit config: (max_requests, window_seconds)
_RATE_LIMITS: dict[str, tuple[int, int]] = {
    "/api/v1/auth/login": (10, 60),        # 10 requests per minute
    "/api/v1/auth/register": (5, 300),      # 5 requests per 5 minutes
}

# Global default: 120 requests per minute per IP per endpoint
_DEFAULT_LIMIT = (120, 60)

# Lua script for atomic INCR + EXPIRE
_INCR_SCRIPT = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return current
"""


def _client_ip(request: Request) -> str:
    """Extract client IP, respecting X-Forwarded-For if configured.

    TRUST_PROXY_HEADERS should only be enabled when the application
    is deployed behind a trusted reverse proxy (e.g. Nginx, Traefik).
    """
    if settings.TRUST_PROXY_HEADERS:
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Fixed-window counter rate limiter backed by Redis."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path.rstrip("/") or "/"

        # Skip safe methods unless path has explicit rate limit
        if request.method in ("GET", "HEAD", "OPTIONS") and path not in _RATE_LIMITS:
            return await call_next(request)

        ip = _client_ip(request)
        max_requests, window = _RATE_LIMITS.get(path, _DEFAULT_LIMIT)
        # Per-endpoint key prevents cross-endpoint interference
        key = f"rl:{ip}:{path}"

        try:
            from app.services.cache_service import get_redis
            redis = await get_redis()
            if redis is not None:
                current = await redis.eval(_INCR_SCRIPT, 1, key, window)
                if current > max_requests:
                    ttl = await redis.ttl(key)
                    logger.warning(
                        "Rate limit exceeded: ip=%s path=%s current=%d limit=%d",
                        ip, path, current, max_requests,
                    )
                    return JSONResponse(
                        status_code=429,
                        content={"detail": "请求过于频繁，请稍后再试"},
                        headers={
                            "Retry-After": str(max(ttl, 1)),
                            "X-RateLimit-Limit": str(max_requests),
                            "X-RateLimit-Remaining": "0",
                        },
                    )
        except Exception as exc:
            logger.warning("Rate limit check skipped: Redis unavailable: %s", exc)

        return await call_next(request)
