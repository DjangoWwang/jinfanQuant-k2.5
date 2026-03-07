"""Redis cache service for hot data (NAV series, metrics, etc.)."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# Lazy Redis connection - gracefully degrade if Redis unavailable
_redis_client = None
_redis_available: bool | None = None
_redis_last_fail: float = 0.0
_REDIS_RETRY_INTERVAL = 60.0  # retry every 60 seconds after failure


async def get_redis():
    """Get or create async Redis connection. Returns None if unavailable."""
    global _redis_client, _redis_available, _redis_last_fail
    if _redis_available is False:
        # Periodically retry instead of permanent disable
        if time.monotonic() - _redis_last_fail < _REDIS_RETRY_INTERVAL:
            return None
        _redis_available = None  # allow retry
        _redis_client = None
    if _redis_client is not None:
        return _redis_client
    try:
        from redis.asyncio import from_url
        from app.config import settings
        _redis_client = from_url(settings.REDIS_URL, decode_responses=True)
        await _redis_client.ping()
        _redis_available = True
        logger.info("Redis connected: %s", settings.REDIS_URL)
        return _redis_client
    except Exception as e:
        _redis_available = False
        _redis_last_fail = time.monotonic()
        _redis_client = None
        logger.warning("Redis unavailable, caching disabled (retry in %ds): %s", int(_REDIS_RETRY_INTERVAL), e)
        return None


class CacheService:
    """Async cache with Redis backend, graceful fallback to no-op."""

    async def get(self, key: str) -> Any | None:
        r = await get_redis()
        if not r:
            return None
        try:
            val = await r.get(key)
            if val is not None:
                return json.loads(val)
        except Exception:
            logger.debug("Cache get failed: %s", key, exc_info=True)
        return None

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        r = await get_redis()
        if not r:
            return
        try:
            from app.config import settings
            ex = ttl or settings.CACHE_TTL_SECONDS
            await r.set(key, json.dumps(value, default=str), ex=ex)
        except Exception:
            logger.debug("Cache set failed: %s", key, exc_info=True)

    async def delete(self, key: str) -> None:
        r = await get_redis()
        if not r:
            return
        try:
            await r.delete(key)
        except Exception:
            pass

    async def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching a pattern using pipeline batch UNLINK."""
        r = await get_redis()
        if not r:
            return 0
        try:
            keys = []
            async for key in r.scan_iter(match=pattern, count=100):
                keys.append(key)
            if not keys:
                return 0
            pipe = r.pipeline()
            for key in keys:
                pipe.unlink(key)
            await pipe.execute()
            return len(keys)
        except Exception:
            return 0

    async def invalidate_fund_cache(self, fund_id: int) -> None:
        """Invalidate all cached data for a fund."""
        await self.delete_pattern(f"nav:{fund_id}:*")
        await self.delete_pattern(f"metrics:{fund_id}:*")

    async def invalidate_product_cache(self, product_id: int) -> None:
        """Invalidate all cached data for a product."""
        await self.delete_pattern(f"product:{product_id}:*")


cache = CacheService()
