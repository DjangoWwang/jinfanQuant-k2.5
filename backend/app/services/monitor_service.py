"""System monitoring service: health checks, Celery status, data freshness."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, timedelta
from typing import Any

from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fund import Fund, NavHistory
from app.models.product import Product, ProductNav, ValuationSnapshot

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def aggregate_status(statuses: list[str]) -> str:
    """Aggregate multiple component statuses into overall system status."""
    if "error" in statuses:
        return "error"
    if any(s in statuses for s in ("warning", "unavailable")):
        return "degraded"
    return "healthy"


# ---------------------------------------------------------------------------
# Database health
# ---------------------------------------------------------------------------

async def check_db_health(
    db: AsyncSession,
    *,
    include_stats: bool = False,
) -> dict[str, Any]:
    """Check PostgreSQL connectivity and optionally return table stats.

    Args:
        include_stats: If True, include per-table row counts and DB size.
            Only used by the ``/overview`` endpoint for admin diagnostics.
    """
    t0 = time.monotonic()
    try:
        row = await asyncio.wait_for(
            db.execute(text("SELECT 1")),
            timeout=5.0,
        )
        value = row.scalar()
        latency_ms = round((time.monotonic() - t0) * 1000, 1)
        if value != 1:
            return {"status": "error", "latency_ms": latency_ms, "detail": "unexpected query result"}

        result: dict[str, Any] = {
            "status": "ok",
            "latency_ms": latency_ms,
        }

        if include_stats:
            counts_sql = text(
                "SELECT relname, n_live_tup "
                "FROM pg_stat_user_tables "
                "WHERE schemaname = :schema "
                "ORDER BY n_live_tup DESC"
            )
            rows = (await db.execute(counts_sql, {"schema": "public"})).all()
            result["table_rows"] = {r.relname: r.n_live_tup for r in rows}

            size_row = (await db.execute(text(
                "SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size"
            ))).first()
            result["db_size"] = size_row.db_size if size_row else None

        return result
    except asyncio.TimeoutError:
        latency_ms = round((time.monotonic() - t0) * 1000, 1)
        return {"status": "error", "latency_ms": latency_ms, "detail": "database check timeout"}
    except Exception:
        latency_ms = round((time.monotonic() - t0) * 1000, 1)
        logger.warning("DB health check failed", exc_info=True)
        return {"status": "error", "latency_ms": latency_ms, "detail": "database check failed"}


# ---------------------------------------------------------------------------
# Redis health
# ---------------------------------------------------------------------------

async def check_redis_health() -> dict[str, Any]:
    """Check Redis connectivity and memory usage."""
    t0 = time.monotonic()
    try:
        from app.services.cache_service import get_redis

        r = await get_redis()
        if r is None:
            return {"status": "unavailable", "detail": "Redis connection unavailable"}

        await asyncio.wait_for(r.ping(), timeout=3.0)
        latency_ms = round((time.monotonic() - t0) * 1000, 1)

        info = await asyncio.wait_for(r.info(), timeout=3.0)
        return {
            "status": "ok",
            "latency_ms": latency_ms,
            "used_memory_human": info.get("used_memory_human", "N/A"),
            "used_memory_peak_human": info.get("used_memory_peak_human", "N/A"),
            "connected_clients": info.get("connected_clients"),
        }
    except asyncio.TimeoutError:
        latency_ms = round((time.monotonic() - t0) * 1000, 1)
        return {"status": "error", "latency_ms": latency_ms, "detail": "redis check timeout"}
    except Exception:
        latency_ms = round((time.monotonic() - t0) * 1000, 1)
        logger.warning("Redis health check failed", exc_info=True)
        return {"status": "error", "latency_ms": latency_ms, "detail": "redis check failed"}


# ---------------------------------------------------------------------------
# Celery health
# ---------------------------------------------------------------------------

async def check_celery_health() -> dict[str, Any]:
    """Check Celery worker status and queue info.

    Celery inspect calls are synchronous RPC — run them in a thread
    to avoid blocking the async event loop.
    """
    base: dict[str, Any] = {
        "status": "unknown",
        "workers": [],
        "worker_count": 0,
        "active_tasks": 0,
        "scheduled_tasks": 0,
        "reserved_tasks": 0,
        "detail": None,
    }

    def _sync_inspect() -> tuple[dict, dict, dict, dict]:
        from app.tasks.celery_app import celery_app
        insp = celery_app.control.inspect(timeout=3.0)
        return (
            insp.ping() or {},
            insp.active() or {},
            insp.scheduled() or {},
            insp.reserved() or {},
        )

    try:
        ping_result, active, scheduled, reserved = await asyncio.wait_for(
            asyncio.to_thread(_sync_inspect),
            timeout=8.0,
        )
    except asyncio.TimeoutError:
        base["status"] = "error"
        base["detail"] = "Celery inspect timeout"
        return base
    except Exception:
        logger.warning("Celery health check failed", exc_info=True)
        base["status"] = "error"
        base["detail"] = "celery check failed"
        return base

    workers = list(ping_result.keys())
    if not workers:
        base["status"] = "warning"
        base["detail"] = "No Celery workers responding"
        return base

    worker_details = []
    for w in workers:
        worker_details.append({
            "name": w,
            "active": len(active.get(w, [])),
            "scheduled": len(scheduled.get(w, [])),
            "reserved": len(reserved.get(w, [])),
        })

    base.update({
        "status": "ok",
        "workers": worker_details,
        "worker_count": len(workers),
        "active_tasks": sum(len(v) for v in active.values()),
        "scheduled_tasks": sum(len(v) for v in scheduled.values()),
        "reserved_tasks": sum(len(v) for v in reserved.values()),
        "detail": None,
    })
    return base


# ---------------------------------------------------------------------------
# Data freshness
# ---------------------------------------------------------------------------

async def check_data_freshness(
    db: AsyncSession,
    *,
    stale_days: int = 7,
    include_detail: bool = False,
    detail_limit: int = 100,
) -> dict[str, Any]:
    """Check data freshness: latest NAV dates for funds and products.

    Args:
        stale_days: Days without update to be considered stale.
        include_detail: Whether to return per-product detail list.
        detail_limit: Max products in detail list.
    """
    try:
        today = date.today()
        stale_cutoff = today - timedelta(days=stale_days)

        # --- Fund NAV freshness ---
        fund_stats_row = (await db.execute(
            select(
                func.count(Fund.id).label("total"),
                func.count().filter(Fund.nav_status == "has_data").label("has_data"),
                func.count().filter(Fund.status == "active").label("active"),
            )
        )).first()

        latest_nav_row = (await db.execute(
            select(func.max(NavHistory.nav_date))
        )).scalar()

        # Stale: latest_nav_date is NULL or older than cutoff
        stale_funds_count = (await db.execute(
            select(func.count(Fund.id)).where(
                Fund.status == "active",
                Fund.nav_status == "has_data",
                or_(
                    Fund.latest_nav_date.is_(None),
                    Fund.latest_nav_date < stale_cutoff,
                ),
            )
        )).scalar() or 0

        # --- Product NAV freshness (active products only) ---
        product_count = (await db.execute(
            select(func.count(Product.id)).where(Product.is_active.is_(True))
        )).scalar() or 0

        latest_product_nav = (await db.execute(
            select(func.max(ProductNav.nav_date))
        )).scalar()

        # Stale active product count (including no_data)
        stale_product_q = (
            select(
                Product.id,
                func.max(ProductNav.nav_date).label("latest_nav_date"),
            )
            .outerjoin(ProductNav, Product.id == ProductNav.product_id)
            .where(Product.is_active.is_(True))
            .group_by(Product.id)
            .subquery()
        )
        stale_products = (await db.execute(
            select(func.count()).select_from(stale_product_q).where(
                or_(
                    stale_product_q.c.latest_nav_date.is_(None),
                    stale_product_q.c.latest_nav_date < stale_cutoff,
                )
            )
        )).scalar() or 0

        # --- Optional per-product detail ---
        products_detail = None
        if include_detail:
            product_freshness_rows = (await db.execute(
                select(
                    Product.id,
                    Product.product_name,
                    func.max(ProductNav.nav_date).label("latest_nav_date"),
                )
                .outerjoin(ProductNav, Product.id == ProductNav.product_id)
                .where(Product.is_active.is_(True))
                .group_by(Product.id, Product.product_name)
                .order_by(Product.id)
                .limit(detail_limit)
            )).all()

            products_detail = []
            for row in product_freshness_rows:
                if row.latest_nav_date is None:
                    freshness_status = "no_data"
                elif row.latest_nav_date < stale_cutoff:
                    freshness_status = "stale"
                else:
                    freshness_status = "fresh"
                products_detail.append({
                    "product_id": row.id,
                    "product_name": row.product_name or f"Product-{row.id}",
                    "latest_nav_date": str(row.latest_nav_date) if row.latest_nav_date else None,
                    "status": freshness_status,
                })

        # --- Valuation snapshot freshness ---
        latest_snapshot = (await db.execute(
            select(func.max(ValuationSnapshot.valuation_date))
        )).scalar()

        freshness_status = "ok"
        if stale_funds_count > 0 or stale_products > 0 or latest_snapshot is None:
            freshness_status = "warning"

        return {
            "status": freshness_status,
            "check_date": str(today),
            "stale_threshold_days": stale_days,
            "funds": {
                "total": fund_stats_row.total if fund_stats_row else 0,
                "active": fund_stats_row.active if fund_stats_row else 0,
                "has_data": fund_stats_row.has_data if fund_stats_row else 0,
                "latest_nav_date": str(latest_nav_row) if latest_nav_row else None,
                "stale_count": stale_funds_count,
            },
            "products": {
                "total": product_count,
                "latest_nav_date": str(latest_product_nav) if latest_product_nav else None,
                "stale_count": stale_products,
                "detail": products_detail,
            },
            "valuation": {
                "latest_snapshot_date": str(latest_snapshot) if latest_snapshot else None,
            },
        }
    except Exception:
        logger.warning("Data freshness check failed", exc_info=True)
        return {
            "status": "error",
            "check_date": str(date.today()),
            "stale_threshold_days": stale_days,
            "detail": "data freshness check failed",
        }


# ---------------------------------------------------------------------------
# System overview
# ---------------------------------------------------------------------------

async def get_system_overview(db: AsyncSession) -> dict[str, Any]:
    """Aggregate all health checks into a single overview.

    DB-bound checks run sequentially (AsyncSession is not concurrency-safe),
    while Redis and Celery checks run in parallel with the DB work.
    """
    # Start non-DB checks in parallel
    redis_task = asyncio.create_task(check_redis_health())
    celery_task = asyncio.create_task(check_celery_health())

    # Run DB checks sequentially on the same session
    db_health = await check_db_health(db, include_stats=True)
    freshness = await check_data_freshness(db)

    # Collect non-DB results
    redis_health = await redis_task
    celery_health = await celery_task

    overall = aggregate_status([
        db_health["status"],
        redis_health["status"],
        celery_health["status"],
        freshness.get("status", "ok"),
    ])

    return {
        "status": overall,
        "components": {
            "database": db_health,
            "redis": redis_health,
            "celery": celery_health,
        },
        "data_freshness": freshness,
    }
