"""System monitoring API: health checks, Celery status, data freshness alerts."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_role
from app.database import get_db
from app.models.user import User
from app.services import monitor_service

router = APIRouter(prefix="/monitor", tags=["monitor"])


@router.get("/health", summary="系统健康检查")
async def health_check(
    _current_user: User = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """全面健康检查: 数据库 + Redis + Celery (并行执行, 仅管理员)。"""
    db_health, redis_health, celery_health = await asyncio.gather(
        monitor_service.check_db_health(db),
        monitor_service.check_redis_health(),
        monitor_service.check_celery_health(),
    )

    overall = monitor_service.aggregate_status([
        db_health["status"],
        redis_health["status"],
        celery_health["status"],
    ])

    return {
        "status": overall,
        "database": db_health,
        "redis": redis_health,
        "celery": celery_health,
    }


@router.get("/celery", summary="Celery工作进程状态")
async def celery_status(
    _current_user: User = Depends(require_role("admin")),
):
    """查看Celery工作进程、活跃任务、队列状态（仅管理员）。"""
    return await monitor_service.check_celery_health()


@router.get("/data-freshness", summary="数据新鲜度报告")
async def data_freshness(
    stale_days: int = Query(7, ge=1, le=90, description="超过N天未更新视为陈旧"),
    include_detail: bool = Query(False, description="是否返回逐产品明细"),
    detail_limit: int = Query(100, ge=1, le=500, description="明细最大条数"),
    _current_user: User = Depends(require_role("admin", "analyst")),
    db: AsyncSession = Depends(get_db),
):
    """检查基金/产品数据新鲜度，标记陈旧数据（管理员/分析师）。"""
    return await monitor_service.check_data_freshness(
        db, stale_days=stale_days, include_detail=include_detail, detail_limit=detail_limit,
    )


@router.get("/overview", summary="系统监控总览")
async def system_overview(
    _current_user: User = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """聚合所有监控数据的完整系统总览 (并行执行, 仅管理员)。"""
    return await monitor_service.get_system_overview(db)
