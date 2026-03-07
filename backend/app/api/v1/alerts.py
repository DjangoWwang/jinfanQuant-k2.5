"""Risk Alert Center API endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.user import User
from app.api.deps import get_current_user, require_role
from app.services.risk_service import risk_service

router = APIRouter(prefix="/alerts", tags=["alerts"])


# ------------------------------------------------------------------
# Request / Response schemas
# ------------------------------------------------------------------

class RuleCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    rule_type: str = Field(..., pattern="^(drawdown|volatility|concentration|nav_anomaly)$")
    target_type: str = Field(..., pattern="^(fund|product)$")
    target_id: int | None = Field(None, description="NULL表示应用于所有同类目标")
    threshold: float = Field(...)
    comparison: str = Field("gt", pattern="^(gt|lt|gte|lte)$")
    severity: str = Field("warning", pattern="^(warning|critical)$")


class RuleUpdateRequest(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=200)
    threshold: float | None = None
    comparison: str | None = Field(None, pattern="^(gt|lt|gte|lte)$")
    severity: str | None = Field(None, pattern="^(warning|critical)$")
    is_active: bool | None = None


class RuleResponse(BaseModel):
    id: int
    name: str
    rule_type: str
    target_type: str
    target_id: int | None = None
    threshold: float
    comparison: str
    severity: str
    is_active: bool
    created_by: int | None = None
    created_at: str | None = None
    updated_at: str | None = None

    model_config = {"from_attributes": True}


class AlertEventResponse(BaseModel):
    id: int
    rule_id: int
    target_type: str
    target_id: int
    target_name: str | None = None
    metric_value: float | None = None
    threshold_value: float | None = None
    severity: str
    message: str | None = None
    is_read: bool
    resolved_at: str | None = None
    created_at: str | None = None

    model_config = {"from_attributes": True}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _rule_to_response(rule) -> RuleResponse:
    return RuleResponse(
        id=rule.id,
        name=rule.name,
        rule_type=rule.rule_type,
        target_type=rule.target_type,
        target_id=rule.target_id,
        threshold=float(rule.threshold),
        comparison=rule.comparison,
        severity=rule.severity,
        is_active=rule.is_active,
        created_by=rule.created_by,
        created_at=rule.created_at.isoformat() if rule.created_at else None,
        updated_at=rule.updated_at.isoformat() if rule.updated_at else None,
    )


def _event_to_response(event) -> AlertEventResponse:
    return AlertEventResponse(
        id=event.id,
        rule_id=event.rule_id,
        target_type=event.target_type,
        target_id=event.target_id,
        target_name=event.target_name,
        metric_value=float(event.metric_value) if event.metric_value is not None else None,
        threshold_value=float(event.threshold_value) if event.threshold_value is not None else None,
        severity=event.severity,
        message=event.message,
        is_read=event.is_read,
        resolved_at=event.resolved_at.isoformat() if event.resolved_at else None,
        created_at=event.created_at.isoformat() if event.created_at else None,
    )


# ------------------------------------------------------------------
# Rule endpoints
# ------------------------------------------------------------------

@router.post("/rules", response_model=RuleResponse, status_code=201)
async def create_rule(
    payload: RuleCreateRequest,
    current_user: User = Depends(require_role("admin", "analyst")),
    db: AsyncSession = Depends(get_db),
):
    """创建风险监控规则（仅管理员/分析师）。"""
    data = payload.model_dump()
    data["created_by"] = current_user.id
    rule = await risk_service.create_rule(db, data)
    await db.commit()
    return _rule_to_response(rule)


@router.get("/rules", response_model=list[RuleResponse])
async def list_rules(
    rule_type: Optional[str] = Query(None, description="按规则类型过滤"),
    is_active: Optional[bool] = Query(None, description="按激活状态过滤"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """获取风险监控规则列表。"""
    rules = await risk_service.list_rules(db, rule_type=rule_type, is_active=is_active)
    return [_rule_to_response(r) for r in rules]


@router.put("/rules/{rule_id}", response_model=RuleResponse)
async def update_rule(
    rule_id: int,
    payload: RuleUpdateRequest,
    current_user: User = Depends(require_role("admin", "analyst")),
    db: AsyncSession = Depends(get_db),
):
    """更新风险监控规则（仅管理员/分析师）。"""
    data = payload.model_dump(exclude_unset=True)
    if not data:
        raise HTTPException(400, "未提供任何更新字段")
    rule = await risk_service.update_rule(db, rule_id, data)
    if not rule:
        raise HTTPException(404, "规则不存在")
    await db.commit()
    return _rule_to_response(rule)


@router.delete("/rules/{rule_id}", status_code=204)
async def delete_rule(
    rule_id: int,
    current_user: User = Depends(require_role("admin", "analyst")),
    db: AsyncSession = Depends(get_db),
):
    """停用风险监控规则（软删除，仅管理员/分析师）。"""
    deleted = await risk_service.delete_rule(db, rule_id)
    if not deleted:
        raise HTTPException(404, "规则不存在")
    await db.commit()


# ------------------------------------------------------------------
# Alert event endpoints
# ------------------------------------------------------------------

@router.get("/events", response_model=list[AlertEventResponse])
async def list_alert_events(
    is_read: Optional[bool] = Query(None, description="按已读状态过滤"),
    severity: Optional[str] = Query(None, description="按严重程度过滤: warning|critical"),
    limit: int = Query(50, ge=1, le=200),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """获取风险预警事件列表。"""
    events = await risk_service.get_alerts(db, is_read=is_read, severity=severity, limit=limit)
    return [_event_to_response(e) for e in events]


@router.put("/events/read-all")
async def mark_all_read(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """将所有未读预警标记为已读。"""
    count = await risk_service.mark_all_read(db)
    await db.commit()
    return {"message": f"已将 {count} 条预警标记为已读", "count": count}


@router.put("/events/{alert_id}/read", response_model=AlertEventResponse)
async def mark_read(
    alert_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """将单条预警标记为已读。"""
    event = await risk_service.mark_read(db, alert_id)
    if not event:
        raise HTTPException(404, "预警事件不存在")
    await db.commit()
    return _event_to_response(event)


# ------------------------------------------------------------------
# Dashboard & manual trigger
# ------------------------------------------------------------------

@router.get("/dashboard")
async def risk_dashboard(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """获取风险仪表盘概览。"""
    return await risk_service.get_risk_dashboard(db)


@router.post("/check")
async def trigger_rule_check(
    current_user: User = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """手动触发所有规则检查（仅管理员）。"""
    results = await risk_service.check_all_rules(db)
    await db.commit()
    return {
        "message": f"规则检查完成，触发 {len(results)} 条新预警",
        "triggered": len(results),
        "alerts": results,
    }
