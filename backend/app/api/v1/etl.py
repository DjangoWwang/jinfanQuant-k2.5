"""ETL管理接口 — 触发/监控数据入库任务。"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.api.deps import require_role
from app.models.user import User

router = APIRouter(prefix="/etl", tags=["etl"])


# ------------------------------------------------------------------
# Request / Response schemas
# ------------------------------------------------------------------

class RefreshNavRequest(BaseModel):
    fund_ids: list[int] | None = None


class RefreshNavFullRequest(BaseModel):
    fund_ids: list[int]


class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result: Any | None = None
    progress: dict | None = None


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------

@router.post(
    "/refresh-nav",
    response_model=TaskResponse,
    summary="增量净值刷新",
    description="触发增量NAV更新（仅限管理员）。可选指定基金ID列表，为空则更新所有 nav_status='has_data' 的基金。",
)
async def trigger_refresh_nav(
    body: RefreshNavRequest | None = None,
    current_user: User = Depends(require_role("admin")),
) -> TaskResponse:
    from app.tasks.analysis import refresh_nav_incremental

    fund_ids = body.fund_ids if body else None
    task = refresh_nav_incremental.delay(fund_ids=fund_ids)
    return TaskResponse(
        task_id=task.id,
        status="queued",
        message=f"增量净值刷新已提交，fund_ids={fund_ids or '全部'}",
    )


@router.post(
    "/refresh-nav-full",
    response_model=TaskResponse,
    summary="全量净值重建",
    description="删除已有净值并全量重新爬取（仅限管理员）。必须指定基金ID列表。",
)
async def trigger_refresh_nav_full(
    body: RefreshNavFullRequest,
    current_user: User = Depends(require_role("admin")),
) -> TaskResponse:
    from app.tasks.analysis import refresh_nav_full

    if not body.fund_ids:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="fund_ids 不能为空",
        )

    task = refresh_nav_full.delay(fund_ids=body.fund_ids)
    return TaskResponse(
        task_id=task.id,
        status="queued",
        message=f"全量净值重建已提交，共 {len(body.fund_ids)} 只基金",
    )


@router.post(
    "/daily-refresh",
    response_model=TaskResponse,
    summary="手动触发每日数据刷新",
    description="手动触发每日刷新流程：增量净值更新 + 数据质量检查（仅限管理员）。",
)
async def trigger_daily_refresh(
    current_user: User = Depends(require_role("admin")),
) -> TaskResponse:
    from app.tasks.analysis import daily_data_refresh

    task = daily_data_refresh.delay()
    return TaskResponse(
        task_id=task.id,
        status="queued",
        message="每日数据刷新已提交",
    )


@router.get(
    "/status",
    response_model=TaskStatusResponse,
    summary="查询ETL任务状态",
    description="通过 task_id 查询Celery任务执行状态。",
)
async def get_etl_status(
    task_id: str,
    current_user: User = Depends(require_role("admin")),
) -> TaskStatusResponse:
    from app.tasks.celery_app import celery_app

    result = celery_app.AsyncResult(task_id)

    progress = None
    task_result = None

    if result.state == "PROGRESS":
        progress = result.info
    elif result.state == "SUCCESS":
        task_result = result.result
    elif result.state == "FAILURE":
        task_result = {"error": "任务执行失败，请查看服务端日志"}

    return TaskStatusResponse(
        task_id=task_id,
        status=result.state,
        result=task_result,
        progress=progress,
    )
