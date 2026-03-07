"""Report generation and attribution analysis API endpoints."""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Optional
from urllib.parse import quote

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, require_role
from app.config import settings
from app.database import get_db
from app.models.user import User
from app.schemas.report import (
    AsyncReportRequest,
    AsyncReportResponse,
    AttributionResponse,
    CorrelationMatrixResponse,
    FundContributionResponse,
    FundContributionItem,
    ReportGenerateRequest,
    TaskStatusResponse,
)
from app.services.attribution_service import attribution_service
from app.services.report_service import report_service
from app.tasks.analysis import generate_report_async

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/reports", tags=["reports"])


@router.post("/generate")
async def generate_report(
    req: ReportGenerateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_role("admin", "analyst")),
):
    """Generate a PDF product report and return it as a downloadable file."""
    try:
        pdf_bytes = await report_service.generate_report(
            db,
            product_id=req.product_id,
            report_type=req.report_type,
            period_start=req.period_start,
            period_end=req.period_end,
            benchmark_id=req.benchmark_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        logger.exception("PDF报告生成失败: product_id=%s", req.product_id)
        raise HTTPException(status_code=500, detail="报告生成失败")

    filename = f"report_{req.product_id}_{req.period_end.isoformat()}.pdf"
    cn_filename = quote(f"报告_{req.product_id}_{req.period_end.isoformat()}.pdf")
    return StreamingResponse(
        iter([pdf_bytes]),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename=\"{filename}\"; filename*=UTF-8''{cn_filename}",
            "Content-Length": str(len(pdf_bytes)),
        },
    )


@router.get("/correlation-matrix", response_model=CorrelationMatrixResponse)
async def get_correlation_matrix(
    fund_ids: str = Query(..., description="逗号分隔的基金ID列表，最多20个"),
    period_start: date = Query(...),
    period_end: date = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_role("admin", "analyst")),
):
    """计算多只基金之间日收益率的Pearson相关系数矩阵。"""
    # 解析并校验 fund_ids
    try:
        id_list = [int(fid.strip()) for fid in fund_ids.split(",") if fid.strip()]
    except ValueError:
        raise HTTPException(status_code=400, detail="fund_ids 参数格式错误，需为逗号分隔的整数")

    if not id_list:
        raise HTTPException(status_code=400, detail="至少需要提供一个基金ID")
    if len(id_list) > 20:
        raise HTTPException(status_code=400, detail="基金数量不能超过20个")

    if period_start >= period_end:
        raise HTTPException(status_code=400, detail="period_start 必须早于 period_end")

    try:
        result = await attribution_service.compute_correlation_matrix(
            db, fund_ids=id_list, period_start=period_start, period_end=period_end,
        )
    except Exception:
        logger.exception("相关系数矩阵计算失败: fund_ids=%s", fund_ids)
        raise HTTPException(status_code=500, detail="相关系数矩阵计算失败")

    return result


@router.post("/{product_id}/excel")
async def generate_excel_report(
    product_id: int,
    req: ReportGenerateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_role("admin", "analyst")),
):
    """Generate an Excel product report and return it as a downloadable file."""
    try:
        config = {
            "report_type": req.report_type,
            "period_start": req.period_start,
            "period_end": req.period_end,
            "benchmark_id": req.benchmark_id,
        }
        excel_bytes = await report_service.generate_excel_report(
            db, product_id=product_id, config=config,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        logger.exception("Excel报告生成失败: product_id=%s", product_id)
        raise HTTPException(status_code=500, detail="Excel报告生成失败")

    filename = f"report_{product_id}_{req.period_end.isoformat()}.xlsx"
    cn_filename = quote(f"报告_{product_id}_{req.period_end.isoformat()}.xlsx")
    return StreamingResponse(
        iter([excel_bytes]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename=\"{filename}\"; filename*=UTF-8''{cn_filename}",
            "Content-Length": str(len(excel_bytes)),
        },
    )


@router.post("/{product_id}/generate-async", response_model=AsyncReportResponse)
async def generate_report_async_endpoint(
    product_id: int,
    req: AsyncReportRequest,
    current_user: User = Depends(require_role("admin", "analyst")),
):
    """Queue async report generation (PDF or Excel) and return a task ID."""
    config = {
        "report_type": req.report_type,
        "period_start": req.period_start.isoformat(),
        "period_end": req.period_end.isoformat(),
        "benchmark_id": req.benchmark_id,
    }
    task = generate_report_async.delay(
        product_id=product_id,
        config=config,
        report_format=req.format,
        requested_by=current_user.id,
    )
    return AsyncReportResponse(
        task_id=task.id,
        product_id=product_id,
        format=req.format,
        status="queued",
    )


@router.get("/tasks/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(
    task_id: str,
    current_user: User = Depends(get_current_user),
):
    """Check the status of an async report generation task."""
    result = AsyncResult(task_id)
    status = result.status  # PENDING, STARTED, SUCCESS, FAILURE, RETRY

    response = TaskStatusResponse(
        task_id=task_id,
        status=status,
        result=None,
    )

    if status == "SUCCESS":
        task_result = result.result or {}
        # Verify task ownership (admin can see all)
        if (
            task_result.get("requested_by")
            and task_result["requested_by"] != current_user.id
            and current_user.role != "admin"
        ):
            raise HTTPException(status_code=403, detail="无权访问该任务")
        # Strip internal fields from response
        response.result = {
            k: v for k, v in task_result.items()
            if k not in ("file_path", "requested_by")
        }
    elif status == "FAILURE":
        response.result = {"error": "任务执行失败，请查看服务端日志"}

    return response


@router.get("/tasks/{task_id}/download")
async def download_task_report(
    task_id: str,
    current_user: User = Depends(get_current_user),
):
    """Download the report file generated by an async task."""
    result = AsyncResult(task_id)

    if result.status != "SUCCESS":
        raise HTTPException(
            status_code=400,
            detail=f"任务尚未完成，当前状态: {result.status}",
        )

    task_result = result.result or {}

    # Verify task ownership (admin can see all)
    if (
        task_result.get("requested_by")
        and task_result["requested_by"] != current_user.id
        and current_user.role != "admin"
    ):
        raise HTTPException(status_code=403, detail="无权下载该报告")

    file_path = task_result.get("file_path")
    report_format = task_result.get("format", "pdf")

    if not file_path:
        raise HTTPException(status_code=404, detail="报告文件路径为空")

    # Path traversal protection: ensure file is within allowed directory
    abs_path = os.path.abspath(file_path)
    allowed_dir = os.path.abspath(settings.REPORT_OUTPUT_DIR)
    if not abs_path.startswith(allowed_dir + os.sep) and abs_path != allowed_dir:
        logger.warning("路径遍历攻击尝试: %s", file_path)
        raise HTTPException(status_code=403, detail="非法文件路径")

    if not os.path.exists(abs_path):
        raise HTTPException(status_code=404, detail="报告文件不存在或已过期")

    if report_format == "excel":
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        media_type = "application/pdf"

    filename = os.path.basename(abs_path)
    cn_filename = quote(filename)
    return FileResponse(
        path=abs_path,
        media_type=media_type,
        headers={
            "Content-Disposition": f"attachment; filename=\"{filename}\"; filename*=UTF-8''{cn_filename}",
        },
    )


@router.get("/{product_id}/attribution", response_model=AttributionResponse)
async def get_attribution(
    product_id: int,
    period_start: date = Query(...),
    period_end: date = Query(...),
    benchmark_id: Optional[int] = Query(None),
    granularity: str = Query("monthly", pattern=r"^(monthly|weekly)$"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_role("admin", "analyst")),
):
    """Compute and return Brinson attribution analysis as JSON."""
    try:
        result = await report_service.compute_attribution(
            db,
            product_id=product_id,
            period_start=period_start,
            period_end=period_end,
            benchmark_id=benchmark_id,
            granularity=granularity,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return result


@router.get("/{product_id}/fund-contribution", response_model=FundContributionResponse)
async def get_fund_contribution(
    product_id: int,
    period_start: date = Query(...),
    period_end: date = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_role("admin", "analyst")),
):
    """计算产品中每只子基金对组合收益的贡献度。

    返回每只基金的权重、收益率和贡献度，按贡献绝对值降序排列。
    """
    if period_start >= period_end:
        raise HTTPException(status_code=400, detail="period_start 必须早于 period_end")

    try:
        contributions = await attribution_service.get_fund_contribution(
            db,
            product_id=product_id,
            period_start=period_start,
            period_end=period_end,
        )
    except Exception:
        logger.exception("基金贡献度计算失败: product_id=%s", product_id)
        raise HTTPException(status_code=500, detail="基金贡献度计算失败")

    return FundContributionResponse(
        product_id=product_id,
        period_start=period_start,
        period_end=period_end,
        contributions=[FundContributionItem(**c) for c in contributions],
    )
