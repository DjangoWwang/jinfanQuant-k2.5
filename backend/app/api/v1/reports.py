"""Report generation and attribution analysis API endpoints."""

from __future__ import annotations

from datetime import date
from typing import Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.report import (
    AttributionResponse,
    ReportGenerateRequest,
)
from app.services.report_service import report_service

router = APIRouter(prefix="/reports", tags=["reports"])


@router.post("/generate")
async def generate_report(
    req: ReportGenerateRequest,
    db: AsyncSession = Depends(get_db),
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"报告生成失败: {str(e)}")

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


@router.get("/{product_id}/attribution", response_model=AttributionResponse)
async def get_attribution(
    product_id: int,
    period_start: date = Query(...),
    period_end: date = Query(...),
    benchmark_id: Optional[int] = Query(None),
    granularity: str = Query("monthly", pattern=r"^(monthly|weekly)$"),
    db: AsyncSession = Depends(get_db),
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
