"""Product management API endpoints (CRUD + valuation upload)."""

import logging
import tempfile
from datetime import date
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

from app.database import get_db
from app.models.user import User
from app.api.deps import require_role
from app.schemas.product import (
    ProductCreate,
    ProductUpdate,
    ProductResponse,
    ProductListResponse,
    ValuationUploadResponse,
    ValuationSnapshotResponse,
    ValuationListResponse,
    ProductNavResponse,
    ProductNavCalcResponse,
    NavCalcPoint,
    NavCalcResultResponse,
    NavStatsResponse,
    NavSeriesPoint,
    StrategyAttributionResponse,
    FactorExposureResponse,
)
from app.config import settings
from app.services.product_service import product_service
from app.services.attribution_service import attribution_service
from app.services import nav_calc_service

router = APIRouter(prefix="/products", tags=["products"])


# ------------------------------------------------------------------
# Product CRUD
# ------------------------------------------------------------------

@router.get("/", response_model=ProductListResponse)
async def list_products(
    product_type: Optional[str] = Query(None, description="live or simulation"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    skip = (page - 1) * page_size
    products, total = await product_service.list_products(
        db, product_type=product_type, skip=skip, limit=page_size
    )
    # Batch load snapshot stats to avoid N+1 queries
    product_ids = [p.id for p in products]
    stats = await product_service.batch_load_snapshot_stats(db, product_ids)
    items = [
        await product_service.to_response(db, p, snapshot_stats=stats.get(p.id))
        for p in products
    ]
    return ProductListResponse(items=items, total=total)


@router.post("/", response_model=ProductResponse, status_code=201)
async def create_product(
    payload: ProductCreate,
    db: AsyncSession = Depends(get_db),
):
    product = await product_service.create_product(db, payload)
    await db.commit()
    resp = await product_service.to_response(db, product)
    return resp


@router.get("/{product_id}", response_model=ProductResponse)
async def get_product(
    product_id: int,
    db: AsyncSession = Depends(get_db),
):
    product = await product_service.get_product(db, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="产品不存在")
    return await product_service.to_response(db, product)


@router.patch("/{product_id}", response_model=ProductResponse)
async def update_product(
    product_id: int,
    payload: ProductUpdate,
    db: AsyncSession = Depends(get_db),
):
    product = await product_service.update_product(db, product_id, payload)
    if not product:
        raise HTTPException(status_code=404, detail="产品不存在")
    await db.commit()
    return await product_service.to_response(db, product)


@router.delete("/{product_id}", status_code=204)
async def delete_product(
    product_id: int,
    db: AsyncSession = Depends(get_db),
):
    ok = await product_service.delete_product(db, product_id)
    if not ok:
        raise HTTPException(status_code=404, detail="产品不存在")
    await db.commit()


# ------------------------------------------------------------------
# Valuation upload & query
# ------------------------------------------------------------------

@router.post("/{product_id}/valuation", response_model=ValuationUploadResponse)
async def upload_valuation(
    product_id: int,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Upload and parse a custodian valuation table Excel file."""
    product = await product_service.get_product(db, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="产品不存在")

    if not file.filename or Path(file.filename).suffix.lower() not in (".xlsx", ".xls"):
        raise HTTPException(status_code=400, detail="仅支持 .xlsx/.xls 格式")

    # Stream to temp file with size limit to avoid memory spike
    suffix = Path(file.filename).suffix
    max_size = settings.MAX_UPLOAD_SIZE_MB * 1024 * 1024
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp_path = tmp.name
            total_size = 0
            while chunk := await file.read(64 * 1024):
                total_size += len(chunk)
                if total_size > max_size:
                    raise HTTPException(status_code=400, detail=f"文件过大，最大允许 {settings.MAX_UPLOAD_SIZE_MB}MB")
                tmp.write(chunk)

        result = await product_service.process_valuation_upload(
            db, product_id, tmp_path
        )
        await db.commit()
        return result
    except HTTPException:
        await db.rollback()
        raise
    except Exception as e:
        await db.rollback()
        logger.exception("估值上传处理失败: product_id=%s, file=%s", product_id, file.filename)
        raise HTTPException(status_code=500, detail="估值上传处理失败，请检查文件格式后重试")
    finally:
        if tmp_path:
            Path(tmp_path).unlink(missing_ok=True)


@router.get("/{product_id}/valuations", response_model=ValuationListResponse)
async def list_valuations(
    product_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    product = await product_service.get_product(db, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="产品不存在")

    skip = (page - 1) * page_size
    items, total = await product_service.list_valuations(
        db, product_id, skip=skip, limit=page_size
    )
    return ValuationListResponse(items=items, total=total)


@router.get("/{product_id}/valuation/latest", response_model=ValuationSnapshotResponse)
async def get_latest_valuation(
    product_id: int,
    db: AsyncSession = Depends(get_db),
):
    result = await product_service.get_latest_valuation(db, product_id)
    if not result:
        raise HTTPException(status_code=404, detail="无估值数据")
    return result


@router.get("/{product_id}/valuation/{snapshot_id}", response_model=ValuationSnapshotResponse)
async def get_valuation_snapshot(
    product_id: int,
    snapshot_id: int,
    db: AsyncSession = Depends(get_db),
):
    result = await product_service.get_valuation_snapshot(db, snapshot_id)
    if not result or result.product_id != product_id:
        raise HTTPException(status_code=404, detail="估值快照不存在")
    return result


# ------------------------------------------------------------------
# NAV calculation
# ------------------------------------------------------------------

@router.post("/{product_id}/nav/calculate", response_model=NavCalcResultResponse)
async def calculate_product_nav(
    product_id: int,
    recalculate: bool = Query(False, description="是否重新计算全部"),
    current_user: User = Depends(require_role("admin", "analyst")),
    db: AsyncSession = Depends(get_db),
):
    """Trigger NAV calculation based on sub-fund holdings in valuation snapshots.

    Requires admin or analyst role.
    """
    product = await product_service.get_product(db, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="产品不存在")

    try:
        calc_result = await nav_calc_service.calculate_product_nav(
            db, product_id, recalculate=recalculate,
        )
        await db.commit()
    except ValueError as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("NAV calculation failed for product %d", product_id)
        await db.rollback()
        raise HTTPException(status_code=500, detail="净值计算失败")

    return NavCalcResultResponse(
        product_id=product_id,
        total_days=calc_result.total_days,
        calculated_days=calc_result.calculated_days,
        snapshot_days=calc_result.snapshot_days,
        skipped_days=calc_result.skipped_days,
        date_range_start=calc_result.date_range[0].isoformat() if calc_result.date_range[0] else None,
        date_range_end=calc_result.date_range[1].isoformat() if calc_result.date_range[1] else None,
        warnings=calc_result.warnings,
    )


@router.get("/{product_id}/nav/calculated", response_model=ProductNavCalcResponse)
async def get_calculated_nav(
    product_id: int,
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    current_user: User = Depends(require_role("admin", "analyst", "viewer")),
    db: AsyncSession = Depends(get_db),
):
    """Get pre-calculated NAV series with detailed breakdown."""
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date 不能晚于 end_date")

    product = await product_service.get_product(db, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="产品不存在")

    records = await nav_calc_service.get_calculated_nav_series(
        db, product_id, start_date, end_date,
    )
    nav_series = [
        NavCalcPoint(
            date=r.nav_date,
            unit_nav=float(r.unit_nav) if r.unit_nav is not None else None,
            cumulative_nav=float(r.cumulative_nav) if r.cumulative_nav is not None else None,
            total_nav=float(r.total_nav) if r.total_nav is not None else None,
            total_shares=float(r.total_shares) if r.total_shares is not None else None,
            fund_assets=float(r.fund_assets) if r.fund_assets is not None else None,
            non_fund_assets=float(r.non_fund_assets) if r.non_fund_assets is not None else None,
            source=r.source,
        )
        for r in records
    ]

    return ProductNavCalcResponse(
        product_id=product_id,
        product_name=product.product_name,
        nav_series=nav_series,
    )


@router.get("/{product_id}/nav/stats", response_model=NavStatsResponse)
async def get_nav_stats(
    product_id: int,
    current_user: User = Depends(require_role("admin", "analyst", "viewer")),
    db: AsyncSession = Depends(get_db),
):
    """Get NAV calculation statistics."""
    product = await product_service.get_product(db, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="产品不存在")

    stats = await nav_calc_service.get_nav_stats(db, product_id)
    return NavStatsResponse(product_id=product_id, **stats)


# ------------------------------------------------------------------
# Strategy Attribution & Factor Exposure
# ------------------------------------------------------------------

@router.get("/{product_id}/strategy-attribution", response_model=StrategyAttributionResponse)
async def get_strategy_attribution(
    product_id: int,
    group_by: str = Query("strategy_type", pattern=r"^(strategy_type|strategy_sub)$", description="分组维度: strategy_type(一级) or strategy_sub(二级)"),
    db: AsyncSession = Depends(get_db),
):
    """Get FOF strategy-level weight allocation and return contribution."""
    product = await product_service.get_product(db, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="产品不存在")
    result = await attribution_service.get_strategy_attribution(db, product_id, group_by=group_by)
    return result


@router.get("/{product_id}/factor-exposure", response_model=FactorExposureResponse)
async def get_factor_exposure(
    product_id: int,
    window: int = Query(60, ge=20, le=250, description="Rolling window size (trading days)"),
    db: AsyncSession = Depends(get_db),
):
    """Run multi-factor regression to estimate FOF factor exposures."""
    product = await product_service.get_product(db, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="产品不存在")
    result = await attribution_service.get_factor_exposure(db, product_id, window=window)
    if "error" in result and result.get("error"):
        return FactorExposureResponse(product_id=product_id, error=result["error"])
    return result


# ------------------------------------------------------------------
# NAV series
# ------------------------------------------------------------------

@router.get("/{product_id}/nav", response_model=ProductNavResponse)
async def get_product_nav(
    product_id: int,
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Get product NAV series.

    Priority: calculated product_navs > linked fund nav_history > valuation snapshots.
    """
    product = await product_service.get_product(db, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="产品不存在")

    # Try calculated NAVs first
    calc_records = await nav_calc_service.get_calculated_nav_series(
        db, product_id, start_date, end_date,
    )
    if calc_records:
        nav_series = [
            NavSeriesPoint(
                date=r.nav_date,
                unit_nav=float(r.unit_nav) if r.unit_nav is not None else None,
                total_nav=float(r.total_nav) if r.total_nav is not None else None,
            )
            for r in calc_records
        ]
        return ProductNavResponse(
            product_id=product_id,
            product_name=product.product_name,
            nav_series=nav_series,
        )

    # Fallback to existing logic (linked fund / valuation snapshots)
    nav_series = await product_service.get_nav_series(
        db, product_id, start_date, end_date
    )
    return ProductNavResponse(
        product_id=product_id,
        product_name=product.product_name,
        nav_series=nav_series,
    )
