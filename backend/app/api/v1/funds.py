from typing import Optional
from fastapi import APIRouter, Query

router = APIRouter(prefix="/funds", tags=["funds"])


@router.get("/")
def list_funds(
    strategy_type: Optional[str] = Query(None),
    strategy_sub: Optional[str] = Query(None),
    nav_frequency: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    return {
        "total": 1,
        "page": page,
        "page_size": page_size,
        "items": [
            {
                "fund_id": "F001",
                "fund_name": "示例基金",
                "strategy_type": strategy_type or "股票多头",
                "strategy_sub": strategy_sub or "量化选股",
                "nav_frequency": nav_frequency or "周度",
            }
        ],
    }


@router.get("/{fund_id}")
def fund_detail(fund_id: str):
    return {
        "fund_id": fund_id,
        "fund_name": "示例基金",
        "strategy_type": "股票多头",
        "strategy_sub": "量化选股",
        "nav_frequency": "周度",
        "manager": "张三",
        "inception_date": "2023-01-01",
        "latest_nav": 1.2345,
    }
