from typing import Optional
from fastapi import APIRouter, Query

router = APIRouter(prefix="/nav", tags=["nav"])


@router.get("/{fund_id}/nav")
def nav_history(
    fund_id: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    return {
        "fund_id": fund_id,
        "start_date": start_date or "2024-01-01",
        "end_date": end_date or "2024-12-31",
        "records": [
            {"date": "2024-01-05", "nav": 1.0000, "cumulative_nav": 1.0000},
            {"date": "2024-01-12", "nav": 1.0120, "cumulative_nav": 1.0120},
            {"date": "2024-01-19", "nav": 1.0085, "cumulative_nav": 1.0085},
        ],
    }
