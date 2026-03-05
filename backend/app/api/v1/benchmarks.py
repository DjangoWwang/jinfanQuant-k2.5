from typing import Optional
from fastapi import APIRouter, Query

router = APIRouter(prefix="/benchmarks", tags=["benchmarks"])


@router.get("/")
def list_benchmarks():
    return {
        "items": [
            {"code": "000300", "name": "沪深300", "index_type": "宽基"},
            {"code": "000905", "name": "中证500", "index_type": "宽基"},
            {"code": "000985", "name": "中证全指", "index_type": "宽基"},
        ]
    }


@router.get("/{code}/nav")
def benchmark_nav(
    code: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    return {
        "code": code,
        "start_date": start_date or "2024-01-01",
        "end_date": end_date or "2024-12-31",
        "records": [
            {"date": "2024-01-02", "close": 3400.0, "change_pct": 0.0},
            {"date": "2024-06-28", "close": 3550.0, "change_pct": 0.0441},
            {"date": "2024-12-31", "close": 3600.0, "change_pct": 0.0588},
        ],
    }
