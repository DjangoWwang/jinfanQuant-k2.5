from typing import List, Optional
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/comparison", tags=["comparison"])


class CompareRequest(BaseModel):
    fund_ids: List[str]
    start_date: Optional[str] = None
    end_date: Optional[str] = None


@router.post("/")
def compare_funds(req: CompareRequest):
    return {
        "fund_ids": req.fund_ids,
        "start_date": req.start_date or "2024-01-01",
        "end_date": req.end_date or "2024-12-31",
        "results": [
            {
                "fund_id": fid,
                "annualized_return": 0.12,
                "max_drawdown": -0.08,
                "sharpe_ratio": 1.5,
                "volatility": 0.10,
            }
            for fid in req.fund_ids
        ],
    }
