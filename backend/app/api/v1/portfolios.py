from typing import Dict, List, Optional
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/portfolios", tags=["portfolios"])


class PortfolioCreate(BaseModel):
    name: str
    description: Optional[str] = None
    fund_ids: List[str] = []


class AllocationUpdate(BaseModel):
    allocations: Dict[str, float]


@router.get("/")
def list_portfolios():
    return {
        "items": [
            {"portfolio_id": "P001", "name": "组合A", "fund_count": 5, "created_at": "2024-06-01"},
        ]
    }


@router.post("/")
def create_portfolio(req: PortfolioCreate):
    return {"portfolio_id": "P002", "name": req.name, "message": "TODO"}


@router.get("/{portfolio_id}")
def portfolio_detail(portfolio_id: str):
    return {
        "portfolio_id": portfolio_id,
        "name": "组合A",
        "allocations": {"F001": 0.4, "F002": 0.3, "F003": 0.3},
        "created_at": "2024-06-01",
    }


@router.put("/{portfolio_id}/allocations")
def update_allocations(portfolio_id: str, req: AllocationUpdate):
    return {"portfolio_id": portfolio_id, "allocations": req.allocations, "message": "TODO"}
