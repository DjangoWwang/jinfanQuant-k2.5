from fastapi import APIRouter

router = APIRouter(prefix="/backtest", tags=["backtest"])


@router.post("/{portfolio_id}/run")
def trigger_backtest(portfolio_id: str):
    return {
        "job_id": "BT-20240701-001",
        "portfolio_id": portfolio_id,
        "status": "pending",
        "message": "TODO",
    }


@router.get("/{job_id}")
def get_results(job_id: str):
    return {
        "job_id": job_id,
        "status": "completed",
        "results": {
            "annualized_return": 0.15,
            "max_drawdown": -0.10,
            "sharpe_ratio": 1.8,
            "volatility": 0.09,
            "nav_series": [
                {"date": "2024-01-01", "nav": 1.0},
                {"date": "2024-06-30", "nav": 1.08},
                {"date": "2024-12-31", "nav": 1.15},
            ],
        },
    }
