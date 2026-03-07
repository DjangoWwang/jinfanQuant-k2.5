from fastapi import APIRouter

from app.api.v1 import funds, pools, comparison, portfolios, backtest, products, valuation, benchmarks, crawler, reports, mobile, auth

api_router = APIRouter()

# 各子路由自带prefix，这里不再重复
api_router.include_router(auth.router)
api_router.include_router(funds.router)
api_router.include_router(pools.router)
api_router.include_router(comparison.router)
api_router.include_router(portfolios.router)
api_router.include_router(backtest.router)
api_router.include_router(products.router)
api_router.include_router(valuation.router)
api_router.include_router(benchmarks.router)
api_router.include_router(crawler.router)
api_router.include_router(reports.router)
api_router.include_router(mobile.router)
