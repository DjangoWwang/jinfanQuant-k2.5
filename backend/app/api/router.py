from fastapi import APIRouter

from app.api.v1 import funds, nav, pools, comparison, portfolios, backtest, products, valuation, benchmarks, crawler

api_router = APIRouter()

api_router.include_router(funds.router, prefix="/funds", tags=["基金"])
api_router.include_router(nav.router, prefix="/funds", tags=["净值"])
api_router.include_router(pools.router, prefix="/pools", tags=["基金池"])
api_router.include_router(comparison.router, prefix="/comparison", tags=["基金比较"])
api_router.include_router(portfolios.router, prefix="/portfolios", tags=["组合"])
api_router.include_router(backtest.router, prefix="/backtest", tags=["回测"])
api_router.include_router(products.router, prefix="/products", tags=["产品"])
api_router.include_router(valuation.router, prefix="/products", tags=["估值表"])
api_router.include_router(benchmarks.router, prefix="/benchmarks", tags=["基准"])
api_router.include_router(crawler.router, prefix="/crawler", tags=["数据采集"])
