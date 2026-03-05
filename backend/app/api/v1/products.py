from fastapi import APIRouter

router = APIRouter(prefix="/products", tags=["products"])


@router.get("/")
def list_products():
    return {
        "items": [
            {"product_id": "PRD001", "product_name": "晋帆FOF1号", "status": "运行中", "inception_date": "2023-06-01"},
            {"product_id": "PRD002", "product_name": "晋帆FOF2号", "status": "募集中", "inception_date": "2024-01-15"},
        ]
    }


@router.get("/{product_id}")
def product_detail(product_id: str):
    return {
        "product_id": product_id,
        "product_name": "晋帆FOF1号",
        "status": "运行中",
        "inception_date": "2023-06-01",
        "latest_nav": 1.1234,
        "total_aum": 50000000.0,
        "holdings": [
            {"fund_id": "F001", "fund_name": "示例基金A", "weight": 0.4},
            {"fund_id": "F002", "fund_name": "示例基金B", "weight": 0.6},
        ],
    }
