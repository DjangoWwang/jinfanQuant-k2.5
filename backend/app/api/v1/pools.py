from fastapi import APIRouter

router = APIRouter(prefix="/pools", tags=["pools"])


@router.get("/{pool_type}")
def list_pool_funds(pool_type: str):
    return {
        "pool_type": pool_type,
        "funds": [
            {"fund_id": "F001", "fund_name": "示例基金A", "added_at": "2024-06-01"},
            {"fund_id": "F002", "fund_name": "示例基金B", "added_at": "2024-07-15"},
        ],
    }


@router.post("/{pool_type}/funds/{fund_id}")
def add_to_pool(pool_type: str, fund_id: str):
    return {"message": "TODO", "pool_type": pool_type, "fund_id": fund_id, "action": "added"}


@router.delete("/{pool_type}/funds/{fund_id}")
def remove_from_pool(pool_type: str, fund_id: str):
    return {"message": "TODO", "pool_type": pool_type, "fund_id": fund_id, "action": "removed"}
