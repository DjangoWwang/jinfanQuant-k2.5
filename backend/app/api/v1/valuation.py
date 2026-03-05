from fastapi import APIRouter, UploadFile, File

router = APIRouter(prefix="/valuation", tags=["valuation"])


@router.post("/{product_id}/valuation")
def upload_valuation(product_id: str, file: UploadFile = File(...)):
    return {
        "product_id": product_id,
        "filename": file.filename,
        "message": "TODO",
    }


@router.get("/{product_id}/valuation/{date}")
def get_valuation_snapshot(product_id: str, date: str):
    return {
        "product_id": product_id,
        "date": date,
        "total_assets": 50000000.0,
        "total_liabilities": 500000.0,
        "net_assets": 49500000.0,
        "nav": 1.1234,
        "shares": 44065000.0,
        "items": [
            {"account": "银行存款", "amount": 5000000.0, "pct": 0.10},
            {"account": "基金投资", "amount": 44500000.0, "pct": 0.90},
        ],
    }
