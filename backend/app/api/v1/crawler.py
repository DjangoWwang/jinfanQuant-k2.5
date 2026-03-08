from fastapi import APIRouter, Depends

from app.api.deps import require_role
from app.models.user import User

router = APIRouter(prefix="/crawler", tags=["crawler"])


@router.post("/trigger")
async def trigger_crawl(
    _current_user: User = Depends(require_role("admin")),
):
    return {
        "job_id": "CRW-20240701-001",
        "status": "started",
    }


@router.get("/jobs")
def list_jobs():
    return {
        "items": [
            {"job_id": "CRW-20240701-001", "status": "completed", "started_at": "2024-07-01T10:00:00", "finished_at": "2024-07-01T10:05:00", "records_fetched": 150},
            {"job_id": "CRW-20240628-001", "status": "completed", "started_at": "2024-06-28T10:00:00", "finished_at": "2024-06-28T10:03:00", "records_fetched": 120},
        ]
    }
