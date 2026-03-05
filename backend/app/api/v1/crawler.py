from fastapi import APIRouter

router = APIRouter(prefix="/crawler", tags=["crawler"])


@router.post("/trigger")
def trigger_crawl():
    return {
        "job_id": "CRW-20240701-001",
        "status": "started",
        "message": "TODO",
    }


@router.get("/jobs")
def list_jobs():
    return {
        "items": [
            {"job_id": "CRW-20240701-001", "status": "completed", "started_at": "2024-07-01T10:00:00", "finished_at": "2024-07-01T10:05:00", "records_fetched": 150},
            {"job_id": "CRW-20240628-001", "status": "completed", "started_at": "2024-06-28T10:00:00", "finished_at": "2024-06-28T10:03:00", "records_fetched": 120},
        ]
    }
