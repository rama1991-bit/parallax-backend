from fastapi import APIRouter, Depends, HTTPException

from app.core.session import get_session_id
from app.services.feed.store import FeedStoreError, list_saved_reports

router = APIRouter()


@router.get("")
async def saved_reports(session_id: str = Depends(get_session_id), limit: int = 50):
    try:
        reports = list_saved_reports(session_id=session_id, limit=limit)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {"reports": reports}
