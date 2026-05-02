from fastapi import APIRouter, Depends, HTTPException

from app.core.session import get_session_id
from app.services.feed.store import FeedStoreError, get_analyze_usage

router = APIRouter()


@router.get("/me")
async def me(session_id: str = Depends(get_session_id)):
    try:
        usage = get_analyze_usage(session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "user": None,
        "session": {
            "id": session_id,
            "type": "anonymous",
        },
        "usage": {
            "analyze": usage,
        },
    }
