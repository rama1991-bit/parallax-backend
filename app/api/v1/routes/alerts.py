from fastapi import APIRouter, Depends, HTTPException

from app.core.session import get_session_id
from app.services.feed.store import (
    FeedStoreError,
    get_unread_alert_count,
    list_alerts,
    mark_alert_read,
    mark_all_alerts_read,
)

router = APIRouter()


@router.get("")
async def get_alerts(
    limit: int = 50,
    unread_only: bool = False,
    session_id: str = Depends(get_session_id),
):
    try:
        alerts = list_alerts(
            session_id=session_id,
            limit=limit,
            unread_only=unread_only,
        )
        unread_count = get_unread_alert_count(session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "alerts": alerts,
        "unread_count": unread_count,
    }


@router.get("/unread-count")
async def unread_count(session_id: str = Depends(get_session_id)):
    try:
        count = get_unread_alert_count(session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {"unread_count": count}


@router.post("/{alert_id}/read")
async def read_alert(alert_id: str, session_id: str = Depends(get_session_id)):
    try:
        alert = mark_alert_read(alert_id, session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if alert:
        return alert
    raise HTTPException(status_code=404, detail="Alert not found")


@router.post("/read-all")
async def read_all_alerts(session_id: str = Depends(get_session_id)):
    try:
        updated_count = mark_all_alerts_read(session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {"updated_count": updated_count}
