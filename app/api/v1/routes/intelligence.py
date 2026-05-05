from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.admin import require_admin_key
from app.core.session import get_session_id
from app.services.feed.store import FeedStoreError
from app.services.intelligence_aggregation import (
    list_recent_intelligence_refresh_runs,
    refresh_intelligence_snapshots,
)

router = APIRouter()


@router.post("/refresh")
async def refresh_intelligence(
    session_id: str = Depends(get_session_id),
    source_limit: int = Query(default=50, ge=1, le=250),
    topic_limit: int = Query(default=50, ge=1, le=100),
    article_limit: int = Query(default=100, ge=1, le=250),
    card_limit: int = Query(default=50, ge=0, le=100),
    create_cards: bool = Query(default=True),
    _: None = Depends(require_admin_key),
):
    try:
        return await refresh_intelligence_snapshots(
            session_id=session_id,
            source_limit=source_limit,
            topic_limit=topic_limit,
            article_limit=article_limit,
            create_cards=create_cards,
            card_limit=card_limit,
        )
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/runs")
async def list_intelligence_runs(
    session_id: str = Depends(get_session_id),
    limit: int = Query(default=25, ge=1, le=100),
    include_all_sessions: bool = Query(default=False),
    _: None = Depends(require_admin_key),
):
    try:
        return list_recent_intelligence_refresh_runs(
            session_id=None if include_all_sessions else session_id,
            limit=limit,
        )
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
