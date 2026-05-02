from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, HttpUrl

from app.core.session import get_session_id
from app.services.feed.store import (
    FeedStoreError,
    create_feed_subscription,
    get_feed_subscription,
    list_feed_subscriptions,
    save_feed_items,
)
from app.services.rss import RSSSyncError, parse_rss_feed

router = APIRouter()


class FeedCreate(BaseModel):
    url: HttpUrl
    title: str | None = None
    description: str | None = None
    topic_id: str | None = None


@router.get("")
async def list_feeds(session_id: str = Depends(get_session_id)):
    try:
        return {"feeds": list_feed_subscriptions(session_id=session_id)}
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("")
async def create_feed(payload: FeedCreate, session_id: str = Depends(get_session_id)):
    try:
        feed = create_feed_subscription(
            url=str(payload.url),
            title=payload.title,
            description=payload.description,
            topic_id=payload.topic_id,
            session_id=session_id,
        )
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {"feed": feed}


@router.post("/{feed_id}/sync")
async def sync_feed(
    feed_id: str,
    session_id: str = Depends(get_session_id),
    limit: int = Query(default=20, ge=1, le=50),
):
    try:
        feed = get_feed_subscription(feed_id, session_id=session_id)
        if not feed:
            raise HTTPException(status_code=404, detail="Feed subscription not found")

        parsed = await parse_rss_feed(feed["url"], limit=limit)
        if feed.get("title") == feed.get("url") and parsed.get("title"):
            feed["title"] = parsed["title"]
        if not feed.get("description") and parsed.get("description"):
            feed["description"] = parsed["description"]

        saved = save_feed_items(
            feed,
            parsed["items"],
            session_id=session_id,
            card_limit=5,
        )
    except RSSSyncError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "feed": feed,
        "item_count": len(saved["items"]),
        "card_count": len(saved["cards"]),
        "items": saved["items"],
        "cards": saved["cards"],
    }
