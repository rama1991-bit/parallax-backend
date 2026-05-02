from fastapi import APIRouter, Depends, HTTPException

from app.core.session import get_session_id
from app.services.feed.store import FeedStoreError
from app.services.sources import SOURCE_LIMITATIONS, get_source, list_sources

router = APIRouter()


@router.get("")
async def get_authors(session_id: str = Depends(get_session_id), limit: int = 50):
    try:
        sources = list_sources(session_id=session_id, limit=limit)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "sources": sources,
        "authors": [],
        "limitations": SOURCE_LIMITATIONS,
    }


@router.get("/{author_id}")
async def get_author(author_id: str, session_id: str = Depends(get_session_id)):
    try:
        source = get_source(author_id, session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    return {
        "author": source["source"],
        "source": source["source"],
        "profile": source["profile"],
        "articles": source["signals"],
        "signals": source["signals"],
        "recent_items": source["recent_items"],
        "key_claims": source["key_claims"],
        "dominant_frames": source["dominant_frames"],
        "topics": source["topics"],
        "entities": source["entities"],
        "metrics": source["metrics"],
        "social_candidates": source["bounded_social_signals"],
        "bounded_social_signals": source["bounded_social_signals"],
        "limitations": source["limitations"],
    }
