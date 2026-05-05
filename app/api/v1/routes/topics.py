from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.admin import require_admin_key
from app.core.session import get_session_id
from app.services.feed.store import (
    FeedStoreError,
    create_topic_monitor,
    get_topic_detail,
    list_topics,
)
from app.services.intelligence_aggregation import build_topic_intelligence, list_topic_intelligence

router = APIRouter()


class TopicCreate(BaseModel):
    name: str
    description: str | None = None
    keywords: list[str] = Field(default_factory=list)


@router.get("")
async def get_topics(session_id: str = Depends(get_session_id)):
    try:
        return {"topics": list_topics(session_id=session_id)}
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("")
async def create_topic(payload: TopicCreate, session_id: str = Depends(get_session_id)):
    return await monitor_topic(payload, session_id=session_id)


@router.post("/monitor")
async def monitor_topic(payload: TopicCreate, session_id: str = Depends(get_session_id)):
    if not payload.name.strip():
        raise HTTPException(status_code=422, detail="Topic name is required.")

    try:
        return create_topic_monitor(
            name=payload.name,
            description=payload.description,
            keywords=payload.keywords,
            session_id=session_id,
        )
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/intelligence")
async def get_topics_intelligence(
    session_id: str = Depends(get_session_id),
    limit: int = Query(default=25, ge=1, le=100),
    article_limit: int = Query(default=100, ge=1, le=250),
):
    try:
        return await list_topic_intelligence(
            session_id=session_id,
            limit=limit,
            article_limit=article_limit,
        )
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/{topic_id}/intelligence")
async def get_topic_intelligence(
    topic_id: str,
    session_id: str = Depends(get_session_id),
    refresh: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=250),
):
    try:
        return await build_topic_intelligence(
            topic_id,
            session_id=session_id,
            refresh=refresh,
            limit=limit,
        )
    except FeedStoreError as exc:
        if str(exc) == "Topic not found.":
            raise HTTPException(status_code=404, detail="Topic not found") from exc
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/{topic_id}/intelligence/refresh")
async def refresh_topic_intelligence(
    topic_id: str,
    session_id: str = Depends(get_session_id),
    limit: int = Query(default=100, ge=1, le=250),
    _: None = Depends(require_admin_key),
):
    try:
        return await build_topic_intelligence(
            topic_id,
            session_id=session_id,
            refresh=True,
            limit=limit,
        )
    except FeedStoreError as exc:
        if str(exc) == "Topic not found.":
            raise HTTPException(status_code=404, detail="Topic not found") from exc
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/{topic_id}")
async def get_topic(topic_id: str, session_id: str = Depends(get_session_id)):
    try:
        topic = get_topic_detail(topic_id, session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if not topic:
        raise HTTPException(status_code=404, detail="Topic not found")

    return topic
