from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.admin import require_admin_key
from app.core.session import get_session_id
from app.services.feed.store import FeedStoreError
from app.services.intelligence_aggregation import (
    list_recent_intelligence_refresh_runs,
    refresh_intelligence_snapshots,
)
from app.services.intelligence_pipeline import run_intelligence_pipeline
from app.services.event_clustering import (
    get_event_cluster_detail,
    list_event_cluster_summaries,
    list_recent_event_cluster_refresh_runs,
    refresh_event_clusters,
)

router = APIRouter()


@router.post("/pipeline/run")
async def run_intelligence_automation_pipeline(
    session_id: str = Depends(get_session_id),
    source_limit: int = Query(default=50, ge=1, le=250),
    feed_limit: int = Query(default=100, ge=1, le=500),
    sync_article_limit: int = Query(default=10, ge=1, le=50),
    sync_card_limit: int = Query(default=25, ge=0, le=250),
    intelligence_source_limit: int = Query(default=50, ge=1, le=250),
    topic_limit: int = Query(default=50, ge=1, le=100),
    intelligence_article_limit: int = Query(default=100, ge=1, le=250),
    intelligence_card_limit: int = Query(default=50, ge=0, le=100),
    cluster_article_limit: int = Query(default=250, ge=1, le=250),
    cluster_limit: int = Query(default=100, ge=1, le=250),
    cluster_card_limit: int = Query(default=50, ge=0, le=100),
    skip_sync: bool = Query(default=False),
    skip_intelligence: bool = Query(default=False),
    skip_clusters: bool = Query(default=False),
    _: None = Depends(require_admin_key),
):
    try:
        return await run_intelligence_pipeline(
            session_id=session_id,
            source_limit=source_limit,
            feed_limit=feed_limit,
            sync_article_limit=sync_article_limit,
            sync_card_limit=sync_card_limit,
            intelligence_source_limit=intelligence_source_limit,
            topic_limit=topic_limit,
            intelligence_article_limit=intelligence_article_limit,
            intelligence_card_limit=intelligence_card_limit,
            cluster_article_limit=cluster_article_limit,
            cluster_limit=cluster_limit,
            cluster_card_limit=cluster_card_limit,
            skip_sync=skip_sync,
            skip_intelligence=skip_intelligence,
            skip_clusters=skip_clusters,
        )
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


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


@router.post("/clusters/refresh")
async def refresh_intelligence_clusters(
    session_id: str = Depends(get_session_id),
    article_limit: int = Query(default=250, ge=1, le=250),
    cluster_limit: int = Query(default=100, ge=1, le=250),
    card_limit: int = Query(default=50, ge=0, le=100),
    create_cards: bool = Query(default=True),
    _: None = Depends(require_admin_key),
):
    try:
        return refresh_event_clusters(
            session_id=session_id,
            article_limit=article_limit,
            cluster_limit=cluster_limit,
            card_limit=card_limit,
            create_cards=create_cards,
        )
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/clusters/runs")
async def list_intelligence_cluster_runs(
    session_id: str = Depends(get_session_id),
    limit: int = Query(default=25, ge=1, le=100),
    include_all_sessions: bool = Query(default=False),
    _: None = Depends(require_admin_key),
):
    try:
        return list_recent_event_cluster_refresh_runs(
            session_id=None if include_all_sessions else session_id,
            limit=limit,
        )
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/clusters")
async def list_intelligence_clusters(
    session_id: str = Depends(get_session_id),
    topic_id: str | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=100),
):
    try:
        return list_event_cluster_summaries(
            session_id=session_id,
            topic_id=topic_id,
            limit=limit,
        )
    except FeedStoreError as exc:
        if str(exc) == "Topic not found.":
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/clusters/{cluster_id}")
async def get_intelligence_cluster(cluster_id: str):
    try:
        return get_event_cluster_detail(cluster_id)
    except FeedStoreError as exc:
        if str(exc) == "Event cluster not found.":
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        raise HTTPException(status_code=503, detail=str(exc)) from exc
