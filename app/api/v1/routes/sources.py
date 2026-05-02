from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, HttpUrl

from app.core.session import get_session_id
from app.services.feed.store import (
    FeedStoreError,
    build_article_node_graph,
    build_ingested_article_detail,
    create_source_feed_record,
    create_source_record,
    get_ingested_article_record,
    get_source_record,
    list_ingested_article_records,
    list_source_feed_records,
    list_source_records,
    save_ingested_articles,
    update_source_feed_sync_result,
)
from app.services.osint import OSINTContextError, build_article_osint_context
from app.services.rss import RSSSyncError, parse_rss_feed

router = APIRouter()


class SourceCreate(BaseModel):
    name: str | None = None
    website_url: HttpUrl | None = None
    rss_url: HttpUrl | None = None
    country: str | None = None
    language: str | None = None
    region: str | None = None
    political_context: str | None = None
    source_size: str | None = None
    source_type: str | None = None
    credibility_notes: str | None = None
    notes: str | None = None
    feed_type: Literal["rss", "homepage", "manual"] | None = None


class SourceFeedCreate(BaseModel):
    feed_url: HttpUrl
    feed_type: Literal["rss", "homepage", "manual"] = "rss"
    title: str | None = None
    language: str | None = None
    country: str | None = None
    status: str = "active"
    fetch_interval_minutes: int = 60


def _url(value: HttpUrl | None) -> str | None:
    return str(value) if value else None


@router.get("")
async def list_sources(
    limit: int = Query(default=100, ge=1, le=250),
    country: str | None = None,
    language: str | None = None,
    source_size: str | None = None,
    source_type: str | None = None,
):
    try:
        return {
            "sources": list_source_records(
                limit=limit,
                country=country,
                language=language,
                source_size=source_size,
                source_type=source_type,
            )
        }
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("")
async def create_source(payload: SourceCreate):
    try:
        source = create_source_record(
            name=payload.name,
            website_url=_url(payload.website_url),
            rss_url=_url(payload.rss_url),
            country=payload.country,
            language=payload.language,
            region=payload.region,
            political_context=payload.political_context,
            source_size=payload.source_size,
            source_type=payload.source_type,
            credibility_notes=payload.credibility_notes,
            notes=payload.notes,
        )

        feed = None
        rss_url = _url(payload.rss_url)
        website_url = _url(payload.website_url)
        if rss_url:
            feed = create_source_feed_record(
                source_id=source["id"],
                feed_url=rss_url,
                feed_type="rss",
                title=source["name"],
                language=source.get("language"),
                country=source.get("country"),
            )
        elif website_url:
            feed = create_source_feed_record(
                source_id=source["id"],
                feed_url=website_url,
                feed_type=payload.feed_type or "homepage",
                title=source["name"],
                language=source.get("language"),
                country=source.get("country"),
            )

        refreshed = get_source_record(source["id"]) or source
        return {"source": refreshed, "feed": feed}
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/articles/{article_id}")
async def get_ingested_article(
    article_id: str,
    session_id: str = Depends(get_session_id),
):
    try:
        detail = build_ingested_article_detail(article_id, session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not detail:
        raise HTTPException(status_code=404, detail="Ingested article not found")
    return detail


@router.get("/articles/{article_id}/nodes")
async def get_ingested_article_nodes(
    article_id: str,
    session_id: str = Depends(get_session_id),
    node_type: str | None = Query(default=None),
):
    try:
        graph = build_article_node_graph(article_id, session_id=session_id, node_type=node_type)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not graph:
        raise HTTPException(status_code=404, detail="Ingested article not found")
    return graph


@router.get("/articles/{article_id}/osint")
async def get_ingested_article_osint(
    article_id: str,
    include_external: bool = Query(default=False),
    limit: int = Query(default=5, ge=1, le=10),
):
    try:
        article = get_ingested_article_record(article_id)
        if not article:
            raise HTTPException(status_code=404, detail="Ingested article not found")
        return await build_article_osint_context(article, include_external=include_external, limit=limit)
    except HTTPException:
        raise
    except OSINTContextError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/{source_id}")
async def get_source(source_id: str, article_limit: int = Query(default=20, ge=1, le=100)):
    try:
        source = get_source_record(source_id)
        if not source:
            raise HTTPException(status_code=404, detail="Source not found")
        return {
            "source": source,
            "feeds": list_source_feed_records(source_id=source_id),
            "articles": list_ingested_article_records(source_id=source_id, limit=article_limit),
        }
    except HTTPException:
        raise
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/{source_id}/feeds")
async def create_source_feed(source_id: str, payload: SourceFeedCreate):
    try:
        source = get_source_record(source_id)
        if not source:
            raise HTTPException(status_code=404, detail="Source not found")
        feed = create_source_feed_record(
            source_id=source_id,
            feed_url=str(payload.feed_url),
            feed_type=payload.feed_type,
            title=payload.title,
            language=payload.language or source.get("language"),
            country=payload.country or source.get("country"),
            status=payload.status,
            fetch_interval_minutes=payload.fetch_interval_minutes,
        )
        return {"source": get_source_record(source_id) or source, "feed": feed}
    except HTTPException:
        raise
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/{source_id}/articles")
async def list_source_articles(
    source_id: str,
    limit: int = Query(default=50, ge=1, le=250),
):
    try:
        if not get_source_record(source_id):
            raise HTTPException(status_code=404, detail="Source not found")
        return {"articles": list_ingested_article_records(source_id=source_id, limit=limit)}
    except HTTPException:
        raise
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/{source_id}/sync")
async def sync_source(
    source_id: str,
    session_id: str = Depends(get_session_id),
    limit: int = Query(default=20, ge=1, le=50),
    card_limit: int = Query(default=10, ge=0, le=50),
):
    try:
        source = get_source_record(source_id)
        if not source:
            raise HTTPException(status_code=404, detail="Source not found")

        rss_feeds = list_source_feed_records(source_id=source_id, feed_type="rss", status="active")
        articles = []
        cards = []
        errors = []

        for feed in rss_feeds:
            try:
                parsed = await parse_rss_feed(feed["feed_url"], limit=limit)
                updated_feed = update_source_feed_sync_result(feed["id"], success=True, title=parsed.get("title"))
                saved = save_ingested_articles(
                    source,
                    updated_feed or feed,
                    parsed.get("items") or [],
                    session_id=session_id,
                    card_limit=max(0, card_limit - len(cards)),
                )
                articles.extend(saved["articles"])
                cards.extend(saved["cards"])
            except RSSSyncError as exc:
                failed_feed = update_source_feed_sync_result(feed["id"], success=False, error=str(exc))
                errors.append(
                    {
                        "feed_id": feed["id"],
                        "feed_url": feed["feed_url"],
                        "error": str(exc),
                        "last_checked_at": (failed_feed or {}).get("last_checked_at"),
                    }
                )

        return {
            "source": get_source_record(source_id) or source,
            "rss_feed_count": len(rss_feeds),
            "article_count": len(articles),
            "card_count": len(cards),
            "articles": articles,
            "cards": cards,
            "errors": errors,
        }
    except HTTPException:
        raise
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
