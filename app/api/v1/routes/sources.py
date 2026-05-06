from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, HttpUrl

from app.core.admin import require_admin_key
from app.core.session import get_session_id
from app.services.default_sources import preview_default_sources, seed_default_sources
from app.services.feed.store import (
    FeedStoreError,
    attach_source_ops_alert_delivery_status,
    attach_source_health,
    attach_sources_health,
    build_article_node_graph,
    build_ingested_article_detail,
    build_source_health_summary,
    calculate_source_quality_report,
    create_source_feed_record,
    create_source_record,
    acknowledge_source_ops_alert,
    evaluate_source_ops_alerts,
    get_ingested_article_record,
    get_source_record,
    list_ingested_article_records,
    list_source_feed_records,
    list_source_ops_alert_deliveries,
    list_source_ops_alerts,
    list_source_records,
    list_sources_needing_review,
    list_source_sync_runs,
    recalculate_source_quality,
    update_source_feed_governance,
    update_source_review_status,
)
from app.services.ops_notifications import deliver_source_ops_alerts
from app.services.intelligence_aggregation import build_source_intelligence
from app.services.ingested_analysis import analyze_pending_ingested_articles
from app.services.osint import OSINTContextError, build_article_osint_context
from app.services.source_sync import sync_active_source_feeds, sync_source_feeds

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


class SourceReviewUpdate(BaseModel):
    review_status: Literal["needs_review", "reviewed", "quarantined", "disabled"]
    review_notes: str | None = None
    disabled_reason: str | None = None
    terms_reviewed: bool = False


class SourceFeedGovernanceUpdate(BaseModel):
    status: Literal["active", "paused", "quarantined", "disabled"]
    disabled_reason: str | None = None
    review_notes: str | None = None


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
        sources = list_source_records(
            limit=limit,
            country=country,
            language=language,
            source_size=source_size,
            source_type=source_type,
        )
        return {
            "sources": attach_sources_health(sources)
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


@router.get("/defaults/preview")
async def preview_default_source_database(
    limit: int | None = Query(default=None, ge=1, le=250),
):
    return preview_default_sources(limit=limit)


@router.post("/defaults/seed")
async def seed_default_source_database(
    limit: int | None = Query(default=None, ge=1, le=250),
    _: None = Depends(require_admin_key),
):
    try:
        return seed_default_sources(limit=limit)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/seed-defaults")
async def seed_default_source_database_alias(
    limit: int | None = Query(default=None, ge=1, le=250),
    _: None = Depends(require_admin_key),
):
    try:
        return seed_default_sources(limit=limit)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/sync-active")
async def sync_active_sources(
    _: None = Depends(require_admin_key),
    session_id: str = Depends(get_session_id),
    source_limit: int = Query(default=50, ge=1, le=250),
    feed_limit: int = Query(default=100, ge=1, le=500),
    article_limit: int = Query(default=10, ge=1, le=50),
    card_limit: int = Query(default=25, ge=0, le=250),
):
    try:
        return await sync_active_source_feeds(
            session_id=session_id,
            source_limit=source_limit,
            feed_limit=feed_limit,
            article_limit=article_limit,
            card_limit=card_limit,
        )
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/sync-runs")
async def list_source_sync_history(
    limit: int = Query(default=50, ge=1, le=250),
    source_id: str | None = None,
    sync_scope: str | None = None,
    _: None = Depends(require_admin_key),
):
    try:
        return {"sync_runs": list_source_sync_runs(source_id=source_id, sync_scope=sync_scope, limit=limit)}
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/needs-review")
async def list_source_review_queue(
    limit: int = Query(default=50, ge=1, le=250),
    _: None = Depends(require_admin_key),
):
    try:
        return list_sources_needing_review(limit=limit)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/ops/alerts")
async def list_source_operational_alerts(
    limit: int = Query(default=50, ge=1, le=250),
    status: str | None = Query(default="active"),
    source_id: str | None = None,
    severity: str | None = None,
    _: None = Depends(require_admin_key),
):
    try:
        alerts = list_source_ops_alerts(
            status=status,
            source_id=source_id,
            severity=severity,
            limit=limit,
        )
        alerts = attach_source_ops_alert_delivery_status(alerts)
        return {
            "alerts": alerts,
            "summary": {
                "alert_count": len(alerts),
                "critical": len([alert for alert in alerts if alert.get("severity") == "critical"]),
                "warning": len([alert for alert in alerts if alert.get("severity") == "warning"]),
                "info": len([alert for alert in alerts if alert.get("severity") == "info"]),
            },
        }
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/ops/alerts/evaluate")
async def evaluate_source_operational_alerts(
    limit: int = Query(default=250, ge=1, le=250),
    source_id: str | None = None,
    _: None = Depends(require_admin_key),
):
    try:
        return evaluate_source_ops_alerts(source_id=source_id, limit=limit)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/ops/alerts/deliver")
async def deliver_source_operational_alerts(
    limit: int = Query(default=50, ge=1, le=250),
    alert_id: str | None = None,
    source_id: str | None = None,
    force: bool = Query(default=False),
    _: None = Depends(require_admin_key),
):
    try:
        return await deliver_source_ops_alerts(
            alert_id=alert_id,
            source_id=source_id,
            limit=limit,
            force=force,
        )
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/ops/alerts/deliveries")
async def list_source_operational_alert_deliveries(
    limit: int = Query(default=50, ge=1, le=250),
    alert_id: str | None = None,
    source_id: str | None = None,
    status: str | None = None,
    _: None = Depends(require_admin_key),
):
    try:
        deliveries = list_source_ops_alert_deliveries(
            alert_id=alert_id,
            source_id=source_id,
            status=status,
            limit=limit,
        )
        return {
            "deliveries": deliveries,
            "summary": {
                "delivery_count": len(deliveries),
                "delivered": len([item for item in deliveries if item.get("status") == "delivered"]),
                "failed": len([item for item in deliveries if item.get("status") == "failed"]),
                "skipped": len([item for item in deliveries if item.get("status") == "skipped"]),
            },
        }
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/ops/alerts/{alert_id}/acknowledge")
async def acknowledge_source_operational_alert(
    alert_id: str,
    _: None = Depends(require_admin_key),
):
    try:
        alert = acknowledge_source_ops_alert(alert_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not alert:
        raise HTTPException(status_code=404, detail="Source operations alert not found")
    return alert


@router.post("/feeds/{feed_id}/status")
async def update_source_feed_status(
    feed_id: str,
    payload: SourceFeedGovernanceUpdate,
    _: None = Depends(require_admin_key),
):
    try:
        return update_source_feed_governance(
            feed_id,
            status=payload.status,
            disabled_reason=payload.disabled_reason,
            review_notes=payload.review_notes,
        )
    except FeedStoreError as exc:
        if str(exc) == "Source feed not found.":
            raise HTTPException(status_code=404, detail="Source feed not found") from exc
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/articles/analyze-pending")
async def analyze_pending_source_articles(
    session_id: str = Depends(get_session_id),
    source_id: str | None = None,
    limit: int = Query(default=25, ge=1, le=100),
    include_failed: bool = Query(default=False),
    _: None = Depends(require_admin_key),
):
    try:
        return await analyze_pending_ingested_articles(
            session_id=session_id,
            source_id=source_id,
            limit=limit,
            include_failed=include_failed,
        )
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


@router.get("/{source_id}/intelligence")
async def get_source_intelligence(
    source_id: str,
    refresh: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=250),
):
    try:
        return await build_source_intelligence(source_id, refresh=refresh, limit=limit)
    except FeedStoreError as exc:
        if str(exc) == "Source not found.":
            raise HTTPException(status_code=404, detail="Source not found") from exc
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/{source_id}/intelligence/refresh")
async def refresh_source_intelligence(
    source_id: str,
    limit: int = Query(default=100, ge=1, le=250),
    _: None = Depends(require_admin_key),
):
    try:
        return await build_source_intelligence(source_id, refresh=True, limit=limit)
    except FeedStoreError as exc:
        if str(exc) == "Source not found.":
            raise HTTPException(status_code=404, detail="Source not found") from exc
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/{source_id}")
async def get_source(source_id: str, article_limit: int = Query(default=20, ge=1, le=100)):
    try:
        source = get_source_record(source_id)
        if not source:
            raise HTTPException(status_code=404, detail="Source not found")
        return {
            "source": attach_source_health(source),
            "feeds": list_source_feed_records(source_id=source_id),
            "articles": list_ingested_article_records(source_id=source_id, limit=article_limit),
            "health": build_source_health_summary(source_id),
            "quality": calculate_source_quality_report(source_id),
            "sync_runs": list_source_sync_runs(source_id=source_id, limit=10),
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


@router.get("/{source_id}/sync-runs")
async def list_single_source_sync_history(
    source_id: str,
    limit: int = Query(default=25, ge=1, le=100),
    _: None = Depends(require_admin_key),
):
    try:
        if not get_source_record(source_id):
            raise HTTPException(status_code=404, detail="Source not found")
        return {"sync_runs": list_source_sync_runs(source_id=source_id, limit=limit)}
    except HTTPException:
        raise
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/{source_id}/review")
async def update_source_review(
    source_id: str,
    payload: SourceReviewUpdate,
    _: None = Depends(require_admin_key),
):
    try:
        return update_source_review_status(
            source_id,
            review_status=payload.review_status,
            review_notes=payload.review_notes,
            disabled_reason=payload.disabled_reason,
            terms_reviewed=payload.terms_reviewed,
        )
    except FeedStoreError as exc:
        if str(exc) == "Source not found.":
            raise HTTPException(status_code=404, detail="Source not found") from exc
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/{source_id}/quality/recalculate")
async def recalculate_source_quality_score(
    source_id: str,
    _: None = Depends(require_admin_key),
):
    try:
        return recalculate_source_quality(source_id)
    except FeedStoreError as exc:
        if str(exc) == "Source not found.":
            raise HTTPException(status_code=404, detail="Source not found") from exc
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/{source_id}/sync")
async def sync_source(
    source_id: str,
    _: None = Depends(require_admin_key),
    session_id: str = Depends(get_session_id),
    limit: int = Query(default=20, ge=1, le=50),
    card_limit: int = Query(default=10, ge=0, le=50),
):
    try:
        try:
            return await sync_source_feeds(
                source_id,
                session_id=session_id,
                article_limit=limit,
                card_limit=card_limit,
            )
        except FeedStoreError as exc:
            if str(exc) != "Source not found.":
                raise
            raise HTTPException(status_code=404, detail="Source not found")
    except HTTPException:
        raise
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
