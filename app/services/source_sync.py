from __future__ import annotations

from datetime import datetime, timezone

from app.core.session import ANONYMOUS_SESSION_ID
from app.services.feed.store import (
    FeedStoreError,
    evaluate_source_ops_alerts,
    get_source_record,
    list_source_feed_records,
    list_source_records,
    record_source_sync_run,
    save_ingested_articles,
    update_source_feed_sync_result,
)
from app.services.ops_notifications import safely_deliver_source_ops_alerts
from app.services.rss import RSSSyncError, parse_rss_feed


def _clamp(value: int | None, default: int, minimum: int, maximum: int) -> int:
    try:
        raw = int(value if value is not None else default)
    except (TypeError, ValueError):
        raw = default
    return max(minimum, min(raw, maximum))


def _sync_status(feed_count: int, synced_feed_count: int, error_count: int) -> str:
    if not feed_count:
        return "skipped"
    if error_count and synced_feed_count:
        return "partial"
    if error_count:
        return "failed"
    return "completed"


def _safe_record_sync_run(**kwargs) -> dict | None:
    try:
        return record_source_sync_run(**kwargs)
    except FeedStoreError:
        return None


def _safe_evaluate_ops_alerts(**kwargs) -> dict | None:
    try:
        return evaluate_source_ops_alerts(**kwargs)
    except FeedStoreError:
        return None


async def _safe_deliver_ops_alerts(**kwargs) -> dict | None:
    return await safely_deliver_source_ops_alerts(**kwargs)


def _skipped_source_result(source: dict, session_id: str, started_at: datetime, reason: str) -> dict:
    limits = {"article_limit_per_feed": 0, "card_limit": 0, "feed_limit": 0}
    summary = {
        "source_id": source.get("id"),
        "source_name": source.get("name"),
        "rss_feed_count": 0,
        "synced_feed_count": 0,
        "article_count": 0,
        "card_count": 0,
        "error_count": 0,
        "skipped_reason": reason,
    }
    run = _safe_record_sync_run(
        sync_scope="source",
        status="skipped",
        session_id=session_id,
        source_id=source.get("id"),
        source_name=source.get("name"),
        started_at=started_at,
        finished_at=datetime.now(timezone.utc),
        source_count=1,
        feed_count=0,
        synced_feed_count=0,
        article_count=0,
        card_count=0,
        error_count=0,
        limits=limits,
        errors=[],
        summary=summary,
    )
    result = {
        "source": source,
        "status": "skipped",
        "skipped_reason": reason,
        "rss_feed_count": 0,
        "synced_feed_count": 0,
        "article_count": 0,
        "card_count": 0,
        "articles": [],
        "cards": [],
        "synced_feeds": [],
        "errors": [],
        "limits": limits,
    }
    if run:
        result["sync_run"] = run
        result["sync_run_id"] = run["id"]
        result["ops_alerts"] = _safe_evaluate_ops_alerts(
            source_id=source.get("id"),
            sync_run_id=run["id"],
        )
    return result


async def sync_source_feeds(
    source_id: str,
    session_id: str = ANONYMOUS_SESSION_ID,
    article_limit: int = 20,
    card_limit: int = 10,
    feed_limit: int = 25,
    deliver_ops_alerts: bool = True,
) -> dict:
    started_at = datetime.now(timezone.utc)
    source = get_source_record(source_id)
    if not source:
        raise FeedStoreError("Source not found.")
    if source.get("review_status") in {"quarantined", "disabled"}:
        skipped = _skipped_source_result(
            source,
            session_id=session_id,
            started_at=started_at,
            reason=f"Source review status is {source.get('review_status')}.",
        )
        if deliver_ops_alerts and skipped.get("ops_alerts"):
            skipped["ops_alert_delivery"] = await _safe_deliver_ops_alerts(
                source_id=source_id,
                sync_run_id=skipped.get("sync_run_id"),
            )
        return skipped

    max_articles = _clamp(article_limit, default=20, minimum=1, maximum=50)
    max_cards = _clamp(card_limit, default=10, minimum=0, maximum=50)
    max_feeds = _clamp(feed_limit, default=25, minimum=1, maximum=100)
    rss_feeds = list_source_feed_records(source_id=source_id, feed_type="rss", status="active")[:max_feeds]

    articles = []
    cards = []
    errors = []
    synced_feeds = []

    for feed in rss_feeds:
        try:
            parsed = await parse_rss_feed(feed["feed_url"], limit=max_articles)
            updated_feed = update_source_feed_sync_result(feed["id"], success=True, title=parsed.get("title"))
            saved = save_ingested_articles(
                source,
                updated_feed or feed,
                parsed.get("items") or [],
                session_id=session_id,
                card_limit=max(0, max_cards - len(cards)),
            )
            articles.extend(saved["articles"])
            cards.extend(saved["cards"])
            synced_feeds.append(
                {
                    "feed_id": feed["id"],
                    "feed_url": feed["feed_url"],
                    "title": (updated_feed or feed).get("title") or parsed.get("title"),
                    "item_count": len(parsed.get("items") or []),
                    "article_count": len(saved["articles"]),
                    "card_count": len(saved["cards"]),
                    "last_success_at": (updated_feed or {}).get("last_success_at"),
                }
            )
        except RSSSyncError as exc:
            failed_feed = update_source_feed_sync_result(feed["id"], success=False, error=str(exc))
            errors.append(
                {
                    "source_id": source_id,
                    "source_name": source.get("name"),
                    "feed_id": feed["id"],
                    "feed_url": feed["feed_url"],
                    "error": str(exc),
                    "last_checked_at": (failed_feed or {}).get("last_checked_at"),
                }
            )

    status = _sync_status(len(rss_feeds), len(synced_feeds), len(errors))
    limits = {
        "article_limit_per_feed": max_articles,
        "card_limit": max_cards,
        "feed_limit": max_feeds,
    }
    summary = {
        "source_id": source_id,
        "source_name": source.get("name"),
        "rss_feed_count": len(rss_feeds),
        "synced_feed_count": len(synced_feeds),
        "article_count": len(articles),
        "card_count": len(cards),
        "error_count": len(errors),
    }
    run = _safe_record_sync_run(
        sync_scope="source",
        status=status,
        session_id=session_id,
        source_id=source_id,
        source_name=source.get("name"),
        started_at=started_at,
        finished_at=datetime.now(timezone.utc),
        source_count=1,
        feed_count=len(rss_feeds),
        synced_feed_count=len(synced_feeds),
        article_count=len(articles),
        card_count=len(cards),
        error_count=len(errors),
        limits=limits,
        errors=errors,
        summary=summary,
    )

    result = {
        "source": get_source_record(source_id) or source,
        "status": status,
        "rss_feed_count": len(rss_feeds),
        "synced_feed_count": len(synced_feeds),
        "article_count": len(articles),
        "card_count": len(cards),
        "articles": articles,
        "cards": cards,
        "synced_feeds": synced_feeds,
        "errors": errors,
        "limits": limits,
    }
    if run:
        result["sync_run"] = run
        result["sync_run_id"] = run["id"]
        result["ops_alerts"] = _safe_evaluate_ops_alerts(
            source_id=source_id,
            sync_run_id=run["id"],
        )
        if deliver_ops_alerts:
            result["ops_alert_delivery"] = await _safe_deliver_ops_alerts(
                source_id=source_id,
                sync_run_id=run["id"],
            )
    else:
        result["sync_run_log_error"] = "Sync completed but run logging failed."
    return result


async def sync_active_source_feeds(
    session_id: str = ANONYMOUS_SESSION_ID,
    source_limit: int = 50,
    feed_limit: int = 100,
    article_limit: int = 10,
    card_limit: int = 25,
) -> dict:
    started_at = datetime.now(timezone.utc)
    max_sources = _clamp(source_limit, default=50, minimum=1, maximum=250)
    max_feeds = _clamp(feed_limit, default=100, minimum=1, maximum=500)
    max_articles = _clamp(article_limit, default=10, minimum=1, maximum=50)
    max_cards = _clamp(card_limit, default=25, minimum=0, maximum=250)

    sources = list_source_records(limit=max_sources)
    source_results = []
    articles = []
    cards = []
    errors = []
    feed_count = 0

    for source in sources:
        if feed_count >= max_feeds:
            break
        remaining_feeds = max(0, max_feeds - feed_count)
        remaining_cards = max(0, max_cards - len(cards))
        if remaining_feeds <= 0:
            break

        result = await sync_source_feeds(
            source["id"],
            session_id=session_id,
            article_limit=max_articles,
            card_limit=remaining_cards,
            feed_limit=remaining_feeds,
            deliver_ops_alerts=False,
        )
        feed_count += result["rss_feed_count"]
        articles.extend(result["articles"])
        cards.extend(result["cards"])
        errors.extend(result["errors"])
        source_results.append(
            {
                "source_id": source["id"],
                "source_name": source.get("name"),
                "rss_feed_count": result["rss_feed_count"],
                "synced_feed_count": result["synced_feed_count"],
                "article_count": result["article_count"],
                "card_count": result["card_count"],
                "error_count": len(result["errors"]),
                "status": result.get("status"),
                "sync_run_id": result.get("sync_run_id"),
            }
        )

    status = _sync_status(feed_count, sum(item["synced_feed_count"] for item in source_results), len(errors))
    limits = {
        "source_limit": max_sources,
        "feed_limit": max_feeds,
        "article_limit_per_feed": max_articles,
        "card_limit": max_cards,
    }
    summary = {
        "source_count": len(source_results),
        "feed_count": feed_count,
        "synced_feed_count": sum(item["synced_feed_count"] for item in source_results),
        "article_count": len(articles),
        "card_count": len(cards),
        "error_count": len(errors),
    }
    run = _safe_record_sync_run(
        sync_scope="active_sources",
        status=status,
        session_id=session_id,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc),
        source_count=len(source_results),
        feed_count=feed_count,
        synced_feed_count=summary["synced_feed_count"],
        article_count=len(articles),
        card_count=len(cards),
        error_count=len(errors),
        limits=limits,
        errors=errors,
        summary=summary,
    )

    result = {
        "status": status,
        "source_count": len(source_results),
        "feed_count": feed_count,
        "synced_feed_count": summary["synced_feed_count"],
        "article_count": len(articles),
        "card_count": len(cards),
        "error_count": len(errors),
        "sources": source_results,
        "articles": articles,
        "cards": cards,
        "errors": errors,
        "limits": limits,
    }
    if run:
        result["sync_run"] = run
        result["sync_run_id"] = run["id"]
        result["ops_alerts"] = _safe_evaluate_ops_alerts(sync_run_id=run["id"], limit=max_sources)
        result["ops_alert_delivery"] = await _safe_deliver_ops_alerts(
            sync_run_id=run["id"],
            limit=max_sources,
        )
    else:
        result["sync_run_log_error"] = "Sync completed but run logging failed."
    return result
