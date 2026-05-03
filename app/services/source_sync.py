from __future__ import annotations

from app.core.session import ANONYMOUS_SESSION_ID
from app.services.feed.store import (
    FeedStoreError,
    get_source_record,
    list_source_feed_records,
    list_source_records,
    save_ingested_articles,
    update_source_feed_sync_result,
)
from app.services.rss import RSSSyncError, parse_rss_feed


def _clamp(value: int | None, default: int, minimum: int, maximum: int) -> int:
    try:
        raw = int(value if value is not None else default)
    except (TypeError, ValueError):
        raw = default
    return max(minimum, min(raw, maximum))


async def sync_source_feeds(
    source_id: str,
    session_id: str = ANONYMOUS_SESSION_ID,
    article_limit: int = 20,
    card_limit: int = 10,
    feed_limit: int = 25,
) -> dict:
    source = get_source_record(source_id)
    if not source:
        raise FeedStoreError("Source not found.")

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

    return {
        "source": get_source_record(source_id) or source,
        "rss_feed_count": len(rss_feeds),
        "synced_feed_count": len(synced_feeds),
        "article_count": len(articles),
        "card_count": len(cards),
        "articles": articles,
        "cards": cards,
        "synced_feeds": synced_feeds,
        "errors": errors,
    }


async def sync_active_source_feeds(
    session_id: str = ANONYMOUS_SESSION_ID,
    source_limit: int = 50,
    feed_limit: int = 100,
    article_limit: int = 10,
    card_limit: int = 25,
) -> dict:
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
            }
        )

    return {
        "status": "completed",
        "source_count": len(source_results),
        "feed_count": feed_count,
        "synced_feed_count": sum(item["synced_feed_count"] for item in source_results),
        "article_count": len(articles),
        "card_count": len(cards),
        "error_count": len(errors),
        "sources": source_results,
        "articles": articles,
        "cards": cards,
        "errors": errors,
        "limits": {
            "source_limit": max_sources,
            "feed_limit": max_feeds,
            "article_limit_per_feed": max_articles,
            "card_limit": max_cards,
        },
    }
