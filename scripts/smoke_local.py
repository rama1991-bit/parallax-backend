"""
Repeatable local smoke checks for the Parallax API surface.

By default this script forces the in-memory store so it is safe to run before
deploys without touching Supabase/Postgres. Set PARALLAX_SMOKE_USE_CONFIG_DB=true
to exercise the configured DATABASE_URL instead.
"""

from __future__ import annotations

import os
from uuid import uuid4


if os.getenv("PARALLAX_SMOKE_USE_CONFIG_DB", "").lower() not in {"1", "true", "yes"}:
    os.environ["DATABASE_URL"] = ""


from fastapi.testclient import TestClient  # noqa: E402

from app.api.v1.routes import sources as sources_route  # noqa: E402
from app.main import app  # noqa: E402
from app.services.articles import ExtractedArticle  # noqa: E402
from app.services.feed import store  # noqa: E402


def _reset_memory_store() -> None:
    if store.database_enabled():
        return

    for collection in (
        store.FEED_CARDS,
        store.TOPICS,
        store.MONITORS,
        store.FEEDS,
        store.FEED_ITEMS,
        store.ALERTS,
        store.BRIEFS,
        store.SOURCES,
        store.SOURCE_FEEDS,
        store.INGESTED_ARTICLES,
    ):
        collection.clear()


def _assert_ok(response, label: str):
    assert 200 <= response.status_code < 300, f"{label}: {response.status_code} {response.text}"
    return response


def main() -> int:
    _reset_memory_store()

    client = TestClient(app)
    session_id = f"smoke-{uuid4()}"
    other_session_id = f"smoke-other-{uuid4()}"
    headers = {"X-Parallax-Session-Id": session_id}

    _assert_ok(client.get("/api/v1/health"), "health")
    _assert_ok(client.get("/api/v1/health/db"), "health/db")
    me = _assert_ok(client.get("/api/v1/auth/me", headers=headers), "auth/me").json()
    assert me["session"]["id"] == session_id, me

    onboarding = _assert_ok(
        client.post(
            "/api/v1/onboarding",
            headers=headers,
            json={
                "topics": [
                    {
                        "name": "Energy transition",
                        "keywords": ["grid", "prices", "renewables"],
                    },
                    {
                        "name": "AI regulation",
                        "keywords": ["model safety", "copyright"],
                    },
                ],
                "feeds": [
                    {
                        "url": "https://example.com/rss.xml",
                        "title": "Example Wire",
                    }
                ],
            },
        ),
        "onboarding",
    ).json()
    assert onboarding["topic_count"] == 2, onboarding
    assert onboarding["feed_count"] == 1, onboarding

    state = _assert_ok(client.get("/api/v1/onboarding/state", headers=headers), "onboarding/state").json()
    assert state["is_configured"] is True, state

    feed_subscription = store.list_feed_subscriptions(session_id=session_id)[0]
    store.save_feed_items(
        feed_subscription,
        [
            {
                "external_id": "item-1",
                "title": "Grid costs rise",
                "summary": "A subscribed source published a grid costs item.",
                "url": "https://example.com/grid-costs",
                "source": "example.com",
            }
        ],
        session_id=session_id,
    )

    source_payload = _assert_ok(
        client.post(
            "/api/v1/sources",
            json={
                "name": "Example Daily",
                "website_url": "https://example.com",
                "country": "United States",
                "language": "English",
                "region": "North America",
                "source_size": "medium",
                "source_type": "newspaper",
                "credibility_notes": "Smoke-test source, not a real editorial profile.",
            },
        ),
        "sources/create",
    ).json()
    source = source_payload["source"]
    source_feed = source_payload["feed"]
    assert source["name"] == "Example Daily", source
    assert source_feed["feed_type"] == "homepage", source_feed

    source_sync = _assert_ok(
        client.post(f"/api/v1/sources/{source['id']}/sync", headers=headers),
        "sources/sync-no-rss",
    ).json()
    assert source_sync["rss_feed_count"] == 0, source_sync

    rss_feed = _assert_ok(
        client.post(
            f"/api/v1/sources/{source['id']}/feeds",
            json={
                "feed_url": "https://example.com/rss.xml",
                "feed_type": "rss",
                "title": "Example Daily RSS",
            },
        ),
        "sources/feed-create",
    ).json()["feed"]
    assert rss_feed["feed_type"] == "rss", rss_feed

    original_parse_rss_feed = sources_route.parse_rss_feed

    async def fake_parse_rss_feed(url: str, limit: int = 20):
        return {
            "url": url,
            "title": "Example Daily RSS",
            "description": "Fixture feed for local smoke.",
            "items": [
                {
                    "external_id": "source-item-1",
                    "title": "Regulators publish grid resilience plan",
                    "summary": "A source item about grid resilience was ingested for Phase 2.",
                    "url": "https://example.com/grid-resilience-plan",
                    "published_at": "2026-05-02T10:00:00+00:00",
                    "raw": {"fixture": True},
                }
            ],
        }

    try:
        sources_route.parse_rss_feed = fake_parse_rss_feed
        ingested = _assert_ok(
            client.post(f"/api/v1/sources/{source['id']}/sync", headers=headers),
            "sources/sync-rss",
        ).json()
    finally:
        sources_route.parse_rss_feed = original_parse_rss_feed

    assert len(ingested["articles"]) == 1, ingested
    assert len(ingested["cards"]) == 1, ingested
    ingested_article_id = ingested["articles"][0]["id"]

    article = ExtractedArticle(
        url="https://example.com/analysis",
        final_url="https://example.com/analysis",
        title="Energy market analysis",
        source="Example Wire",
        domain="example.com",
        text="Officials reported grid costs because market prices changed. " * 35,
        excerpt="Officials reported grid costs because market prices changed.",
    )
    card = store.save_analysis_card(
        article,
        {
            "title": "Energy market analysis",
            "summary": "High-priority article analysis for production smoke checks.",
            "key_claims": [
                "Officials reported grid costs",
                "Market prices changed",
            ],
            "narrative_framing": [
                "economic_consequence",
                "institutional_response",
            ],
            "entities": ["Example Wire"],
            "topics": ["energy transition"],
            "confidence": 0.89,
            "priority": "high",
        },
        session_id=session_id,
    )
    store.update_report_saved(card["report_id"], session_id=session_id, is_saved=True)

    feed = _assert_ok(client.get("/api/v1/feed", headers=headers), "feed").json()
    assert len(feed["cards"]) >= 5, feed
    assert any(item["card_type"] == "ingested_article" for item in feed["cards"]), feed

    alerts = _assert_ok(client.get("/api/v1/alerts", headers=headers), "alerts").json()
    assert alerts["unread_count"] >= 4, alerts
    first_alert = alerts["alerts"][0]
    _assert_ok(client.post(f"/api/v1/alerts/{first_alert['id']}/read", headers=headers), "alert/read")
    _assert_ok(client.post("/api/v1/alerts/read-all", headers=headers), "alerts/read-all")
    assert _assert_ok(client.get("/api/v1/alerts/unread-count", headers=headers), "alerts/count").json()[
        "unread_count"
    ] == 0

    sources = _assert_ok(client.get("/api/v1/authors", headers=headers), "authors").json()["sources"]
    assert sources and sources[0]["signal_count"] >= 2, sources
    source_id = sources[0]["id"]
    _assert_ok(client.get(f"/api/v1/authors/{source_id}", headers=headers), "author/detail")
    _assert_ok(client.get("/api/v1/authors/example.com", headers=headers), "author/domain-alias")
    other_sources = _assert_ok(
        client.get("/api/v1/authors", headers={"X-Parallax-Session-Id": other_session_id}),
        "authors/session-isolation",
    ).json()["sources"]
    assert other_sources == [], other_sources

    phase2_sources = _assert_ok(client.get("/api/v1/sources"), "sources/list").json()["sources"]
    assert any(item["id"] == source["id"] for item in phase2_sources), phase2_sources
    source_detail = _assert_ok(client.get(f"/api/v1/sources/{source['id']}"), "sources/detail").json()
    assert source_detail["source"]["article_count"] == 1, source_detail
    assert any(feed["feed_type"] == "homepage" for feed in source_detail["feeds"]), source_detail
    assert any(feed["feed_type"] == "rss" for feed in source_detail["feeds"]), source_detail
    source_articles = _assert_ok(
        client.get(f"/api/v1/sources/{source['id']}/articles"),
        "sources/articles",
    ).json()["articles"]
    assert source_articles and source_articles[0]["id"] == ingested_article_id, source_articles
    article_detail = _assert_ok(
        client.get(f"/api/v1/sources/articles/{ingested_article_id}"),
        "sources/article-detail",
    ).json()["article"]
    assert article_detail["event_fingerprint"], article_detail

    report = _assert_ok(client.get(f"/api/v1/reports/{card['report_id']}", headers=headers), "report").json()
    assert report["id"] == card["report_id"], report
    _assert_ok(client.get(f"/api/v1/reports/{card['report_id']}/export?format=json", headers=headers), "report/json")
    markdown = _assert_ok(
        client.get(f"/api/v1/reports/{card['report_id']}/export?format=markdown", headers=headers),
        "report/markdown",
    ).text
    assert "# Energy market analysis" in markdown, markdown

    saved = _assert_ok(client.get("/api/v1/saved-reports", headers=headers), "saved-reports").json()
    assert len(saved["reports"]) == 1, saved

    briefs = _assert_ok(client.get("/api/v1/public/briefs", headers=headers), "briefs").json()["briefs"]
    latest = next(brief for brief in briefs if brief["scope"] == "latest")
    brief = _assert_ok(client.get(f"/api/v1/public/briefs/{latest['token']}"), "brief/detail").json()
    assert brief["signal_count"] >= 4, brief
    assert brief["sources"] and brief["sources"][0]["id"], brief
    _assert_ok(client.get(f"/api/v1/public/briefs/{latest['token']}/export?format=markdown"), "brief/markdown")

    topics = _assert_ok(client.get("/api/v1/topics", headers=headers), "topics").json()
    assert len(topics["topics"]) == 2, topics
    feeds = _assert_ok(client.get("/api/v1/feeds", headers=headers), "feeds").json()
    assert len(feeds["feeds"]) == 1, feeds

    print(
        "smoke passed",
        {
            "session_id": session_id,
            "cards": len(feed["cards"]),
            "sources": len(sources),
            "brief_signals": brief["signal_count"],
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
