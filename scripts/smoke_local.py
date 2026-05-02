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
os.environ.setdefault("ANALYZE_COOLDOWN_SECONDS", "0")


from fastapi.testclient import TestClient  # noqa: E402

from app.api.v1.routes import analyze as analyze_route  # noqa: E402
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
        store.ARTICLE_COMPARISONS,
        store.NODES,
        store.NODE_EDGES,
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

    original_fetch_article = analyze_route.fetch_article

    async def fake_fetch_article(url: str):
        raise analyze_route.ArticleFetchError("Fixture fetch failure for metadata fallback.")

    try:
        analyze_route.fetch_article = fake_fetch_article
        ingested_analysis = _assert_ok(
            client.post(
                "/api/v1/analyze",
                headers=headers,
                json={"ingested_article_id": ingested_article_id},
            ),
            "analyze/ingested-article",
        ).json()
    finally:
        analyze_route.fetch_article = original_fetch_article

    assert ingested_analysis["ingested_article_id"] == ingested_article_id, ingested_analysis
    assert ingested_analysis["card"]["payload"]["analysis_status"] == "analyzed", ingested_analysis
    assert ingested_analysis["intelligence"]["article"]["url"] == "https://example.com/grid-resilience-plan", ingested_analysis

    comparison_source = store.create_source_record(
        name="Regional Grid Monitor",
        website_url="https://regional.example.com",
        country="Canada",
        language="English",
        region="North America",
        source_size="small",
        source_type="independent",
    )
    comparison_feed = store.create_source_feed_record(
        source_id=comparison_source["id"],
        feed_url="https://regional.example.com/rss.xml",
        feed_type="rss",
        title="Regional Grid Monitor RSS",
    )
    comparison_ingested = store.save_ingested_articles(
        comparison_source,
        comparison_feed,
        [
            {
                "external_id": "source-item-2",
                "title": "Agency unveils grid resilience plan",
                "summary": "Regional coverage says regulators outlined a grid resilience plan with cost questions.",
                "url": "https://regional.example.com/grid-resilience-plan",
                "published_at": "2026-05-02T11:30:00+00:00",
                "raw": {"fixture": True},
            }
        ],
        session_id=session_id,
    )
    assert comparison_ingested["articles"], comparison_ingested

    article_compare = _assert_ok(
        client.get(f"/api/v1/compare/{ingested_article_id}?limit=5", headers=headers),
        "compare/ingested-article",
    ).json()
    assert article_compare["base_article"]["id"] == ingested_article_id, article_compare
    assert article_compare["similar_articles"], article_compare
    assert article_compare["comparison"]["confidence"] > 0, article_compare
    assert store.ARTICLE_COMPARISONS, article_compare

    article = ExtractedArticle(
        url="https://example.com/analysis",
        final_url="https://example.com/analysis",
        title="Energy market analysis",
        source="Example Wire",
        domain="example.com",
        text="Officials reported grid costs because market prices changed. " * 35,
        excerpt="Officials reported grid costs because market prices changed.",
    )

    async def fake_success_fetch_article(url: str):
        return article

    try:
        analyze_route.fetch_article = fake_success_fetch_article
        url_analysis = _assert_ok(
            client.post(
                "/api/v1/analyze",
                headers=headers,
                json={"url": article.url},
            ),
            "analyze/url",
        ).json()
    finally:
        analyze_route.fetch_article = original_fetch_article

    card = url_analysis["card"]
    assert url_analysis["intelligence"]["article"]["title"] == "Energy market analysis", url_analysis
    store.update_report_saved(card["report_id"], session_id=session_id, is_saved=True)

    feed = _assert_ok(client.get("/api/v1/feed", headers=headers), "feed").json()
    assert len(feed["cards"]) >= 5, feed
    ingested_feed_card = next(
        item
        for item in feed["cards"]
        if (item.get("payload") or {}).get("ingested_article_id") == ingested_article_id
    )
    assert ingested_feed_card["report_id"] == ingested_analysis["report_id"], ingested_feed_card
    assert ingested_feed_card["analysis"]["intelligence"]["comparison_hooks"]["event_fingerprint"], ingested_feed_card

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
    assert source_articles[0]["analysis_status"] == "analyzed", source_articles
    article_detail_payload = _assert_ok(
        client.get(f"/api/v1/sources/articles/{ingested_article_id}", headers=headers),
        "sources/article-detail",
    ).json()
    article_detail = article_detail_payload["article"]
    assert article_detail["event_fingerprint"], article_detail
    assert article_detail["analysis"]["intelligence"]["scores"]["cross_source_need"] > 0, article_detail
    assert article_detail_payload["feed_card"]["report_id"] == ingested_analysis["report_id"], article_detail_payload
    assert article_detail_payload["source"]["name"] == "Example Daily", article_detail_payload
    assert article_detail_payload["comparison_hooks"]["event_fingerprint"] == article_detail["event_fingerprint"], article_detail_payload
    assert any(node["node_type"] == "claim" for node in article_detail_payload["nodes_preview"]), article_detail_payload
    assert article_detail_payload["node_graph"]["node_type_counts"]["article"] == 1, article_detail_payload
    assert article_detail_payload["node_graph"]["node_type_counts"]["claim"] >= 1, article_detail_payload
    assert article_detail_payload["node_graph"]["edges"], article_detail_payload
    article_nodes = _assert_ok(
        client.get(f"/api/v1/sources/articles/{ingested_article_id}/nodes?node_type=author", headers=headers),
        "sources/article-nodes",
    ).json()
    assert article_nodes["selected_node"]["node_type"] == "author", article_nodes
    assert article_nodes["selected_perspective"]["question"], article_nodes
    assert store.NODES and store.NODE_EDGES, article_nodes

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
