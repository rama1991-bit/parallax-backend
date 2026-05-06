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
os.environ.setdefault("ADMIN_API_KEY", "smoke-admin-key")


from fastapi.testclient import TestClient  # noqa: E402

from app.api.v1.routes import analyze as analyze_route  # noqa: E402
from app.main import app  # noqa: E402
from app.services.articles import ExtractedArticle  # noqa: E402
from app.services import analysis as analysis_service  # noqa: E402
from app.services import ingested_analysis as ingested_analysis_service  # noqa: E402
from app.services import intelligence as intelligence_service  # noqa: E402
from app.services import ops_notifications as ops_notification_service  # noqa: E402
from app.services import osint as osint_service  # noqa: E402
from app.services import source_sync as source_sync_service  # noqa: E402
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
        store.SOURCE_SYNC_RUNS,
        store.SOURCE_OPS_ALERTS,
        store.SOURCE_OPS_ALERT_DELIVERIES,
        store.INTELLIGENCE_SNAPSHOTS,
        store.INTELLIGENCE_REFRESH_RUNS,
        store.EVENT_CLUSTERS,
        store.EVENT_CLUSTER_ARTICLES,
        store.EVENT_CLUSTER_REFRESH_RUNS,
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
    admin_headers = {**headers, "X-Parallax-Admin-Key": "smoke-admin-key"}

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

    denied_source_sync = client.post(f"/api/v1/sources/{source['id']}/sync", headers=headers)
    assert denied_source_sync.status_code == 403, denied_source_sync.text

    original_parse_homepage_feed = source_sync_service.parse_homepage_feed

    async def fake_parse_homepage_feed(url: str, limit: int = 20):
        return {
            "url": url,
            "title": "Example Daily Homepage",
            "description": "Fixture homepage discovery.",
            "items": [
                {
                    "external_id": "homepage-item-1",
                    "title": "Homepage highlights grid resilience preview",
                    "summary": "A homepage-discovered story previews grid resilience coverage.",
                    "url": "https://example.com/homepage-grid-resilience-preview",
                    "published_at": "2026-05-02T08:00:00+00:00",
                    "raw": {"fixture": True, "discovery_method": "homepage_link_heuristic"},
                }
            ],
        }

    try:
        source_sync_service.parse_homepage_feed = fake_parse_homepage_feed
        source_sync = _assert_ok(
            client.post(f"/api/v1/sources/{source['id']}/sync", headers=admin_headers),
            "sources/sync-homepage",
        ).json()
    finally:
        source_sync_service.parse_homepage_feed = original_parse_homepage_feed

    assert source_sync["feed_count"] == 1, source_sync
    assert source_sync["rss_feed_count"] == 0, source_sync
    assert source_sync["homepage_feed_count"] == 1, source_sync
    assert source_sync["article_count"] == 1, source_sync
    assert source_sync["status"] == "completed", source_sync
    assert source_sync["sync_run_id"], source_sync

    default_preview = _assert_ok(client.get("/api/v1/sources/defaults/preview"), "sources/defaults-preview").json()
    assert default_preview["summary"]["source_count"] >= 40, default_preview
    assert default_preview["summary"]["language_count"] >= 8, default_preview
    assert default_preview["summary"]["rss_count"] >= 20, default_preview

    denied_default_seed = client.post("/api/v1/sources/defaults/seed")
    assert denied_default_seed.status_code == 403, denied_default_seed.text

    default_seed = _assert_ok(
        client.post("/api/v1/sources/defaults/seed", headers=admin_headers),
        "sources/defaults-seed",
    ).json()
    assert default_seed["summary"]["seeded_source_count"] == default_preview["summary"]["source_count"], default_seed
    assert default_seed["summary"]["seeded_feed_count"] == default_preview["summary"]["rss_count"], default_seed
    default_seed_alias = _assert_ok(
        client.post("/api/v1/sources/seed-defaults?limit=5", headers=admin_headers),
        "sources/defaults-seed-alias",
    ).json()
    assert default_seed_alias["summary"]["seeded_source_count"] == 5, default_seed_alias

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

    denied_source_review = client.post(
        f"/api/v1/sources/{source['id']}/review",
        headers=headers,
        json={"review_status": "reviewed"},
    )
    assert denied_source_review.status_code == 403, denied_source_review.text
    reviewed_source = _assert_ok(
        client.post(
            f"/api/v1/sources/{source['id']}/review",
            headers=admin_headers,
            json={
                "review_status": "reviewed",
                "review_notes": "Smoke reviewed source metadata and feed terms.",
                "terms_reviewed": True,
            },
        ),
        "sources/review",
    ).json()
    assert reviewed_source["source"]["review_status"] == "reviewed", reviewed_source
    assert reviewed_source["quality"]["review_status"] == "reviewed", reviewed_source

    review_queue = _assert_ok(
        client.get("/api/v1/sources/needs-review", headers=admin_headers),
        "sources/needs-review",
    ).json()
    assert review_queue["summary"]["source_count"] >= 1, review_queue
    denied_review_queue = client.get("/api/v1/sources/needs-review", headers=headers)
    assert denied_review_queue.status_code == 403, denied_review_queue.text

    quarantined_feed = _assert_ok(
        client.post(
            f"/api/v1/sources/feeds/{rss_feed['id']}/status",
            headers=admin_headers,
            json={
                "status": "quarantined",
                "disabled_reason": "Smoke test quarantine before ingestion.",
                "review_notes": "Feed should be skipped while quarantined.",
            },
        ),
        "sources/feed-quarantine",
    ).json()["feed"]
    assert quarantined_feed["status"] == "quarantined", quarantined_feed
    original_parse_homepage_feed_quarantine = source_sync_service.parse_homepage_feed
    try:
        source_sync_service.parse_homepage_feed = fake_parse_homepage_feed
        quarantined_sync = _assert_ok(
            client.post(f"/api/v1/sources/{source['id']}/sync", headers=admin_headers),
            "sources/sync-quarantined-feed",
        ).json()
    finally:
        source_sync_service.parse_homepage_feed = original_parse_homepage_feed_quarantine

    assert quarantined_sync["status"] == "completed", quarantined_sync
    assert quarantined_sync["feed_count"] == 1, quarantined_sync
    assert quarantined_sync["homepage_feed_count"] == 1, quarantined_sync
    assert quarantined_sync["rss_feed_count"] == 0, quarantined_sync
    assert quarantined_sync["ops_alerts"]["generated_alert_count"] >= 1, quarantined_sync
    reactivated_feed = _assert_ok(
        client.post(
            f"/api/v1/sources/feeds/{rss_feed['id']}/status",
            headers=admin_headers,
            json={"status": "active", "review_notes": "Feed re-enabled after smoke quarantine."},
        ),
        "sources/feed-reactivate",
    ).json()["feed"]
    assert reactivated_feed["status"] == "active", reactivated_feed

    disabled_source_payload = _assert_ok(
        client.post(
            "/api/v1/sources",
            json={
                "name": "Disabled Governance Wire",
                "website_url": "https://disabled.example.com",
                "rss_url": "https://disabled.example.com/rss.xml",
                "country": "United States",
                "language": "English",
                "source_type": "independent",
            },
        ),
        "sources/create-disabled-fixture",
    ).json()
    disabled_source = disabled_source_payload["source"]
    disabled_review = _assert_ok(
        client.post(
            f"/api/v1/sources/{disabled_source['id']}/review",
            headers=admin_headers,
            json={
                "review_status": "disabled",
                "disabled_reason": "Smoke test source-level ingestion skip.",
            },
        ),
        "sources/disable-source",
    ).json()
    assert disabled_review["source"]["review_status"] == "disabled", disabled_review
    disabled_sync = _assert_ok(
        client.post(f"/api/v1/sources/{disabled_source['id']}/sync", headers=admin_headers),
        "sources/sync-disabled-source",
    ).json()
    assert disabled_sync["status"] == "skipped", disabled_sync
    assert "disabled" in disabled_sync["skipped_reason"], disabled_sync
    assert disabled_sync["ops_alerts"]["generated_alert_count"] >= 1, disabled_sync

    ops_evaluation = _assert_ok(
        client.post("/api/v1/sources/ops/alerts/evaluate?limit=25", headers=admin_headers),
        "sources/ops-alerts-evaluate",
    ).json()
    assert ops_evaluation["active_alert_count"] >= 1, ops_evaluation
    ops_alerts = _assert_ok(
        client.get("/api/v1/sources/ops/alerts", headers=admin_headers),
        "sources/ops-alerts",
    ).json()["alerts"]
    assert any(alert["alert_type"] == "source_disabled" for alert in ops_alerts), ops_alerts
    assert "delivery_status" in ops_alerts[0], ops_alerts
    denied_ops_alerts = client.get("/api/v1/sources/ops/alerts", headers=headers)
    assert denied_ops_alerts.status_code == 403, denied_ops_alerts.text
    denied_ops_delivery = client.post("/api/v1/sources/ops/alerts/deliver", headers=headers)
    assert denied_ops_delivery.status_code == 403, denied_ops_delivery.text

    previous_ops_enabled = ops_notification_service.settings.OPS_NOTIFICATIONS_ENABLED
    previous_ops_url = ops_notification_service.settings.OPS_WEBHOOK_URL
    previous_ops_secret = ops_notification_service.settings.OPS_WEBHOOK_SECRET
    original_post_webhook = ops_notification_service._post_webhook

    async def fake_post_webhook(url: str, payload: dict):
        assert url == "https://hooks.example.test/parallax"
        assert payload["event"] == "source_ops_alert"
        return {"status": "delivered", "response_status": 202, "error": None}

    async def fake_failed_post_webhook(url: str, payload: dict):
        return {"status": "failed", "response_status": 503, "error": "Fixture webhook failure."}

    try:
        ops_notification_service.settings.OPS_NOTIFICATIONS_ENABLED = True
        ops_notification_service.settings.OPS_WEBHOOK_URL = "https://hooks.example.test/parallax"
        ops_notification_service.settings.OPS_WEBHOOK_SECRET = "smoke-webhook-secret"
        ops_notification_service._post_webhook = fake_post_webhook
        delivered_ops_alerts = _assert_ok(
            client.post("/api/v1/sources/ops/alerts/deliver?limit=25", headers=admin_headers),
            "sources/ops-alerts-deliver",
        ).json()
        assert delivered_ops_alerts["delivery_count"] >= 1, delivered_ops_alerts
        assert delivered_ops_alerts["summary"]["delivered"] >= 1, delivered_ops_alerts
        deliveries = _assert_ok(
            client.get("/api/v1/sources/ops/alerts/deliveries", headers=admin_headers),
            "sources/ops-alert-deliveries",
        ).json()
        assert deliveries["summary"]["delivered"] >= 1, deliveries
        delivered_list_alerts = _assert_ok(
            client.get("/api/v1/sources/ops/alerts", headers=admin_headers),
            "sources/ops-alerts-with-delivery",
        ).json()["alerts"]
        assert any(alert["delivery_status"] == "delivered" for alert in delivered_list_alerts), delivered_list_alerts

        ops_notification_service._post_webhook = fake_failed_post_webhook
        failed_ops_alert = _assert_ok(
            client.post(
                f"/api/v1/sources/ops/alerts/deliver?alert_id={ops_alerts[0]['id']}&force=true",
                headers=admin_headers,
            ),
            "sources/ops-alert-deliver-failure",
        ).json()
        assert failed_ops_alert["summary"]["failed"] == 1, failed_ops_alert
    finally:
        ops_notification_service.settings.OPS_NOTIFICATIONS_ENABLED = previous_ops_enabled
        ops_notification_service.settings.OPS_WEBHOOK_URL = previous_ops_url
        ops_notification_service.settings.OPS_WEBHOOK_SECRET = previous_ops_secret
        ops_notification_service._post_webhook = original_post_webhook

    acknowledged_ops_alert = _assert_ok(
        client.post(f"/api/v1/sources/ops/alerts/{ops_alerts[0]['id']}/acknowledge", headers=admin_headers),
        "sources/ops-alert-acknowledge",
    ).json()
    assert acknowledged_ops_alert["status"] == "acknowledged", acknowledged_ops_alert

    original_parse_rss_feed = source_sync_service.parse_rss_feed
    original_parse_homepage_feed_scheduler = source_sync_service.parse_homepage_feed
    previous_ops_enabled_scheduler = ops_notification_service.settings.OPS_NOTIFICATIONS_ENABLED
    previous_ops_url_scheduler = ops_notification_service.settings.OPS_WEBHOOK_URL
    previous_ops_secret_scheduler = ops_notification_service.settings.OPS_WEBHOOK_SECRET
    original_post_webhook_scheduler = ops_notification_service._post_webhook

    async def fake_parse_rss_feed(url: str, limit: int = 20):
        article_url = (
            "https://example.com/grid-resilience-plan"
            if url == "https://example.com/rss.xml"
            else f"{url.rstrip('/')}/grid-resilience-plan"
        )
        return {
            "url": url,
            "title": "Fixture RSS",
            "description": "Fixture feed for local smoke.",
            "items": [
                {
                    "external_id": f"source-item-{abs(hash(url))}",
                    "title": "Regulators publish grid resilience plan",
                    "summary": "A source item about grid resilience was ingested for Phase 2.",
                    "url": article_url,
                    "published_at": "2026-05-02T10:00:00+00:00",
                    "raw": {"fixture": True},
                }
            ],
        }

    try:
        source_sync_service.parse_rss_feed = fake_parse_rss_feed
        source_sync_service.parse_homepage_feed = fake_parse_homepage_feed
        ops_notification_service.settings.OPS_NOTIFICATIONS_ENABLED = True
        ops_notification_service.settings.OPS_WEBHOOK_URL = "https://hooks.example.test/parallax"
        ops_notification_service.settings.OPS_WEBHOOK_SECRET = "smoke-webhook-secret"
        ops_notification_service._post_webhook = fake_failed_post_webhook
        active_sync = _assert_ok(
            client.post(
                "/api/v1/sources/sync-active?source_limit=2&feed_limit=2&article_limit=2&card_limit=2",
                headers=admin_headers,
            ),
            "sources/sync-active",
        ).json()
        assert active_sync["source_count"] >= 1, active_sync
        assert active_sync["feed_count"] >= 1, active_sync
        assert active_sync["article_count"] >= 1, active_sync
        assert active_sync["card_count"] >= 1, active_sync
        assert active_sync["sync_run_id"], active_sync
        ingested = _assert_ok(
            client.post(f"/api/v1/sources/{source['id']}/sync", headers=admin_headers),
            "sources/sync-rss",
        ).json()
    finally:
        source_sync_service.parse_rss_feed = original_parse_rss_feed
        source_sync_service.parse_homepage_feed = original_parse_homepage_feed_scheduler
        ops_notification_service.settings.OPS_NOTIFICATIONS_ENABLED = previous_ops_enabled_scheduler
        ops_notification_service.settings.OPS_WEBHOOK_URL = previous_ops_url_scheduler
        ops_notification_service.settings.OPS_WEBHOOK_SECRET = previous_ops_secret_scheduler
        ops_notification_service._post_webhook = original_post_webhook_scheduler

    assert len(ingested["articles"]) == 1, ingested
    assert len(ingested["cards"]) == 1, ingested
    assert ingested["status"] == "completed", ingested
    assert ingested["sync_run_id"], ingested
    assert "ops_alert_delivery" in active_sync, active_sync
    ingested_article_id = ingested["articles"][0]["id"]

    sync_runs = _assert_ok(
        client.get("/api/v1/sources/sync-runs", headers=admin_headers),
        "sources/sync-runs",
    ).json()["sync_runs"]
    assert len(sync_runs) >= 3, sync_runs
    assert any(run["sync_scope"] == "active_sources" for run in sync_runs), sync_runs
    assert any(run["source_id"] == source["id"] for run in sync_runs), sync_runs
    denied_sync_runs = client.get("/api/v1/sources/sync-runs", headers=headers)
    assert denied_sync_runs.status_code == 403, denied_sync_runs.text
    source_sync_runs = _assert_ok(
        client.get(f"/api/v1/sources/{source['id']}/sync-runs", headers=admin_headers),
        "sources/source-sync-runs",
    ).json()["sync_runs"]
    assert source_sync_runs and source_sync_runs[0]["source_id"] == source["id"], source_sync_runs

    original_fetch_article = analyze_route.fetch_article
    original_ingested_fetch_article = ingested_analysis_service.fetch_article

    async def fake_fetch_article(url: str):
        raise ingested_analysis_service.ArticleFetchError("Fixture fetch failure for metadata fallback.")

    try:
        ingested_analysis_service.fetch_article = fake_fetch_article
        ingested_analysis = _assert_ok(
            client.post(
                "/api/v1/analyze",
                headers=headers,
                json={"ingested_article_id": ingested_article_id},
            ),
            "analyze/ingested-article",
        ).json()
    finally:
        ingested_analysis_service.fetch_article = original_ingested_fetch_article

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

    spanish_source = store.create_source_record(
        name="LatAm Energia",
        website_url="https://latam.example.com",
        country="Mexico",
        language="Spanish",
        region="Latin America",
        source_size="medium",
        source_type="newspaper",
    )
    spanish_feed = store.create_source_feed_record(
        source_id=spanish_source["id"],
        feed_url="https://latam.example.com/rss.xml",
        feed_type="rss",
        title="LatAm Energia RSS",
    )
    spanish_ingested = store.save_ingested_articles(
        spanish_source,
        spanish_feed,
        [
            {
                "external_id": "source-item-3",
                "title": "Reguladores publican plan de resiliencia de la red electrica",
                "summary": "El gobierno presenta un plan de resiliencia de la red electrica con controles de costos.",
                "url": "https://latam.example.com/energia/resiliencia-red-electrica",
                "published_at": "2026-05-02T12:00:00+00:00",
                "raw": {"fixture": True, "language": "Spanish"},
            }
        ],
        session_id=session_id,
    )
    assert spanish_ingested["articles"], spanish_ingested

    article_compare = _assert_ok(
        client.get(f"/api/v1/compare/{ingested_article_id}?limit=5", headers=headers),
        "compare/ingested-article",
    ).json()
    assert article_compare["base_article"]["id"] == ingested_article_id, article_compare
    assert article_compare["similar_articles"], article_compare
    assert article_compare["comparison"]["confidence"] > 0, article_compare
    assert "title_differences" in article_compare["comparison"], article_compare
    assert "missing_claims" in article_compare["comparison"], article_compare
    assert "added_claims" in article_compare["comparison"], article_compare
    assert "coverage_gaps" in article_compare["comparison"], article_compare
    assert "shared" in article_compare["entities"], article_compare
    assert article_compare["provider_metadata"]["status"] == "heuristic", article_compare
    assert store.ARTICLE_COMPARISONS, article_compare

    source_intelligence = _assert_ok(
        client.get(f"/api/v1/sources/{source['id']}/intelligence", headers=headers),
        "sources/intelligence",
    ).json()
    assert source_intelligence["sample"]["article_count"] >= 1, source_intelligence
    assert source_intelligence["provider_metadata"]["status"] == "heuristic", source_intelligence
    assert source_intelligence["snapshot"]["id"], source_intelligence
    denied_source_intelligence_refresh = client.post(
        f"/api/v1/sources/{source['id']}/intelligence/refresh",
        headers=headers,
    )
    assert denied_source_intelligence_refresh.status_code == 403, denied_source_intelligence_refresh.text
    refreshed_source_intelligence = _assert_ok(
        client.post(f"/api/v1/sources/{source['id']}/intelligence/refresh", headers=admin_headers),
        "sources/intelligence-refresh",
    ).json()
    assert refreshed_source_intelligence["snapshot"]["id"] != source_intelligence["snapshot"]["id"], refreshed_source_intelligence

    energy_topic = next(
        topic for topic in store.list_topics(session_id=session_id) if topic["name"] == "Energy transition"
    )
    topics_intelligence = _assert_ok(
        client.get("/api/v1/topics/intelligence", headers=headers),
        "topics/intelligence",
    ).json()
    assert topics_intelligence["summary"]["topic_count"] == 2, topics_intelligence
    assert topics_intelligence["items"], topics_intelligence
    topic_intelligence = _assert_ok(
        client.get(f"/api/v1/topics/{energy_topic['id']}/intelligence", headers=headers),
        "topics/topic-intelligence",
    ).json()
    assert topic_intelligence["subject"]["id"] == energy_topic["id"], topic_intelligence
    assert topic_intelligence["sample"]["article_count"] >= 1, topic_intelligence
    denied_topic_intelligence_refresh = client.post(
        f"/api/v1/topics/{energy_topic['id']}/intelligence/refresh",
        headers=headers,
    )
    assert denied_topic_intelligence_refresh.status_code == 403, denied_topic_intelligence_refresh.text
    denied_intelligence_refresh = client.post("/api/v1/intelligence/refresh", headers=headers)
    assert denied_intelligence_refresh.status_code == 403, denied_intelligence_refresh.text
    denied_pipeline = client.post("/api/v1/intelligence/pipeline/run", headers=headers)
    assert denied_pipeline.status_code == 403, denied_pipeline.text
    denied_pending_analysis = client.post("/api/v1/sources/articles/analyze-pending", headers=headers)
    assert denied_pending_analysis.status_code == 403, denied_pending_analysis.text

    original_pending_fetch_article = ingested_analysis_service.fetch_article
    try:
        ingested_analysis_service.fetch_article = fake_fetch_article
        pending_analysis = _assert_ok(
            client.post(
                f"/api/v1/sources/articles/analyze-pending?source_id={comparison_source['id']}&limit=2",
                headers=admin_headers,
            ),
            "sources/articles-analyze-pending",
        ).json()
    finally:
        ingested_analysis_service.fetch_article = original_pending_fetch_article

    assert pending_analysis["status"] == "completed", pending_analysis
    assert pending_analysis["candidate_count"] == 1, pending_analysis
    assert pending_analysis["analyzed_count"] == 1, pending_analysis
    assert pending_analysis["results"][0]["report_id"], pending_analysis

    batch_intelligence = _assert_ok(
        client.post(
            "/api/v1/intelligence/refresh?source_limit=2&topic_limit=2&article_limit=20&card_limit=6",
            headers=admin_headers,
        ),
        "intelligence/refresh",
    ).json()
    assert batch_intelligence["run"]["id"], batch_intelligence
    assert batch_intelligence["snapshot_count"] >= 2, batch_intelligence
    assert batch_intelligence["card_count"] >= 2, batch_intelligence
    assert store.INTELLIGENCE_REFRESH_RUNS, batch_intelligence
    intelligence_runs = _assert_ok(
        client.get("/api/v1/intelligence/runs", headers=admin_headers),
        "intelligence/runs",
    ).json()
    assert intelligence_runs["runs"][0]["id"] == batch_intelligence["run"]["id"], intelligence_runs
    denied_intelligence_runs = client.get("/api/v1/intelligence/runs", headers=headers)
    assert denied_intelligence_runs.status_code == 403, denied_intelligence_runs.text
    intelligence_feed = _assert_ok(
        client.get("/api/v1/feed?filter_type=narrative&limit=20", headers=headers),
        "feed/intelligence-cards",
    ).json()
    intelligence_card_types = {item["card_type"] for item in intelligence_feed["cards"]}
    assert {"source_pattern", "topic_shift"} & intelligence_card_types, intelligence_feed
    assert "recurring_claim" in intelligence_card_types, intelligence_feed

    denied_cluster_refresh = client.post("/api/v1/intelligence/clusters/refresh", headers=headers)
    assert denied_cluster_refresh.status_code == 403, denied_cluster_refresh.text
    cluster_refresh = _assert_ok(
        client.post(
            "/api/v1/intelligence/clusters/refresh?article_limit=50&cluster_limit=20&card_limit=10",
            headers=admin_headers,
        ),
        "intelligence/clusters-refresh",
    ).json()
    assert cluster_refresh["run"]["id"], cluster_refresh
    assert cluster_refresh["cluster_count"] >= 1, cluster_refresh
    assert cluster_refresh["article_count"] >= 2, cluster_refresh
    assert cluster_refresh["card_count"] >= 1, cluster_refresh
    assert cluster_refresh["run"]["summary"]["cross_language_cluster_count"] >= 1, cluster_refresh
    assert cluster_refresh["run"]["summary"]["coverage_gap_task_count"] >= 1, cluster_refresh
    assert cluster_refresh["run"]["summary"]["suggested_source_search_count"] >= 1, cluster_refresh
    cross_language_clusters = [
        cluster
        for cluster in cluster_refresh["clusters"]
        if ((cluster["provider_metadata"].get("source_diversity") or {}).get("cross_language"))
    ]
    assert cross_language_clusters, cluster_refresh
    assert cross_language_clusters[0]["provider_metadata"]["language_bridge_terms"], cross_language_clusters
    assert cross_language_clusters[0]["provider_metadata"]["automation"]["coverage_gap_tasks"], cross_language_clusters
    assert cross_language_clusters[0]["provider_metadata"]["automation"]["suggested_source_searches"], cross_language_clusters
    assert store.EVENT_CLUSTERS and store.EVENT_CLUSTER_ARTICLES, cluster_refresh
    cluster_runs = _assert_ok(
        client.get("/api/v1/intelligence/clusters/runs", headers=admin_headers),
        "intelligence/clusters-runs",
    ).json()
    assert cluster_runs["runs"][0]["id"] == cluster_refresh["run"]["id"], cluster_runs
    denied_cluster_runs = client.get("/api/v1/intelligence/clusters/runs", headers=headers)
    assert denied_cluster_runs.status_code == 403, denied_cluster_runs.text
    clusters = _assert_ok(
        client.get("/api/v1/intelligence/clusters?limit=10", headers=headers),
        "intelligence/clusters",
    ).json()
    assert clusters["items"], clusters
    assert clusters["items"][0]["provider_metadata"]["cluster_quality"]["quality_score"] > 0, clusters
    topic_clusters = _assert_ok(
        client.get(f"/api/v1/intelligence/clusters?topic_id={energy_topic['id']}&limit=10", headers=headers),
        "intelligence/clusters-topic",
    ).json()
    assert topic_clusters["items"], topic_clusters
    cluster_detail = _assert_ok(
        client.get(f"/api/v1/intelligence/clusters/{clusters['items'][0]['id']}", headers=headers),
        "intelligence/cluster-detail",
    ).json()
    assert cluster_detail["cluster"]["id"] == clusters["items"][0]["id"], cluster_detail
    assert cluster_detail["articles"], cluster_detail
    assert "coverage_gap_tasks" in cluster_detail, cluster_detail
    clustered_compare = _assert_ok(
        client.get(f"/api/v1/compare/{ingested_article_id}?limit=5", headers=headers),
        "compare/ingested-article-clustered",
    ).json()
    assert clustered_compare["event_cluster"]["id"], clustered_compare
    assert clustered_compare["event_cluster"]["cluster_quality"]["quality_score"] > 0, clustered_compare
    assert clustered_compare["event_cluster"]["coverage_gap_tasks"], clustered_compare
    cluster_feed = _assert_ok(
        client.get("/api/v1/feed?filter_type=narrative&limit=30", headers=headers),
        "feed/event-cluster-cards",
    ).json()
    cluster_card_types = {item["card_type"] for item in cluster_feed["cards"]}
    assert "cross_language_cluster" in cluster_card_types, cluster_feed
    assert "coverage_gap" in cluster_card_types, cluster_feed

    pipeline_article = ExtractedArticle(
        url="https://example.com/pipeline-grid-resilience-follow-up",
        final_url="https://example.com/pipeline-grid-resilience-follow-up",
        title="Pipeline detects grid resilience follow-up",
        source="Example Daily",
        domain="example.com",
        text="Officials reported a grid resilience follow-up because regulators announced cost controls. " * 30,
        excerpt="Officials reported a grid resilience follow-up because regulators announced cost controls.",
    )

    async def fake_pipeline_fetch_article(url: str):
        return pipeline_article

    original_pipeline_parse_rss_feed = source_sync_service.parse_rss_feed
    original_pipeline_parse_homepage_feed = source_sync_service.parse_homepage_feed
    original_pipeline_fetch_article = ingested_analysis_service.fetch_article

    async def fake_pipeline_parse_rss_feed(url: str, limit: int = 20):
        return {
            "url": url,
            "title": "Pipeline Fixture RSS",
            "description": "Fixture feed for pipeline automation.",
            "items": [
                {
                    "external_id": f"pipeline-item-{abs(hash(url))}",
                    "title": "Pipeline detects grid resilience follow-up",
                    "summary": "Pipeline automation ingests a related grid resilience follow-up item.",
                    "url": f"{url.rstrip('/')}/pipeline-grid-resilience-follow-up",
                    "published_at": "2026-05-02T12:30:00+00:00",
                    "raw": {"fixture": True, "pipeline": True},
                }
            ],
        }

    try:
        source_sync_service.parse_rss_feed = fake_pipeline_parse_rss_feed
        source_sync_service.parse_homepage_feed = fake_parse_homepage_feed
        ingested_analysis_service.fetch_article = fake_pipeline_fetch_article
        pipeline = _assert_ok(
            client.post(
                "/api/v1/intelligence/pipeline/run?source_limit=2&feed_limit=2&sync_article_limit=1&sync_card_limit=2&analysis_article_limit=4&intelligence_source_limit=2&topic_limit=2&intelligence_article_limit=20&intelligence_card_limit=4&cluster_article_limit=50&cluster_limit=20&cluster_card_limit=6",
                headers=admin_headers,
            ),
            "intelligence/pipeline-run",
        ).json()
    finally:
        source_sync_service.parse_rss_feed = original_pipeline_parse_rss_feed
        source_sync_service.parse_homepage_feed = original_pipeline_parse_homepage_feed
        ingested_analysis_service.fetch_article = original_pipeline_fetch_article

    assert pipeline["pipeline_id"], pipeline
    assert pipeline["status"] in {"completed", "partial"}, pipeline
    phase_names = {phase["name"] for phase in pipeline["phases"]}
    assert {"source_sync", "article_analysis", "intelligence_refresh", "event_clusters"} <= phase_names, pipeline
    assert pipeline["summary"]["analyzed_article_count"] >= 1, pipeline
    assert pipeline["summary"]["feed_card_count"] >= 1, pipeline
    assert pipeline["summary"]["cluster_count"] >= 1, pipeline

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
    assert url_analysis["intelligence"]["provider_metadata"]["status"] == "heuristic", url_analysis
    assert url_analysis["card"]["analysis"]["provider_metadata"]["status"] == "heuristic", url_analysis
    store.update_report_saved(card["report_id"], session_id=session_id, is_saved=True)

    previous_ai_provider = analysis_service.settings.AI_PROVIDER
    previous_openai_key = analysis_service.settings.OPENAI_API_KEY
    previous_openai_model = analysis_service.settings.OPENAI_MODEL
    original_openai_analysis = analysis_service._openai_analysis
    original_openai_json = intelligence_service.openai_json

    async def fake_openai_analysis(article: ExtractedArticle):
        return {
            "title": "Model-backed energy market analysis",
            "source": article.source,
            "url": article.final_url,
            "summary": "Model-backed summary focused on market framing and source context.",
            "key_claims": [
                "Officials linked grid costs to market-price changes.",
                "The article frames regulatory action as the primary response.",
            ],
            "narrative_framing": ["economic_consequence", "institutional_response"],
            "entities": ["Energy Market Agency", "Grid Council"],
            "topics": ["energy market", "grid costs"],
            "confidence": 0.82,
            "priority": "high",
            "card_type": "article",
        }

    async def fake_openai_json(**kwargs):
        task = kwargs.get("task")
        if task == "article_intelligence":
            return {
                "summary": "Provider-enhanced intelligence summary for source comparison.",
                "key_claims": [
                    "Officials linked grid costs to market-price changes.",
                    "The source emphasizes institutional response.",
                ],
                "entities": {
                    "people": [],
                    "organizations": ["Energy Market Agency", "Grid Council"],
                    "locations": [],
                    "events": ["Grid resilience plan"],
                },
                "narrative": {
                    "main_frame": "institutional_response",
                    "secondary_frames": ["economic_consequence"],
                    "tone": "analytical",
                    "implied_causality": "Market-price changes are presented as driving grid-cost concerns.",
                    "missing_context": ["Independent price data is not provided in the extracted article."],
                },
                "source_analysis": {
                    "source_profile": "Model-enhanced source context.",
                    "known_angle": "Institutional and market framing.",
                    "reliability_notes": "Provider output describes framing, not truth.",
                    "limitations": ["Needs cross-source comparison."],
                },
                "comparison_hooks": {
                    "search_queries": ["grid costs market prices official response"],
                    "similarity_keywords": ["grid", "costs", "market", "regulator"],
                    "event_fingerprint": "grid-costs-market-response",
                },
                "scores": {
                    "importance_score": 0.81,
                    "confidence_score": 0.82,
                    "controversy_score": 0.52,
                    "cross_source_need": 0.77,
                },
                "node_perspectives": {
                    "article": {
                        "summary": "Provider article-node perspective.",
                        "signals": ["Model-backed article signal"],
                        "limitations": ["Still not a truth verdict."],
                    },
                    "source": {
                        "summary": "Provider source-node perspective.",
                        "signals": ["Model-backed source signal"],
                        "limitations": ["Needs source history."],
                    },
                    "claim": {
                        "summary": "Provider claim-node perspective.",
                        "signals": ["Model-backed claim signal"],
                        "limitations": ["Needs evidence checks."],
                    },
                },
            }
        if task == "cross_source_compare":
            return {
                "comparison": {
                    "shared_claims": [
                        {
                            "base_claim": "Officials linked grid costs to market-price changes.",
                            "comparison_claim": "Regional coverage cites cost questions around the plan.",
                            "similarity": 0.64,
                        }
                    ],
                    "unique_claims_by_source": [
                        {"source": "Example Daily", "claims": ["Institutional response is emphasized."]},
                        {"source": "Regional Grid Monitor", "claims": ["Cost questions are emphasized."]},
                    ],
                    "framing_differences": [
                        {
                            "comparison_source": "Regional Grid Monitor",
                            "shared_frames": ["institutional_response"],
                            "base_only": ["economic_consequence"],
                            "comparison_only": ["accountability"],
                        }
                    ],
                    "tone_differences": [{"base_tone": "analytical", "comparison_tone": "skeptical"}],
                    "missing_context": ["Neither article independently verifies cost projections."],
                    "timeline_difference": [],
                    "source_difference": [],
                    "confidence": 0.74,
                }
            }
        if task == "osint_context_synthesis":
            return {
                "risks": ["Provider-highlighted OSINT risk: references are contextual leads only."],
                "contradictions": [
                    {
                        "type": "provider_context",
                        "description": "Provider noted a need to check official data before interpretation.",
                        "items": [],
                    }
                ],
                "relevance": {
                    "overall": 0.73,
                    "basis": ["Provider weighted source article, archive, and public-search probes."],
                },
                "limitations": ["Provider synthesis does not validate claims."],
            }
        if task == "source_intelligence_aggregation":
            return {
                "summary": "Provider-enhanced source aggregation focused on repeated grid-resilience framing.",
                "framing_pattern": {
                    "dominant_frames": ["institutional_response", "economic_consequence"],
                    "frame_distribution": [
                        {"label": "institutional_response", "count": 1, "share": 1.0}
                    ],
                },
                "recurring_claims": ["Officials linked grid resilience to market and cost pressures."],
                "entity_pattern": {
                    "people": [],
                    "organizations": ["Energy Market Agency"],
                    "locations": [],
                    "events": ["Grid resilience plan"],
                },
                "tone_pattern": {
                    "dominant_tones": ["analytical"],
                    "tone_distribution": [{"label": "analytical", "count": 1, "share": 1.0}],
                },
                "coverage_cadence": {},
                "source_diversity": {},
                "disagreement_zones": ["Provider says compare against regional coverage before inference."],
                "weak_spots": ["Provider says the source sample remains small."],
                "limitations": ["Provider aggregation is contextual and not a factual verdict."],
            }
        if task == "topic_intelligence_aggregation":
            return {
                "summary": "Provider-enhanced topic aggregation for energy-transition grid coverage.",
                "framing_pattern": {
                    "dominant_frames": ["institutional_response"],
                    "frame_distribution": [
                        {"label": "institutional_response", "count": 1, "share": 1.0}
                    ],
                },
                "recurring_claims": ["Grid resilience coverage links policy response with cost concerns."],
                "entity_pattern": {
                    "people": [],
                    "organizations": ["Grid Council"],
                    "locations": [],
                    "events": ["Grid resilience plan"],
                },
                "tone_pattern": {
                    "dominant_tones": ["analytical"],
                    "tone_distribution": [{"label": "analytical", "count": 1, "share": 1.0}],
                },
                "coverage_cadence": {},
                "source_diversity": {},
                "disagreement_zones": ["Provider says source-level comparison should be checked."],
                "weak_spots": ["Provider says the topic sample is still thin."],
                "limitations": ["Provider topic aggregation remains contextual."],
            }
        return {}

    try:
        analysis_service.settings.AI_PROVIDER = "openai"
        analysis_service.settings.OPENAI_API_KEY = "smoke-openai-key"
        analysis_service.settings.OPENAI_MODEL = "smoke-model"
        analysis_service._openai_analysis = fake_openai_analysis
        intelligence_service.openai_json = fake_openai_json
        analyze_route.fetch_article = fake_success_fetch_article
        model_url_analysis = _assert_ok(
            client.post(
                "/api/v1/analyze",
                headers=headers,
                json={"url": "https://example.com/model-analysis"},
            ),
            "analyze/openai-provider",
        ).json()
        model_compare = _assert_ok(
            client.get(f"/api/v1/compare/{ingested_article_id}?limit=5", headers=headers),
            "compare/openai-provider",
        ).json()
        model_osint = _assert_ok(
            client.get(f"/api/v1/sources/articles/{ingested_article_id}/osint", headers=headers),
            "sources/article-osint-openai-provider",
        ).json()
        model_source_intelligence = _assert_ok(
            client.post(
                f"/api/v1/sources/{source['id']}/intelligence/refresh",
                headers=admin_headers,
            ),
            "sources/intelligence-openai-provider",
        ).json()
        model_topic_intelligence = _assert_ok(
            client.post(
                f"/api/v1/topics/{energy_topic['id']}/intelligence/refresh",
                headers=admin_headers,
            ),
            "topics/intelligence-openai-provider",
        ).json()
    finally:
        analysis_service.settings.AI_PROVIDER = previous_ai_provider
        analysis_service.settings.OPENAI_API_KEY = previous_openai_key
        analysis_service.settings.OPENAI_MODEL = previous_openai_model
        analysis_service._openai_analysis = original_openai_analysis
        intelligence_service.openai_json = original_openai_json
        analyze_route.fetch_article = original_fetch_article

    assert model_url_analysis["card"]["analysis"]["provider_metadata"]["status"] == "model", model_url_analysis
    assert model_url_analysis["intelligence"]["provider_metadata"]["status"] == "model", model_url_analysis
    assert model_url_analysis["intelligence"]["node_perspectives"]["article"]["summary"], model_url_analysis
    assert model_compare["provider_metadata"]["status"] == "model", model_compare
    assert model_compare["comparison"]["confidence"] == 0.74, model_compare
    assert model_osint["provider_metadata"]["status"] == "model", model_osint
    assert "Provider-highlighted OSINT risk" in model_osint["risks"][0], model_osint
    assert model_source_intelligence["provider_metadata"]["status"] == "model", model_source_intelligence
    assert "Provider-enhanced source aggregation" in model_source_intelligence["summary"], model_source_intelligence
    assert model_topic_intelligence["provider_metadata"]["status"] == "model", model_topic_intelligence
    assert "Provider-enhanced topic aggregation" in model_topic_intelligence["summary"], model_topic_intelligence

    feed = _assert_ok(client.get("/api/v1/feed?limit=100", headers=headers), "feed").json()
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
    assert any(item["is_default"] for item in phase2_sources), phase2_sources
    source_record = next(item for item in phase2_sources if item["id"] == source["id"])
    assert source_record["health"]["status"] == "healthy", source_record
    assert source_record["health"]["articles_24h"] >= 1, source_record
    assert source_record["review_status"] == "reviewed", source_record
    assert source_record["quality_score"] >= 0, source_record
    source_detail = _assert_ok(client.get(f"/api/v1/sources/{source['id']}"), "sources/detail").json()
    assert source_detail["source"]["article_count"] >= 2, source_detail
    assert source_detail["health"]["status"] == "healthy", source_detail
    assert source_detail["quality"]["quality_score"] >= 0.55, source_detail
    assert source_detail["sync_runs"], source_detail
    assert any(feed["feed_type"] == "homepage" for feed in source_detail["feeds"]), source_detail
    assert any(feed["feed_type"] == "rss" for feed in source_detail["feeds"]), source_detail
    source_articles = _assert_ok(
        client.get(f"/api/v1/sources/{source['id']}/articles"),
        "sources/articles",
    ).json()["articles"]
    assert source_articles and any(article["id"] == ingested_article_id for article in source_articles), source_articles
    analyzed_source_article = next(article for article in source_articles if article["id"] == ingested_article_id)
    assert analyzed_source_article["analysis_status"] == "analyzed", source_articles
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
    assert article_detail_payload["osint_context"]["status"] == "bounded_context_ready", article_detail_payload
    assert article_detail_payload["osint_context"]["discovered_references"], article_detail_payload
    assert article_detail_payload["osint_context"]["risks"], article_detail_payload
    article_nodes = _assert_ok(
        client.get(f"/api/v1/sources/articles/{ingested_article_id}/nodes?node_type=author", headers=headers),
        "sources/article-nodes",
    ).json()
    assert article_nodes["selected_node"]["node_type"] == "author", article_nodes
    assert article_nodes["selected_perspective"]["question"], article_nodes
    assert store.NODES and store.NODE_EDGES, article_nodes
    osint_context = _assert_ok(
        client.get(f"/api/v1/sources/articles/{ingested_article_id}/osint", headers=headers),
        "sources/article-osint",
    ).json()
    assert osint_context["article_id"] == ingested_article_id, osint_context
    assert "source_article" in osint_context["source_type"], osint_context
    assert osint_context["citations"], osint_context
    previous_external = osint_service.settings.EXTERNAL_RETRIEVAL_ENABLED
    previous_provider = osint_service.settings.RETRIEVAL_PROVIDER
    try:
        osint_service.settings.EXTERNAL_RETRIEVAL_ENABLED = True
        osint_service.settings.RETRIEVAL_PROVIDER = "mock"
        external_osint_context = _assert_ok(
            client.get(
                f"/api/v1/sources/articles/{ingested_article_id}/osint?include_external=true&limit=2",
                headers=headers,
            ),
            "sources/article-osint-external-mock",
        ).json()
    finally:
        osint_service.settings.EXTERNAL_RETRIEVAL_ENABLED = previous_external
        osint_service.settings.RETRIEVAL_PROVIDER = previous_provider
    assert external_osint_context["retrieval_mode"]["external_results_included"] >= 1, external_osint_context
    assert "mock_public_search_result" in external_osint_context["source_type"], external_osint_context

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

    manual_source_payload = _assert_ok(
        client.post(
            "/api/v1/sources",
            json={
                "name": "Manual Desk",
                "website_url": "https://manual.example.com",
                "country": "Germany",
                "language": "English",
                "source_type": "independent",
                "feed_type": "manual",
            },
        ),
        "sources/create-manual",
    ).json()
    manual_source = manual_source_payload["source"]
    assert manual_source_payload["feed"]["feed_type"] == "manual", manual_source_payload
    manual_sync = _assert_ok(
        client.post(f"/api/v1/sources/{manual_source['id']}/sync", headers=admin_headers),
        "sources/sync-manual",
    ).json()
    assert manual_sync["status"] == "skipped", manual_sync
    assert manual_sync["feed_count"] == 1, manual_sync
    assert manual_sync["syncable_feed_count"] == 0, manual_sync
    assert manual_sync["manual_feed_count"] == 1, manual_sync
    assert manual_sync["skipped_feed_count"] == 1, manual_sync
    assert manual_sync["skipped_feeds"][0]["feed_type"] == "manual", manual_sync

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
