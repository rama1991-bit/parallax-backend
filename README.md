# Parallax Backend

FastAPI backend for Parallax / Narrative Intelligence.

This deployable MVP includes:
- smart feed cards
- live article fetch and structured narrative analysis
- compare coverage
- topic monitors
- RSS feed subscriptions
- session-scoped alerts
- saved reports
- public briefs with signed share tokens
- source intelligence profiles
- onboarding setup flow
- Phase 2 source records, source feeds, RSS/homepage sync, manual source entries, ingested-article feed cards, and pending article analysis automation
- Phase 2 node-based article detail, article-id comparisons, bounded OSINT context, and default source seeds
- source ingestion observability with sync-run history and source health summaries
- source quality governance with review statuses, quality scores, and feed quarantine controls
- source operational alerts, webhook delivery attempts, and env-driven OSINT retrieval providers
- provider-backed article intelligence, cross-source compare enhancement, node perspective hints, and OSINT synthesis when `AI_PROVIDER=openai`
- source/topic intelligence aggregation snapshots, refresh runs, and feed cards for source patterns, topic shifts, recurring claims, and coverage gaps
- event clustering for source/topic/node intelligence, with event, cross-language, source-divergence, missing-coverage feed cards, cluster quality, language-bridge terms, coverage-gap automation tasks, and source candidate drafts
- a scheduler-friendly intelligence pipeline that runs ingestion, source/topic snapshots, and event clustering as one admin job

Run locally:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Health:
```bash
curl http://localhost:8000/api/v1/health
curl http://localhost:8000/api/v1/health/db
curl http://localhost:8000/api/v1/auth/me
curl http://localhost:8000/api/v1/saved-reports
```

Repeatable smoke:

```bash
python scripts/smoke_local.py
```

By default the smoke script forces the in-memory store so it is safe before deploys. To run it against the configured Postgres/Supabase database:

```bash
PARALLAX_SMOKE_USE_CONFIG_DB=true python scripts/smoke_local.py
```

Read-only live smoke against a deployed backend:

```bash
PARALLAX_LIVE_API_URL=https://your-backend.example.com python scripts/smoke_live.py
```

Deploy readiness check:

```bash
python scripts/check_deploy_readiness.py
python scripts/check_deploy_readiness.py --json
python scripts/check_deploy_readiness.py --strict
```

The readiness script validates production-critical configuration without opening a database connection. It checks production env posture, signed-token/admin secrets, Postgres/Supabase settings, SSL mode, frontend/CORS origins, AI provider settings, quota/fetch limits, OSINT retrieval posture, ops notification posture, and prints the recommended recurring source-ingestion scheduler command.

Analyze flow:

```bash
curl -X POST http://localhost:8000/api/v1/analyze \
  -H "Content-Type: application/json" \
  -d "{\"url\":\"https://example.com/article\"}"

curl -X POST http://localhost:8000/api/v1/analyze \
  -H "Content-Type: application/json" \
  -d "{\"ingested_article_id\":\"<ingested_article_id>\"}"
```

The endpoint fetches and extracts article text, produces structured narrative analysis, saves a feed card when `DATABASE_URL` points at Postgres/Supabase, and returns the normalized card for the frontend to insert into the feed. When called with `ingested_article_id`, the analysis is linked back to `ingested_articles.article_analysis_id`, marks the article as `analyzed`, enriches the existing feed card, and returns a Phase 2 `intelligence` object with article, claims, entities, narrative, source analysis, comparison hooks, scores, provider metadata, and optional node perspective hints. With `AI_PROVIDER=openai`, the backend asks the configured model for structured JSON and falls back to deterministic heuristics if provider delivery fails.

Compare flow:

```bash
curl -X POST http://localhost:8000/api/v1/compare \
  -H "Content-Type: application/json" \
  -d "{\"left_url\":\"https://example.com/a\",\"right_url\":\"https://example.com/b\"}"

curl http://localhost:8000/api/v1/compare/<ingested_article_id>
```

URL compare fetches, analyzes, and saves both articles for the current session, then returns claim overlap and framing divergence signals. Article-id compare uses persisted `ingested_articles` to find similar coverage across sources, returns the Phase 2 `base_article`, `similar_articles`, title differences, missing and added claims, coverage gaps, entity overlap, and the structured `comparison` object, and persists lightweight `article_comparisons` rows for matching pairs. When model-backed analysis is enabled, the comparison object is provider-enhanced while preserving the same response shape and fallback behavior.

Topic monitor flow:

```bash
curl -X POST http://localhost:8000/api/v1/topics/monitor \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"AI regulation\",\"keywords\":[\"AI Act\",\"model safety\"]}"
```

Topic monitors are session-scoped and create a feed card confirming the subscription.

RSS/feed subscription flow:

```bash
curl -X POST http://localhost:8000/api/v1/feeds \
  -H "Content-Type: application/json" \
  -d "{\"url\":\"https://example.com/rss.xml\"}"

curl -X POST http://localhost:8000/api/v1/feeds/<feed_id>/sync
```

Feed sync parses RSS/Atom items, stores new items, and creates lightweight `feed_item` cards for the smart feed.

Phase 2 source ingestion flow:

```bash
curl -X POST http://localhost:8000/api/v1/sources \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"Example Daily\",\"website_url\":\"https://example.com\",\"rss_url\":\"https://example.com/rss.xml\",\"country\":\"United States\",\"language\":\"English\",\"source_size\":\"medium\",\"source_type\":\"newspaper\"}"

curl -X POST http://localhost:8000/api/v1/sources/<source_id>/sync \
  -H "X-Parallax-Admin-Key: <admin_api_key>"

curl http://localhost:8000/api/v1/sources/defaults/preview
curl -X POST http://localhost:8000/api/v1/sources/defaults/seed \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl -X POST http://localhost:8000/api/v1/sources/sync-active \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl http://localhost:8000/api/v1/sources/sync-runs \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl http://localhost:8000/api/v1/sources/needs-review \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl http://localhost:8000/api/v1/sources/ops/alerts \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl -X POST http://localhost:8000/api/v1/sources/ops/alerts/evaluate \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl -X POST http://localhost:8000/api/v1/sources/ops/alerts/deliver \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl http://localhost:8000/api/v1/sources/ops/alerts/deliveries \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl -X POST http://localhost:8000/api/v1/sources/ops/alerts/<alert_id>/acknowledge \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl -X POST http://localhost:8000/api/v1/sources/<source_id>/review \
  -H "Content-Type: application/json" \
  -H "X-Parallax-Admin-Key: <admin_api_key>" \
  -d "{\"review_status\":\"reviewed\",\"review_notes\":\"Metadata and feed terms reviewed.\",\"terms_reviewed\":true}"
curl -X POST http://localhost:8000/api/v1/sources/<source_id>/quality/recalculate \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl -X POST http://localhost:8000/api/v1/sources/feeds/<feed_id>/status \
  -H "Content-Type: application/json" \
  -H "X-Parallax-Admin-Key: <admin_api_key>" \
  -d "{\"status\":\"quarantined\",\"disabled_reason\":\"Feed returned repeated bad data.\"}"
python scripts/seed_default_sources.py --preview
python scripts/seed_default_sources.py
python scripts/sync_active_sources.py --source-limit 50 --feed-limit 100 --article-limit 10 --card-limit 25

curl http://localhost:8000/api/v1/sources/<source_id>/articles
curl http://localhost:8000/api/v1/sources/<source_id>/sync-runs \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl http://localhost:8000/api/v1/sources/articles/<ingested_article_id>
curl http://localhost:8000/api/v1/sources/articles/<ingested_article_id>/nodes
curl http://localhost:8000/api/v1/sources/articles/<ingested_article_id>/osint
curl "http://localhost:8000/api/v1/sources/articles/<ingested_article_id>/osint?include_external=true&limit=5"
curl http://localhost:8000/api/v1/sources/<source_id>/intelligence
curl -X POST http://localhost:8000/api/v1/sources/<source_id>/intelligence/refresh \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl http://localhost:8000/api/v1/topics/intelligence
curl http://localhost:8000/api/v1/topics/<topic_id>/intelligence
curl -X POST http://localhost:8000/api/v1/topics/<topic_id>/intelligence/refresh \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl -X POST "http://localhost:8000/api/v1/intelligence/refresh?source_limit=50&topic_limit=50&article_limit=100&card_limit=50" \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl http://localhost:8000/api/v1/intelligence/runs \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
python scripts/refresh_intelligence_snapshots.py --source-limit 50 --topic-limit 50 --article-limit 100 --card-limit 50
curl -X POST "http://localhost:8000/api/v1/intelligence/clusters/refresh?article_limit=250&cluster_limit=100&card_limit=50" \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl http://localhost:8000/api/v1/intelligence/clusters
curl "http://localhost:8000/api/v1/intelligence/clusters?topic_id=<topic_id>"
curl http://localhost:8000/api/v1/intelligence/clusters/<cluster_id>
curl http://localhost:8000/api/v1/intelligence/clusters/<cluster_id>/source-drafts \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
curl http://localhost:8000/api/v1/intelligence/clusters/runs \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
python scripts/refresh_event_clusters.py --article-limit 250 --cluster-limit 100 --card-limit 50
curl -X POST "http://localhost:8000/api/v1/intelligence/pipeline/run?source_limit=50&feed_limit=100&sync_article_limit=10&sync_card_limit=25&analysis_article_limit=25&intelligence_source_limit=50&topic_limit=50&intelligence_article_limit=100&intelligence_card_limit=50&cluster_article_limit=250&cluster_limit=100&cluster_card_limit=50" \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
python scripts/run_intelligence_pipeline.py --source-limit 50 --feed-limit 100 --sync-article-limit 10 --sync-card-limit 25 --analysis-article-limit 25 --intelligence-source-limit 50 --topic-limit 50 --intelligence-article-limit 100 --intelligence-card-limit 50 --cluster-article-limit 250 --cluster-limit 100 --cluster-card-limit 50
```

Source sync parses active RSS feeds and news homepages into `ingested_articles` and creates lightweight `ingested_article` cards for the smart feed. Homepage sync uses bounded same-domain link discovery with article-like URL heuristics; manual feeds are preserved for provenance and explicitly skipped by automated fetching. `POST /api/v1/sources/articles/analyze-pending` and `scripts/analyze_pending_articles.py` batch-analyze pending ingested articles, enrich their existing feed cards, persist structured intelligence, and rely on metadata fallback when full article fetch is unavailable. Default source seeding is idempotent and creates multilingual source records plus RSS feed records where a public feed is known. Seed, sync, sync-run history, review, quality, feed governance, pending-analysis, operational alert, alert-delivery, and intelligence-refresh API routes require `X-Parallax-Admin-Key`; direct scripts use backend environment access instead. `sync-active` and `scripts/sync_active_sources.py` provide the scheduler-friendly ingestion runner with source/feed/article/card limits, per-feed error isolation, JSON scheduler output, `source_sync_runs` logging, source health summaries, source operational alert evaluation, and best-effort alert delivery summaries. `scripts/refresh_intelligence_snapshots.py` and `POST /api/v1/intelligence/refresh` refresh source/topic aggregation snapshots from ingested articles, record `intelligence_refresh_runs`, and create feed cards with card types `source_pattern`, `topic_shift`, `recurring_claim`, and `coverage_gap`. Source and topic pages use the same snapshots for source diversity, recurring claims, framing patterns, tone patterns, weak spots, and provider metadata without recomputing every request. Alert delivery posts active alerts at or above `OPS_NOTIFICATION_MIN_SEVERITY` to `OPS_WEBHOOK_URL`, records each attempt in `source_ops_alert_deliveries`, signs payloads with `X-Parallax-Signature` when `OPS_WEBHOOK_SECRET` is set, and never fails the scheduler when a webhook is down. Source list/detail responses include health fields such as status, last success, last error, success rate, articles in the last 24 hours, review status, and quality score. Feed statuses can be `active`, `paused`, `quarantined`, or `disabled`; only active RSS and homepage feeds are fetched. Source review statuses can be `needs_review`, `reviewed`, `quarantined`, or `disabled`; quarantined/disabled sources are skipped by ingestion. Article detail returns the article, source, source feed, analysis, intelligence payload, hydrated feed card, comparison hooks, node preview, materialized `node_graph`, and bounded `osint_context`. The nodes endpoint returns article, source, author, topic, event/background, claim, narrative, and entity perspectives with edges; model-backed article analysis can add node perspective hints while retaining heuristic fallbacks. The OSINT endpoint returns contextual references, source types, reliability levels, relevance, risks, contradictions, and citations, and can synthesize risks/contradictions with the configured model without treating OSINT as truth. OSINT external retrieval is provider-based: `RETRIEVAL_PROVIDER=mock` produces deterministic probes, while `RETRIEVAL_PROVIDER=web` fetches public web search results when `EXTERNAL_RETRIEVAL_ENABLED=true`.

`scripts/refresh_event_clusters.py` and `POST /api/v1/intelligence/clusters/refresh` rebuild deterministic event clusters from ingested article metadata, saved analysis, comparison hooks, entities, claims, frames, URLs, event fingerprints, and bounded cross-language alias terms. The cluster layer persists `event_clusters`, `event_cluster_articles`, and `event_cluster_refresh_runs`; compare responses include `event_cluster` when the selected article belongs to one; topic detail can request topic-matched clusters; feed cards surface `event_cluster`, `cross_language_cluster`, `source_divergence`, `coverage_gap`, and `missing_coverage` signals. Cluster provider metadata includes quality score, average similarity, strategy counts, source/language diversity, language bridge terms, coverage-gap tasks, suggested source searches, source candidate drafts, and recommended actions. `GET /api/v1/intelligence/clusters/<cluster_id>/source-drafts` returns admin-gated source-manager prefill drafts generated from those gaps. Clusters are retrieval context, not truth judgments.

`scripts/run_intelligence_pipeline.py` and `POST /api/v1/intelligence/pipeline/run` execute the operational sequence in one call: active source ingestion, pending article analysis, source/topic intelligence snapshot refresh, and event cluster refresh. The response includes one JSON `pipeline_id`, phase summaries, run IDs from each existing subsystem, limits, errors, and limitations. It does not add a separate persistence table; the durable records remain `source_sync_runs`, ingested article analysis records, `intelligence_refresh_runs`, and `event_cluster_refresh_runs`.

Recurring ingestion scheduler:

```bash
*/15 * * * * cd /app && python scripts/sync_active_sources.py --source-limit 50 --feed-limit 100 --article-limit 10 --card-limit 25
*/20 * * * * cd /app && python scripts/analyze_pending_articles.py --limit 25
*/30 * * * * cd /app && python scripts/refresh_intelligence_snapshots.py --source-limit 50 --topic-limit 50 --article-limit 100 --card-limit 50
*/30 * * * * cd /app && python scripts/refresh_event_clusters.py --article-limit 250 --cluster-limit 100 --card-limit 50
*/30 * * * * cd /app && python scripts/run_intelligence_pipeline.py --source-limit 50 --feed-limit 100 --sync-article-limit 10 --sync-card-limit 25 --analysis-article-limit 25 --intelligence-source-limit 50 --topic-limit 50 --intelligence-article-limit 100 --intelligence-card-limit 50 --cluster-article-limit 250 --cluster-limit 100 --cluster-card-limit 50
```

For platforms with scheduled jobs, either run the separate jobs (`sync_active_sources.py`, `analyze_pending_articles.py`, `refresh_intelligence_snapshots.py`, `refresh_event_clusters.py`) or run the single pipeline job every 15-60 minutes, depending on feed volume. For platforms that prefer HTTP jobs, call `POST /api/v1/sources/sync-active`, `POST /api/v1/sources/articles/analyze-pending`, `POST /api/v1/intelligence/refresh`, `POST /api/v1/intelligence/clusters/refresh`, or the combined `POST /api/v1/intelligence/pipeline/run` with `X-Parallax-Admin-Key`. Keep the batch limits in place until the source database has been reviewed for feed quality and traffic behavior.

Scheduler templates live in `deploy/scheduler/` for crontab and HTTP-triggered GitHub Actions style schedulers.

Onboarding flow:

```bash
curl -X POST http://localhost:8000/api/v1/onboarding \
  -H "Content-Type: application/json" \
  -d "{\"topics\":[{\"name\":\"AI regulation\",\"keywords\":[\"model safety\"]}],\"feeds\":[{\"url\":\"https://example.com/rss.xml\"}]}"
```

Sources, alerts, and briefs:

```bash
curl http://localhost:8000/api/v1/authors
curl http://localhost:8000/api/v1/alerts
curl http://localhost:8000/api/v1/public/briefs
curl http://localhost:8000/api/v1/reports/<report_id>/export?format=markdown
```

Client session:

The MVP uses an anonymous client session header to keep feed cards and reports separated before full auth is added:

```http
X-Parallax-Session-Id: <stable-client-session-id>
```

If the header is missing, the backend uses the shared `anonymous` fallback for local and legacy smoke tests.

Environment:

- `ENV`: use `production` in deployed environments.
- `DEBUG`: keep `false` in production. `/debug/env` is hidden when `ENV=production` and `DEBUG=false`.
- `SECRET_KEY`: required for signed public brief tokens. Use a long random value in production.
- `ADMIN_API_KEY`: required for admin-only source seed and sync API routes. Send it as `X-Parallax-Admin-Key`.
- `DATABASE_URL`: Supabase/Postgres connection string. Non-Postgres values use the in-memory local fallback.
- `DATABASE_SSLMODE`: set to `require` for Supabase if the connection string does not already include SSL settings.
- `FRONTEND_URL`: deployed frontend origin, used for CORS and public share URLs.
- `BACKEND_CORS_ORIGINS`: optional comma-separated extra frontend origins.
- `AI_PROVIDER`: `heuristic` for local deterministic analysis, or `openai` for OpenAI-compatible chat completions.
- `OPENAI_API_KEY`, `OPENAI_MODEL`, `OPENAI_BASE_URL`: required when `AI_PROVIDER=openai`.
- `ARTICLE_FETCH_TIMEOUT_SECONDS`, `ARTICLE_MAX_CHARS`: article retrieval and analysis limits.
- `ANALYZE_QUOTA_ENABLED`: enables per-session analyze limits.
- `ANALYZE_DAILY_LIMIT`: maximum analyzed articles per session in the quota window.
- `ANALYZE_QUOTA_WINDOW_SECONDS`: rolling quota window length.
- `ANALYZE_COOLDOWN_SECONDS`: minimum wait between analyze requests for the same session.
- `EXTERNAL_RETRIEVAL_ENABLED`: when true, OSINT can fetch public web search results for bounded context; default is false.
- `RETRIEVAL_PROVIDER`: `mock` for deterministic OSINT probes, or `web` for public web search retrieval when external retrieval is enabled.
- `OPS_NOTIFICATIONS_ENABLED`: enables best-effort source operational alert webhook delivery; default is false.
- `OPS_WEBHOOK_URL`: HTTP/HTTPS endpoint for receiving source operational alerts.
- `OPS_WEBHOOK_SECRET`: optional HMAC secret used to set `X-Parallax-Signature: sha256=<digest>`.
- `OPS_NOTIFICATION_MIN_SEVERITY`: minimum alert severity to deliver: `info`, `warning`, or `critical`.
- `OPS_NOTIFICATION_TIMEOUT_SECONDS`: bounded webhook timeout in seconds.

Database setup:

```bash
python scripts/apply_migrations.py
```

The app still performs an idempotent schema readiness check at runtime for MVP deploy safety, but production deployments should run the SQL migrations explicitly before serving traffic.

Phase 2 implementation status:

- Step 1 complete: database schema for `sources`, `source_feeds`, `ingested_articles`, `nodes`, `node_edges`, and `article_comparisons`.
- Step 2 complete: `/api/v1/sources` source manager, source feed records, RSS/homepage sync, manual feed provenance, ingested article persistence, and `ingested_article` feed cards.
- Step 3 complete: `POST /api/v1/analyze` accepts `ingested_article_id`, links analysis back to ingested articles, enriches existing source-ingested feed cards, and returns structured Phase 2 intelligence.
- Step 4 complete: feed cards and article detail hydrate from persisted ingested-article analysis, including comparison hooks and node preview.
- Step 5 complete: `GET /api/v1/compare/{article_id}` finds similar ingested articles, returns structured cross-source comparison with title differences, missing/added claims, coverage gaps, source/timeline contrasts, and entity overlap, and persists `article_comparisons`.
- Step 6 complete: article detail materializes `nodes` and `node_edges`, exposes node perspectives, and supports node-based detail tabs in the frontend.
- Step 7 complete: `GET /api/v1/sources/articles/{article_id}/osint` returns bounded OSINT context and article detail includes `osint_context`.
- Step 8 complete: `/api/v1/sources/defaults/preview`, `/api/v1/sources/defaults/seed`, and `scripts/seed_default_sources.py` provide an idempotent multilingual default source database.
- Production hardening 1 complete: `POST /api/v1/sources/sync-active` and `scripts/sync_active_sources.py` make active RSS/homepage ingestion scheduler-friendly with batch limits, manual-feed skip summaries, and error isolation.
- Production hardening 2 complete: source seed/sync API routes require `X-Parallax-Admin-Key` backed by `ADMIN_API_KEY`.
- Production hardening 3 complete: `scripts/check_deploy_readiness.py` validates production configuration and CI emits deploy-readiness JSON.
- Production hardening 4 complete: `source_sync_runs`, sync-run endpoints, source health summaries, scheduler JSON run IDs, and smoke coverage make ingestion observable.
- Production hardening 5 complete: source review status, quality scoring, review queue, feed pause/quarantine/disable controls, and ingestion skip behavior make source governance manageable.
- Production hardening 6 complete: source operational alerts, scheduler alert summaries, alert acknowledgement, and OSINT retrieval provider boundaries are in place.
- Production hardening 7 complete: source operational alerts can be delivered to signed webhooks, delivery attempts are persisted, scheduler output includes delivery summaries, and admin APIs expose delivery history.
- Intelligence provider upgrade complete: article intelligence, cross-source comparisons, node perspective hints, and OSINT synthesis support model-backed structured JSON with provider metadata and deterministic fallback.
- Source/topic aggregation complete: `intelligence_snapshots`, source intelligence endpoints, topic intelligence endpoints, scheduler script, readiness output, and smoke coverage provide persisted source/topic intelligence summaries.
- Intelligence feed operationalization complete: `intelligence_refresh_runs`, `POST /api/v1/intelligence/refresh`, run history, and generated intelligence feed cards make source/topic aggregation visible in the main feed.
- Event clustering operationalization complete: `event_clusters`, event-cluster feed cards, compare/topic cluster context, scheduler script, pipeline controls, cross-language alias matching, cluster quality metadata, language-bridge terms, coverage-gap tasks, suggested source searches, and admin source candidate drafts persist cross-source event groups.
- Pending article analysis automation complete: `scripts/analyze_pending_articles.py`, `POST /api/v1/sources/articles/analyze-pending`, and the pipeline `article_analysis` phase batch-analyze pending ingested articles before aggregation and clustering.
- Next work should add production scheduler configuration templates for the selected host and richer cross-language clustering.

Production deploy checklist:

1. Create Supabase/Postgres database.
2. Set backend env vars: `ENV=production`, `DEBUG=false`, `SECRET_KEY`, `ADMIN_API_KEY`, `DATABASE_URL`, `DATABASE_SSLMODE=require`, `FRONTEND_URL`, optional `BACKEND_CORS_ORIGINS`, and AI settings.
3. Run `python scripts/check_deploy_readiness.py --strict` and fix blocking errors.
4. Run `python scripts/apply_migrations.py` before serving production traffic.
5. Run `python scripts/smoke_local.py` locally, then optionally `PARALLAX_SMOKE_USE_CONFIG_DB=true python scripts/smoke_local.py` against a staging database.
6. Deploy backend, confirm `/api/v1/health` and `/api/v1/health/db`.
7. Configure a recurring worker for `python scripts/sync_active_sources.py --source-limit 50 --feed-limit 100 --article-limit 10 --card-limit 25`, or a protected HTTP job for `POST /api/v1/sources/sync-active`. Set `OPS_NOTIFICATIONS_ENABLED=true` and `OPS_WEBHOOK_URL` when operational alerts should leave the app.
8. Set frontend `NEXT_PUBLIC_API_URL` to the backend origin and deploy the frontend.
9. Confirm one browser session can complete onboarding, analyze or ingest a source, view alerts, open sources, save a report, and generate a brief.

CI:

GitHub Actions runs backend compile, deploy-readiness JSON output, and `scripts/smoke_local.py` on pushes and pull requests. The backend workflow also supports manual live smoke: open the workflow dispatch form and pass the deployed backend URL as `api_url`.
