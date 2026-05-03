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
- Phase 2 source records, source feeds, RSS sync, and ingested-article feed cards
- Phase 2 node-based article detail, article-id comparisons, bounded OSINT context, and default source seeds

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

Analyze flow:

```bash
curl -X POST http://localhost:8000/api/v1/analyze \
  -H "Content-Type: application/json" \
  -d "{\"url\":\"https://example.com/article\"}"

curl -X POST http://localhost:8000/api/v1/analyze \
  -H "Content-Type: application/json" \
  -d "{\"ingested_article_id\":\"<ingested_article_id>\"}"
```

The endpoint fetches and extracts article text, produces structured narrative analysis, saves a feed card when `DATABASE_URL` points at Postgres/Supabase, and returns the normalized card for the frontend to insert into the feed. When called with `ingested_article_id`, the analysis is linked back to `ingested_articles.article_analysis_id`, marks the article as `analyzed`, enriches the existing feed card, and returns a Phase 2 `intelligence` object with article, claims, entities, narrative, source analysis, comparison hooks, and scores.

Compare flow:

```bash
curl -X POST http://localhost:8000/api/v1/compare \
  -H "Content-Type: application/json" \
  -d "{\"left_url\":\"https://example.com/a\",\"right_url\":\"https://example.com/b\"}"

curl http://localhost:8000/api/v1/compare/<ingested_article_id>
```

URL compare fetches, analyzes, and saves both articles for the current session, then returns claim overlap and framing divergence signals. Article-id compare uses persisted `ingested_articles` to find similar coverage across sources, returns the Phase 2 `base_article`, `similar_articles`, and structured `comparison` object, and persists lightweight `article_comparisons` rows for matching pairs.

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
python scripts/seed_default_sources.py --preview
python scripts/seed_default_sources.py
python scripts/sync_active_sources.py --source-limit 50 --feed-limit 100 --article-limit 10 --card-limit 25

curl http://localhost:8000/api/v1/sources/<source_id>/articles
curl http://localhost:8000/api/v1/sources/articles/<ingested_article_id>
curl http://localhost:8000/api/v1/sources/articles/<ingested_article_id>/nodes
curl http://localhost:8000/api/v1/sources/articles/<ingested_article_id>/osint
```

Source sync parses active RSS feeds into `ingested_articles` and creates lightweight `ingested_article` cards for the smart feed. Default source seeding is idempotent and creates multilingual source records plus RSS feed records where a public feed is known. Seed and sync API routes require `X-Parallax-Admin-Key`; direct scripts use backend environment access instead. `sync-active` and `scripts/sync_active_sources.py` provide the scheduler-friendly ingestion runner with source/feed/article/card limits and per-feed error isolation. Article detail returns the article, source, source feed, analysis, intelligence payload, hydrated feed card, comparison hooks, node preview, materialized `node_graph`, and bounded `osint_context`. The nodes endpoint returns article, source, author, topic, event/background, claim, narrative, and entity perspectives with edges. The OSINT endpoint returns contextual references, source types, reliability levels, relevance, risks, contradictions, and citations. Homepage and manual source entries are stored now; recurring homepage crawling is intentionally left for a later step.

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
- `RETRIEVAL_PROVIDER`: labels the retrieval provider used in OSINT payloads.

Database setup:

```bash
python scripts/apply_migrations.py
```

The app still performs an idempotent schema readiness check at runtime for MVP deploy safety, but production deployments should run the SQL migrations explicitly before serving traffic.

Phase 2 implementation status:

- Step 1 complete: database schema for `sources`, `source_feeds`, `ingested_articles`, `nodes`, `node_edges`, and `article_comparisons`.
- Step 2 complete: `/api/v1/sources` source manager, source feed records, RSS sync, ingested article persistence, and `ingested_article` feed cards.
- Step 3 complete: `POST /api/v1/analyze` accepts `ingested_article_id`, links analysis back to ingested articles, enriches existing source-ingested feed cards, and returns structured Phase 2 intelligence.
- Step 4 complete: feed cards and article detail hydrate from persisted ingested-article analysis, including comparison hooks and node preview.
- Step 5 complete: `GET /api/v1/compare/{article_id}` finds similar ingested articles, returns structured cross-source comparison, and persists `article_comparisons`.
- Step 6 complete: article detail materializes `nodes` and `node_edges`, exposes node perspectives, and supports node-based detail tabs in the frontend.
- Step 7 complete: `GET /api/v1/sources/articles/{article_id}/osint` returns bounded OSINT context and article detail includes `osint_context`.
- Step 8 complete: `/api/v1/sources/defaults/preview`, `/api/v1/sources/defaults/seed`, and `scripts/seed_default_sources.py` provide an idempotent multilingual default source database.
- Production hardening 1 complete: `POST /api/v1/sources/sync-active` and `scripts/sync_active_sources.py` make active RSS ingestion scheduler-friendly with batch limits and error isolation.
- Production hardening 2 complete: source seed/sync API routes require `X-Parallax-Admin-Key` backed by `ADMIN_API_KEY`.
- Next work should harden deployment automation and production data quality review.

Production deploy checklist:

1. Create Supabase/Postgres database.
2. Set backend env vars: `ENV=production`, `DEBUG=false`, `SECRET_KEY`, `ADMIN_API_KEY`, `DATABASE_URL`, `DATABASE_SSLMODE=require`, `FRONTEND_URL`, optional `BACKEND_CORS_ORIGINS`, and AI settings.
3. Run `python scripts/apply_migrations.py` before serving production traffic.
4. Run `python scripts/smoke_local.py` locally, then optionally `PARALLAX_SMOKE_USE_CONFIG_DB=true python scripts/smoke_local.py` against a staging database.
5. Deploy backend, confirm `/api/v1/health` and `/api/v1/health/db`.
6. Set frontend `NEXT_PUBLIC_API_URL` to the backend origin and deploy the frontend.
7. Confirm one browser session can complete onboarding, analyze or ingest a source, view alerts, open sources, save a report, and generate a brief.

CI:

GitHub Actions runs backend compile plus `scripts/smoke_local.py` on pushes and pull requests. The backend workflow also supports manual live smoke: open the workflow dispatch form and pass the deployed backend URL as `api_url`.
