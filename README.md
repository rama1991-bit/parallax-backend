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

Analyze flow:

```bash
curl -X POST http://localhost:8000/api/v1/analyze \
  -H "Content-Type: application/json" \
  -d "{\"url\":\"https://example.com/article\"}"
```

The endpoint fetches and extracts article text, produces structured narrative analysis, saves a feed card when `DATABASE_URL` points at Postgres/Supabase, and returns the normalized card for the frontend to insert into the feed.

Compare flow:

```bash
curl -X POST http://localhost:8000/api/v1/compare \
  -H "Content-Type: application/json" \
  -d "{\"left_url\":\"https://example.com/a\",\"right_url\":\"https://example.com/b\"}"
```

Compare fetches, analyzes, and saves both articles for the current session, then returns claim overlap and framing divergence signals.

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

Database setup:

```bash
python scripts/apply_migrations.py
```

The app still performs an idempotent schema readiness check at runtime for MVP deploy safety, but production deployments should run the SQL migrations explicitly before serving traffic.

Production deploy checklist:

1. Create Supabase/Postgres database.
2. Set backend env vars: `ENV=production`, `DEBUG=false`, `SECRET_KEY`, `DATABASE_URL`, `DATABASE_SSLMODE=require`, `FRONTEND_URL`, optional `BACKEND_CORS_ORIGINS`, and AI settings.
3. Run `python scripts/apply_migrations.py` before serving production traffic.
4. Run `python scripts/smoke_local.py` locally, then optionally `PARALLAX_SMOKE_USE_CONFIG_DB=true python scripts/smoke_local.py` against a staging database.
5. Deploy backend, confirm `/api/v1/health` and `/api/v1/health/db`.
6. Set frontend `NEXT_PUBLIC_API_URL` to the backend origin and deploy the frontend.
7. Confirm one browser session can complete onboarding, analyze or ingest a source, view alerts, open sources, save a report, and generate a brief.
