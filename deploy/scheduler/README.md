# Parallax Scheduler Templates

Use one of these templates when the production host supports recurring jobs. Keep limits conservative until the source database has been reviewed and the backend has a production Postgres/Supabase database.

## Single Pipeline Job

Run every 15-60 minutes:

```bash
python scripts/run_intelligence_pipeline.py --source-limit 50 --feed-limit 100 --sync-article-limit 10 --sync-card-limit 25 --analysis-article-limit 25 --intelligence-source-limit 50 --topic-limit 50 --intelligence-article-limit 100 --intelligence-card-limit 50 --cluster-article-limit 250 --cluster-limit 100 --cluster-card-limit 50
```

This runs active source ingestion, pending article analysis, source/topic intelligence snapshots, and event clustering in one protected operation.

## Split Jobs

Use split jobs when the host has separate worker queues or when ingestion/fetch latency should not block aggregation:

```bash
python scripts/sync_active_sources.py --source-limit 50 --feed-limit 100 --article-limit 10 --card-limit 25
python scripts/analyze_pending_articles.py --limit 25
python scripts/refresh_intelligence_snapshots.py --source-limit 50 --topic-limit 50 --article-limit 100 --card-limit 50
python scripts/refresh_event_clusters.py --article-limit 250 --cluster-limit 100 --card-limit 50
```

Recommended cadence:

- Ingestion: every 15 minutes.
- Pending analysis: every 20-30 minutes.
- Intelligence snapshots: every 30 minutes.
- Event clusters: every 30 minutes after analysis.

## HTTP Scheduler Alternative

If the platform only supports HTTP cron, call the protected backend endpoint:

```bash
curl -X POST "https://YOUR_BACKEND/api/v1/intelligence/pipeline/run?source_limit=50&feed_limit=100&sync_article_limit=10&sync_card_limit=25&analysis_article_limit=25&intelligence_source_limit=50&topic_limit=50&intelligence_article_limit=100&intelligence_card_limit=50&cluster_article_limit=250&cluster_limit=100&cluster_card_limit=50" \
  -H "X-Parallax-Admin-Key: $ADMIN_API_KEY"
```

## Required Production Environment

- `ENV=production`
- `DEBUG=false`
- `SECRET_KEY`
- `ADMIN_API_KEY`
- `DATABASE_URL`
- `DATABASE_SSLMODE=require`
- `FRONTEND_URL`
- Optional model-backed analysis: `AI_PROVIDER=openai`, `OPENAI_API_KEY`, `OPENAI_MODEL`
- Optional alert delivery: `OPS_NOTIFICATIONS_ENABLED=true`, `OPS_WEBHOOK_URL`, `OPS_WEBHOOK_SECRET`

Before enabling recurring jobs, run:

```bash
python scripts/check_deploy_readiness.py --strict
python scripts/apply_migrations.py
python scripts/smoke_local.py
```
