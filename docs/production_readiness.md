# Production Readiness Notes

## Schema

The current product surface is backed by these persisted tables:

- `feed_cards`
- `article_analyses`
- `topics`
- `monitors`
- `feeds`
- `feed_items`
- `sources`
- `source_feeds`
- `ingested_articles`
- `source_sync_runs`
- `nodes`
- `node_edges`
- `article_comparisons`
- `schema_migrations`

Phase 1 author/source intelligence, alerts, saved reports, and public briefs are still derived from `feed_cards` plus related payloads. Phase 2 source records, source feeds, ingested articles, node graphs, article comparisons, bounded OSINT context, and default source seeds are now first-class.

Before production traffic:

```bash
python scripts/check_deploy_readiness.py --strict
python scripts/apply_migrations.py
```

`scripts/check_deploy_readiness.py` does not open a database connection. It validates the deploy-time environment, reports blocking production risks, and prints the scheduler command that should be configured after the backend is live. Use `--json` when a CI or hosting platform needs machine-readable output.

## Route Coverage

These frontend surfaces have real backend support:

- `/` smart feed: `GET /api/v1/feed`, feed card actions, `POST /api/v1/analyze` by URL or `ingested_article_id`
- `/topics`: `GET/POST /api/v1/topics`, `POST /api/v1/topics/monitor`
- `/feeds`: `GET/POST /api/v1/feeds`, `POST /api/v1/feeds/{feed_id}/sync`
- `/sources`: `GET/POST /api/v1/sources`, source health summaries, default source preview, admin-key-protected default source seed, admin-key-protected `POST /api/v1/sources/sync-active`, admin-key-protected `POST /api/v1/sources/{source_id}/sync`, admin-key-protected sync-run history, source feed routes, hydrated article detail, `GET /api/v1/sources/articles/{article_id}/nodes`, and `GET /api/v1/sources/articles/{article_id}/osint`
- `/compare`: `POST /api/v1/compare`, `GET /api/v1/compare/{article_id}`
- `/notifications`: `GET /api/v1/alerts`, read/read-all/unread count
- `/reports/{id}`: report detail, save/unsave, JSON/Markdown export
- `/saved`: `GET /api/v1/saved-reports`
- `/briefs`: `GET /api/v1/public/briefs`
- `/briefs/{token}`: signed public brief detail and export
- legacy source intelligence: `GET /api/v1/authors`, `GET /api/v1/authors/{id}`
- `/onboarding`: onboarding state and setup

Known MVP gaps:

- Full user auth is not implemented; browser sessions use `X-Parallax-Session-Id`.
- Admin-only source seed and sync routes use `X-Parallax-Admin-Key` backed by `ADMIN_API_KEY`; rotate it like any production secret.
- Source intelligence is sample-based and session-scoped.
- Feed and source sync are schedulable API/script calls; configure a hosted recurring worker or protected HTTP scheduled job in the deployment platform.
- Analyze uses heuristic mode unless `AI_PROVIDER=openai` is configured.
- Brief share tokens are signed but not revocable because briefs are derived, not persisted.
- Phase 2 source ingestion, default source seeds, ingested-article analysis linkage, hydrated article detail, feed card hydration, article-id compare, node-based article detail, and bounded OSINT panels exist.
- Default source metadata is intentionally conservative and should be reviewed for legal/feed-term compliance before production-scale ingestion.
- Active RSS ingestion can be scheduled with `POST /api/v1/sources/sync-active` or `python scripts/sync_active_sources.py`.
- Source ingestion now records `source_sync_runs` and exposes source health summaries; treat these as operational signals, not editorial quality judgments.

## Deploy Readiness

Run this check before every production deploy:

```bash
python scripts/check_deploy_readiness.py --strict
```

For CI output:

```bash
python scripts/check_deploy_readiness.py --json
```

The strict check blocks deploys when production-critical values are unsafe: `DEBUG=true`, missing or weak `SECRET_KEY`, missing or weak `ADMIN_API_KEY`, non-Postgres storage, missing Postgres SSL mode, invalid/local frontend origin, invalid AI provider settings, or invalid quota/fetch limits. It warns when production intentionally uses deterministic heuristic analysis or mock retrieval.

## Scheduler

Recommended recurring job:

```bash
python scripts/sync_active_sources.py --source-limit 50 --feed-limit 100 --article-limit 10 --card-limit 25
```

Run it every 15 minutes once the backend has migrations applied and the default source database has been reviewed. The script isolates feed-level failures so one broken feed does not stop the whole batch. Keep conservative limits in production until source quality, hosting quotas, and upstream feed behavior are understood.

The scheduler output is JSON and includes `sync_run_id`, status, feed/article/card counts, error count, and the applied limits. A non-zero exit is reserved for runs that have errors and no saved articles, so hosted schedulers can flag fully failed batches without paging on partial successes.

HTTP scheduler alternative:

```bash
curl -X POST https://your-backend.example.com/api/v1/sources/sync-active \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
```

Use the HTTP route when the hosting platform supports scheduled HTTP calls but not scheduled shell commands.

## Ingestion Observability

Read source health from the public source list/detail payloads:

```bash
curl https://your-backend.example.com/api/v1/sources
curl https://your-backend.example.com/api/v1/sources/<source_id>
```

Read detailed sync history with the admin key:

```bash
curl https://your-backend.example.com/api/v1/sources/sync-runs \
  -H "X-Parallax-Admin-Key: <admin_api_key>"

curl https://your-backend.example.com/api/v1/sources/<source_id>/sync-runs \
  -H "X-Parallax-Admin-Key: <admin_api_key>"
```

Watch these fields after deploy:

- `health.status`: `healthy`, `stale`, `error`, or `needs_review`
- `health.last_success_at`
- `health.last_error`
- `health.success_rate`
- `health.articles_24h`
- `sync_runs.error_count`

Initial alert candidates: no successful source sync in 24 hours, repeated `error` status, `success_rate` below 50% after several runs, or zero `articles_24h` for high-priority active sources.

## Smoke

Safe local smoke:

```bash
python scripts/smoke_local.py
```

Staging database smoke:

```bash
PARALLAX_SMOKE_USE_CONFIG_DB=true python scripts/smoke_local.py
```

The smoke covers health, session identity, onboarding, feed cards, alerts, source profiles, Phase 2 source ingestion, admin-key rejection/acceptance for default source seed, sync, and sync-run history, default source seed preview/import, active source sync, sync-run persistence, source health summaries, ingested-article analysis persistence, hydrated article detail, node graph materialization, bounded OSINT context, article-id compare, reports, saved reports, public briefs, topics, feeds, and session isolation.

Read-only deployed backend smoke:

```bash
PARALLAX_LIVE_API_URL=https://your-backend.example.com python scripts/smoke_live.py
```

GitHub Actions:

- Pushes and pull requests run backend compile, deploy-readiness JSON output, and local smoke.
- Manual workflow dispatch can run live smoke when `api_url` is provided.
