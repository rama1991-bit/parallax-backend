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
- `nodes`
- `node_edges`
- `article_comparisons`
- `schema_migrations`

Phase 1 author/source intelligence, alerts, saved reports, and public briefs are still derived from `feed_cards` plus related payloads. Phase 2 source records, source feeds, ingested articles, node graphs, and article comparisons are now first-class.

Before production traffic:

```bash
python scripts/apply_migrations.py
```

## Route Coverage

These frontend surfaces have real backend support:

- `/` smart feed: `GET /api/v1/feed`, feed card actions, `POST /api/v1/analyze` by URL or `ingested_article_id`
- `/topics`: `GET/POST /api/v1/topics`, `POST /api/v1/topics/monitor`
- `/feeds`: `GET/POST /api/v1/feeds`, `POST /api/v1/feeds/{feed_id}/sync`
- `/sources`: `GET/POST /api/v1/sources`, `POST /api/v1/sources/{source_id}/sync`, source feed routes, hydrated article detail, and `GET /api/v1/sources/articles/{article_id}/nodes`
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
- Source intelligence is sample-based and session-scoped.
- Feed and source sync are manual/schedulable API calls; recurring background ingestion is not implemented yet.
- Analyze uses heuristic mode unless `AI_PROVIDER=openai` is configured.
- Brief share tokens are signed but not revocable because briefs are derived, not persisted.
- Phase 2 source ingestion, ingested-article analysis linkage, hydrated article detail, feed card hydration, article-id compare, and node-based article detail exist. OSINT panels and default source seeds are follow-up implementation steps.

## Smoke

Safe local smoke:

```bash
python scripts/smoke_local.py
```

Staging database smoke:

```bash
PARALLAX_SMOKE_USE_CONFIG_DB=true python scripts/smoke_local.py
```

The smoke covers health, session identity, onboarding, feed cards, alerts, source profiles, Phase 2 source ingestion, ingested-article analysis persistence, hydrated article detail, node graph materialization, article-id compare, reports, saved reports, public briefs, topics, feeds, and session isolation.

Read-only deployed backend smoke:

```bash
PARALLAX_LIVE_API_URL=https://your-backend.example.com python scripts/smoke_live.py
```

GitHub Actions:

- Pushes and pull requests run backend compile and local smoke.
- Manual workflow dispatch can run live smoke when `api_url` is provided.
