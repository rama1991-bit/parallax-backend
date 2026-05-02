# Production Readiness Notes

## Schema

The current product surface is backed by these persisted tables:

- `feed_cards`
- `article_analyses`
- `topics`
- `monitors`
- `feeds`
- `feed_items`
- `schema_migrations`

Sources, alerts, saved reports, and public briefs are derived from `feed_cards` plus related payloads, so no additional tables are required for this MVP slice.

Before production traffic:

```bash
python scripts/apply_migrations.py
```

## Route Coverage

These frontend surfaces have real backend support:

- `/` smart feed: `GET /api/v1/feed`, feed card actions, `POST /api/v1/analyze`
- `/topics`: `GET/POST /api/v1/topics`, `POST /api/v1/topics/monitor`
- `/feeds`: `GET/POST /api/v1/feeds`, `POST /api/v1/feeds/{feed_id}/sync`
- `/compare`: `POST /api/v1/compare`
- `/notifications`: `GET /api/v1/alerts`, read/read-all/unread count
- `/reports/{id}`: report detail, save/unsave, JSON/Markdown export
- `/saved`: `GET /api/v1/saved-reports`
- `/briefs`: `GET /api/v1/public/briefs`
- `/briefs/{token}`: signed public brief detail and export
- `/sources`: `GET /api/v1/authors`
- `/sources/{id}`: `GET /api/v1/authors/{id}`
- `/onboarding`: onboarding state and setup

Known MVP gaps:

- Full user auth is not implemented; browser sessions use `X-Parallax-Session-Id`.
- Source intelligence is sample-based and session-scoped.
- Feed sync is manual; recurring background ingestion is not implemented yet.
- Analyze uses heuristic mode unless `AI_PROVIDER=openai` is configured.
- Brief share tokens are signed but not revocable because briefs are derived, not persisted.

## Smoke

Safe local smoke:

```bash
python scripts/smoke_local.py
```

Staging database smoke:

```bash
PARALLAX_SMOKE_USE_CONFIG_DB=true python scripts/smoke_local.py
```

The smoke covers health, session identity, onboarding, feed cards, alerts, source profiles, reports, saved reports, public briefs, topics, feeds, and session isolation.

Read-only deployed backend smoke:

```bash
PARALLAX_LIVE_API_URL=https://your-backend.example.com python scripts/smoke_live.py
```

GitHub Actions:

- Pushes and pull requests run backend compile and local smoke.
- Manual workflow dispatch can run live smoke when `api_url` is provided.
