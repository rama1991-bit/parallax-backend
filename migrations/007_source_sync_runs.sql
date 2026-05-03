create table if not exists public.source_sync_runs (
    id uuid primary key,
    session_id text,
    sync_scope text not null default 'source' check (
        sync_scope in ('source', 'active_sources', 'feed')
    ),
    status text not null default 'completed' check (
        status in ('completed', 'partial', 'failed', 'skipped')
    ),
    source_id uuid references public.sources(id) on delete set null,
    source_feed_id uuid references public.source_feeds(id) on delete set null,
    source_name text,
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    duration_ms integer not null default 0,
    source_count integer not null default 0,
    feed_count integer not null default 0,
    synced_feed_count integer not null default 0,
    article_count integer not null default 0,
    card_count integer not null default 0,
    error_count integer not null default 0,
    limits jsonb not null default '{}'::jsonb,
    errors jsonb not null default '[]'::jsonb,
    summary jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_source_sync_runs_started
    on public.source_sync_runs (started_at desc);

create index if not exists idx_source_sync_runs_source_started
    on public.source_sync_runs (source_id, started_at desc);
