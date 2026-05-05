create table if not exists public.intelligence_refresh_runs (
    id uuid primary key,
    session_id text,
    status text not null default 'completed' check (status in ('completed', 'partial', 'failed')),
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    duration_ms integer not null default 0,
    source_count integer not null default 0,
    topic_count integer not null default 0,
    snapshot_count integer not null default 0,
    card_count integer not null default 0,
    error_count integer not null default 0,
    limits jsonb not null default '{}'::jsonb,
    errors jsonb not null default '[]'::jsonb,
    summary jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_intelligence_refresh_runs_started
    on public.intelligence_refresh_runs (started_at desc);

create index if not exists idx_intelligence_refresh_runs_session_started
    on public.intelligence_refresh_runs (session_id, started_at desc);
