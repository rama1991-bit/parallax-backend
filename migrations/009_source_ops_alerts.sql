create table if not exists public.source_ops_alerts (
    id uuid primary key,
    alert_key text not null unique,
    alert_type text not null,
    severity text not null default 'warning',
    status text not null default 'active',
    source_id uuid references public.sources(id) on delete cascade,
    source_feed_id uuid references public.source_feeds(id) on delete set null,
    sync_run_id uuid references public.source_sync_runs(id) on delete set null,
    title text not null,
    message text not null,
    evidence jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    acknowledged_at timestamptz,
    resolved_at timestamptz
);

create index if not exists idx_source_ops_alerts_status_severity
    on public.source_ops_alerts (status, severity, updated_at desc);

create index if not exists idx_source_ops_alerts_source
    on public.source_ops_alerts (source_id, status, updated_at desc);

create index if not exists idx_source_ops_alerts_feed
    on public.source_ops_alerts (source_feed_id, status, updated_at desc);
