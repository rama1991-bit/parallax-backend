create table if not exists public.source_ops_alert_deliveries (
    id uuid primary key,
    alert_id uuid not null references public.source_ops_alerts(id) on delete cascade,
    alert_updated_at timestamptz,
    destination_type text not null default 'webhook',
    destination_url text,
    status text not null default 'failed' check (status in ('delivered', 'failed', 'skipped')),
    attempt_count integer not null default 1,
    response_status integer,
    error text,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    delivered_at timestamptz
);

create index if not exists idx_source_ops_alert_deliveries_alert_created
    on public.source_ops_alert_deliveries (alert_id, created_at desc);

create index if not exists idx_source_ops_alert_deliveries_status_created
    on public.source_ops_alert_deliveries (status, created_at desc);
