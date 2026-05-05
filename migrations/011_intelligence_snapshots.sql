create table if not exists public.intelligence_snapshots (
    id uuid primary key,
    snapshot_type text not null check (snapshot_type in ('source', 'topic')),
    subject_id text not null,
    title text,
    payload jsonb not null default '{}'::jsonb,
    provider_metadata jsonb not null default '{}'::jsonb,
    sample_size integer not null default 0,
    created_at timestamptz not null default now()
);

create index if not exists idx_intelligence_snapshots_subject_created
    on public.intelligence_snapshots (snapshot_type, subject_id, created_at desc);

create index if not exists idx_intelligence_snapshots_created
    on public.intelligence_snapshots (created_at desc);
