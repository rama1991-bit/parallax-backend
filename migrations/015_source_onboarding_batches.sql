create table if not exists public.source_onboarding_batches (
    id uuid primary key,
    session_id text,
    status text not null default 'completed' check (
        status in ('completed', 'partial', 'failed', 'skipped')
    ),
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    duration_ms integer not null default 0,
    candidate_count integer not null default 0,
    completed_count integer not null default 0,
    partial_count integer not null default 0,
    failed_count integer not null default 0,
    skipped_count integer not null default 0,
    source_count integer not null default 0,
    article_count integer not null default 0,
    card_count integer not null default 0,
    review_gate_count integer not null default 0,
    request_payload jsonb not null default '{}'::jsonb,
    result_payload jsonb not null default '{}'::jsonb,
    errors jsonb not null default '[]'::jsonb,
    summary jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_source_onboarding_batches_created
    on public.source_onboarding_batches (created_at desc);

create index if not exists idx_source_onboarding_batches_session_created
    on public.source_onboarding_batches (session_id, created_at desc);
