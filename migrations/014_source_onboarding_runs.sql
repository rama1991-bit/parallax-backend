create table if not exists public.source_discovery_runs (
    id uuid primary key,
    session_id text,
    query text not null,
    cluster_id uuid,
    candidate_id text,
    include_external boolean not null default false,
    status text not null default 'completed' check (
        status in ('completed', 'partial', 'failed', 'skipped')
    ),
    candidate_count integer not null default 0,
    existing_source_match_count integer not null default 0,
    request_payload jsonb not null default '{}'::jsonb,
    response_payload jsonb not null default '{}'::jsonb,
    retrieval_mode jsonb not null default '{}'::jsonb,
    provider_metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists public.source_candidate_validation_runs (
    id uuid primary key,
    session_id text,
    candidate_id text,
    source_id uuid references public.sources(id) on delete set null,
    status text not null default 'failed' check (
        status in ('validated', 'needs_review', 'failed')
    ),
    selected_feed_type text,
    selected_feed_url text,
    item_count integer not null default 0,
    candidate_payload jsonb not null default '{}'::jsonb,
    validation_payload jsonb not null default '{}'::jsonb,
    provider_metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists public.source_onboarding_runs (
    id uuid primary key,
    session_id text,
    status text not null default 'completed' check (
        status in ('completed', 'partial', 'failed', 'skipped')
    ),
    source_id uuid references public.sources(id) on delete set null,
    source_feed_id uuid references public.source_feeds(id) on delete set null,
    cluster_id uuid,
    candidate_id text,
    source_name text,
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    duration_ms integer not null default 0,
    phases jsonb not null default '[]'::jsonb,
    errors jsonb not null default '[]'::jsonb,
    request_payload jsonb not null default '{}'::jsonb,
    result_payload jsonb not null default '{}'::jsonb,
    summary jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_source_discovery_runs_created
    on public.source_discovery_runs (created_at desc);

create index if not exists idx_source_discovery_runs_cluster
    on public.source_discovery_runs (cluster_id, created_at desc)
    where cluster_id is not null;

create index if not exists idx_source_candidate_validation_runs_created
    on public.source_candidate_validation_runs (created_at desc);

create index if not exists idx_source_candidate_validation_runs_candidate
    on public.source_candidate_validation_runs (candidate_id, created_at desc)
    where candidate_id is not null;

create index if not exists idx_source_onboarding_runs_created
    on public.source_onboarding_runs (created_at desc);

create index if not exists idx_source_onboarding_runs_source
    on public.source_onboarding_runs (source_id, created_at desc)
    where source_id is not null;

create index if not exists idx_source_onboarding_runs_cluster
    on public.source_onboarding_runs (cluster_id, created_at desc)
    where cluster_id is not null;
