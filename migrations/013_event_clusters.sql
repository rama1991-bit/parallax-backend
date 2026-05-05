create table if not exists public.event_clusters (
    id uuid primary key,
    cluster_key text not null unique,
    title text not null,
    summary text,
    status text not null default 'active' check (status in ('active', 'archived')),
    primary_topic text,
    languages jsonb not null default '[]'::jsonb,
    countries jsonb not null default '[]'::jsonb,
    source_ids jsonb not null default '[]'::jsonb,
    article_count integer not null default 0,
    analyzed_count integer not null default 0,
    first_seen_at timestamptz,
    latest_seen_at timestamptz,
    fingerprint_terms jsonb not null default '[]'::jsonb,
    claims jsonb not null default '[]'::jsonb,
    frames jsonb not null default '[]'::jsonb,
    provider_metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.event_cluster_articles (
    id uuid primary key,
    cluster_id uuid not null references public.event_clusters(id) on delete cascade,
    article_id uuid not null references public.ingested_articles(id) on delete cascade,
    similarity_score double precision not null default 0,
    matched_terms jsonb not null default '[]'::jsonb,
    language text,
    country text,
    source_id uuid references public.sources(id) on delete set null,
    created_at timestamptz not null default now(),
    unique (cluster_id, article_id)
);

create table if not exists public.event_cluster_refresh_runs (
    id uuid primary key,
    session_id text,
    status text not null default 'completed' check (status in ('completed', 'partial', 'failed')),
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    duration_ms integer not null default 0,
    cluster_count integer not null default 0,
    article_count integer not null default 0,
    card_count integer not null default 0,
    error_count integer not null default 0,
    limits jsonb not null default '{}'::jsonb,
    errors jsonb not null default '[]'::jsonb,
    summary jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_event_clusters_latest
    on public.event_clusters (latest_seen_at desc, article_count desc);

create index if not exists idx_event_clusters_primary_topic
    on public.event_clusters (primary_topic);

create index if not exists idx_event_cluster_articles_cluster
    on public.event_cluster_articles (cluster_id, similarity_score desc);

create index if not exists idx_event_cluster_articles_article
    on public.event_cluster_articles (article_id);

create index if not exists idx_event_cluster_refresh_runs_started
    on public.event_cluster_refresh_runs (started_at desc);

create index if not exists idx_event_cluster_refresh_runs_session_started
    on public.event_cluster_refresh_runs (session_id, started_at desc);
