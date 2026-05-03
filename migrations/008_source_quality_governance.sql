alter table public.sources add column if not exists review_status text not null default 'needs_review';
alter table public.sources add column if not exists review_notes text;
alter table public.sources add column if not exists disabled_reason text;
alter table public.sources add column if not exists quality_score double precision not null default 0;
alter table public.sources add column if not exists terms_reviewed_at timestamptz;
alter table public.sources add column if not exists last_reviewed_at timestamptz;

alter table public.source_feeds add column if not exists disabled_reason text;
alter table public.source_feeds add column if not exists review_notes text;
alter table public.source_feeds add column if not exists last_reviewed_at timestamptz;

create index if not exists idx_sources_review_status
    on public.sources (review_status);

create index if not exists idx_sources_quality_score
    on public.sources (quality_score);

create index if not exists idx_source_feeds_status
    on public.source_feeds (status);
