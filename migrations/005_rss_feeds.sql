create table if not exists public.feeds (
    id uuid primary key,
    session_id text,
    topic_id text,
    url text not null,
    title text,
    description text,
    status text not null default 'active',
    last_synced_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.feed_items (
    id uuid primary key,
    session_id text,
    feed_id uuid,
    external_id text,
    title text,
    summary text,
    url text,
    source text,
    published_at timestamptz,
    raw jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

alter table public.feeds add column if not exists session_id text;
alter table public.feeds add column if not exists topic_id text;
alter table public.feeds add column if not exists description text;
alter table public.feeds add column if not exists status text not null default 'active';
alter table public.feeds add column if not exists last_synced_at timestamptz;
alter table public.feeds add column if not exists created_at timestamptz not null default now();
alter table public.feeds add column if not exists updated_at timestamptz not null default now();

alter table public.feed_items add column if not exists session_id text;
alter table public.feed_items add column if not exists feed_id uuid;
alter table public.feed_items add column if not exists external_id text;
alter table public.feed_items add column if not exists source text;
alter table public.feed_items add column if not exists published_at timestamptz;
alter table public.feed_items add column if not exists raw jsonb not null default '{}'::jsonb;
alter table public.feed_items add column if not exists created_at timestamptz not null default now();

create index if not exists idx_feeds_session_created
    on public.feeds (session_id, created_at desc);

create index if not exists idx_feed_items_session_feed
    on public.feed_items (session_id, feed_id, created_at desc);

create unique index if not exists idx_feed_items_feed_url_unique
    on public.feed_items (feed_id, url)
    where url is not null;
