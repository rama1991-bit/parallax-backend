create table if not exists public.feed_cards (
    id uuid primary key,
    session_id text,
    topic_id text,
    article_id text,
    alert_id text,
    report_id text,
    title text not null,
    summary text not null,
    source text,
    url text,
    topic text,
    card_type text not null default 'article_insight',
    priority double precision not null default 0.5,
    priority_score double precision not null default 0.5,
    personalized_score double precision not null default 0.5,
    narrative_signal text,
    evidence_score double precision,
    framing text,
    payload jsonb not null default '{}'::jsonb,
    recommendations jsonb not null default '[]'::jsonb,
    explanation jsonb not null default '{}'::jsonb,
    analysis jsonb not null default '{}'::jsonb,
    is_read boolean not null default false,
    is_saved boolean not null default false,
    is_dismissed boolean not null default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.article_analyses (
    id uuid primary key,
    session_id text,
    url text not null,
    final_url text not null,
    title text,
    source text,
    domain text,
    extracted_text text,
    analysis jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists public.topics (
    id uuid primary key,
    session_id text,
    name text not null,
    description text,
    keywords jsonb not null default '[]'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.monitors (
    id uuid primary key,
    session_id text,
    topic_id uuid,
    name text not null,
    keywords jsonb not null default '[]'::jsonb,
    status text not null default 'active',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

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

alter table public.feed_cards add column if not exists session_id text;
alter table public.feed_cards add column if not exists topic_id text;
alter table public.feed_cards add column if not exists article_id text;
alter table public.feed_cards add column if not exists alert_id text;
alter table public.feed_cards add column if not exists report_id text;
alter table public.feed_cards add column if not exists source text;
alter table public.feed_cards add column if not exists url text;
alter table public.feed_cards add column if not exists topic text;
alter table public.feed_cards add column if not exists priority_score double precision default 0.5;
alter table public.feed_cards add column if not exists personalized_score double precision default 0.5;
alter table public.feed_cards add column if not exists narrative_signal text;
alter table public.feed_cards add column if not exists evidence_score double precision;
alter table public.feed_cards add column if not exists framing text;
alter table public.feed_cards add column if not exists payload jsonb not null default '{}'::jsonb;
alter table public.feed_cards add column if not exists recommendations jsonb not null default '[]'::jsonb;
alter table public.feed_cards add column if not exists explanation jsonb not null default '{}'::jsonb;
alter table public.feed_cards add column if not exists analysis jsonb not null default '{}'::jsonb;
alter table public.feed_cards add column if not exists is_read boolean not null default false;
alter table public.feed_cards add column if not exists is_saved boolean not null default false;
alter table public.feed_cards add column if not exists is_dismissed boolean not null default false;
alter table public.feed_cards add column if not exists created_at timestamptz not null default now();
alter table public.feed_cards add column if not exists updated_at timestamptz not null default now();
alter table public.article_analyses add column if not exists session_id text;
alter table public.topics add column if not exists session_id text;
alter table public.topics add column if not exists description text;
alter table public.topics add column if not exists keywords jsonb not null default '[]'::jsonb;
alter table public.topics add column if not exists created_at timestamptz not null default now();
alter table public.topics add column if not exists updated_at timestamptz not null default now();
alter table public.monitors add column if not exists session_id text;
alter table public.monitors add column if not exists topic_id uuid;
alter table public.monitors add column if not exists keywords jsonb not null default '[]'::jsonb;
alter table public.monitors add column if not exists status text not null default 'active';
alter table public.monitors add column if not exists created_at timestamptz not null default now();
alter table public.monitors add column if not exists updated_at timestamptz not null default now();
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

create index if not exists idx_feed_cards_visible_created
    on public.feed_cards (session_id, is_dismissed, created_at desc);

create index if not exists idx_feed_cards_saved
    on public.feed_cards (is_saved)
    where is_saved = true;

create index if not exists idx_feed_cards_unread
    on public.feed_cards (is_read)
    where is_read = false;

create index if not exists idx_feed_cards_article_id
    on public.feed_cards (session_id, article_id);

create index if not exists idx_feed_cards_report_id
    on public.feed_cards (session_id, report_id);

create index if not exists idx_article_analyses_final_url
    on public.article_analyses (session_id, final_url);

create index if not exists idx_article_analyses_session_created
    on public.article_analyses (session_id, created_at desc);

create index if not exists idx_topics_session_created
    on public.topics (session_id, created_at desc);

create index if not exists idx_monitors_session_topic
    on public.monitors (session_id, topic_id);

create index if not exists idx_feeds_session_created
    on public.feeds (session_id, created_at desc);

create index if not exists idx_feed_items_session_feed
    on public.feed_items (session_id, feed_id, created_at desc);

create unique index if not exists idx_feed_items_feed_url_unique
    on public.feed_items (feed_id, url)
    where url is not null;
