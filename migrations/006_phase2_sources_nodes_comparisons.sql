create table if not exists public.sources (
    id uuid primary key,
    name text not null,
    website_url text,
    rss_url text,
    country text,
    language text,
    region text,
    political_context text,
    source_size text check (source_size in ('major', 'medium', 'small', 'niche')),
    source_type text check (
        source_type in (
            'news_agency',
            'newspaper',
            'broadcaster',
            'magazine',
            'independent',
            'state_media',
            'NGO',
            'official'
        )
    ),
    credibility_notes text,
    notes text,
    is_default boolean not null default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.source_feeds (
    id uuid primary key,
    source_id uuid references public.sources(id) on delete cascade,
    feed_url text not null,
    feed_type text not null default 'rss' check (feed_type in ('rss', 'homepage', 'manual')),
    title text,
    language text,
    country text,
    status text not null default 'active',
    fetch_interval_minutes integer not null default 60,
    last_checked_at timestamptz,
    last_success_at timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.ingested_articles (
    id uuid primary key,
    source_id uuid references public.sources(id) on delete set null,
    source_feed_id uuid references public.source_feeds(id) on delete set null,
    feed_item_id uuid references public.feed_items(id) on delete set null,
    article_analysis_id uuid references public.article_analyses(id) on delete set null,
    url text not null,
    canonical_url text,
    title text,
    author text,
    published_at timestamptz,
    language text,
    country text,
    summary text,
    extracted_text text,
    content_hash text,
    event_fingerprint text,
    comparison_keywords jsonb not null default '[]'::jsonb,
    raw_metadata jsonb not null default '{}'::jsonb,
    ingestion_status text not null default 'pending' check (
        ingestion_status in ('pending', 'fetched', 'failed', 'skipped')
    ),
    analysis_status text not null default 'pending' check (
        analysis_status in ('pending', 'analyzed', 'failed', 'skipped')
    ),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.nodes (
    id uuid primary key,
    session_id text,
    node_type text not null check (
        node_type in (
            'article',
            'author',
            'source',
            'topic',
            'event',
            'claim',
            'narrative',
            'location',
            'person',
            'organization'
        )
    ),
    label text not null,
    slug text,
    source_id uuid references public.sources(id) on delete set null,
    ingested_article_id uuid references public.ingested_articles(id) on delete cascade,
    topic_id uuid references public.topics(id) on delete set null,
    claim_text text,
    node_metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.node_edges (
    id uuid primary key,
    session_id text,
    from_node_id uuid not null references public.nodes(id) on delete cascade,
    to_node_id uuid not null references public.nodes(id) on delete cascade,
    edge_type text not null,
    weight double precision not null default 0.5,
    evidence jsonb not null default '[]'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists public.article_comparisons (
    id uuid primary key,
    base_article_id uuid not null references public.ingested_articles(id) on delete cascade,
    comparison_article_id uuid not null references public.ingested_articles(id) on delete cascade,
    similarity_score double precision not null default 0,
    shared_claims jsonb not null default '[]'::jsonb,
    unique_claims_by_source jsonb not null default '[]'::jsonb,
    framing_differences jsonb not null default '[]'::jsonb,
    tone_differences jsonb not null default '[]'::jsonb,
    missing_context jsonb not null default '[]'::jsonb,
    timeline_difference jsonb not null default '{}'::jsonb,
    source_difference jsonb not null default '{}'::jsonb,
    confidence double precision not null default 0,
    comparison_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table public.feed_cards add column if not exists source_id uuid;
alter table public.feed_cards add column if not exists source_feed_id uuid;
alter table public.feed_cards add column if not exists ingested_article_id uuid;
alter table public.feed_cards add column if not exists node_id uuid;
alter table public.feed_cards add column if not exists comparison_id uuid;

alter table public.article_analyses add column if not exists ingested_article_id uuid;

create unique index if not exists idx_sources_website_url_unique
    on public.sources (website_url)
    where website_url is not null;

create unique index if not exists idx_sources_rss_url_unique
    on public.sources (rss_url)
    where rss_url is not null;

create index if not exists idx_sources_country_language
    on public.sources (country, language);

create index if not exists idx_source_feeds_source_status
    on public.source_feeds (source_id, status);

create unique index if not exists idx_source_feeds_source_url_unique
    on public.source_feeds (source_id, feed_url);

create index if not exists idx_ingested_articles_source_published
    on public.ingested_articles (source_id, published_at desc);

create index if not exists idx_ingested_articles_fingerprint
    on public.ingested_articles (event_fingerprint);

create unique index if not exists idx_ingested_articles_source_url_unique
    on public.ingested_articles (source_id, url);

create index if not exists idx_nodes_type_label
    on public.nodes (node_type, label);

create index if not exists idx_nodes_source
    on public.nodes (source_id);

create index if not exists idx_nodes_ingested_article
    on public.nodes (ingested_article_id);

create index if not exists idx_node_edges_from_type
    on public.node_edges (from_node_id, edge_type);

create index if not exists idx_node_edges_to_type
    on public.node_edges (to_node_id, edge_type);

create unique index if not exists idx_article_comparisons_pair_unique
    on public.article_comparisons (base_article_id, comparison_article_id);

create index if not exists idx_article_comparisons_base_similarity
    on public.article_comparisons (base_article_id, similarity_score desc);

create index if not exists idx_feed_cards_ingested_article
    on public.feed_cards (session_id, ingested_article_id);
