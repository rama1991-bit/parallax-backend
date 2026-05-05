import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import urlparse
from uuid import uuid4

import psycopg2
from psycopg2.extras import Json, RealDictCursor

from app.core.config import settings
from app.core.session import ANONYMOUS_SESSION_ID
from app.services.articles import ExtractedArticle
from app.services.osint import build_bounded_osint_context


class FeedStoreError(Exception):
    pass


class QuotaExceededError(Exception):
    def __init__(self, message: str, usage: dict):
        super().__init__(message)
        self.usage = usage


# In-memory fallback for local smoke tests when DATABASE_URL is not Postgres.
FEED_CARDS = []
TOPICS = []
MONITORS = []
FEEDS = []
FEED_ITEMS = []
ALERTS = []
BRIEFS = []
AUTHORS = []
SOURCES = []
SOURCE_FEEDS = []
INGESTED_ARTICLES = []
SOURCE_SYNC_RUNS = []
SOURCE_OPS_ALERTS = []
SOURCE_OPS_ALERT_DELIVERIES = []
ARTICLE_COMPARISONS = []
NODES = []
NODE_EDGES = []

_SCHEMA_READY = False
_HIGH_PRIORITY_ALERT_THRESHOLD = 0.75
_ALERT_CARD_TYPES = (
    "narrative_frame_shift",
    "coverage_change",
    "source_ecosystem_change",
    "divergence_increase",
    "topic_monitor",
    "feed_item",
    "ingested_article",
)
_SOURCE_SIZES = {"major", "medium", "small", "niche"}
_SOURCE_TYPES = {
    "news_agency",
    "newspaper",
    "broadcaster",
    "magazine",
    "independent",
    "state_media",
    "NGO",
    "official",
}
_SOURCE_FEED_TYPES = {"rss", "homepage", "manual"}
_SOURCE_SYNC_SCOPES = {"source", "active_sources", "feed"}
_SOURCE_SYNC_STATUSES = {"completed", "partial", "failed", "skipped"}
_SOURCE_REVIEW_STATUSES = {"needs_review", "reviewed", "quarantined", "disabled"}
_SOURCE_FEED_STATUSES = {"active", "paused", "quarantined", "disabled"}
_SOURCE_OPS_ALERT_SEVERITIES = {"info", "warning", "critical"}
_SOURCE_OPS_ALERT_STATUSES = {"active", "acknowledged", "resolved"}
_SOURCE_OPS_DELIVERY_STATUSES = {"delivered", "failed", "skipped"}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def _as_float(value, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _parse_datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _database_url() -> str:
    url = settings.DATABASE_URL or ""
    if url.startswith("postgres://"):
        return "postgresql://" + url.removeprefix("postgres://")
    if url.startswith("postgresql+asyncpg://"):
        return "postgresql://" + url.removeprefix("postgresql+asyncpg://")
    if url.startswith("postgresql+psycopg2://"):
        return "postgresql://" + url.removeprefix("postgresql+psycopg2://")
    return url


def database_enabled() -> bool:
    return _database_url().startswith("postgresql://")


def _connect():
    kwargs = {"connect_timeout": 10}
    if settings.DATABASE_SSLMODE:
        kwargs["sslmode"] = settings.DATABASE_SSLMODE
    return psycopg2.connect(_database_url(), **kwargs)


def _ensure_schema():
    global _SCHEMA_READY
    if _SCHEMA_READY or not database_enabled():
        return

    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                    """
                )
                cur.execute(
                    """
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
                    """
                )
                cur.execute(
                    """
                    create table if not exists public.topics (
                        id uuid primary key,
                        session_id text,
                        name text not null,
                        description text,
                        keywords jsonb not null default '[]'::jsonb,
                        created_at timestamptz not null default now(),
                        updated_at timestamptz not null default now()
                    );
                    """
                )
                cur.execute(
                    """
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
                    """
                )
                cur.execute(
                    """
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
                    """
                )
                cur.execute(
                    """
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
                    """
                )
                cur.execute(
                    """
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
                        review_status text not null default 'needs_review',
                        review_notes text,
                        disabled_reason text,
                        quality_score double precision not null default 0,
                        terms_reviewed_at timestamptz,
                        last_reviewed_at timestamptz,
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
                        disabled_reason text,
                        review_notes text,
                        last_reviewed_at timestamptz,
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

                    create table if not exists public.source_sync_runs (
                        id uuid primary key,
                        session_id text,
                        sync_scope text not null default 'source' check (
                            sync_scope in ('source', 'active_sources', 'feed')
                        ),
                        status text not null default 'completed' check (
                            status in ('completed', 'partial', 'failed', 'skipped')
                        ),
                        source_id uuid references public.sources(id) on delete set null,
                        source_feed_id uuid references public.source_feeds(id) on delete set null,
                        source_name text,
                        started_at timestamptz not null default now(),
                        finished_at timestamptz,
                        duration_ms integer not null default 0,
                        source_count integer not null default 0,
                        feed_count integer not null default 0,
                        synced_feed_count integer not null default 0,
                        article_count integer not null default 0,
                        card_count integer not null default 0,
                        error_count integer not null default 0,
                        limits jsonb not null default '{}'::jsonb,
                        errors jsonb not null default '[]'::jsonb,
                        summary jsonb not null default '{}'::jsonb,
                        created_at timestamptz not null default now()
                    );

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

                    create table if not exists public.source_ops_alert_deliveries (
                        id uuid primary key,
                        alert_id uuid not null references public.source_ops_alerts(id) on delete cascade,
                        alert_updated_at timestamptz,
                        destination_type text not null default 'webhook',
                        destination_url text,
                        status text not null default 'failed' check (
                            status in ('delivered', 'failed', 'skipped')
                        ),
                        attempt_count integer not null default 1,
                        response_status integer,
                        error text,
                        payload jsonb not null default '{}'::jsonb,
                        created_at timestamptz not null default now(),
                        delivered_at timestamptz
                    );
                    """
                )

                for statement in [
                    "alter table public.feed_cards add column if not exists session_id text;",
                    "alter table public.feed_cards add column if not exists topic_id text;",
                    "alter table public.feed_cards add column if not exists article_id text;",
                    "alter table public.feed_cards add column if not exists alert_id text;",
                    "alter table public.feed_cards add column if not exists report_id text;",
                    "alter table public.feed_cards add column if not exists source_id uuid;",
                    "alter table public.feed_cards add column if not exists source_feed_id uuid;",
                    "alter table public.feed_cards add column if not exists ingested_article_id uuid;",
                    "alter table public.feed_cards add column if not exists node_id uuid;",
                    "alter table public.feed_cards add column if not exists comparison_id uuid;",
                    "alter table public.feed_cards add column if not exists source text;",
                    "alter table public.feed_cards add column if not exists url text;",
                    "alter table public.feed_cards add column if not exists topic text;",
                    "alter table public.feed_cards add column if not exists priority_score double precision default 0.5;",
                    "alter table public.feed_cards add column if not exists personalized_score double precision default 0.5;",
                    "alter table public.feed_cards add column if not exists narrative_signal text;",
                    "alter table public.feed_cards add column if not exists evidence_score double precision;",
                    "alter table public.feed_cards add column if not exists framing text;",
                    "alter table public.feed_cards add column if not exists payload jsonb not null default '{}'::jsonb;",
                    "alter table public.feed_cards add column if not exists recommendations jsonb not null default '[]'::jsonb;",
                    "alter table public.feed_cards add column if not exists explanation jsonb not null default '{}'::jsonb;",
                    "alter table public.feed_cards add column if not exists analysis jsonb not null default '{}'::jsonb;",
                    "alter table public.feed_cards add column if not exists is_read boolean not null default false;",
                    "alter table public.feed_cards add column if not exists is_saved boolean not null default false;",
                    "alter table public.feed_cards add column if not exists is_dismissed boolean not null default false;",
                    "alter table public.feed_cards add column if not exists created_at timestamptz not null default now();",
                    "alter table public.feed_cards add column if not exists updated_at timestamptz not null default now();",
                    "alter table public.article_analyses add column if not exists session_id text;",
                    "alter table public.article_analyses add column if not exists ingested_article_id uuid;",
                    "alter table public.topics add column if not exists session_id text;",
                    "alter table public.topics add column if not exists description text;",
                    "alter table public.topics add column if not exists keywords jsonb not null default '[]'::jsonb;",
                    "alter table public.topics add column if not exists created_at timestamptz not null default now();",
                    "alter table public.topics add column if not exists updated_at timestamptz not null default now();",
                    "alter table public.monitors add column if not exists session_id text;",
                    "alter table public.monitors add column if not exists topic_id uuid;",
                    "alter table public.monitors add column if not exists keywords jsonb not null default '[]'::jsonb;",
                    "alter table public.monitors add column if not exists status text not null default 'active';",
                    "alter table public.monitors add column if not exists created_at timestamptz not null default now();",
                    "alter table public.monitors add column if not exists updated_at timestamptz not null default now();",
                    "alter table public.feeds add column if not exists session_id text;",
                    "alter table public.feeds add column if not exists topic_id text;",
                    "alter table public.feeds add column if not exists description text;",
                    "alter table public.feeds add column if not exists status text not null default 'active';",
                    "alter table public.feeds add column if not exists last_synced_at timestamptz;",
                    "alter table public.feeds add column if not exists created_at timestamptz not null default now();",
                    "alter table public.feeds add column if not exists updated_at timestamptz not null default now();",
                    "alter table public.feed_items add column if not exists session_id text;",
                    "alter table public.feed_items add column if not exists feed_id uuid;",
                    "alter table public.feed_items add column if not exists external_id text;",
                    "alter table public.feed_items add column if not exists source text;",
                    "alter table public.feed_items add column if not exists published_at timestamptz;",
                    "alter table public.feed_items add column if not exists raw jsonb not null default '{}'::jsonb;",
                    "alter table public.feed_items add column if not exists created_at timestamptz not null default now();",
                    "alter table public.sources add column if not exists review_status text not null default 'needs_review';",
                    "alter table public.sources add column if not exists review_notes text;",
                    "alter table public.sources add column if not exists disabled_reason text;",
                    "alter table public.sources add column if not exists quality_score double precision not null default 0;",
                    "alter table public.sources add column if not exists terms_reviewed_at timestamptz;",
                    "alter table public.sources add column if not exists last_reviewed_at timestamptz;",
                    "alter table public.source_feeds add column if not exists disabled_reason text;",
                    "alter table public.source_feeds add column if not exists review_notes text;",
                    "alter table public.source_feeds add column if not exists last_reviewed_at timestamptz;",
                    """
                    create index if not exists idx_feed_cards_visible_created
                    on public.feed_cards (session_id, is_dismissed, created_at desc);
                    """,
                    """
                    create index if not exists idx_feed_cards_saved
                    on public.feed_cards (is_saved)
                    where is_saved = true;
                    """,
                    """
                    create index if not exists idx_feed_cards_unread
                    on public.feed_cards (is_read)
                    where is_read = false;
                    """,
                    """
                    create index if not exists idx_feed_cards_article_id
                    on public.feed_cards (session_id, article_id);
                    """,
                    """
                    create index if not exists idx_feed_cards_report_id
                    on public.feed_cards (session_id, report_id);
                    """,
                    """
                    create index if not exists idx_article_analyses_final_url
                    on public.article_analyses (session_id, final_url);
                    """,
                    """
                    create index if not exists idx_article_analyses_session_created
                    on public.article_analyses (session_id, created_at desc);
                    """,
                    """
                    create index if not exists idx_topics_session_created
                    on public.topics (session_id, created_at desc);
                    """,
                    """
                    create index if not exists idx_monitors_session_topic
                    on public.monitors (session_id, topic_id);
                    """,
                    """
                    create index if not exists idx_feeds_session_created
                    on public.feeds (session_id, created_at desc);
                    """,
                    """
                    create index if not exists idx_feed_items_session_feed
                    on public.feed_items (session_id, feed_id, created_at desc);
                    """,
                    """
                    create unique index if not exists idx_feed_items_feed_url_unique
                    on public.feed_items (feed_id, url)
                    where url is not null;
                    """,
                    """
                    create unique index if not exists idx_sources_website_url_unique
                    on public.sources (website_url)
                    where website_url is not null;
                    """,
                    """
                    create unique index if not exists idx_sources_rss_url_unique
                    on public.sources (rss_url)
                    where rss_url is not null;
                    """,
                    """
                    create index if not exists idx_sources_country_language
                    on public.sources (country, language);
                    """,
                    """
                    create index if not exists idx_sources_review_status
                    on public.sources (review_status);
                    """,
                    """
                    create index if not exists idx_sources_quality_score
                    on public.sources (quality_score);
                    """,
                    """
                    create index if not exists idx_source_feeds_source_status
                    on public.source_feeds (source_id, status);
                    """,
                    """
                    create index if not exists idx_source_feeds_status
                    on public.source_feeds (status);
                    """,
                    """
                    create unique index if not exists idx_source_feeds_source_url_unique
                    on public.source_feeds (source_id, feed_url);
                    """,
                    """
                    create index if not exists idx_ingested_articles_source_published
                    on public.ingested_articles (source_id, published_at desc);
                    """,
                    """
                    create index if not exists idx_ingested_articles_fingerprint
                    on public.ingested_articles (event_fingerprint);
                    """,
                    """
                    create unique index if not exists idx_ingested_articles_source_url_unique
                    on public.ingested_articles (source_id, url);
                    """,
                    """
                    create index if not exists idx_nodes_type_label
                    on public.nodes (node_type, label);
                    """,
                    """
                    create index if not exists idx_nodes_source
                    on public.nodes (source_id);
                    """,
                    """
                    create index if not exists idx_nodes_ingested_article
                    on public.nodes (ingested_article_id);
                    """,
                    """
                    create index if not exists idx_node_edges_from_type
                    on public.node_edges (from_node_id, edge_type);
                    """,
                    """
                    create index if not exists idx_node_edges_to_type
                    on public.node_edges (to_node_id, edge_type);
                    """,
                    """
                    create unique index if not exists idx_article_comparisons_pair_unique
                    on public.article_comparisons (base_article_id, comparison_article_id);
                    """,
                    """
                    create index if not exists idx_article_comparisons_base_similarity
                    on public.article_comparisons (base_article_id, similarity_score desc);
                    """,
                    """
                    create index if not exists idx_feed_cards_ingested_article
                    on public.feed_cards (session_id, ingested_article_id);
                    """,
                    """
                    create table if not exists public.source_sync_runs (
                        id uuid primary key,
                        session_id text,
                        sync_scope text not null default 'source' check (
                            sync_scope in ('source', 'active_sources', 'feed')
                        ),
                        status text not null default 'completed' check (
                            status in ('completed', 'partial', 'failed', 'skipped')
                        ),
                        source_id uuid references public.sources(id) on delete set null,
                        source_feed_id uuid references public.source_feeds(id) on delete set null,
                        source_name text,
                        started_at timestamptz not null default now(),
                        finished_at timestamptz,
                        duration_ms integer not null default 0,
                        source_count integer not null default 0,
                        feed_count integer not null default 0,
                        synced_feed_count integer not null default 0,
                        article_count integer not null default 0,
                        card_count integer not null default 0,
                        error_count integer not null default 0,
                        limits jsonb not null default '{}'::jsonb,
                        errors jsonb not null default '[]'::jsonb,
                        summary jsonb not null default '{}'::jsonb,
                        created_at timestamptz not null default now()
                    );
                    """,
                    """
                    create index if not exists idx_source_sync_runs_started
                    on public.source_sync_runs (started_at desc);
                    """,
                    """
                    create index if not exists idx_source_sync_runs_source_started
                    on public.source_sync_runs (source_id, started_at desc);
                    """,
                    """
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
                    """,
                    """
                    create table if not exists public.source_ops_alert_deliveries (
                        id uuid primary key,
                        alert_id uuid not null references public.source_ops_alerts(id) on delete cascade,
                        alert_updated_at timestamptz,
                        destination_type text not null default 'webhook',
                        destination_url text,
                        status text not null default 'failed' check (
                            status in ('delivered', 'failed', 'skipped')
                        ),
                        attempt_count integer not null default 1,
                        response_status integer,
                        error text,
                        payload jsonb not null default '{}'::jsonb,
                        created_at timestamptz not null default now(),
                        delivered_at timestamptz
                    );
                    """,
                    """
                    create index if not exists idx_source_ops_alert_deliveries_alert_created
                    on public.source_ops_alert_deliveries (alert_id, created_at desc);
                    """,
                    """
                    create index if not exists idx_source_ops_alert_deliveries_status_created
                    on public.source_ops_alert_deliveries (status, created_at desc);
                    """,
                    """
                    create index if not exists idx_source_ops_alerts_status_severity
                    on public.source_ops_alerts (status, severity, updated_at desc);
                    """,
                    """
                    create index if not exists idx_source_ops_alerts_source
                    on public.source_ops_alerts (source_id, status, updated_at desc);
                    """,
                    """
                    create index if not exists idx_source_ops_alerts_feed
                    on public.source_ops_alerts (source_feed_id, status, updated_at desc);
                    """,
                ]:
                    cur.execute(statement)
        _SCHEMA_READY = True
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Database schema check failed: {exc}") from exc


def check_database() -> dict:
    if not database_enabled():
        return {
            "status": "fallback",
            "enabled": False,
            "message": "DATABASE_URL is not Postgres; using in-memory feed storage.",
        }

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("select count(*) as feed_card_count from public.feed_cards;")
                row = cur.fetchone() or {}
                return {
                    "status": "ok",
                    "enabled": True,
                    "feed_card_count": row.get("feed_card_count", 0),
                }
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Database readiness check failed: {exc}") from exc


def _analyzed_memory_cards(session_id: str, window_seconds: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc).timestamp() - window_seconds
    cards = []
    for card in FEED_CARDS:
        if (card.get("session_id") or ANONYMOUS_SESSION_ID) != session_id:
            continue
        payload = card.get("payload") or {}
        if not payload.get("article_excerpt"):
            continue
        created_at = _parse_datetime(card.get("created_at"))
        if created_at and created_at.timestamp() >= cutoff:
            cards.append(card)
    return cards


def get_analyze_usage(session_id: str) -> dict:
    limit = max(0, settings.ANALYZE_DAILY_LIMIT)
    window_seconds = max(60, settings.ANALYZE_QUOTA_WINDOW_SECONDS)
    cooldown_seconds = max(0, settings.ANALYZE_COOLDOWN_SECONDS)
    now = datetime.now(timezone.utc)

    if not settings.ANALYZE_QUOTA_ENABLED:
        return {
            "quota_enabled": False,
            "session_id": session_id,
            "daily_limit": limit,
            "used": 0,
            "remaining": limit,
            "window_seconds": window_seconds,
            "cooldown_seconds": cooldown_seconds,
            "cooldown_remaining_seconds": 0,
            "reset_at": None,
        }

    if not database_enabled():
        cards = _analyzed_memory_cards(session_id, window_seconds)
        created_values = [_parse_datetime(card.get("created_at")) for card in cards]
        created_values = [value for value in created_values if value]
        used = len(created_values)
        oldest = min(created_values) if created_values else None
        latest = max(created_values) if created_values else None
    else:
        _ensure_schema()
        try:
            with _connect() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        select
                            count(*) as used,
                            min(created_at) as oldest_created_at,
                            max(created_at) as latest_created_at
                        from public.article_analyses
                        where coalesce(session_id, %s) = %s
                        and created_at >= now() - (%s * interval '1 second');
                        """,
                        (ANONYMOUS_SESSION_ID, session_id, window_seconds),
                    )
                    row = cur.fetchone() or {}
                    used = int(row.get("used") or 0)
                    oldest = _parse_datetime(row.get("oldest_created_at"))
                    latest = _parse_datetime(row.get("latest_created_at"))
        except psycopg2.Error as exc:
            raise FeedStoreError(f"Could not load analyze usage: {exc}") from exc

    cooldown_remaining = 0
    if latest and cooldown_seconds:
        elapsed = max(0, int((now - latest).total_seconds()))
        cooldown_remaining = max(0, cooldown_seconds - elapsed)

    reset_at = None
    if oldest:
        reset_at = datetime.fromtimestamp(
            oldest.timestamp() + window_seconds,
            tz=timezone.utc,
        ).isoformat()

    return {
        "quota_enabled": True,
        "session_id": session_id,
        "daily_limit": limit,
        "used": used,
        "remaining": max(0, limit - used),
        "window_seconds": window_seconds,
        "cooldown_seconds": cooldown_seconds,
        "cooldown_remaining_seconds": cooldown_remaining,
        "reset_at": reset_at,
    }


def enforce_analyze_quota(session_id: str) -> dict:
    usage = get_analyze_usage(session_id)
    if not usage.get("quota_enabled"):
        return usage

    if usage["cooldown_remaining_seconds"] > 0:
        seconds = usage["cooldown_remaining_seconds"]
        raise QuotaExceededError(
            f"Please wait {seconds} seconds before analyzing another article.",
            usage,
        )

    if usage["used"] >= usage["daily_limit"]:
        raise QuotaExceededError(
            "Daily analyze quota reached. Try again after the quota window resets.",
            usage,
        )

    return usage


def _priority_score(priority: str, confidence: float) -> float:
    base = {"low": 0.35, "medium": 0.65, "high": 0.88}.get(priority, 0.65)
    return round(max(0.05, min(0.98, base * 0.75 + confidence * 0.25)), 3)


def _dominant_frame(analysis: dict) -> str:
    frames = analysis.get("narrative_framing") or []
    return frames[0] if frames else "general_news_frame"


def build_card(article: ExtractedArticle, analysis: dict, session_id: str = ANONYMOUS_SESSION_ID) -> dict:
    card_id = str(uuid4())
    article_id = str(uuid4())
    confidence = float(analysis.get("confidence") or 0.5)
    priority = analysis.get("priority") or "medium"
    score = _priority_score(priority, confidence)
    dominant_frame = _dominant_frame(analysis)
    claim_count = len(analysis.get("key_claims") or [])

    payload = {
        "source": analysis.get("source") or article.source,
        "domain": article.domain,
        "url": analysis.get("url") or article.final_url,
        "claim_count": claim_count,
        "dominant_frame": dominant_frame,
        "key_claims": analysis.get("key_claims") or [],
        "narrative_framing": analysis.get("narrative_framing") or [],
        "entities": analysis.get("entities") or [],
        "topics": analysis.get("topics") or [],
        "confidence": confidence,
        "priority": priority,
        "article_excerpt": article.excerpt,
    }

    return {
        "id": card_id,
        "session_id": session_id,
        "topic_id": None,
        "article_id": article_id,
        "alert_id": None,
        "report_id": article_id,
        "card_type": "article_insight",
        "title": analysis.get("title") or article.title,
        "summary": analysis.get("summary") or article.excerpt,
        "source": payload["source"],
        "url": payload["url"],
        "topic": ", ".join((analysis.get("topics") or [])[:2]) or "Article analysis",
        "priority": score,
        "priority_score": score,
        "personalized_score": score,
        "narrative_signal": (
            f"Dominant frame: {dominant_frame}. Confidence reflects extraction and analysis quality, "
            "not truth certainty."
        ),
        "evidence_score": confidence,
        "framing": dominant_frame,
        "payload": payload,
        "recommendations": [
            {
                "type": "open_report",
                "label": "Open report",
                "href": f"/reports/{article_id}",
                "reason": "Review claims, framing, and extracted evidence signals.",
            },
            {
                "type": "compare",
                "label": "Compare with another article",
                "href": "/compare",
                "reason": "Check whether other sources frame the same story differently.",
            },
        ],
        "explanation": {
            "why_this_matters": (
                "This article was converted into structured narrative signals so its claims, "
                "framing, and source context can be inspected."
            ),
            "what_changed": {
                "key_claims": analysis.get("key_claims") or [],
                "narrative_framing": analysis.get("narrative_framing") or [],
                "entities": analysis.get("entities") or [],
            },
            "recommended_action": "Open the report or compare it against another article before drawing conclusions.",
        },
        "analysis": analysis,
        "is_read": False,
        "is_saved": False,
        "is_dismissed": False,
        "created_at": now_iso(),
    }


def _row_to_card(row: dict) -> dict:
    if not row:
        return {}

    created_at = row.get("created_at")
    if isinstance(created_at, datetime):
        created_at = created_at.isoformat()

    priority_score = row.get("priority_score")
    if priority_score is None:
        priority_score = row.get("priority") or 0.5

    def as_float(value, fallback: float = 0.5) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return fallback

    return {
        "id": str(row.get("id")),
        "session_id": row.get("session_id") or ANONYMOUS_SESSION_ID,
        "topic_id": row.get("topic_id"),
        "article_id": str(row.get("article_id")) if row.get("article_id") else None,
        "alert_id": row.get("alert_id"),
        "report_id": str(row.get("report_id")) if row.get("report_id") else None,
        "source_id": str(row.get("source_id")) if row.get("source_id") else None,
        "source_feed_id": str(row.get("source_feed_id")) if row.get("source_feed_id") else None,
        "ingested_article_id": str(row.get("ingested_article_id")) if row.get("ingested_article_id") else None,
        "node_id": str(row.get("node_id")) if row.get("node_id") else None,
        "comparison_id": str(row.get("comparison_id")) if row.get("comparison_id") else None,
        "card_type": row.get("card_type") or "article_insight",
        "title": row.get("title") or "Untitled analysis",
        "summary": row.get("summary") or "",
        "source": row.get("source"),
        "url": row.get("url"),
        "topic": row.get("topic"),
        "priority": as_float(row.get("priority"), as_float(priority_score)),
        "priority_score": as_float(priority_score),
        "personalized_score": as_float(row.get("personalized_score"), as_float(priority_score)),
        "narrative_signal": row.get("narrative_signal"),
        "evidence_score": row.get("evidence_score"),
        "framing": row.get("framing"),
        "payload": row.get("payload") or {},
        "recommendations": row.get("recommendations") or [],
        "explanation": row.get("explanation") or {},
        "analysis": row.get("analysis") or {},
        "is_read": bool(row.get("is_read")),
        "is_saved": bool(row.get("is_saved")),
        "is_dismissed": bool(row.get("is_dismissed")),
        "created_at": created_at or now_iso(),
    }


def _card_columns() -> str:
    return """
        id,
        session_id,
        topic_id,
        article_id,
        alert_id,
        report_id,
        source_id,
        source_feed_id,
        ingested_article_id,
        node_id,
        comparison_id,
        title,
        summary,
        source,
        url,
        topic,
        card_type,
        priority,
        priority_score,
        personalized_score,
        narrative_signal,
        evidence_score,
        framing,
        payload,
        recommendations,
        explanation,
        analysis,
        is_read,
        is_saved,
        is_dismissed,
        created_at
    """


def _normalize_keywords(keywords: list[str] | None) -> list[str]:
    cleaned = []
    for keyword in keywords or []:
        value = " ".join(str(keyword).strip().split())
        if value and value.lower() not in {item.lower() for item in cleaned}:
            cleaned.append(value[:80])
        if len(cleaned) >= 20:
            break
    return cleaned


def _row_created_at(row: dict) -> str:
    created_at = row.get("created_at")
    if isinstance(created_at, datetime):
        return created_at.isoformat()
    return created_at or now_iso()


def _row_to_topic(row: dict, monitor: dict | None = None) -> dict:
    return {
        "id": str(row.get("id")),
        "session_id": row.get("session_id") or ANONYMOUS_SESSION_ID,
        "name": row.get("name") or "Untitled topic",
        "description": row.get("description"),
        "keywords": row.get("keywords") or [],
        "monitor": monitor,
        "created_at": _row_created_at(row),
        "article_count": int(row.get("article_count") or 0),
    }


def _row_to_monitor(row: dict) -> dict:
    return {
        "id": str(row.get("id")),
        "session_id": row.get("session_id") or ANONYMOUS_SESSION_ID,
        "topic_id": str(row.get("topic_id")) if row.get("topic_id") else None,
        "name": row.get("name") or "Untitled monitor",
        "keywords": row.get("keywords") or [],
        "status": row.get("status") or "active",
        "created_at": _row_created_at(row),
    }


def _build_topic_feed_card(topic: dict, monitor: dict) -> dict:
    topic_id = topic["id"]
    keywords = topic.get("keywords") or []
    keyword_text = ", ".join(keywords[:5]) if keywords else "No keywords yet"
    return {
        "id": str(uuid4()),
        "session_id": topic.get("session_id") or ANONYMOUS_SESSION_ID,
        "topic_id": topic_id,
        "article_id": None,
        "alert_id": None,
        "report_id": None,
        "card_type": "topic_monitor",
        "title": f"Topic monitor created: {topic['name']}",
        "summary": f"Monitoring started for: {keyword_text}. New matching coverage will appear in this feed.",
        "source": None,
        "url": None,
        "topic": topic["name"],
        "priority": 0.55,
        "priority_score": 0.55,
        "personalized_score": 0.55,
        "narrative_signal": "Topic subscription is active. This is a monitoring signal, not a truth verdict.",
        "evidence_score": None,
        "framing": None,
        "payload": {
            "topic_id": topic_id,
            "monitor_id": monitor["id"],
            "keywords": keywords,
            "status": monitor.get("status", "active"),
        },
        "recommendations": [
            {
                "type": "open_topic",
                "label": "Open topic",
                "href": f"/topics/{topic_id}",
                "reason": "Review monitored keywords and future coverage changes.",
            }
        ],
        "explanation": {
            "why_this_matters": "A topic monitor turns keyword subscriptions into feed signals.",
            "what_changed": {"monitor_status": monitor.get("status", "active"), "keywords": keywords},
            "recommended_action": "Use the topic page to track coverage as new sources are ingested.",
        },
        "analysis": {},
        "is_read": False,
        "is_saved": False,
        "is_dismissed": False,
        "created_at": now_iso(),
    }


def _insert_memory_topic_card(topic: dict, monitor: dict) -> dict:
    card = _build_topic_feed_card(topic, monitor)
    FEED_CARDS.insert(0, card)
    return card


def create_topic_monitor(
    name: str,
    description: str | None = None,
    keywords: list[str] | None = None,
    session_id: str = ANONYMOUS_SESSION_ID,
) -> dict:
    topic_id = str(uuid4())
    monitor_id = str(uuid4())
    clean_keywords = _normalize_keywords(keywords)
    now = now_iso()

    if not database_enabled():
        topic = {
            "id": topic_id,
            "session_id": session_id,
            "name": name.strip()[:160],
            "description": description.strip()[:500] if description else None,
            "keywords": clean_keywords,
            "created_at": now,
            "article_count": 0,
        }
        monitor = {
            "id": monitor_id,
            "session_id": session_id,
            "topic_id": topic_id,
            "name": topic["name"],
            "keywords": clean_keywords,
            "status": "active",
            "created_at": now,
        }
        topic["monitor"] = monitor
        TOPICS.insert(0, topic)
        MONITORS.insert(0, monitor)
        card = _insert_memory_topic_card(topic, monitor)
        return {"topic": topic, "monitor": monitor, "card": card}

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    insert into public.topics (
                        id, session_id, name, description, keywords, created_at, updated_at
                    )
                    values (%s, %s, %s, %s, %s, now(), now())
                    returning id, session_id, name, description, keywords, created_at;
                    """,
                    (
                        topic_id,
                        session_id,
                        name.strip()[:160],
                        description.strip()[:500] if description else None,
                        Json(clean_keywords),
                    ),
                )
                topic = _row_to_topic(cur.fetchone())
                cur.execute(
                    """
                    insert into public.monitors (
                        id, session_id, topic_id, name, keywords, status, created_at, updated_at
                    )
                    values (%s, %s, %s, %s, %s, 'active', now(), now())
                    returning id, session_id, topic_id, name, keywords, status, created_at;
                    """,
                    (
                        monitor_id,
                        session_id,
                        topic_id,
                        topic["name"],
                        Json(clean_keywords),
                    ),
                )
                monitor = _row_to_monitor(cur.fetchone())
                topic["monitor"] = monitor
                card = _build_topic_feed_card(topic, monitor)
                cur.execute(
                    f"""
                    insert into public.feed_cards (
                        id, session_id, topic_id, article_id, alert_id, report_id, title,
                        summary, source, url, topic, card_type, priority, priority_score,
                        personalized_score, narrative_signal, evidence_score, framing,
                        payload, recommendations, explanation, analysis, is_read, is_saved,
                        is_dismissed, created_at, updated_at
                    )
                    values (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
                    )
                    returning {_card_columns()};
                    """,
                    (
                        card["id"],
                        card["session_id"],
                        card["topic_id"],
                        card["article_id"],
                        card["alert_id"],
                        card["report_id"],
                        card["title"],
                        card["summary"],
                        card["source"],
                        card["url"],
                        card["topic"],
                        card["card_type"],
                        card["priority"],
                        card["priority_score"],
                        card["personalized_score"],
                        card["narrative_signal"],
                        card["evidence_score"],
                        card["framing"],
                        Json(card["payload"]),
                        Json(card["recommendations"]),
                        Json(card["explanation"]),
                        Json(card["analysis"]),
                        card["is_read"],
                        card["is_saved"],
                        card["is_dismissed"],
                        card["created_at"],
                    ),
                )
                return {"topic": topic, "monitor": monitor, "card": _row_to_card(cur.fetchone())}
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not create topic monitor: {exc}") from exc


def list_topics(session_id: str = ANONYMOUS_SESSION_ID) -> list[dict]:
    if not database_enabled():
        topics = [
            topic
            for topic in TOPICS
            if (topic.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
        ]
        return sorted(topics, key=lambda topic: topic.get("created_at", ""), reverse=True)

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select
                        t.id,
                        t.session_id,
                        t.name,
                        t.description,
                        t.keywords,
                        t.created_at,
                        count(fc.id) filter (where fc.card_type = 'article_insight') as article_count
                    from public.topics t
                    left join public.feed_cards fc
                        on fc.topic_id = t.id::text
                        and coalesce(fc.session_id, %s) = %s
                    where coalesce(t.session_id, %s) = %s
                    group by t.id, t.session_id, t.name, t.description, t.keywords, t.created_at
                    order by t.created_at desc;
                    """,
                    (ANONYMOUS_SESSION_ID, session_id, ANONYMOUS_SESSION_ID, session_id),
                )
                topic_rows = cur.fetchall()
                if not topic_rows:
                    return []

                topic_ids = [row["id"] for row in topic_rows]
                cur.execute(
                    """
                    select id, session_id, topic_id, name, keywords, status, created_at
                    from public.monitors
                    where coalesce(session_id, %s) = %s
                    and topic_id = any(%s);
                    """,
                    (ANONYMOUS_SESSION_ID, session_id, topic_ids),
                )
                monitors = {
                    str(row["topic_id"]): _row_to_monitor(row) for row in cur.fetchall()
                }
                return [
                    _row_to_topic(row, monitor=monitors.get(str(row["id"])))
                    for row in topic_rows
                ]
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not list topics: {exc}") from exc


def get_topic_detail(topic_id: str, session_id: str = ANONYMOUS_SESSION_ID) -> dict | None:
    topics = list_topics(session_id=session_id)
    topic = next((item for item in topics if item["id"] == topic_id), None)
    if not topic:
        return None

    related_cards = [
        card
        for card in list_feed_cards(filter_type="all", limit=100, session_id=session_id)
        if card.get("topic_id") == topic_id
    ]
    latest_articles = [card for card in related_cards if card.get("card_type") == "article_insight"][:10]
    return {
        "topic": topic,
        "ecosystem_summary": {
            "article_count": len(latest_articles),
            "claim_count": sum((card.get("payload") or {}).get("claim_count", 0) for card in latest_articles),
            "source_count": len({card.get("source") for card in latest_articles if card.get("source")}),
            "dominant_frame": None,
            "source_diversity_score": 0.0,
            "dominant_sources": [],
            "frame_distribution": [],
        },
        "latest_articles": latest_articles,
        "narrative_clusters": [],
        "narrative_timeline": [],
    }


def _row_to_feed(row: dict) -> dict:
    last_synced_at = row.get("last_synced_at")
    if isinstance(last_synced_at, datetime):
        last_synced_at = last_synced_at.isoformat()

    return {
        "id": str(row.get("id")),
        "session_id": row.get("session_id") or ANONYMOUS_SESSION_ID,
        "topic_id": row.get("topic_id"),
        "url": row.get("url"),
        "title": row.get("title") or row.get("url"),
        "description": row.get("description"),
        "status": row.get("status") or "active",
        "last_synced_at": last_synced_at,
        "created_at": _row_created_at(row),
        "item_count": int(row.get("item_count") or 0),
    }


def _row_to_feed_item(row: dict) -> dict:
    published_at = row.get("published_at")
    if isinstance(published_at, datetime):
        published_at = published_at.isoformat()

    return {
        "id": str(row.get("id")),
        "session_id": row.get("session_id") or ANONYMOUS_SESSION_ID,
        "feed_id": str(row.get("feed_id")) if row.get("feed_id") else None,
        "external_id": row.get("external_id"),
        "title": row.get("title") or "Untitled feed item",
        "summary": row.get("summary") or "",
        "url": row.get("url"),
        "source": row.get("source"),
        "published_at": published_at,
        "raw": row.get("raw") or {},
        "created_at": _row_created_at(row),
    }


def _clean_text(value, limit: int, default: str | None = None) -> str | None:
    if value is None:
        return default
    cleaned = " ".join(str(value).strip().split())
    if not cleaned:
        return default
    return cleaned[:limit]


def _clean_url(value) -> str | None:
    cleaned = _clean_text(value, 1000)
    return cleaned


def _domain_from_url(url: str | None) -> str | None:
    if not url:
        return None
    try:
        domain = urlparse(url).netloc.lower().removeprefix("www.")
        return domain or None
    except ValueError:
        return None


def _source_name_from_inputs(name: str | None, website_url: str | None, rss_url: str | None) -> str:
    clean_name = _clean_text(name, 180)
    if clean_name:
        return clean_name
    domain = _domain_from_url(website_url) or _domain_from_url(rss_url)
    return domain or "Manual source"


def _normalize_source_size(value: str | None) -> str:
    clean = _clean_text(value, 20)
    return clean if clean in _SOURCE_SIZES else "medium"


def _normalize_source_type(value: str | None) -> str:
    clean = _clean_text(value, 40)
    if clean and clean.lower() == "ngo":
        return "NGO"
    return clean if clean in _SOURCE_TYPES else "newspaper"


def _normalize_source_feed_type(value: str | None) -> str:
    clean = _clean_text(value, 30)
    return clean if clean in _SOURCE_FEED_TYPES else "rss"


def _normalize_source_review_status(value: str | None) -> str:
    clean = _clean_text(value, 40)
    return clean if clean in _SOURCE_REVIEW_STATUSES else "needs_review"


def _normalize_source_feed_status(value: str | None) -> str:
    clean = _clean_text(value, 40)
    return clean if clean in _SOURCE_FEED_STATUSES else "active"


def _row_optional_datetime(row: dict, key: str) -> str | None:
    value = row.get(key)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _row_to_source_record(row: dict) -> dict:
    return {
        "id": str(row.get("id")),
        "name": row.get("name") or "Untitled source",
        "website_url": row.get("website_url"),
        "rss_url": row.get("rss_url"),
        "country": row.get("country"),
        "language": row.get("language"),
        "region": row.get("region"),
        "political_context": row.get("political_context"),
        "source_size": row.get("source_size") or "medium",
        "source_type": row.get("source_type") or "newspaper",
        "credibility_notes": row.get("credibility_notes"),
        "notes": row.get("notes"),
        "is_default": bool(row.get("is_default")),
        "review_status": row.get("review_status") or "needs_review",
        "review_notes": row.get("review_notes"),
        "disabled_reason": row.get("disabled_reason"),
        "quality_score": _as_float(row.get("quality_score"), 0.0),
        "terms_reviewed_at": _row_optional_datetime(row, "terms_reviewed_at"),
        "last_reviewed_at": _row_optional_datetime(row, "last_reviewed_at"),
        "feed_count": int(row.get("feed_count") or 0),
        "article_count": int(row.get("article_count") or 0),
        "created_at": _row_created_at(row),
        "updated_at": _row_created_at({"created_at": row.get("updated_at") or row.get("created_at")}),
    }


def _row_to_source_feed_record(row: dict) -> dict:
    last_checked_at = row.get("last_checked_at")
    last_success_at = row.get("last_success_at")
    if isinstance(last_checked_at, datetime):
        last_checked_at = last_checked_at.isoformat()
    if isinstance(last_success_at, datetime):
        last_success_at = last_success_at.isoformat()

    return {
        "id": str(row.get("id")),
        "source_id": str(row.get("source_id")) if row.get("source_id") else None,
        "feed_url": row.get("feed_url"),
        "feed_type": row.get("feed_type") or "rss",
        "title": row.get("title"),
        "language": row.get("language"),
        "country": row.get("country"),
        "status": row.get("status") or "active",
        "fetch_interval_minutes": int(row.get("fetch_interval_minutes") or 60),
        "last_checked_at": last_checked_at,
        "last_success_at": last_success_at,
        "last_error": row.get("last_error"),
        "disabled_reason": row.get("disabled_reason"),
        "review_notes": row.get("review_notes"),
        "last_reviewed_at": _row_optional_datetime(row, "last_reviewed_at"),
        "created_at": _row_created_at(row),
        "updated_at": _row_created_at({"created_at": row.get("updated_at") or row.get("created_at")}),
    }


def _row_to_ingested_article_record(row: dict) -> dict:
    published_at = row.get("published_at")
    if isinstance(published_at, datetime):
        published_at = published_at.isoformat()

    article = {
        "id": str(row.get("id")),
        "source_id": str(row.get("source_id")) if row.get("source_id") else None,
        "source_feed_id": str(row.get("source_feed_id")) if row.get("source_feed_id") else None,
        "feed_item_id": str(row.get("feed_item_id")) if row.get("feed_item_id") else None,
        "article_analysis_id": str(row.get("article_analysis_id")) if row.get("article_analysis_id") else None,
        "url": row.get("url"),
        "canonical_url": row.get("canonical_url"),
        "title": row.get("title") or "Untitled article",
        "author": row.get("author"),
        "published_at": published_at,
        "language": row.get("language"),
        "country": row.get("country"),
        "summary": row.get("summary") or "",
        "extracted_text": row.get("extracted_text"),
        "content_hash": row.get("content_hash"),
        "event_fingerprint": row.get("event_fingerprint"),
        "comparison_keywords": row.get("comparison_keywords") or [],
        "raw_metadata": row.get("raw_metadata") or {},
        "ingestion_status": row.get("ingestion_status") or "pending",
        "analysis_status": row.get("analysis_status") or "pending",
        "created_at": _row_created_at(row),
        "updated_at": _row_created_at({"created_at": row.get("updated_at") or row.get("created_at")}),
    }
    analysis = row.get("analysis")
    if analysis:
        analysis_created_at = row.get("analysis_created_at")
        if isinstance(analysis_created_at, datetime):
            analysis_created_at = analysis_created_at.isoformat()
        article["article_analysis_id"] = (
            str(row.get("analysis_id"))
            if row.get("analysis_id")
            else article.get("article_analysis_id")
        )
        article["analysis"] = analysis
        article["analysis_created_at"] = analysis_created_at

    if row.get("source_name"):
        article["source"] = {
            "id": str(row.get("source_id")) if row.get("source_id") else None,
            "name": row.get("source_name"),
            "website_url": row.get("source_website_url"),
            "country": row.get("source_country"),
            "language": row.get("source_language"),
            "source_size": row.get("source_size"),
            "source_type": row.get("source_type"),
            "credibility_notes": row.get("source_credibility_notes"),
            "political_context": row.get("source_political_context"),
        }
    if row.get("feed_url"):
        article["source_feed"] = {
            "id": str(row.get("source_feed_id")) if row.get("source_feed_id") else None,
            "feed_url": row.get("feed_url"),
            "feed_type": row.get("feed_type"),
            "title": row.get("feed_title"),
        }

    return article


def _source_counts(source_id: str) -> tuple[int, int]:
    feed_count = len([feed for feed in SOURCE_FEEDS if feed.get("source_id") == source_id])
    article_count = len([article for article in INGESTED_ARTICLES if article.get("source_id") == source_id])
    return feed_count, article_count


def _source_with_memory_counts(source: dict) -> dict:
    feed_count, article_count = _source_counts(source["id"])
    enriched = {**source}
    enriched["feed_count"] = feed_count
    enriched["article_count"] = article_count
    return enriched


def _find_memory_source(website_url: str | None = None, rss_url: str | None = None) -> dict | None:
    for source in SOURCES:
        if website_url and source.get("website_url") == website_url:
            return _source_with_memory_counts(source)
        if rss_url and source.get("rss_url") == rss_url:
            return _source_with_memory_counts(source)
    return None


def create_source_record(
    name: str | None = None,
    website_url: str | None = None,
    rss_url: str | None = None,
    country: str | None = None,
    language: str | None = None,
    region: str | None = None,
    political_context: str | None = None,
    source_size: str | None = None,
    source_type: str | None = None,
    credibility_notes: str | None = None,
    notes: str | None = None,
    is_default: bool = False,
) -> dict:
    clean_website_url = _clean_url(website_url)
    clean_rss_url = _clean_url(rss_url)
    clean_source = {
        "name": _source_name_from_inputs(name, clean_website_url, clean_rss_url),
        "website_url": clean_website_url,
        "rss_url": clean_rss_url,
        "country": _clean_text(country, 80),
        "language": _clean_text(language, 80),
        "region": _clean_text(region, 80),
        "political_context": _clean_text(political_context, 1000),
        "source_size": _normalize_source_size(source_size),
        "source_type": _normalize_source_type(source_type),
        "credibility_notes": _clean_text(credibility_notes, 1000),
        "notes": _clean_text(notes, 1000),
        "is_default": bool(is_default),
        "review_status": "needs_review",
        "review_notes": None,
        "disabled_reason": None,
        "quality_score": 0.0,
        "terms_reviewed_at": None,
        "last_reviewed_at": None,
    }

    if not database_enabled():
        existing = _find_memory_source(clean_website_url, clean_rss_url)
        if existing:
            return existing
        now = now_iso()
        source = {
            "id": str(uuid4()),
                        **clean_source,
                        "feed_count": 0,
                        "article_count": 0,
            "created_at": now,
            "updated_at": now,
        }
        SOURCES.insert(0, source)
        return source

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select
                        s.*,
                        count(distinct sf.id) as feed_count,
                        count(distinct ia.id) as article_count
                    from public.sources s
                    left join public.source_feeds sf on sf.source_id = s.id
                    left join public.ingested_articles ia on ia.source_id = s.id
                    where (%s is not null and s.website_url = %s)
                    or (%s is not null and s.rss_url = %s)
                    group by s.id
                    limit 1;
                    """,
                    (clean_website_url, clean_website_url, clean_rss_url, clean_rss_url),
                )
                row = cur.fetchone()
                if row:
                    return _row_to_source_record(row)

                source_id = str(uuid4())
                cur.execute(
                    """
                    insert into public.sources (
                        id, name, website_url, rss_url, country, language, region,
                        political_context, source_size, source_type, credibility_notes,
                        notes, is_default, review_status, review_notes, disabled_reason,
                        quality_score, terms_reviewed_at, last_reviewed_at, created_at, updated_at
                    )
                    values (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
                    )
                    returning *, 0 as feed_count, 0 as article_count;
                    """,
                    (
                        source_id,
                        clean_source["name"],
                        clean_source["website_url"],
                        clean_source["rss_url"],
                        clean_source["country"],
                        clean_source["language"],
                        clean_source["region"],
                        clean_source["political_context"],
                        clean_source["source_size"],
                        clean_source["source_type"],
                        clean_source["credibility_notes"],
                        clean_source["notes"],
                        clean_source["is_default"],
                        clean_source["review_status"],
                        clean_source["review_notes"],
                        clean_source["disabled_reason"],
                        clean_source["quality_score"],
                        clean_source["terms_reviewed_at"],
                        clean_source["last_reviewed_at"],
                    ),
                )
                return _row_to_source_record(cur.fetchone())
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not create source: {exc}") from exc


def list_source_records(
    limit: int = 100,
    country: str | None = None,
    language: str | None = None,
    source_size: str | None = None,
    source_type: str | None = None,
) -> list[dict]:
    limit = max(1, min(limit, 250))
    clean_country = _clean_text(country, 80)
    clean_language = _clean_text(language, 80)
    clean_size = _clean_text(source_size, 20)
    clean_type = _clean_text(source_type, 40)

    def matches(source: dict) -> bool:
        return (
            (not clean_country or source.get("country") == clean_country)
            and (not clean_language or source.get("language") == clean_language)
            and (not clean_size or source.get("source_size") == clean_size)
            and (not clean_type or source.get("source_type") == clean_type)
        )

    if not database_enabled():
        sources = [_source_with_memory_counts(source) for source in SOURCES if matches(source)]
        return sorted(sources, key=lambda item: (not item.get("is_default"), item.get("name", "")))[:limit]

    _ensure_schema()
    where = []
    params: list[object] = []
    if clean_country:
        where.append("s.country = %s")
        params.append(clean_country)
    if clean_language:
        where.append("s.language = %s")
        params.append(clean_language)
    if clean_size:
        where.append("s.source_size = %s")
        params.append(clean_size)
    if clean_type:
        where.append("s.source_type = %s")
        params.append(clean_type)

    where_sql = f"where {' and '.join(where)}" if where else ""
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select
                        s.*,
                        count(distinct sf.id) as feed_count,
                        count(distinct ia.id) as article_count
                    from public.sources s
                    left join public.source_feeds sf on sf.source_id = s.id
                    left join public.ingested_articles ia on ia.source_id = s.id
                    {where_sql}
                    group by s.id
                    order by s.is_default desc, s.name asc
                    limit %s;
                    """,
                    (*params, limit),
                )
                return [_row_to_source_record(row) for row in cur.fetchall()]
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not list sources: {exc}") from exc


def get_source_record(source_id: str) -> dict | None:
    if not database_enabled():
        for source in SOURCES:
            if source.get("id") == source_id:
                return _source_with_memory_counts(source)
        return None

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select
                        s.*,
                        count(distinct sf.id) as feed_count,
                        count(distinct ia.id) as article_count
                    from public.sources s
                    left join public.source_feeds sf on sf.source_id = s.id
                    left join public.ingested_articles ia on ia.source_id = s.id
                    where s.id::text = %s
                    group by s.id
                    limit 1;
                    """,
                    (source_id,),
                )
                row = cur.fetchone()
                return _row_to_source_record(row) if row else None
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not load source: {exc}") from exc


def create_source_feed_record(
    source_id: str,
    feed_url: str,
    feed_type: str = "rss",
    title: str | None = None,
    language: str | None = None,
    country: str | None = None,
    status: str = "active",
    fetch_interval_minutes: int = 60,
) -> dict:
    clean_url = _clean_url(feed_url)
    if not clean_url:
        raise FeedStoreError("Source feed URL is required.")
    clean_type = _normalize_source_feed_type(feed_type)
    clean_title = _clean_text(title, 180)
    clean_language = _clean_text(language, 80)
    clean_country = _clean_text(country, 80)
    clean_status = _normalize_source_feed_status(status)
    interval = max(15, min(int(fetch_interval_minutes or 60), 1440))

    if not database_enabled():
        if not get_source_record(source_id):
            raise FeedStoreError("Source not found.")
        for feed in SOURCE_FEEDS:
            if feed.get("source_id") == source_id and feed.get("feed_url") == clean_url:
                feed.update(
                    {
                        "feed_type": clean_type,
                        "title": clean_title or feed.get("title"),
                        "language": clean_language or feed.get("language"),
                        "country": clean_country or feed.get("country"),
                        "status": clean_status,
                        "fetch_interval_minutes": interval,
                        "updated_at": now_iso(),
                    }
                )
                return feed
        now = now_iso()
        feed = {
            "id": str(uuid4()),
            "source_id": source_id,
            "feed_url": clean_url,
            "feed_type": clean_type,
            "title": clean_title,
            "language": clean_language,
            "country": clean_country,
            "status": clean_status,
            "fetch_interval_minutes": interval,
            "last_checked_at": None,
            "last_success_at": None,
            "last_error": None,
            "disabled_reason": None,
            "review_notes": None,
            "last_reviewed_at": None,
            "created_at": now,
            "updated_at": now,
        }
        SOURCE_FEEDS.insert(0, feed)
        return feed

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("select 1 from public.sources where id::text = %s limit 1;", (source_id,))
                if not cur.fetchone():
                    raise FeedStoreError("Source not found.")
                cur.execute(
                    """
                    insert into public.source_feeds (
                        id, source_id, feed_url, feed_type, title, language, country,
                        status, fetch_interval_minutes, created_at, updated_at
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                    on conflict (source_id, feed_url) do update set
                        feed_type = excluded.feed_type,
                        title = coalesce(excluded.title, public.source_feeds.title),
                        language = coalesce(excluded.language, public.source_feeds.language),
                        country = coalesce(excluded.country, public.source_feeds.country),
                        status = excluded.status,
                        fetch_interval_minutes = excluded.fetch_interval_minutes,
                        updated_at = now()
                    returning *;
                    """,
                    (
                        str(uuid4()),
                        source_id,
                        clean_url,
                        clean_type,
                        clean_title,
                        clean_language,
                        clean_country,
                        clean_status,
                        interval,
                    ),
                )
                return _row_to_source_feed_record(cur.fetchone())
    except FeedStoreError:
        raise
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not create source feed: {exc}") from exc


def list_source_feed_records(
    source_id: str | None = None,
    feed_type: str | None = None,
    status: str | None = None,
) -> list[dict]:
    clean_type = _clean_text(feed_type, 30)
    clean_status = _clean_text(status, 40)

    def matches(feed: dict) -> bool:
        return (
            (not source_id or feed.get("source_id") == source_id)
            and (not clean_type or feed.get("feed_type") == clean_type)
            and (not clean_status or feed.get("status") == clean_status)
        )

    if not database_enabled():
        feeds = [feed for feed in SOURCE_FEEDS if matches(feed)]
        return sorted(feeds, key=lambda item: item.get("created_at", ""), reverse=True)

    _ensure_schema()
    where = []
    params: list[object] = []
    if source_id:
        where.append("source_id::text = %s")
        params.append(source_id)
    if clean_type:
        where.append("feed_type = %s")
        params.append(clean_type)
    if clean_status:
        where.append("status = %s")
        params.append(clean_status)
    where_sql = f"where {' and '.join(where)}" if where else ""
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select *
                    from public.source_feeds
                    {where_sql}
                    order by created_at desc;
                    """,
                    tuple(params),
                )
                return [_row_to_source_feed_record(row) for row in cur.fetchall()]
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not list source feeds: {exc}") from exc


def update_source_feed_sync_result(
    source_feed_id: str,
    success: bool,
    title: str | None = None,
    error: str | None = None,
) -> dict | None:
    clean_title = _clean_text(title, 180)
    clean_error = _clean_text(error, 1000)
    checked_at = now_iso()

    if not database_enabled():
        for feed in SOURCE_FEEDS:
            if feed.get("id") != source_feed_id:
                continue
            feed["last_checked_at"] = checked_at
            feed["updated_at"] = checked_at
            if clean_title:
                feed["title"] = clean_title
            if success:
                feed["last_success_at"] = checked_at
                feed["last_error"] = None
            else:
                feed["last_error"] = clean_error or "Sync failed."
            return feed
        return None

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    update public.source_feeds
                    set
                        title = coalesce(%s, title),
                        last_checked_at = now(),
                        last_success_at = case when %s then now() else last_success_at end,
                        last_error = case when %s then null else %s end,
                        updated_at = now()
                    where id::text = %s
                    returning *;
                    """,
                    (clean_title, success, success, clean_error or "Sync failed.", source_feed_id),
                )
                row = cur.fetchone()
                return _row_to_source_feed_record(row) if row else None
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not update source feed sync state: {exc}") from exc


def _normalize_sync_scope(value: str | None) -> str:
    clean = _clean_text(value, 40) or "source"
    return clean if clean in _SOURCE_SYNC_SCOPES else "source"


def _normalize_sync_status(value: str | None) -> str:
    clean = _clean_text(value, 40) or "completed"
    return clean if clean in _SOURCE_SYNC_STATUSES else "completed"


def _datetime_iso(value) -> str | None:
    if isinstance(value, datetime):
        return (value if value.tzinfo else value.replace(tzinfo=timezone.utc)).isoformat()
    if not value:
        return None
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value)


def _duration_ms(started_at, finished_at) -> int:
    started = _parse_datetime(started_at)
    finished = _parse_datetime(finished_at) or datetime.now(timezone.utc)
    if not started:
        return 0
    return max(0, int((finished - started).total_seconds() * 1000))


def _row_to_source_sync_run(row: dict) -> dict:
    return {
        "id": str(row.get("id")),
        "session_id": row.get("session_id") or ANONYMOUS_SESSION_ID,
        "sync_scope": row.get("sync_scope") or "source",
        "status": row.get("status") or "completed",
        "source_id": str(row.get("source_id")) if row.get("source_id") else None,
        "source_feed_id": str(row.get("source_feed_id")) if row.get("source_feed_id") else None,
        "source_name": row.get("source_name"),
        "started_at": _datetime_iso(row.get("started_at")) or now_iso(),
        "finished_at": _datetime_iso(row.get("finished_at")),
        "duration_ms": int(row.get("duration_ms") or 0),
        "source_count": int(row.get("source_count") or 0),
        "feed_count": int(row.get("feed_count") or 0),
        "synced_feed_count": int(row.get("synced_feed_count") or 0),
        "article_count": int(row.get("article_count") or 0),
        "card_count": int(row.get("card_count") or 0),
        "error_count": int(row.get("error_count") or 0),
        "limits": row.get("limits") or {},
        "errors": row.get("errors") or [],
        "summary": row.get("summary") or {},
        "created_at": _row_created_at(row),
    }


def record_source_sync_run(
    *,
    sync_scope: str = "source",
    status: str = "completed",
    session_id: str = ANONYMOUS_SESSION_ID,
    source_id: str | None = None,
    source_feed_id: str | None = None,
    source_name: str | None = None,
    started_at=None,
    finished_at=None,
    source_count: int = 0,
    feed_count: int = 0,
    synced_feed_count: int = 0,
    article_count: int = 0,
    card_count: int = 0,
    error_count: int = 0,
    limits: dict | None = None,
    errors: list[dict] | None = None,
    summary: dict | None = None,
) -> dict:
    sync_scope = _normalize_sync_scope(sync_scope)
    status = _normalize_sync_status(status)
    started = _parse_datetime(started_at) or datetime.now(timezone.utc)
    finished = _parse_datetime(finished_at) or datetime.now(timezone.utc)
    duration = _duration_ms(started, finished)
    clean_source_name = _clean_text(source_name, 180)
    run = {
        "id": str(uuid4()),
        "session_id": session_id or ANONYMOUS_SESSION_ID,
        "sync_scope": sync_scope,
        "status": status,
        "source_id": source_id,
        "source_feed_id": source_feed_id,
        "source_name": clean_source_name,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "duration_ms": duration,
        "source_count": max(0, int(source_count or 0)),
        "feed_count": max(0, int(feed_count or 0)),
        "synced_feed_count": max(0, int(synced_feed_count or 0)),
        "article_count": max(0, int(article_count or 0)),
        "card_count": max(0, int(card_count or 0)),
        "error_count": max(0, int(error_count or 0)),
        "limits": limits or {},
        "errors": errors or [],
        "summary": summary or {},
        "created_at": now_iso(),
    }

    if not database_enabled():
        SOURCE_SYNC_RUNS.insert(0, run)
        return run

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    insert into public.source_sync_runs (
                        id, session_id, sync_scope, status, source_id, source_feed_id,
                        source_name, started_at, finished_at, duration_ms, source_count,
                        feed_count, synced_feed_count, article_count, card_count,
                        error_count, limits, errors, summary, created_at
                    )
                    values (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, now()
                    )
                    returning *;
                    """,
                    (
                        run["id"],
                        run["session_id"],
                        run["sync_scope"],
                        run["status"],
                        run["source_id"],
                        run["source_feed_id"],
                        run["source_name"],
                        run["started_at"],
                        run["finished_at"],
                        run["duration_ms"],
                        run["source_count"],
                        run["feed_count"],
                        run["synced_feed_count"],
                        run["article_count"],
                        run["card_count"],
                        run["error_count"],
                        Json(run["limits"]),
                        Json(run["errors"]),
                        Json(run["summary"]),
                    ),
                )
                return _row_to_source_sync_run(cur.fetchone())
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not record source sync run: {exc}") from exc


def list_source_sync_runs(
    source_id: str | None = None,
    sync_scope: str | None = None,
    limit: int = 50,
) -> list[dict]:
    limit = max(1, min(int(limit or 50), 250))
    clean_scope = _clean_text(sync_scope, 40)

    def matches(run: dict) -> bool:
        return (
            (not source_id or run.get("source_id") == source_id)
            and (not clean_scope or run.get("sync_scope") == clean_scope)
        )

    if not database_enabled():
        runs = [run for run in SOURCE_SYNC_RUNS if matches(run)]
        return sorted(runs, key=lambda item: item.get("started_at") or "", reverse=True)[:limit]

    _ensure_schema()
    where = []
    params: list[object] = []
    if source_id:
        where.append("source_id::text = %s")
        params.append(source_id)
    if clean_scope:
        where.append("sync_scope = %s")
        params.append(clean_scope)
    where_sql = f"where {' and '.join(where)}" if where else ""
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select *
                    from public.source_sync_runs
                    {where_sql}
                    order by started_at desc
                    limit %s;
                    """,
                    (*params, limit),
                )
                return [_row_to_source_sync_run(row) for row in cur.fetchall()]
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not list source sync runs: {exc}") from exc


def _article_counts_for_source(source_id: str) -> tuple[int, int]:
    if not database_enabled():
        now = datetime.now(timezone.utc)
        source_articles = [
            article for article in INGESTED_ARTICLES if article.get("source_id") == source_id
        ]
        recent_count = 0
        for article in source_articles:
            created_at = _parse_datetime(article.get("created_at"))
            if created_at and (now - created_at).total_seconds() <= 86400:
                recent_count += 1
        return len(source_articles), recent_count

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select
                        count(*) as article_count,
                        count(*) filter (
                            where created_at >= now() - interval '24 hours'
                        ) as articles_24h
                    from public.ingested_articles
                    where source_id::text = %s;
                    """,
                    (source_id,),
                )
                row = cur.fetchone() or {}
                return int(row.get("article_count") or 0), int(row.get("articles_24h") or 0)
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not load source article health counts: {exc}") from exc


def _latest_feed_error(feeds: list[dict]) -> tuple[str | None, datetime | None]:
    latest_error = None
    latest_error_at = None
    for feed in feeds:
        if not feed.get("last_error"):
            continue
        checked_at = _parse_datetime(feed.get("last_checked_at")) or _parse_datetime(feed.get("updated_at"))
        if latest_error_at is None or (checked_at and checked_at > latest_error_at):
            latest_error = feed.get("last_error")
            latest_error_at = checked_at
    return latest_error, latest_error_at


def _source_health_from_parts(
    source: dict,
    feeds: list[dict],
    runs: list[dict],
    article_count: int,
    articles_24h: int,
) -> dict:
    active_feeds = [feed for feed in feeds if feed.get("status") == "active"]
    checked_values = [_parse_datetime(feed.get("last_checked_at")) for feed in feeds]
    success_values = [_parse_datetime(feed.get("last_success_at")) for feed in feeds]
    checked_values = [value for value in checked_values if value]
    success_values = [value for value in success_values if value]
    last_checked_at = max(checked_values).isoformat() if checked_values else None
    last_success_at_dt = max(success_values) if success_values else None
    last_success_at = last_success_at_dt.isoformat() if last_success_at_dt else None
    latest_error, latest_error_at = _latest_feed_error(feeds)
    run_count = len(runs)
    rate_runs = [
        run
        for run in runs
        if run.get("status") != "skipped" or int(run.get("feed_count") or 0) > 0
    ]
    successful_runs = len(
        [
            run
            for run in rate_runs
            if run.get("status") == "completed" and int(run.get("error_count") or 0) == 0
        ]
    )
    success_rate = round(successful_runs / len(rate_runs), 3) if rate_runs else None
    last_run = runs[0] if runs else None

    status = "healthy"
    recommendation = "Source ingestion is producing recent successful sync signals."
    now = datetime.now(timezone.utc)
    max_interval = max([int(feed.get("fetch_interval_minutes") or 60) for feed in active_feeds] or [60])
    stale_after_minutes = max(1440, max_interval * 2)
    stale_feed = False
    needs_review = False

    if not active_feeds:
        status = "needs_review"
        needs_review = True
        recommendation = "Add or activate at least one RSS feed before scheduling this source."
    elif not last_success_at_dt:
        status = "needs_review"
        needs_review = True
        recommendation = "Run a source sync and inspect feed parsing before relying on this source."
    else:
        age_minutes = (now - last_success_at_dt).total_seconds() / 60
        stale_feed = age_minutes > stale_after_minutes
        if latest_error and latest_error_at and latest_error_at >= last_success_at_dt:
            status = "error"
            recommendation = "Inspect the last feed error and verify that the source RSS URL still works."
        elif stale_feed:
            status = "stale"
            recommendation = "The source has not produced a successful sync in the expected window."
        elif success_rate is not None and run_count >= 3 and success_rate < 0.5:
            status = "error"
            recommendation = "Recent sync attempts are failing more often than succeeding."

    return {
        "status": status,
        "label": {
            "healthy": "Healthy",
            "stale": "Stale",
            "error": "Error",
            "needs_review": "Needs review",
        }.get(status, "Needs review"),
        "feed_count": len(feeds),
        "active_feed_count": len(active_feeds),
        "article_count": article_count,
        "articles_24h": articles_24h,
        "run_count": run_count,
        "success_rate": success_rate,
        "last_checked_at": last_checked_at,
        "last_success_at": last_success_at,
        "last_error": latest_error,
        "last_run": last_run,
        "stale_feed": stale_feed,
        "needs_review": needs_review,
        "recommendation": recommendation,
    }


def build_source_health_summary(source_id: str) -> dict:
    source = get_source_record(source_id)
    if not source:
        raise FeedStoreError("Source not found.")

    feeds = list_source_feed_records(source_id=source_id)
    runs = list_source_sync_runs(source_id=source_id, limit=20)
    article_count, articles_24h = _article_counts_for_source(source_id)
    return _source_health_from_parts(source, feeds, runs, article_count, articles_24h)


def attach_source_health(source: dict) -> dict:
    enriched = {**source}
    try:
        enriched["health"] = build_source_health_summary(source["id"])
    except FeedStoreError:
        enriched["health"] = {
            "status": "needs_review",
            "label": "Needs review",
            "feed_count": source.get("feed_count") or 0,
            "active_feed_count": 0,
            "article_count": source.get("article_count") or 0,
            "articles_24h": 0,
            "run_count": 0,
            "success_rate": None,
            "last_checked_at": None,
            "last_success_at": None,
            "last_error": None,
            "last_run": None,
            "stale_feed": False,
            "needs_review": True,
            "recommendation": "Source health could not be calculated.",
        }
    return enriched


def attach_sources_health(sources: list[dict]) -> list[dict]:
    if not sources:
        return []

    if not database_enabled():
        return [attach_source_health(source) for source in sources]

    source_ids = [source["id"] for source in sources if source.get("id")]
    if not source_ids:
        return sources

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select *
                    from public.source_feeds
                    where source_id::text = any(%s)
                    order by created_at desc;
                    """,
                    (source_ids,),
                )
                feeds_by_source: dict[str, list[dict]] = {}
                for row in cur.fetchall():
                    feed = _row_to_source_feed_record(row)
                    if feed.get("source_id"):
                        feeds_by_source.setdefault(feed["source_id"], []).append(feed)

                cur.execute(
                    """
                    select *
                    from (
                        select
                            sr.*,
                            row_number() over (
                                partition by sr.source_id
                                order by sr.started_at desc
                            ) as rn
                        from public.source_sync_runs sr
                        where sr.source_id::text = any(%s)
                    ) ranked
                    where rn <= 20
                    order by started_at desc;
                    """,
                    (source_ids,),
                )
                runs_by_source: dict[str, list[dict]] = {}
                for row in cur.fetchall():
                    run = _row_to_source_sync_run(row)
                    if run.get("source_id"):
                        runs_by_source.setdefault(run["source_id"], []).append(run)

                cur.execute(
                    """
                    select
                        source_id::text as source_id,
                        count(*) as article_count,
                        count(*) filter (
                            where created_at >= now() - interval '24 hours'
                        ) as articles_24h
                    from public.ingested_articles
                    where source_id::text = any(%s)
                    group by source_id;
                    """,
                    (source_ids,),
                )
                article_counts = {
                    row["source_id"]: (
                        int(row.get("article_count") or 0),
                        int(row.get("articles_24h") or 0),
                    )
                    for row in cur.fetchall()
                }

        enriched_sources = []
        for source in sources:
            source_id = source.get("id")
            article_count, articles_24h = article_counts.get(
                source_id,
                (int(source.get("article_count") or 0), 0),
            )
            enriched_sources.append(
                {
                    **source,
                    "health": _source_health_from_parts(
                        source,
                        feeds_by_source.get(source_id, []),
                        runs_by_source.get(source_id, []),
                        article_count,
                        articles_24h,
                    ),
                }
            )
        return enriched_sources
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not attach source health summaries: {exc}") from exc


def _metadata_completeness(source: dict) -> tuple[float, list[str]]:
    checks = [
        ("website_url", "website URL"),
        ("country", "country"),
        ("language", "language"),
        ("region", "region"),
        ("source_size", "source size"),
        ("source_type", "source type"),
        ("credibility_notes", "credibility notes"),
    ]
    present = [label for key, label in checks if source.get(key)]
    return round(len(present) / len(checks), 3), present


def calculate_source_quality_report(source_id: str) -> dict:
    source = get_source_record(source_id)
    if not source:
        raise FeedStoreError("Source not found.")

    feeds = list_source_feed_records(source_id=source_id)
    health = build_source_health_summary(source_id)
    metadata_score, present_metadata = _metadata_completeness(source)
    active_feeds = [feed for feed in feeds if feed.get("status") == "active"]
    disabled_feeds = [feed for feed in feeds if feed.get("status") in {"quarantined", "disabled"}]
    success_rate = health.get("success_rate")

    score = 0.0
    factors = []
    risks = []

    score += 0.35 * metadata_score
    if present_metadata:
        factors.append(f"Metadata present: {', '.join(present_metadata[:5])}.")
    else:
        risks.append("No useful source metadata has been reviewed.")

    if source.get("rss_url") or active_feeds:
        score += 0.18
        factors.append("At least one active RSS feed is configured.")
    elif feeds:
        score += 0.06
        risks.append("Feeds exist but none are active.")
    else:
        risks.append("No feed is configured for recurring ingestion.")

    if success_rate is None:
        score += 0.06
        risks.append("No successful sync history has been recorded yet.")
    else:
        score += 0.22 * max(0.0, min(success_rate, 1.0))
        factors.append(f"Recent sync success rate is {round(success_rate * 100)}%.")

    if health.get("articles_24h", 0) > 0:
        score += 0.1
        factors.append("The source produced articles in the last 24 hours.")
    elif health.get("article_count", 0) > 0:
        score += 0.05
        factors.append("The source has historical ingested articles.")
    else:
        risks.append("No articles have been ingested from this source.")

    if source.get("terms_reviewed_at"):
        score += 0.08
        factors.append("Terms/feed review has been marked complete.")
    else:
        risks.append("Terms/feed usage has not been marked reviewed.")

    if health.get("last_error"):
        score -= 0.08
        risks.append("The latest feed sync reported an error.")
    if disabled_feeds:
        score -= 0.08
        risks.append(f"{len(disabled_feeds)} feed(s) are quarantined or disabled.")
    if source.get("review_status") in {"quarantined", "disabled"}:
        score = min(score, 0.25)
        risks.append(f"Source review status is {source.get('review_status')}.")
    elif source.get("review_status") == "reviewed":
        score += 0.07
        factors.append("An admin marked the source reviewed.")

    score = round(max(0.0, min(score, 1.0)), 3)
    if score >= 0.8:
        grade = "high"
    elif score >= 0.55:
        grade = "medium"
    elif score >= 0.35:
        grade = "low"
    else:
        grade = "poor"

    needs_review = (
        source.get("review_status") != "reviewed"
        or score < 0.55
        or health.get("status") != "healthy"
        or bool(disabled_feeds)
    )
    return {
        "source_id": source_id,
        "quality_score": score,
        "quality_grade": grade,
        "needs_review": needs_review,
        "review_status": source.get("review_status") or "needs_review",
        "metadata_completeness": metadata_score,
        "feed_count": len(feeds),
        "active_feed_count": len(active_feeds),
        "disabled_feed_count": len(disabled_feeds),
        "health_status": health.get("status"),
        "factors": factors,
        "risks": risks,
        "recommendation": (
            "Review metadata and feed terms before broad ingestion."
            if needs_review
            else "Source is reviewed and operationally ready for scheduled ingestion."
        ),
    }


def _source_with_quality(source: dict) -> dict:
    enriched = attach_source_health(source)
    try:
        enriched["quality"] = calculate_source_quality_report(source["id"])
    except FeedStoreError:
        enriched["quality"] = {
            "source_id": source.get("id"),
            "quality_score": _as_float(source.get("quality_score"), 0.0),
            "quality_grade": "poor",
            "needs_review": True,
            "review_status": source.get("review_status") or "needs_review",
            "metadata_completeness": 0.0,
            "feed_count": source.get("feed_count") or 0,
            "active_feed_count": 0,
            "disabled_feed_count": 0,
            "health_status": "needs_review",
            "factors": [],
            "risks": ["Quality report could not be calculated."],
            "recommendation": "Review this source manually.",
        }
    return enriched


def recalculate_source_quality(source_id: str) -> dict:
    source = get_source_record(source_id)
    if not source:
        raise FeedStoreError("Source not found.")
    quality = calculate_source_quality_report(source_id)
    now = now_iso()

    if not database_enabled():
        for item in SOURCES:
            if item.get("id") == source_id:
                item["quality_score"] = quality["quality_score"]
                item["updated_at"] = now
                source = _source_with_memory_counts(item)
                break
        return {"source": _source_with_quality(source), "quality": quality}

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    update public.sources
                    set quality_score = %s, updated_at = now()
                    where id::text = %s
                    returning *, 0 as feed_count, 0 as article_count;
                    """,
                    (quality["quality_score"], source_id),
                )
                row = cur.fetchone()
        return {"source": _source_with_quality(_row_to_source_record(row)), "quality": quality}
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not update source quality score: {exc}") from exc


def update_source_review_status(
    source_id: str,
    review_status: str,
    review_notes: str | None = None,
    disabled_reason: str | None = None,
    terms_reviewed: bool = False,
) -> dict:
    source = get_source_record(source_id)
    if not source:
        raise FeedStoreError("Source not found.")
    clean_status = _normalize_source_review_status(review_status)
    clean_notes = _clean_text(review_notes, 1000)
    clean_reason = _clean_text(disabled_reason, 1000)
    now = now_iso()

    if not database_enabled():
        for item in SOURCES:
            if item.get("id") != source_id:
                continue
            item["review_status"] = clean_status
            item["review_notes"] = clean_notes
            item["disabled_reason"] = clean_reason if clean_status in {"quarantined", "disabled"} else None
            if terms_reviewed:
                item["terms_reviewed_at"] = now
            item["last_reviewed_at"] = now
            item["updated_at"] = now
            break
        return recalculate_source_quality(source_id)

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update public.sources
                    set
                        review_status = %s,
                        review_notes = %s,
                        disabled_reason = case when %s in ('quarantined', 'disabled') then %s else null end,
                        terms_reviewed_at = case when %s then now() else terms_reviewed_at end,
                        last_reviewed_at = now(),
                        updated_at = now()
                    where id::text = %s;
                    """,
                    (
                        clean_status,
                        clean_notes,
                        clean_status,
                        clean_reason,
                        terms_reviewed,
                        source_id,
                    ),
                )
        return recalculate_source_quality(source_id)
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not update source review status: {exc}") from exc


def update_source_feed_governance(
    source_feed_id: str,
    status: str,
    disabled_reason: str | None = None,
    review_notes: str | None = None,
) -> dict:
    clean_status = _normalize_source_feed_status(status)
    clean_reason = _clean_text(disabled_reason, 1000)
    clean_notes = _clean_text(review_notes, 1000)
    now = now_iso()

    if not database_enabled():
        for feed in SOURCE_FEEDS:
            if feed.get("id") != source_feed_id:
                continue
            feed["status"] = clean_status
            feed["disabled_reason"] = (
                clean_reason if clean_status in {"paused", "quarantined", "disabled"} else None
            )
            feed["review_notes"] = clean_notes
            feed["last_reviewed_at"] = now
            feed["updated_at"] = now
            source_id = feed.get("source_id")
            quality = recalculate_source_quality(source_id) if source_id else None
            return {"feed": feed, "source_quality": (quality or {}).get("quality")}
        raise FeedStoreError("Source feed not found.")

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    update public.source_feeds
                    set
                        status = %s,
                        disabled_reason = case when %s in ('paused', 'quarantined', 'disabled') then %s else null end,
                        review_notes = %s,
                        last_reviewed_at = now(),
                        updated_at = now()
                    where id::text = %s
                    returning *;
                    """,
                    (
                        clean_status,
                        clean_status,
                        clean_reason,
                        clean_notes,
                        source_feed_id,
                    ),
                )
                row = cur.fetchone()
                if not row:
                    raise FeedStoreError("Source feed not found.")
                feed = _row_to_source_feed_record(row)
        quality = recalculate_source_quality(feed["source_id"]) if feed.get("source_id") else None
        return {"feed": feed, "source_quality": (quality or {}).get("quality")}
    except FeedStoreError:
        raise
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not update source feed governance: {exc}") from exc


def list_sources_needing_review(limit: int = 50) -> dict:
    sources = list_source_records(limit=max(1, min(limit, 250)))
    reviewed = []
    for source in sources:
        enriched = _source_with_quality(source)
        quality = enriched.get("quality") or {}
        health = enriched.get("health") or {}
        if (
            quality.get("needs_review")
            or source.get("review_status") != "reviewed"
            or health.get("status") != "healthy"
        ):
            reviewed.append(enriched)
        if len(reviewed) >= limit:
            break

    return {
        "sources": reviewed,
        "summary": {
            "source_count": len(reviewed),
            "disabled_count": len([item for item in reviewed if item.get("review_status") == "disabled"]),
            "quarantined_count": len([item for item in reviewed if item.get("review_status") == "quarantined"]),
            "low_quality_count": len(
                [
                    item
                    for item in reviewed
                    if (item.get("quality") or {}).get("quality_score", 0) < 0.55
                ]
            ),
        },
    }


def _normalize_ops_alert_severity(value: str | None) -> str:
    clean = _clean_text(value, 40) or "warning"
    return clean if clean in _SOURCE_OPS_ALERT_SEVERITIES else "warning"


def _normalize_ops_alert_status(value: str | None) -> str:
    clean = _clean_text(value, 40) or "active"
    return clean if clean in _SOURCE_OPS_ALERT_STATUSES else "active"


def _normalize_ops_delivery_status(value: str | None) -> str:
    clean = _clean_text(value, 40) or "failed"
    return clean if clean in _SOURCE_OPS_DELIVERY_STATUSES else "failed"


def _ops_alert_key(
    alert_type: str,
    source_id: str | None = None,
    source_feed_id: str | None = None,
) -> str:
    return f"{alert_type}:{source_id or 'global'}:{source_feed_id or 'source'}"


def _row_to_source_ops_alert(row: dict) -> dict:
    return {
        "id": str(row.get("id")),
        "alert_key": row.get("alert_key"),
        "alert_type": row.get("alert_type"),
        "severity": row.get("severity") or "warning",
        "status": row.get("status") or "active",
        "source_id": str(row.get("source_id")) if row.get("source_id") else None,
        "source_feed_id": str(row.get("source_feed_id")) if row.get("source_feed_id") else None,
        "sync_run_id": str(row.get("sync_run_id")) if row.get("sync_run_id") else None,
        "title": row.get("title") or "Source operations alert",
        "message": row.get("message") or "",
        "evidence": row.get("evidence") or {},
        "created_at": _row_created_at(row),
        "updated_at": _row_optional_datetime(row, "updated_at") or _row_created_at(row),
        "acknowledged_at": _row_optional_datetime(row, "acknowledged_at"),
        "resolved_at": _row_optional_datetime(row, "resolved_at"),
    }


def upsert_source_ops_alert(
    *,
    alert_type: str,
    severity: str,
    title: str,
    message: str,
    source_id: str | None = None,
    source_feed_id: str | None = None,
    sync_run_id: str | None = None,
    evidence: dict | None = None,
) -> dict:
    clean_type = _clean_text(alert_type, 80) or "source_ops_alert"
    clean_severity = _normalize_ops_alert_severity(severity)
    clean_title = _clean_text(title, 220, "Source operations alert")
    clean_message = _clean_text(message, 1200, "")
    alert_key = _ops_alert_key(clean_type, source_id, source_feed_id)
    now = now_iso()

    if not database_enabled():
        existing = next((alert for alert in SOURCE_OPS_ALERTS if alert.get("alert_key") == alert_key), None)
        alert = {
            "id": existing.get("id") if existing else str(uuid4()),
            "alert_key": alert_key,
            "alert_type": clean_type,
            "severity": clean_severity,
            "status": "active",
            "source_id": source_id,
            "source_feed_id": source_feed_id,
            "sync_run_id": sync_run_id,
            "title": clean_title,
            "message": clean_message,
            "evidence": evidence or {},
            "created_at": existing.get("created_at") if existing else now,
            "updated_at": now,
            "acknowledged_at": None,
            "resolved_at": None,
        }
        if existing:
            existing.clear()
            existing.update(alert)
        else:
            SOURCE_OPS_ALERTS.insert(0, alert)
        return alert

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    insert into public.source_ops_alerts (
                        id, alert_key, alert_type, severity, status, source_id,
                        source_feed_id, sync_run_id, title, message, evidence,
                        created_at, updated_at
                    )
                    values (%s, %s, %s, %s, 'active', %s, %s, %s, %s, %s, %s, now(), now())
                    on conflict (alert_key) do update set
                        alert_type = excluded.alert_type,
                        severity = excluded.severity,
                        status = 'active',
                        source_id = excluded.source_id,
                        source_feed_id = excluded.source_feed_id,
                        sync_run_id = excluded.sync_run_id,
                        title = excluded.title,
                        message = excluded.message,
                        evidence = excluded.evidence,
                        updated_at = now(),
                        acknowledged_at = null,
                        resolved_at = null
                    returning *;
                    """,
                    (
                        str(uuid4()),
                        alert_key,
                        clean_type,
                        clean_severity,
                        source_id,
                        source_feed_id,
                        sync_run_id,
                        clean_title,
                        clean_message,
                        Json(evidence or {}),
                    ),
                )
                return _row_to_source_ops_alert(cur.fetchone())
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not save source ops alert: {exc}") from exc


def list_source_ops_alerts(
    *,
    status: str | None = "active",
    source_id: str | None = None,
    severity: str | None = None,
    limit: int = 50,
) -> list[dict]:
    limit = max(1, min(int(limit or 50), 250))
    clean_status = _clean_text(status, 40)
    clean_severity = _clean_text(severity, 40)

    def matches(alert: dict) -> bool:
        return (
            (not clean_status or alert.get("status") == clean_status)
            and (not source_id or alert.get("source_id") == source_id)
            and (not clean_severity or alert.get("severity") == clean_severity)
        )

    if not database_enabled():
        severity_rank = {"critical": 3, "warning": 2, "info": 1}
        alerts = [alert for alert in SOURCE_OPS_ALERTS if matches(alert)]
        alerts.sort(
            key=lambda item: (
                severity_rank.get(item.get("severity"), 0),
                item.get("updated_at") or item.get("created_at") or "",
            ),
            reverse=True,
        )
        return alerts[:limit]

    _ensure_schema()
    where = []
    params: list[object] = []
    if clean_status:
        where.append("status = %s")
        params.append(clean_status)
    if source_id:
        where.append("source_id::text = %s")
        params.append(source_id)
    if clean_severity:
        where.append("severity = %s")
        params.append(clean_severity)
    where_sql = f"where {' and '.join(where)}" if where else ""
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select *
                    from public.source_ops_alerts
                    {where_sql}
                    order by
                        case severity
                            when 'critical' then 3
                            when 'warning' then 2
                            else 1
                        end desc,
                        updated_at desc
                    limit %s;
                    """,
                    (*params, limit),
                )
                return [_row_to_source_ops_alert(row) for row in cur.fetchall()]
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not list source ops alerts: {exc}") from exc


def acknowledge_source_ops_alert(alert_id: str) -> dict | None:
    if not database_enabled():
        for alert in SOURCE_OPS_ALERTS:
            if alert.get("id") != alert_id:
                continue
            alert["status"] = "acknowledged"
            alert["acknowledged_at"] = now_iso()
            alert["updated_at"] = alert["acknowledged_at"]
            return alert
        return None

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    update public.source_ops_alerts
                    set status = 'acknowledged',
                        acknowledged_at = now(),
                        updated_at = now()
                    where id::text = %s
                    returning *;
                    """,
                    (alert_id,),
                )
                row = cur.fetchone()
                return _row_to_source_ops_alert(row) if row else None
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not acknowledge source ops alert: {exc}") from exc


def get_source_ops_alert(alert_id: str) -> dict | None:
    if not database_enabled():
        alert = next((item for item in SOURCE_OPS_ALERTS if item.get("id") == alert_id), None)
        return dict(alert) if alert else None

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select *
                    from public.source_ops_alerts
                    where id::text = %s;
                    """,
                    (alert_id,),
                )
                row = cur.fetchone()
                return _row_to_source_ops_alert(row) if row else None
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not load source ops alert: {exc}") from exc


def _row_to_source_ops_alert_delivery(row: dict) -> dict:
    return {
        "id": str(row.get("id")),
        "alert_id": str(row.get("alert_id")) if row.get("alert_id") else None,
        "alert_updated_at": _datetime_iso(row.get("alert_updated_at")),
        "destination_type": row.get("destination_type") or "webhook",
        "destination_url": row.get("destination_url"),
        "status": row.get("status") or "failed",
        "attempt_count": int(row.get("attempt_count") or 1),
        "response_status": int(row.get("response_status")) if row.get("response_status") is not None else None,
        "error": row.get("error"),
        "payload": row.get("payload") or {},
        "created_at": _row_created_at(row),
        "delivered_at": _datetime_iso(row.get("delivered_at")),
        "source_id": str(row.get("source_id")) if row.get("source_id") else None,
        "source_feed_id": str(row.get("source_feed_id")) if row.get("source_feed_id") else None,
        "alert_severity": row.get("alert_severity"),
        "alert_title": row.get("alert_title"),
    }


def record_source_ops_alert_delivery(
    *,
    alert_id: str,
    alert_updated_at: str | None = None,
    destination_type: str = "webhook",
    destination_url: str | None = None,
    status: str = "failed",
    attempt_count: int = 1,
    response_status: int | None = None,
    error: str | None = None,
    payload: dict | None = None,
) -> dict:
    clean_status = _normalize_ops_delivery_status(status)
    clean_destination_type = _clean_text(destination_type, 80, "webhook")
    clean_destination_url = _clean_text(destination_url, 1000)
    clean_error = _clean_text(error, 1200)
    alert_updated = _datetime_iso(alert_updated_at)
    now = now_iso()
    delivered_at = now if clean_status == "delivered" else None
    record = {
        "id": str(uuid4()),
        "alert_id": alert_id,
        "alert_updated_at": alert_updated,
        "destination_type": clean_destination_type,
        "destination_url": clean_destination_url,
        "status": clean_status,
        "attempt_count": max(1, int(attempt_count or 1)),
        "response_status": response_status,
        "error": clean_error,
        "payload": payload or {},
        "created_at": now,
        "delivered_at": delivered_at,
    }

    if not database_enabled():
        SOURCE_OPS_ALERT_DELIVERIES.insert(0, record)
        return record

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    insert into public.source_ops_alert_deliveries (
                        id, alert_id, alert_updated_at, destination_type,
                        destination_url, status, attempt_count, response_status,
                        error, payload, created_at, delivered_at
                    )
                    values (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(),
                        case when %s = 'delivered' then now() else null end
                    )
                    returning *;
                    """,
                    (
                        record["id"],
                        alert_id,
                        alert_updated,
                        clean_destination_type,
                        clean_destination_url,
                        clean_status,
                        record["attempt_count"],
                        response_status,
                        clean_error,
                        Json(payload or {}),
                        clean_status,
                    ),
                )
                return _row_to_source_ops_alert_delivery(cur.fetchone())
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not record source ops alert delivery: {exc}") from exc


def list_source_ops_alert_deliveries(
    *,
    alert_id: str | None = None,
    source_id: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict]:
    limit = max(1, min(int(limit or 50), 250))
    clean_status = _clean_text(status, 40)

    if not database_enabled():
        alerts_by_id = {alert.get("id"): alert for alert in SOURCE_OPS_ALERTS}

        def matches(delivery: dict) -> bool:
            alert = alerts_by_id.get(delivery.get("alert_id")) or {}
            return (
                (not alert_id or delivery.get("alert_id") == alert_id)
                and (not source_id or alert.get("source_id") == source_id)
                and (not clean_status or delivery.get("status") == clean_status)
            )

        deliveries = []
        for delivery in SOURCE_OPS_ALERT_DELIVERIES:
            if not matches(delivery):
                continue
            alert = alerts_by_id.get(delivery.get("alert_id")) or {}
            deliveries.append(
                {
                    **delivery,
                    "source_id": alert.get("source_id"),
                    "source_feed_id": alert.get("source_feed_id"),
                    "alert_severity": alert.get("severity"),
                    "alert_title": alert.get("title"),
                }
            )
        return sorted(deliveries, key=lambda item: item.get("created_at") or "", reverse=True)[:limit]

    _ensure_schema()
    where = []
    params: list[object] = []
    if alert_id:
        where.append("d.alert_id::text = %s")
        params.append(alert_id)
    if source_id:
        where.append("a.source_id::text = %s")
        params.append(source_id)
    if clean_status:
        where.append("d.status = %s")
        params.append(clean_status)
    where_sql = f"where {' and '.join(where)}" if where else ""
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select
                        d.*,
                        a.source_id,
                        a.source_feed_id,
                        a.severity as alert_severity,
                        a.title as alert_title
                    from public.source_ops_alert_deliveries d
                    join public.source_ops_alerts a on a.id = d.alert_id
                    {where_sql}
                    order by d.created_at desc
                    limit %s;
                    """,
                    (*params, limit),
                )
                return [_row_to_source_ops_alert_delivery(row) for row in cur.fetchall()]
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not list source ops alert deliveries: {exc}") from exc


def latest_source_ops_alert_delivery(alert_id: str) -> dict | None:
    deliveries = list_source_ops_alert_deliveries(alert_id=alert_id, limit=1)
    return deliveries[0] if deliveries else None


def attach_source_ops_alert_delivery_status(alerts: list[dict]) -> list[dict]:
    if not alerts:
        return []

    alert_ids = [alert.get("id") for alert in alerts if alert.get("id")]
    latest_by_alert: dict[str, dict] = {}

    if not database_enabled():
        for delivery in SOURCE_OPS_ALERT_DELIVERIES:
            alert_id = delivery.get("alert_id")
            if alert_id in alert_ids and alert_id not in latest_by_alert:
                latest_by_alert[alert_id] = delivery
    else:
        _ensure_schema()
        try:
            with _connect() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        select distinct on (d.alert_id)
                            d.*,
                            a.source_id,
                            a.source_feed_id,
                            a.severity as alert_severity,
                            a.title as alert_title
                        from public.source_ops_alert_deliveries d
                        join public.source_ops_alerts a on a.id = d.alert_id
                        where d.alert_id::text = any(%s)
                        order by d.alert_id, d.created_at desc;
                        """,
                        (alert_ids,),
                    )
                    latest_by_alert = {
                        str(row.get("alert_id")): _row_to_source_ops_alert_delivery(row)
                        for row in cur.fetchall()
                    }
        except psycopg2.Error as exc:
            raise FeedStoreError(f"Could not attach source ops alert delivery status: {exc}") from exc

    enriched = []
    for alert in alerts:
        delivery = latest_by_alert.get(alert.get("id"))
        enriched.append(
            {
                **alert,
                "delivery_status": (delivery or {}).get("status") or "not_sent",
                "delivery": delivery,
            }
        )
    return enriched


def _source_ops_alert_payload(
    source: dict,
    health: dict,
    quality: dict,
    sync_run_id: str | None,
) -> dict:
    return {
        "source": {
            "id": source.get("id"),
            "name": source.get("name"),
            "review_status": source.get("review_status"),
            "quality_score": source.get("quality_score"),
        },
        "health": {
            "status": health.get("status"),
            "last_success_at": health.get("last_success_at"),
            "last_error": health.get("last_error"),
            "success_rate": health.get("success_rate"),
            "articles_24h": health.get("articles_24h"),
            "run_count": health.get("run_count"),
        },
        "quality": {
            "quality_score": quality.get("quality_score"),
            "quality_grade": quality.get("quality_grade"),
            "risks": quality.get("risks") or [],
        },
        "sync_run_id": sync_run_id,
    }


def evaluate_source_ops_alerts(
    *,
    source_id: str | None = None,
    sync_run_id: str | None = None,
    limit: int = 250,
) -> dict:
    if source_id:
        source = get_source_record(source_id)
        sources = [source] if source else []
    else:
        sources = list_source_records(limit=limit)

    generated: list[dict] = []
    evaluated_count = 0
    for source in sources:
        if not source or not source.get("id"):
            continue
        evaluated_count += 1
        source_id_value = source["id"]
        health = build_source_health_summary(source_id_value)
        quality = calculate_source_quality_report(source_id_value)
        feeds = list_source_feed_records(source_id=source_id_value)
        evidence = _source_ops_alert_payload(source, health, quality, sync_run_id)
        source_name = source.get("name") or "Source"

        if source.get("review_status") in {"quarantined", "disabled"}:
            generated.append(
                upsert_source_ops_alert(
                    alert_type=f"source_{source.get('review_status')}",
                    severity="critical" if source.get("review_status") == "disabled" else "warning",
                    source_id=source_id_value,
                    sync_run_id=sync_run_id,
                    title=f"{source_name} is {source.get('review_status')}",
                    message="Ingestion is skipped until the source review status is changed.",
                    evidence=evidence,
                )
            )

        if health.get("status") == "stale":
            generated.append(
                upsert_source_ops_alert(
                    alert_type="stale_source",
                    severity="warning",
                    source_id=source_id_value,
                    sync_run_id=sync_run_id,
                    title=f"{source_name} has stale ingestion",
                    message=health.get("recommendation") or "No recent successful sync was detected.",
                    evidence=evidence,
                )
            )
        elif health.get("status") == "error":
            generated.append(
                upsert_source_ops_alert(
                    alert_type="sync_failure",
                    severity="critical",
                    source_id=source_id_value,
                    sync_run_id=sync_run_id,
                    title=f"{source_name} sync is failing",
                    message=health.get("last_error") or health.get("recommendation") or "Recent sync attempts failed.",
                    evidence=evidence,
                )
            )

        if source.get("review_status") == "reviewed" and health.get("active_feed_count", 0) > 0 and health.get("articles_24h", 0) == 0:
            generated.append(
                upsert_source_ops_alert(
                    alert_type="zero_articles_24h",
                    severity="warning",
                    source_id=source_id_value,
                    sync_run_id=sync_run_id,
                    title=f"{source_name} produced no articles in 24h",
                    message="A reviewed source with active feeds has not produced articles in the last 24 hours.",
                    evidence=evidence,
                )
            )

        if quality.get("quality_score", 0) < 0.55:
            generated.append(
                upsert_source_ops_alert(
                    alert_type="low_quality_source",
                    severity="warning",
                    source_id=source_id_value,
                    sync_run_id=sync_run_id,
                    title=f"{source_name} has low ingestion quality",
                    message=quality.get("recommendation") or "Source metadata or ingestion quality needs review.",
                    evidence=evidence,
                )
            )

        for feed in feeds:
            if feed.get("status") in {"quarantined", "disabled"}:
                generated.append(
                    upsert_source_ops_alert(
                        alert_type=f"feed_{feed.get('status')}",
                        severity="critical" if feed.get("status") == "disabled" else "warning",
                        source_id=source_id_value,
                        source_feed_id=feed.get("id"),
                        sync_run_id=sync_run_id,
                        title=f"{source_name} feed is {feed.get('status')}",
                        message=feed.get("disabled_reason") or "Feed is excluded from scheduled ingestion.",
                        evidence={**evidence, "feed": feed},
                    )
                )
            elif feed.get("last_error"):
                generated.append(
                    upsert_source_ops_alert(
                        alert_type="feed_error",
                        severity="warning",
                        source_id=source_id_value,
                        source_feed_id=feed.get("id"),
                        sync_run_id=sync_run_id,
                        title=f"{source_name} feed reported an error",
                        message=feed.get("last_error") or "Feed sync reported an error.",
                        evidence={**evidence, "feed": feed},
                    )
                )

    active = list_source_ops_alerts(status="active", limit=limit)
    return {
        "status": "evaluated",
        "evaluated_source_count": evaluated_count,
        "generated_alert_count": len(generated),
        "alerts": generated,
        "active_alert_count": len(active),
        "summary": {
            "critical": len([alert for alert in active if alert.get("severity") == "critical"]),
            "warning": len([alert for alert in active if alert.get("severity") == "warning"]),
            "info": len([alert for alert in active if alert.get("severity") == "info"]),
        },
    }


def _article_keyword_tokens(*values: str | None) -> list[str]:
    stopwords = {
        "about",
        "after",
        "again",
        "against",
        "being",
        "from",
        "have",
        "into",
        "over",
        "said",
        "says",
        "that",
        "their",
        "there",
        "this",
        "with",
        "will",
        "would",
    }
    seen = set()
    tokens = []
    text = " ".join(value or "" for value in values)
    for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9'-]{2,}", text.lower()):
        if token in stopwords or token in seen:
            continue
        seen.add(token)
        tokens.append(token[:40])
        if len(tokens) >= 16:
            break
    return tokens


def _article_content_hash(raw_item: dict) -> str:
    content = "|".join(
        [
            str(raw_item.get("url") or ""),
            str(raw_item.get("title") or ""),
            str(raw_item.get("summary") or ""),
            str(raw_item.get("published_at") or ""),
        ]
    )
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _article_event_fingerprint(raw_item: dict) -> str:
    tokens = _article_keyword_tokens(raw_item.get("title"), raw_item.get("summary"))
    if not tokens:
        tokens = _article_keyword_tokens(raw_item.get("url"))
    basis = " ".join(tokens[:12]) or str(raw_item.get("url") or uuid4())
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]


def _build_ingested_article_card(
    source: dict,
    source_feed: dict | None,
    article: dict,
    session_id: str = ANONYMOUS_SESSION_ID,
) -> dict:
    source_name = source.get("name") or "Unknown source"
    summary = article.get("summary") or "A configured source published a new article."
    return {
        "id": str(uuid4()),
        "session_id": session_id,
        "topic_id": None,
        "article_id": None,
        "alert_id": None,
        "report_id": None,
        "source_id": article.get("source_id") or source.get("id"),
        "source_feed_id": article.get("source_feed_id") or (source_feed or {}).get("id"),
        "ingested_article_id": article.get("id"),
        "node_id": None,
        "comparison_id": None,
        "card_type": "ingested_article",
        "title": article.get("title") or "New ingested article",
        "summary": summary,
        "source": source_name,
        "url": article.get("url"),
        "topic": source.get("country") or source.get("language") or "Source ingestion",
        "priority": 0.52,
        "priority_score": 0.52,
        "personalized_score": 0.52,
        "narrative_signal": "Source ingestion signal only. Analyze and compare before drawing conclusions.",
        "evidence_score": None,
        "framing": None,
        "payload": {
            "source_id": source.get("id"),
            "source_name": source_name,
            "source_type": source.get("source_type"),
            "source_size": source.get("source_size"),
            "source_country": source.get("country"),
            "source_language": source.get("language"),
            "source_feed_id": (source_feed or {}).get("id"),
            "source_feed_type": (source_feed or {}).get("feed_type"),
            "feed_url": (source_feed or {}).get("feed_url"),
            "ingested_article_id": article.get("id"),
            "published_at": article.get("published_at"),
            "event_fingerprint": article.get("event_fingerprint"),
            "comparison_keywords": article.get("comparison_keywords") or [],
            "analysis_status": article.get("analysis_status"),
        },
        "recommendations": [
            {
                "type": "open_source",
                "label": "Open source article",
                "href": article.get("url"),
                "reason": "Inspect the original publication before analysis.",
            },
            {
                "type": "compare",
                "label": "Compare story",
                "href": f"/compare?articleId={article.get('id')}",
                "reason": "Look for similar coverage from other sources.",
            },
        ],
        "explanation": {
            "why_this_matters": "A saved source published a new article that can now be analyzed or compared.",
            "what_changed": {
                "source": source_name,
                "feed_type": (source_feed or {}).get("feed_type"),
                "article_url": article.get("url"),
            },
            "recommended_action": "Analyze the article or compare it across sources before treating it as evidence.",
        },
        "analysis": {},
        "is_read": False,
        "is_saved": False,
        "is_dismissed": False,
        "created_at": now_iso(),
    }


def save_ingested_articles(
    source: dict,
    source_feed: dict | None,
    items: list[dict],
    session_id: str = ANONYMOUS_SESSION_ID,
    card_limit: int = 10,
) -> dict:
    new_articles = []
    new_cards = []
    source_id = source.get("id")
    source_feed_id = (source_feed or {}).get("id")
    card_limit = max(0, min(card_limit, 50))

    if not source_id:
        raise FeedStoreError("Source id is required for ingestion.")

    if not database_enabled():
        existing_urls = {
            article.get("url")
            for article in INGESTED_ARTICLES
            if article.get("source_id") == source_id and article.get("url")
        }
        for raw_item in items:
            url = _clean_url(raw_item.get("url"))
            if not url or url in existing_urls:
                continue
            now = now_iso()
            article = {
                "id": str(uuid4()),
                "source_id": source_id,
                "source_feed_id": source_feed_id,
                "feed_item_id": None,
                "article_analysis_id": None,
                "url": url,
                "canonical_url": _clean_url(raw_item.get("canonical_url")) or url,
                "title": _clean_text(raw_item.get("title"), 300, "Untitled article"),
                "author": _clean_text(raw_item.get("author"), 200),
                "published_at": raw_item.get("published_at"),
                "language": raw_item.get("language") or source.get("language"),
                "country": raw_item.get("country") or source.get("country"),
                "summary": _clean_text(raw_item.get("summary"), 2000, ""),
                "extracted_text": None,
                "content_hash": _article_content_hash(raw_item),
                "event_fingerprint": _article_event_fingerprint(raw_item),
                "comparison_keywords": _article_keyword_tokens(raw_item.get("title"), raw_item.get("summary")),
                "raw_metadata": raw_item.get("raw") or {},
                "ingestion_status": "fetched",
                "analysis_status": "pending",
                "created_at": now,
                "updated_at": now,
            }
            INGESTED_ARTICLES.insert(0, article)
            existing_urls.add(url)
            new_articles.append(article)
            if len(new_cards) < card_limit:
                card = _build_ingested_article_card(source, source_feed, article, session_id=session_id)
                FEED_CARDS.insert(0, card)
                new_cards.append(card)
        return {"articles": new_articles, "cards": new_cards}

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for raw_item in items:
                    url = _clean_url(raw_item.get("url"))
                    if not url:
                        continue
                    comparison_keywords = _article_keyword_tokens(raw_item.get("title"), raw_item.get("summary"))
                    cur.execute(
                        """
                        insert into public.ingested_articles (
                            id, source_id, source_feed_id, feed_item_id, article_analysis_id,
                            url, canonical_url, title, author, published_at, language, country,
                            summary, extracted_text, content_hash, event_fingerprint,
                            comparison_keywords, raw_metadata, ingestion_status, analysis_status,
                            created_at, updated_at
                        )
                        values (
                            %s, %s, %s, null, null, %s, %s, %s, %s, %s, %s, %s,
                            %s, null, %s, %s, %s, %s, 'fetched', 'pending', now(), now()
                        )
                        on conflict (source_id, url) do nothing
                        returning
                            id, source_id, source_feed_id, feed_item_id, article_analysis_id,
                            url, canonical_url, title, author, published_at, language, country,
                            summary, extracted_text, content_hash, event_fingerprint,
                            comparison_keywords, raw_metadata, ingestion_status, analysis_status,
                            created_at, updated_at;
                        """,
                        (
                            str(uuid4()),
                            source_id,
                            source_feed_id,
                            url,
                            _clean_url(raw_item.get("canonical_url")) or url,
                            _clean_text(raw_item.get("title"), 300, "Untitled article"),
                            _clean_text(raw_item.get("author"), 200),
                            raw_item.get("published_at"),
                            raw_item.get("language") or source.get("language"),
                            raw_item.get("country") or source.get("country"),
                            _clean_text(raw_item.get("summary"), 2000, ""),
                            _article_content_hash(raw_item),
                            _article_event_fingerprint(raw_item),
                            Json(comparison_keywords),
                            Json(raw_item.get("raw") or {}),
                        ),
                    )
                    row = cur.fetchone()
                    if not row:
                        continue
                    article = _row_to_ingested_article_record(row)
                    new_articles.append(article)

                    if len(new_cards) < card_limit:
                        card = _build_ingested_article_card(source, source_feed, article, session_id=session_id)
                        cur.execute(
                            f"""
                            insert into public.feed_cards (
                                id, session_id, topic_id, article_id, alert_id, report_id,
                                source_id, source_feed_id, ingested_article_id, node_id, comparison_id,
                                title, summary, source, url, topic, card_type, priority,
                                priority_score, personalized_score, narrative_signal, evidence_score,
                                framing, payload, recommendations, explanation, analysis, is_read,
                                is_saved, is_dismissed, created_at, updated_at
                            )
                            values (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, now()
                            )
                            returning {_card_columns()};
                            """,
                            (
                                card["id"],
                                card["session_id"],
                                card["topic_id"],
                                card["article_id"],
                                card["alert_id"],
                                card["report_id"],
                                card["source_id"],
                                card["source_feed_id"],
                                card["ingested_article_id"],
                                card["node_id"],
                                card["comparison_id"],
                                card["title"],
                                card["summary"],
                                card["source"],
                                card["url"],
                                card["topic"],
                                card["card_type"],
                                card["priority"],
                                card["priority_score"],
                                card["personalized_score"],
                                card["narrative_signal"],
                                card["evidence_score"],
                                card["framing"],
                                Json(card["payload"]),
                                Json(card["recommendations"]),
                                Json(card["explanation"]),
                                Json(card["analysis"]),
                                card["is_read"],
                                card["is_saved"],
                                card["is_dismissed"],
                                card["created_at"],
                            ),
                        )
                        new_cards.append(_row_to_card(cur.fetchone()))
        return {"articles": new_articles, "cards": new_cards}
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not save ingested articles: {exc}") from exc


def list_ingested_article_records(source_id: str | None = None, limit: int = 100) -> list[dict]:
    limit = max(1, min(limit, 250))

    if not database_enabled():
        articles = [
            get_ingested_article_record(article["id"]) or article
            for article in INGESTED_ARTICLES
            if not source_id or article.get("source_id") == source_id
        ]
        return sorted(
            articles,
            key=lambda item: item.get("published_at") or item.get("created_at", ""),
            reverse=True,
        )[:limit]

    _ensure_schema()
    where_sql = "where ia.source_id::text = %s" if source_id else ""
    params: tuple[object, ...] = (source_id, limit) if source_id else (limit,)
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select
                        ia.*,
                        aa.id as analysis_id,
                        aa.analysis,
                        aa.created_at as analysis_created_at,
                        s.name as source_name,
                        s.website_url as source_website_url,
                        s.country as source_country,
                        s.language as source_language,
                        s.source_size,
                        s.source_type,
                        s.credibility_notes as source_credibility_notes,
                        s.political_context as source_political_context,
                        sf.feed_url,
                        sf.feed_type,
                        sf.title as feed_title
                    from public.ingested_articles ia
                    left join public.article_analyses aa on aa.id = ia.article_analysis_id
                    left join public.sources s on s.id = ia.source_id
                    left join public.source_feeds sf on sf.id = ia.source_feed_id
                    {where_sql}
                    order by coalesce(ia.published_at, ia.created_at) desc
                    limit %s;
                    """,
                    params,
                )
                return [_row_to_ingested_article_record(row) for row in cur.fetchall()]
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not list ingested articles: {exc}") from exc


def get_ingested_article_record(article_id: str) -> dict | None:
    if not database_enabled():
        for article in INGESTED_ARTICLES:
            if article.get("id") == article_id:
                enriched = {**article}
                source = next((item for item in SOURCES if item.get("id") == article.get("source_id")), None)
                source_feed = next((item for item in SOURCE_FEEDS if item.get("id") == article.get("source_feed_id")), None)
                if source:
                    enriched["source"] = source
                if source_feed:
                    enriched["source_feed"] = source_feed
                return enriched
        return None

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select
                        ia.*,
                        aa.id as analysis_id,
                        aa.analysis,
                        aa.created_at as analysis_created_at,
                        s.name as source_name,
                        s.website_url as source_website_url,
                        s.country as source_country,
                        s.language as source_language,
                        s.source_size,
                        s.source_type,
                        s.credibility_notes as source_credibility_notes,
                        s.political_context as source_political_context,
                        sf.feed_url,
                        sf.feed_type,
                        sf.title as feed_title
                    from public.ingested_articles ia
                    left join public.article_analyses aa on aa.id = ia.article_analysis_id
                    left join public.sources s on s.id = ia.source_id
                    left join public.source_feeds sf on sf.id = ia.source_feed_id
                    where ia.id::text = %s
                    limit 1;
                    """,
                    (article_id,),
                )
                row = cur.fetchone()
                return _row_to_ingested_article_record(row) if row else None
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not load ingested article: {exc}") from exc


def _article_analysis_payload(article: dict | None) -> dict:
    if not article:
        return {}
    analysis = article.get("analysis") or {}
    return analysis if isinstance(analysis, dict) else {}


def _hydrate_card_from_ingested_article(card: dict, article: dict | None) -> dict:
    if not card or not article:
        return card

    analysis = _article_analysis_payload(article)
    intelligence = analysis.get("intelligence") if isinstance(analysis.get("intelligence"), dict) else None
    source = article.get("source") if isinstance(article.get("source"), dict) else {}
    source_feed = article.get("source_feed") if isinstance(article.get("source_feed"), dict) else {}
    payload = {
        **(card.get("payload") or {}),
        "source_id": article.get("source_id") or card.get("source_id"),
        "source_name": source.get("name") or card.get("source"),
        "source_type": source.get("source_type"),
        "source_size": source.get("source_size"),
        "source_country": source.get("country") or article.get("country"),
        "source_language": source.get("language") or article.get("language"),
        "source_feed_id": article.get("source_feed_id") or card.get("source_feed_id"),
        "source_feed_type": source_feed.get("feed_type"),
        "feed_url": source_feed.get("feed_url"),
        "ingested_article_id": article.get("id"),
        "published_at": article.get("published_at"),
        "event_fingerprint": article.get("event_fingerprint"),
        "comparison_keywords": article.get("comparison_keywords") or [],
        "analysis_status": article.get("analysis_status") or "pending",
    }
    if intelligence:
        payload["intelligence"] = intelligence

    hydrated = {
        **card,
        "source_id": article.get("source_id") or card.get("source_id"),
        "source_feed_id": article.get("source_feed_id") or card.get("source_feed_id"),
        "ingested_article_id": article.get("id") or card.get("ingested_article_id"),
        "source": source.get("name") or card.get("source"),
        "payload": payload,
    }

    if analysis:
        confidence = _as_float(analysis.get("confidence"), _as_float(card.get("evidence_score"), 0.5))
        priority = analysis.get("priority") or (card.get("payload") or {}).get("priority") or "medium"
        score = _priority_score(priority, confidence)
        frames = analysis.get("narrative_framing") or []
        dominant_frame = frames[0] if frames else card.get("framing")
        report_id = article.get("article_analysis_id") or card.get("report_id") or card.get("article_id")
        hydrated.update(
            {
                "article_id": article.get("article_analysis_id") or card.get("article_id"),
                "report_id": report_id,
                "title": analysis.get("title") or article.get("title") or card.get("title"),
                "summary": analysis.get("summary") or article.get("summary") or card.get("summary"),
                "priority": score,
                "priority_score": score,
                "personalized_score": max(score, _as_float(card.get("personalized_score"), score)),
                "narrative_signal": (
                    f"Dominant frame: {dominant_frame}. Confidence reflects extraction and analysis quality, "
                    "not truth certainty."
                    if dominant_frame
                    else card.get("narrative_signal")
                ),
                "evidence_score": confidence,
                "framing": dominant_frame,
                "analysis": analysis,
            }
        )
        hydrated["recommendations"] = [
            {
                "type": "open_report",
                "label": "Open report",
                "href": f"/reports/{report_id}",
                "reason": "Review claims, framing, and extracted evidence signals.",
            },
            {
                "type": "compare",
                "label": "Compare story",
                "href": f"/compare?articleId={article.get('id')}",
                "reason": "Look for similar coverage from other sources.",
            },
            {
                "type": "open_source_article",
                "label": "Open source article",
                "href": article.get("url"),
                "reason": "Inspect the original publication before drawing conclusions.",
            },
        ]
    return hydrated


def hydrate_feed_card(card: dict | None) -> dict | None:
    if not card:
        return None
    ingested_article_id = card.get("ingested_article_id") or (card.get("payload") or {}).get("ingested_article_id")
    if not ingested_article_id:
        return card
    article = get_ingested_article_record(ingested_article_id)
    return _hydrate_card_from_ingested_article(card, article)


def get_feed_card_for_ingested_article(
    ingested_article_id: str,
    session_id: str = ANONYMOUS_SESSION_ID,
) -> dict | None:
    if not database_enabled():
        for card in FEED_CARDS:
            if (
                (card.get("ingested_article_id") or (card.get("payload") or {}).get("ingested_article_id"))
                == ingested_article_id
                and (card.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
                and not card.get("is_dismissed")
            ):
                return _hydrate_card_from_ingested_article(card, get_ingested_article_record(ingested_article_id))
        return None

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select {_card_columns()}
                    from public.feed_cards
                    where ingested_article_id::text = %s
                    and coalesce(session_id, %s) = %s
                    and is_dismissed = false
                    order by created_at desc
                    limit 1;
                    """,
                    (ingested_article_id, ANONYMOUS_SESSION_ID, session_id),
                )
                row = cur.fetchone()
                return hydrate_feed_card(_row_to_card(row)) if row else None
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not load ingested article feed card: {exc}") from exc


def _safe_text_list(value, limit: int = 20) -> list[str]:
    if not isinstance(value, list):
        return []

    cleaned = []
    seen = set()
    for item in value:
        if isinstance(item, dict):
            text = item.get("text") or item.get("claim") or item.get("label") or item.get("name")
        else:
            text = item
        text = re.sub(r"\s+", " ", str(text or "")).strip()
        key = text.lower()
        if text and key not in seen:
            cleaned.append(text)
            seen.add(key)
        if len(cleaned) >= limit:
            break
    return cleaned


def _compact_text(value: object, fallback: str = "", limit: int = 180) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip() or fallback
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def _node_slug(node_type: str, label: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (label or "").lower()).strip("-")
    return f"{node_type}-{slug[:72]}" if slug else f"{node_type}-{uuid4().hex[:12]}"


def _node_perspective(
    title: str,
    question: str,
    summary: str,
    signals: list[str] | None = None,
    limitations: list[str] | None = None,
    actions: list[dict] | None = None,
) -> dict:
    return {
        "title": title,
        "question": question,
        "summary": summary,
        "signals": _safe_text_list(signals or [], limit=8),
        "limitations": _safe_text_list(limitations or [], limit=8),
        "actions": actions or [],
    }


def _node_record(
    node_type: str,
    label: str,
    article: dict,
    session_id: str,
    node_metadata: dict,
    claim_text: str | None = None,
    source_id: str | None = None,
) -> dict:
    now = now_iso()
    clean_label = _compact_text(label, fallback=node_type.title(), limit=240)
    return {
        "id": str(uuid4()),
        "session_id": session_id,
        "node_type": node_type,
        "label": clean_label,
        "slug": _node_slug(node_type, clean_label),
        "source_id": source_id or article.get("source_id"),
        "ingested_article_id": article.get("id"),
        "topic_id": None,
        "claim_text": claim_text,
        "node_metadata": node_metadata,
        "created_at": now,
        "updated_at": now,
    }


def _edge_record(
    from_node_id: str,
    to_node_id: str,
    edge_type: str,
    weight: float = 0.5,
    evidence: list[dict] | None = None,
    session_id: str = ANONYMOUS_SESSION_ID,
) -> dict:
    return {
        "id": str(uuid4()),
        "session_id": session_id,
        "from_node_id": from_node_id,
        "to_node_id": to_node_id,
        "edge_type": edge_type,
        "weight": round(max(0.0, min(_as_float(weight, 0.5), 1.0)), 3),
        "evidence": evidence or [],
        "created_at": now_iso(),
    }


def _row_to_node(row: dict) -> dict:
    created_at = row.get("created_at")
    updated_at = row.get("updated_at")
    if isinstance(created_at, datetime):
        created_at = created_at.isoformat()
    if isinstance(updated_at, datetime):
        updated_at = updated_at.isoformat()
    return {
        "id": str(row.get("id")),
        "session_id": row.get("session_id") or ANONYMOUS_SESSION_ID,
        "node_type": row.get("node_type"),
        "label": row.get("label"),
        "slug": row.get("slug"),
        "source_id": str(row.get("source_id")) if row.get("source_id") else None,
        "ingested_article_id": str(row.get("ingested_article_id")) if row.get("ingested_article_id") else None,
        "topic_id": str(row.get("topic_id")) if row.get("topic_id") else None,
        "claim_text": row.get("claim_text"),
        "node_metadata": row.get("node_metadata") or {},
        "created_at": created_at or now_iso(),
        "updated_at": updated_at or created_at or now_iso(),
    }


def _row_to_node_edge(row: dict) -> dict:
    created_at = row.get("created_at")
    if isinstance(created_at, datetime):
        created_at = created_at.isoformat()
    return {
        "id": str(row.get("id")),
        "session_id": row.get("session_id") or ANONYMOUS_SESSION_ID,
        "from_node_id": str(row.get("from_node_id")),
        "to_node_id": str(row.get("to_node_id")),
        "edge_type": row.get("edge_type"),
        "weight": _as_float(row.get("weight"), 0.5),
        "evidence": row.get("evidence") or [],
        "created_at": created_at or now_iso(),
    }


def _entity_nodes_from_intelligence(intelligence: dict) -> list[tuple[str, str]]:
    entities = intelligence.get("entities") if isinstance(intelligence.get("entities"), dict) else {}
    entity_nodes: list[tuple[str, str]] = []
    for node_type, key in (
        ("person", "people"),
        ("organization", "organizations"),
        ("location", "locations"),
        ("event", "events"),
    ):
        for label in _safe_text_list(entities.get(key) or [], limit=4):
            entity_nodes.append((node_type, label))
    return entity_nodes[:10]


def _apply_node_perspective_hints(nodes: list[dict], intelligence: dict) -> list[dict]:
    hints = intelligence.get("node_perspectives") if isinstance(intelligence.get("node_perspectives"), dict) else {}
    provider_metadata = (
        intelligence.get("provider_metadata") if isinstance(intelligence.get("provider_metadata"), dict) else None
    )
    if not hints and not provider_metadata:
        return nodes

    for node in nodes:
        metadata = node.get("node_metadata") if isinstance(node.get("node_metadata"), dict) else {}
        perspective = metadata.get("perspective") if isinstance(metadata.get("perspective"), dict) else {}
        hint = hints.get(node.get("node_type")) or {}
        if node.get("node_type") in {"person", "organization", "location"}:
            hint = hint or hints.get("entity") or {}
        if hint:
            summary = _compact_text(
                hint.get("summary"),
                fallback=perspective.get("summary", ""),
                limit=420,
            )
            signals = _safe_text_list(
                [*(hint.get("signals") or []), *(perspective.get("signals") or [])],
                limit=8,
            )
            limitations = _safe_text_list(
                [*(hint.get("limitations") or []), *(perspective.get("limitations") or [])],
                limit=8,
            )
            perspective = {
                **perspective,
                "summary": summary,
                "signals": signals,
                "limitations": limitations,
            }
        if provider_metadata:
            perspective["provider_metadata"] = provider_metadata
            metadata["provider_metadata"] = provider_metadata
        metadata["perspective"] = perspective
        node["node_metadata"] = metadata
    return nodes


def _build_article_node_records(article: dict, session_id: str) -> tuple[list[dict], list[dict]]:
    analysis = _article_analysis_payload(article)
    intelligence = analysis.get("intelligence") if isinstance(analysis.get("intelligence"), dict) else {}
    source = article.get("source") if isinstance(article.get("source"), dict) else {}
    narrative = intelligence.get("narrative") if isinstance(intelligence.get("narrative"), dict) else {}
    source_analysis = (
        intelligence.get("source_analysis") if isinstance(intelligence.get("source_analysis"), dict) else {}
    )
    comparison_hooks = (
        intelligence.get("comparison_hooks") if isinstance(intelligence.get("comparison_hooks"), dict) else {}
    )

    claims = _safe_text_list(analysis.get("key_claims") or intelligence.get("key_claims") or [], limit=10)
    topics = _safe_text_list(analysis.get("topics") or [], limit=8)
    if not topics:
        topics = _safe_text_list(article.get("comparison_keywords") or comparison_hooks.get("similarity_keywords") or [], limit=8)
    frames = _safe_text_list(analysis.get("narrative_framing") or [], limit=8)
    if narrative.get("main_frame"):
        frames = _safe_text_list([narrative.get("main_frame"), *frames, *(narrative.get("secondary_frames") or [])], limit=8)
    missing_context = _safe_text_list(narrative.get("missing_context") or [], limit=8)
    title = article.get("title") or (intelligence.get("article") or {}).get("title") or article.get("url") or "Article"
    summary = intelligence.get("summary") or analysis.get("summary") or article.get("summary") or ""
    confidence = _as_float(analysis.get("confidence") or (intelligence.get("scores") or {}).get("confidence_score"), 0.5)
    compare_href = f"/compare?articleId={article.get('id')}"

    nodes: list[dict] = []
    edges: list[dict] = []

    article_node = _node_record(
        "article",
        title,
        article,
        session_id,
        {
            "tab": "Summary",
            "url": article.get("url"),
            "published_at": article.get("published_at"),
            "language": article.get("language") or source.get("language"),
            "country": article.get("country") or source.get("country"),
            "analysis_status": article.get("analysis_status") or "pending",
            "summary": summary,
            "key_claim_count": len(claims),
            "confidence": round(confidence, 3),
            "perspective": _node_perspective(
                "Article node",
                "Analyze this story as a standalone article.",
                _compact_text(summary, fallback="No article summary has been saved yet.", limit=320),
                signals=[
                    f"{len(claims)} extracted claims",
                    f"Main frame: {frames[0]}" if frames else "No dominant frame extracted",
                    f"Confidence: {round(confidence, 2)}",
                ],
                limitations=[
                    "This node summarizes extracted structure, not factual truth.",
                    "Full context requires source, compare, and OSINT review.",
                ],
                actions=[
                    {"type": "compare", "label": "Compare coverage", "href": compare_href},
                    {"type": "open_source_article", "label": "Open source article", "href": article.get("url")},
                ],
            ),
        },
        source_id=article.get("source_id"),
    )
    nodes.append(article_node)

    source_label = source.get("name") or article.get("source_id") or "Unknown source"
    source_actions = []
    if article.get("source_id"):
        source_actions.append(
            {
                "type": "open_source_profile",
                "label": "Open source profile",
                "href": f"/sources/{article.get('source_id')}",
            }
        )
    source_node = _node_record(
        "source",
        source_label,
        article,
        session_id,
        {
            "tab": "Source",
            "source": source,
            "source_analysis": source_analysis,
            "perspective": _node_perspective(
                "Source node",
                "Analyze this story through the publishing source.",
                source_analysis.get("source_profile")
                or f"{source_label} is the publishing source for this ingested article.",
                signals=[
                    source.get("source_type") or "Source type unknown",
                    source.get("source_size") or "Source size unknown",
                    source.get("political_context") or source_analysis.get("known_angle") or "No political context saved",
                ],
                limitations=[
                    source.get("credibility_notes") or "No credibility notes have been added for this source.",
                    "Source metadata is contextual and should not be treated as a verdict.",
                ],
                actions=source_actions,
            ),
        },
        source_id=article.get("source_id"),
    )
    nodes.append(source_node)
    edges.append(
        _edge_record(
            article_node["id"],
            source_node["id"],
            "published_by",
            weight=0.9,
            evidence=[{"field": "source_id", "value": article.get("source_id")}],
            session_id=session_id,
        )
    )

    author = article.get("author") or (intelligence.get("article") or {}).get("author") or "Unknown author"
    author_known = author != "Unknown author"
    author_node = _node_record(
        "author",
        author,
        article,
        session_id,
        {
            "tab": "Author",
            "known": author_known,
            "perspective": _node_perspective(
                "Author node",
                "Analyze this story through the author/byline node.",
                f"The byline is {author}." if author_known else "No author byline was extracted from this article.",
                signals=[f"Author: {author}", f"Source: {source_label}"],
                limitations=[
                    "Author identity can be absent, syndicated, or different from editorial framing.",
                    "Pattern analysis needs more articles by this author before drawing conclusions.",
                ],
            ),
        },
        source_id=article.get("source_id"),
    )
    nodes.append(author_node)
    edges.append(
        _edge_record(
            article_node["id"],
            author_node["id"],
            "attributed_to",
            weight=0.7 if author_known else 0.35,
            evidence=[{"field": "author", "value": author if author_known else None}],
            session_id=session_id,
        )
    )

    event_label = (
        article.get("event_fingerprint")
        or (comparison_hooks.get("event_fingerprint") if isinstance(comparison_hooks, dict) else None)
        or _compact_text(title, fallback="Article background", limit=90)
    )
    event_node = _node_record(
        "event",
        f"Background: {event_label}",
        article,
        session_id,
        {
            "tab": "Background",
            "event_fingerprint": article.get("event_fingerprint") or comparison_hooks.get("event_fingerprint"),
            "search_queries": comparison_hooks.get("search_queries") or [title],
            "missing_context": missing_context,
            "perspective": _node_perspective(
                "Event/background node",
                "Analyze the historical or event background around this story.",
                "This node groups background hooks, missing context, and event fingerprint signals.",
                signals=[
                    f"Event fingerprint: {article.get('event_fingerprint') or 'not set'}",
                    f"{len(missing_context)} missing-context notes",
                ],
                limitations=[
                    "Background is extracted from the article analysis and is not independent OSINT.",
                    "Step 7 will add bounded external OSINT references.",
                ],
            ),
        },
        source_id=article.get("source_id"),
    )
    nodes.append(event_node)
    edges.append(
        _edge_record(
            article_node["id"],
            event_node["id"],
            "contextualized_by",
            weight=0.75,
            evidence=[{"field": "event_fingerprint", "value": article.get("event_fingerprint")}],
            session_id=session_id,
        )
    )

    topic_nodes = []
    for topic in topics[:6]:
        node = _node_record(
            "topic",
            topic,
            article,
            session_id,
            {
                "tab": "Background",
                "keywords": _article_keyword_tokens(topic)[:8],
                "perspective": _node_perspective(
                    "Topic node",
                    "Analyze this topic across sources.",
                    f"This article contributes to the topic: {topic}.",
                    signals=[f"Topic label: {topic}", f"Source: {source_label}"],
                    limitations=[
                        "Topic grouping is keyword-based until broader clustering is added.",
                        "Use compare before inferring ecosystem-level coverage patterns.",
                    ],
                    actions=[{"type": "compare", "label": "Compare this story", "href": compare_href}],
                ),
            },
            source_id=article.get("source_id"),
        )
        topic_nodes.append(node)
        nodes.append(node)
        edges.append(
            _edge_record(
                article_node["id"],
                node["id"],
                "covers_topic",
                weight=0.68,
                evidence=[{"field": "topics", "value": topic}],
                session_id=session_id,
            )
        )

    claim_nodes = []
    for claim in claims[:8]:
        node = _node_record(
            "claim",
            claim,
            article,
            session_id,
            {
                "tab": "Claims",
                "claim": claim,
                "perspective": _node_perspective(
                    "Claim node",
                    "Compare this claim across agencies and sources.",
                    _compact_text(claim, limit=320),
                    signals=[f"Source: {source_label}", f"Article confidence: {round(confidence, 2)}"],
                    limitations=[
                        "Claim extraction captures what is asserted, not whether it is true.",
                        "Cross-source confirmation requires compare and OSINT context.",
                    ],
                    actions=[{"type": "compare", "label": "Compare related coverage", "href": compare_href}],
                ),
            },
            claim_text=claim,
            source_id=article.get("source_id"),
        )
        claim_nodes.append(node)
        nodes.append(node)
        edges.append(
            _edge_record(
                article_node["id"],
                node["id"],
                "contains_claim",
                weight=0.82,
                evidence=[{"field": "key_claims", "value": claim}],
                session_id=session_id,
            )
        )
        if topic_nodes:
            edges.append(
                _edge_record(
                    node["id"],
                    topic_nodes[0]["id"],
                    "relates_to_topic",
                    weight=0.52,
                    evidence=[{"field": "claim_topic_link", "value": topic_nodes[0]["label"]}],
                    session_id=session_id,
                )
            )

    narrative_nodes = []
    for frame in frames[:5]:
        node = _node_record(
            "narrative",
            frame,
            article,
            session_id,
            {
                "tab": "Summary",
                "frame": frame,
                "tone": narrative.get("tone"),
                "implied_causality": narrative.get("implied_causality"),
                "missing_context": missing_context,
                "perspective": _node_perspective(
                    "Narrative node",
                    "Analyze this article through its framing pattern.",
                    f"The story is framed around: {frame}.",
                    signals=[
                        f"Tone: {narrative.get('tone') or 'unknown'}",
                        f"Implied causality: {_compact_text(narrative.get('implied_causality'), fallback='not extracted', limit=140)}",
                    ],
                    limitations=[
                        "Framing labels are analytic summaries and can miss rhetorical nuance.",
                        "Narrative comparison needs other source coverage for contrast.",
                    ],
                ),
            },
            source_id=article.get("source_id"),
        )
        narrative_nodes.append(node)
        nodes.append(node)
        edges.append(
            _edge_record(
                article_node["id"],
                node["id"],
                "framed_as",
                weight=0.72,
                evidence=[{"field": "narrative_framing", "value": frame}],
                session_id=session_id,
            )
        )
        if claim_nodes:
            edges.append(
                _edge_record(
                    node["id"],
                    claim_nodes[0]["id"],
                    "frames_claim",
                    weight=0.5,
                    evidence=[{"field": "dominant_claim", "value": claim_nodes[0]["label"]}],
                    session_id=session_id,
                )
            )

    for entity_type, label in _entity_nodes_from_intelligence(intelligence):
        node = _node_record(
            entity_type,
            label,
            article,
            session_id,
            {
                "tab": "Background",
                "entity_type": entity_type,
                "perspective": _node_perspective(
                    f"{entity_type.title()} node",
                    f"Analyze this story through the {entity_type} reference.",
                    f"{label} appears in the extracted entity set for this article.",
                    signals=[f"Entity type: {entity_type}", f"Source: {source_label}"],
                    limitations=[
                        "Entity extraction can be incomplete or misclassified.",
                        "Use source article and OSINT context before interpreting entity significance.",
                    ],
                ),
            },
            source_id=article.get("source_id"),
        )
        nodes.append(node)
        edges.append(
            _edge_record(
                article_node["id"],
                node["id"],
                f"mentions_{entity_type}",
                weight=0.48,
                evidence=[{"field": "entities", "value": label}],
                session_id=session_id,
            )
        )

    return _apply_node_perspective_hints(nodes, intelligence), edges


def _persist_article_node_graph(article: dict, nodes: list[dict], edges: list[dict], session_id: str) -> None:
    article_id = article.get("id")
    if not article_id:
        return

    if not database_enabled():
        existing_ids = {
            node["id"]
            for node in NODES
            if node.get("ingested_article_id") == article_id
            and (node.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
        }
        NODES[:] = [
            node
            for node in NODES
            if not (
                node.get("ingested_article_id") == article_id
                and (node.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
            )
        ]
        NODE_EDGES[:] = [
            edge
            for edge in NODE_EDGES
            if edge.get("from_node_id") not in existing_ids and edge.get("to_node_id") not in existing_ids
        ]
        NODES.extend(nodes)
        NODE_EDGES.extend(edges)
        return

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    delete from public.nodes
                    where ingested_article_id::text = %s
                    and coalesce(session_id, %s) = %s;
                    """,
                    (article_id, ANONYMOUS_SESSION_ID, session_id),
                )
                cur.executemany(
                    """
                    insert into public.nodes (
                        id, session_id, node_type, label, slug, source_id,
                        ingested_article_id, topic_id, claim_text, node_metadata,
                        created_at, updated_at
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    [
                        (
                            node["id"],
                            node.get("session_id"),
                            node["node_type"],
                            node["label"],
                            node.get("slug"),
                            node.get("source_id"),
                            node.get("ingested_article_id"),
                            node.get("topic_id"),
                            node.get("claim_text"),
                            Json(node.get("node_metadata") or {}),
                            node.get("created_at"),
                            node.get("updated_at"),
                        )
                        for node in nodes
                    ],
                )
                cur.executemany(
                    """
                    insert into public.node_edges (
                        id, session_id, from_node_id, to_node_id,
                        edge_type, weight, evidence, created_at
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    [
                        (
                            edge["id"],
                            edge.get("session_id"),
                            edge["from_node_id"],
                            edge["to_node_id"],
                            edge["edge_type"],
                            edge["weight"],
                            Json(edge.get("evidence") or []),
                            edge.get("created_at"),
                        )
                        for edge in edges
                    ],
                )
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not persist article node graph: {exc}") from exc


def _node_type_counts(nodes: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for node in nodes:
        counts[node["node_type"]] = counts.get(node["node_type"], 0) + 1
    return counts


def build_article_node_graph(
    article_id: str,
    session_id: str = ANONYMOUS_SESSION_ID,
    node_type: str | None = None,
) -> dict | None:
    article = get_ingested_article_record(article_id)
    if not article:
        return None

    nodes, edges = _build_article_node_records(article, session_id=session_id)
    _persist_article_node_graph(article, nodes, edges, session_id=session_id)

    selected_node = None
    if node_type:
        selected_node = next((node for node in nodes if node.get("node_type") == node_type), None)
    if not selected_node:
        selected_node = next((node for node in nodes if node.get("node_type") == "article"), nodes[0] if nodes else None)

    return {
        "article_id": article_id,
        "status": "materialized",
        "nodes": nodes,
        "edges": edges,
        "node_type_counts": _node_type_counts(nodes),
        "selected_node": selected_node,
        "selected_perspective": (selected_node or {}).get("node_metadata", {}).get("perspective") if selected_node else None,
        "limitations": [
            "Node analysis exposes structured perspectives, not final factual judgment.",
            "Author/source/topic patterns need more articles before trend-level conclusions.",
            "OSINT references are added in the next Phase 2 step.",
        ],
    }


def build_ingested_article_detail(
    article_id: str,
    session_id: str = ANONYMOUS_SESSION_ID,
) -> dict | None:
    article = get_ingested_article_record(article_id)
    if not article:
        return None

    analysis = _article_analysis_payload(article)
    intelligence = analysis.get("intelligence") if isinstance(analysis.get("intelligence"), dict) else {}
    source = article.get("source") if isinstance(article.get("source"), dict) else None
    source_feed = article.get("source_feed") if isinstance(article.get("source_feed"), dict) else None
    feed_card = get_feed_card_for_ingested_article(article_id, session_id=session_id)
    comparison_hooks = intelligence.get("comparison_hooks") or {
        "search_queries": [article.get("title") or article.get("url")],
        "similarity_keywords": article.get("comparison_keywords") or [],
        "event_fingerprint": article.get("event_fingerprint") or "",
    }
    node_graph = build_article_node_graph(article_id, session_id=session_id)
    osint_context = build_bounded_osint_context(article)

    nodes_preview = [
        {
            "id": node.get("id"),
            "node_type": node.get("node_type"),
            "label": node.get("label"),
            "tab": (node.get("node_metadata") or {}).get("tab"),
            "status": article.get("analysis_status") or "pending",
        }
        for node in (node_graph or {}).get("nodes", [])[:14]
    ]

    return {
        "article": article,
        "source": source,
        "source_feed": source_feed,
        "analysis": analysis,
        "intelligence": intelligence,
        "feed_card": feed_card,
        "nodes_preview": nodes_preview,
        "node_graph": node_graph,
        "osint_context": osint_context,
        "comparison_hooks": comparison_hooks,
        "tabs": [
            "Summary",
            "Claims",
            "Compare",
            "Source",
            "Author",
            "Background",
            "OSINT",
        ],
        "limitations": [
            "This detail view reports extracted structure, not truth certainty.",
            "OSINT and compare panels are bounded context surfaces and should not be treated as final judgment.",
            "Node perspectives are derived from saved analysis and source metadata; they improve as more articles are ingested.",
        ],
    }


def _row_to_article_comparison(row: dict) -> dict:
    created_at = row.get("created_at")
    updated_at = row.get("updated_at")
    if isinstance(created_at, datetime):
        created_at = created_at.isoformat()
    if isinstance(updated_at, datetime):
        updated_at = updated_at.isoformat()

    return {
        "id": str(row.get("id")),
        "base_article_id": str(row.get("base_article_id")) if row.get("base_article_id") else None,
        "comparison_article_id": (
            str(row.get("comparison_article_id")) if row.get("comparison_article_id") else None
        ),
        "similarity_score": _as_float(row.get("similarity_score"), 0.0),
        "shared_claims": row.get("shared_claims") or [],
        "unique_claims_by_source": row.get("unique_claims_by_source") or [],
        "framing_differences": row.get("framing_differences") or [],
        "tone_differences": row.get("tone_differences") or [],
        "missing_context": row.get("missing_context") or [],
        "timeline_difference": row.get("timeline_difference") or {},
        "source_difference": row.get("source_difference") or {},
        "confidence": _as_float(row.get("confidence"), 0.0),
        "comparison_payload": row.get("comparison_payload") or {},
        "created_at": created_at or now_iso(),
        "updated_at": updated_at or created_at or now_iso(),
    }


def save_article_comparison_record(
    base_article_id: str,
    comparison_article_id: str,
    similarity_score: float,
    shared_claims: list[dict] | None = None,
    unique_claims_by_source: list[dict] | None = None,
    framing_differences: list[dict] | None = None,
    tone_differences: list[dict] | None = None,
    missing_context: list[str] | None = None,
    timeline_difference: dict | None = None,
    source_difference: dict | None = None,
    confidence: float = 0.0,
    comparison_payload: dict | None = None,
) -> dict:
    score = round(max(0.0, min(float(similarity_score or 0.0), 1.0)), 3)
    confidence_value = round(max(0.0, min(float(confidence or 0.0), 1.0)), 3)
    payload = comparison_payload or {}

    if not database_enabled():
        existing = next(
            (
                item
                for item in ARTICLE_COMPARISONS
                if item.get("base_article_id") == base_article_id
                and item.get("comparison_article_id") == comparison_article_id
            ),
            None,
        )
        now = now_iso()
        record = {
            "id": existing.get("id") if existing else str(uuid4()),
            "base_article_id": base_article_id,
            "comparison_article_id": comparison_article_id,
            "similarity_score": score,
            "shared_claims": shared_claims or [],
            "unique_claims_by_source": unique_claims_by_source or [],
            "framing_differences": framing_differences or [],
            "tone_differences": tone_differences or [],
            "missing_context": missing_context or [],
            "timeline_difference": timeline_difference or {},
            "source_difference": source_difference or {},
            "confidence": confidence_value,
            "comparison_payload": payload,
            "created_at": existing.get("created_at") if existing else now,
            "updated_at": now,
        }
        if existing:
            existing.clear()
            existing.update(record)
        else:
            ARTICLE_COMPARISONS.insert(0, record)
        return record

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    insert into public.article_comparisons (
                        id,
                        base_article_id,
                        comparison_article_id,
                        similarity_score,
                        shared_claims,
                        unique_claims_by_source,
                        framing_differences,
                        tone_differences,
                        missing_context,
                        timeline_difference,
                        source_difference,
                        confidence,
                        comparison_payload,
                        created_at,
                        updated_at
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                    on conflict (base_article_id, comparison_article_id) do update set
                        similarity_score = excluded.similarity_score,
                        shared_claims = excluded.shared_claims,
                        unique_claims_by_source = excluded.unique_claims_by_source,
                        framing_differences = excluded.framing_differences,
                        tone_differences = excluded.tone_differences,
                        missing_context = excluded.missing_context,
                        timeline_difference = excluded.timeline_difference,
                        source_difference = excluded.source_difference,
                        confidence = excluded.confidence,
                        comparison_payload = excluded.comparison_payload,
                        updated_at = now()
                    returning *;
                    """,
                    (
                        str(uuid4()),
                        base_article_id,
                        comparison_article_id,
                        score,
                        Json(shared_claims or []),
                        Json(unique_claims_by_source or []),
                        Json(framing_differences or []),
                        Json(tone_differences or []),
                        Json(missing_context or []),
                        Json(timeline_difference or {}),
                        Json(source_difference or {}),
                        confidence_value,
                        Json(payload),
                    ),
                )
                return _row_to_article_comparison(cur.fetchone())
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not save article comparison: {exc}") from exc


def create_feed_subscription(
    url: str,
    title: str | None = None,
    description: str | None = None,
    topic_id: str | None = None,
    session_id: str = ANONYMOUS_SESSION_ID,
) -> dict:
    feed_id = str(uuid4())
    now = now_iso()
    clean_url = url.strip()
    clean_title = title.strip()[:180] if title else clean_url
    clean_description = description.strip()[:500] if description else None

    if not database_enabled():
        feed = {
            "id": feed_id,
            "session_id": session_id,
            "topic_id": topic_id,
            "url": clean_url,
            "title": clean_title,
            "description": clean_description,
            "status": "active",
            "last_synced_at": None,
            "created_at": now,
            "item_count": 0,
        }
        FEEDS.insert(0, feed)
        return feed

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    insert into public.feeds (
                        id, session_id, topic_id, url, title, description, status, created_at, updated_at
                    )
                    values (%s, %s, %s, %s, %s, %s, 'active', now(), now())
                    returning id, session_id, topic_id, url, title, description, status, last_synced_at, created_at;
                    """,
                    (feed_id, session_id, topic_id, clean_url, clean_title, clean_description),
                )
                return _row_to_feed(cur.fetchone())
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not create feed subscription: {exc}") from exc


def list_feed_subscriptions(session_id: str = ANONYMOUS_SESSION_ID) -> list[dict]:
    if not database_enabled():
        feeds = [
            feed
            for feed in FEEDS
            if (feed.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
        ]
        for feed in feeds:
            feed["item_count"] = len(
                [
                    item
                    for item in FEED_ITEMS
                    if item.get("feed_id") == feed["id"]
                    and (item.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
                ]
            )
        return sorted(feeds, key=lambda feed: feed.get("created_at", ""), reverse=True)

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select
                        f.id,
                        f.session_id,
                        f.topic_id,
                        f.url,
                        f.title,
                        f.description,
                        f.status,
                        f.last_synced_at,
                        f.created_at,
                        count(fi.id) as item_count
                    from public.feeds f
                    left join public.feed_items fi
                        on fi.feed_id = f.id
                        and coalesce(fi.session_id, %s) = %s
                    where coalesce(f.session_id, %s) = %s
                    group by f.id, f.session_id, f.topic_id, f.url, f.title, f.description,
                        f.status, f.last_synced_at, f.created_at
                    order by f.created_at desc;
                    """,
                    (ANONYMOUS_SESSION_ID, session_id, ANONYMOUS_SESSION_ID, session_id),
                )
                return [_row_to_feed(row) for row in cur.fetchall()]
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not list feed subscriptions: {exc}") from exc


def get_feed_subscription(feed_id: str, session_id: str = ANONYMOUS_SESSION_ID) -> dict | None:
    if not database_enabled():
        for feed in FEEDS:
            if feed["id"] == feed_id and (feed.get("session_id") or ANONYMOUS_SESSION_ID) == session_id:
                return feed
        return None

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select id, session_id, topic_id, url, title, description, status, last_synced_at, created_at
                    from public.feeds
                    where id = %s and coalesce(session_id, %s) = %s
                    limit 1;
                    """,
                    (feed_id, ANONYMOUS_SESSION_ID, session_id),
                )
                row = cur.fetchone()
                return _row_to_feed(row) if row else None
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not load feed subscription: {exc}") from exc


def _feed_item_exists(feed_id: str, session_id: str, url: str | None, external_id: str | None) -> bool:
    if not database_enabled():
        for item in FEED_ITEMS:
            if item.get("feed_id") != feed_id:
                continue
            if (item.get("session_id") or ANONYMOUS_SESSION_ID) != session_id:
                continue
            if url and item.get("url") == url:
                return True
            if external_id and item.get("external_id") == external_id:
                return True
        return False

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select 1
                    from public.feed_items
                    where feed_id = %s
                    and coalesce(session_id, %s) = %s
                    and ((%s is not null and url = %s) or (%s is not null and external_id = %s))
                    limit 1;
                    """,
                    (feed_id, ANONYMOUS_SESSION_ID, session_id, url, url, external_id, external_id),
                )
                return bool(cur.fetchone())
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not check feed item duplicate: {exc}") from exc


def _build_feed_item_card(feed: dict, item: dict) -> dict:
    return {
        "id": str(uuid4()),
        "session_id": item.get("session_id") or ANONYMOUS_SESSION_ID,
        "topic_id": feed.get("topic_id"),
        "article_id": None,
        "alert_id": None,
        "report_id": None,
        "card_type": "feed_item",
        "title": item.get("title") or "New feed item",
        "summary": item.get("summary") or "A subscribed feed published a new item.",
        "source": feed.get("title"),
        "url": item.get("url"),
        "topic": feed.get("title"),
        "priority": 0.5,
        "priority_score": 0.5,
        "personalized_score": 0.5,
        "narrative_signal": "RSS ingestion signal only. Analyze the article before drawing conclusions.",
        "evidence_score": None,
        "framing": None,
        "payload": {
            "feed_id": feed.get("id"),
            "feed_title": feed.get("title"),
            "item_id": item.get("id"),
            "domain": item.get("source"),
            "published_at": item.get("published_at"),
            "url": item.get("url"),
        },
        "recommendations": [
            {
                "type": "open_source",
                "label": "Open source",
                "href": item.get("url") or feed.get("url"),
                "reason": "Review the source item before analysis.",
            }
        ],
        "explanation": {
            "why_this_matters": "A subscribed RSS source published a new item.",
            "what_changed": {"feed": feed.get("title"), "item_url": item.get("url")},
            "recommended_action": "Open the item or analyze the article URL for narrative signals.",
        },
        "analysis": {},
        "is_read": False,
        "is_saved": False,
        "is_dismissed": False,
        "created_at": now_iso(),
    }


def save_feed_items(
    feed: dict,
    items: list[dict],
    session_id: str = ANONYMOUS_SESSION_ID,
    card_limit: int = 5,
) -> dict:
    new_items = []
    new_cards = []

    if not database_enabled():
        for raw_item in items:
            url = raw_item.get("url")
            external_id = raw_item.get("external_id")
            if _feed_item_exists(feed["id"], session_id, url, external_id):
                continue

            item = {
                "id": str(uuid4()),
                "session_id": session_id,
                "feed_id": feed["id"],
                "external_id": external_id,
                "title": raw_item.get("title") or "Untitled feed item",
                "summary": raw_item.get("summary") or "",
                "url": url,
                "source": raw_item.get("source") or feed.get("title"),
                "published_at": raw_item.get("published_at"),
                "raw": raw_item.get("raw") or {},
                "created_at": now_iso(),
            }
            FEED_ITEMS.insert(0, item)
            new_items.append(item)
            if len(new_cards) < card_limit:
                card = _build_feed_item_card(feed, item)
                FEED_CARDS.insert(0, card)
                new_cards.append(card)

        feed["last_synced_at"] = now_iso()
        feed["item_count"] = len([item for item in FEED_ITEMS if item.get("feed_id") == feed["id"]])
        return {"items": new_items, "cards": new_cards}

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for raw_item in items:
                    url = raw_item.get("url")
                    external_id = raw_item.get("external_id")
                    if _feed_item_exists(feed["id"], session_id, url, external_id):
                        continue

                    item_id = str(uuid4())
                    cur.execute(
                        """
                        insert into public.feed_items (
                            id, session_id, feed_id, external_id, title, summary, url,
                            source, published_at, raw, created_at
                        )
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                        returning id, session_id, feed_id, external_id, title, summary, url,
                            source, published_at, raw, created_at;
                        """,
                        (
                            item_id,
                            session_id,
                            feed["id"],
                            external_id,
                            raw_item.get("title") or "Untitled feed item",
                            raw_item.get("summary") or "",
                            url,
                            raw_item.get("source") or feed.get("title"),
                            raw_item.get("published_at"),
                            Json(raw_item.get("raw") or {}),
                        ),
                    )
                    item = _row_to_feed_item(cur.fetchone())
                    new_items.append(item)

                    if len(new_cards) < card_limit:
                        card = _build_feed_item_card(feed, item)
                        cur.execute(
                            f"""
                            insert into public.feed_cards (
                                id, session_id, topic_id, article_id, alert_id, report_id, title,
                                summary, source, url, topic, card_type, priority, priority_score,
                                personalized_score, narrative_signal, evidence_score, framing,
                                payload, recommendations, explanation, analysis, is_read, is_saved,
                                is_dismissed, created_at, updated_at
                            )
                            values (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
                            )
                            returning {_card_columns()};
                            """,
                            (
                                card["id"],
                                card["session_id"],
                                card["topic_id"],
                                card["article_id"],
                                card["alert_id"],
                                card["report_id"],
                                card["title"],
                                card["summary"],
                                card["source"],
                                card["url"],
                                card["topic"],
                                card["card_type"],
                                card["priority"],
                                card["priority_score"],
                                card["personalized_score"],
                                card["narrative_signal"],
                                card["evidence_score"],
                                card["framing"],
                                Json(card["payload"]),
                                Json(card["recommendations"]),
                                Json(card["explanation"]),
                                Json(card["analysis"]),
                                card["is_read"],
                                card["is_saved"],
                                card["is_dismissed"],
                                card["created_at"],
                            ),
                        )
                        new_cards.append(_row_to_card(cur.fetchone()))

                cur.execute(
                    """
                    update public.feeds
                    set last_synced_at = now(), updated_at = now()
                    where id = %s and coalesce(session_id, %s) = %s;
                    """,
                    (feed["id"], ANONYMOUS_SESSION_ID, session_id),
                )
        return {"items": new_items, "cards": new_cards}
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not save feed items: {exc}") from exc


def _source_from_ingested_article(ingested_article: dict) -> dict:
    source_value = ingested_article.get("source") or {}
    source = source_value if isinstance(source_value, dict) else {}
    source_name = source.get("name") or (source_value if isinstance(source_value, str) else None)
    return {
        "id": ingested_article.get("source_id") or source.get("id"),
        "name": source_name or ingested_article.get("source_name") or "Unknown source",
        "website_url": source.get("website_url"),
        "country": ingested_article.get("country") or source.get("country"),
        "language": ingested_article.get("language") or source.get("language"),
        "source_size": source.get("source_size"),
        "source_type": source.get("source_type"),
        "credibility_notes": source.get("credibility_notes"),
        "political_context": source.get("political_context"),
    }


def _source_feed_from_ingested_article(ingested_article: dict) -> dict:
    source_feed_value = ingested_article.get("source_feed") or {}
    source_feed = source_feed_value if isinstance(source_feed_value, dict) else {}
    return {
        "id": ingested_article.get("source_feed_id") or source_feed.get("id"),
        "feed_url": source_feed.get("feed_url"),
        "feed_type": source_feed.get("feed_type"),
        "title": source_feed.get("title"),
    }


def _build_analyzed_ingested_card(
    ingested_article: dict,
    article: ExtractedArticle,
    analysis: dict,
    session_id: str,
    structured_analysis: dict | None = None,
    existing_card: dict | None = None,
) -> dict:
    card = build_card(article, analysis, session_id=session_id)
    source = _source_from_ingested_article(ingested_article)
    source_feed = _source_feed_from_ingested_article(ingested_article)
    analysis_payload = {**analysis}
    if structured_analysis:
        analysis_payload["intelligence"] = structured_analysis

    if existing_card:
        card["id"] = existing_card.get("id") or card["id"]
        card["is_read"] = bool(existing_card.get("is_read"))
        card["is_saved"] = bool(existing_card.get("is_saved"))
        card["is_dismissed"] = bool(existing_card.get("is_dismissed"))
        card["created_at"] = existing_card.get("created_at") or card["created_at"]

    card.update(
        {
            "source_id": source.get("id"),
            "source_feed_id": source_feed.get("id"),
            "ingested_article_id": ingested_article.get("id"),
            "node_id": None,
            "comparison_id": None,
            "analysis": analysis_payload,
        }
    )
    card["payload"] = {
        **(card.get("payload") or {}),
        "source_id": source.get("id"),
        "source_name": source.get("name"),
        "source_type": source.get("source_type"),
        "source_size": source.get("source_size"),
        "source_country": source.get("country"),
        "source_language": source.get("language"),
        "source_feed_id": source_feed.get("id"),
        "source_feed_type": source_feed.get("feed_type"),
        "feed_url": source_feed.get("feed_url"),
        "ingested_article_id": ingested_article.get("id"),
        "published_at": ingested_article.get("published_at"),
        "event_fingerprint": ingested_article.get("event_fingerprint"),
        "comparison_keywords": ingested_article.get("comparison_keywords") or [],
        "analysis_status": "analyzed",
        "intelligence": structured_analysis,
    }
    card["recommendations"] = [
        {
            "type": "open_report",
            "label": "Open report",
            "href": f"/reports/{card['report_id']}",
            "reason": "Review the structured analysis generated from the ingested article.",
        },
        {
            "type": "compare",
            "label": "Compare story",
            "href": f"/compare?articleId={ingested_article.get('id')}",
            "reason": "Look for similar coverage from other sources.",
        },
        {
            "type": "open_source_article",
            "label": "Open source article",
            "href": article.final_url,
            "reason": "Inspect the original publication before drawing conclusions.",
        },
    ]
    card["explanation"] = {
        "why_this_matters": (
            "This ingested article has been converted into structured narrative intelligence "
            "and linked back to its source record."
        ),
        "what_changed": {
            "analysis_status": "analyzed",
            "key_claims": analysis.get("key_claims") or [],
            "narrative_framing": analysis.get("narrative_framing") or [],
            "source": source.get("name"),
        },
        "recommended_action": "Open the report, then compare the same story across sources before acting on it.",
    }
    return card


def mark_ingested_article_analysis_failed(
    ingested_article_id: str,
    error: str,
) -> dict | None:
    clean_error = _clean_text(error, 1000, "Analysis failed.")

    if not database_enabled():
        for article in INGESTED_ARTICLES:
            if article.get("id") == ingested_article_id:
                raw_metadata = article.get("raw_metadata") or {}
                article["analysis_status"] = "failed"
                article["raw_metadata"] = {**raw_metadata, "analysis_error": clean_error}
                article["updated_at"] = now_iso()
                return article
        return None

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    update public.ingested_articles
                    set
                        analysis_status = 'failed',
                        raw_metadata = raw_metadata || %s::jsonb,
                        updated_at = now()
                    where id::text = %s
                    returning *;
                    """,
                    (Json({"analysis_error": clean_error}), ingested_article_id),
                )
                row = cur.fetchone()
                return _row_to_ingested_article_record(row) if row else None
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not mark ingested article analysis failed: {exc}") from exc


def save_ingested_article_analysis_card(
    ingested_article: dict,
    article: ExtractedArticle,
    analysis: dict,
    session_id: str = ANONYMOUS_SESSION_ID,
    structured_analysis: dict | None = None,
) -> dict:
    ingested_article_id = ingested_article.get("id")
    if not ingested_article_id:
        raise FeedStoreError("Ingested article id is required for analysis persistence.")

    if not database_enabled():
        existing_card = next(
            (
                card
                for card in FEED_CARDS
                if card.get("ingested_article_id") == ingested_article_id
                and (card.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
            ),
            None,
        )
        card = _build_analyzed_ingested_card(
            ingested_article,
            article,
            analysis,
            session_id=session_id,
            structured_analysis=structured_analysis,
            existing_card=existing_card,
        )
        for item in INGESTED_ARTICLES:
            if item.get("id") == ingested_article_id:
                item.update(
                    {
                        "article_analysis_id": card["article_id"],
                        "title": article.title,
                        "summary": analysis.get("summary") or item.get("summary"),
                        "extracted_text": article.text,
                        "analysis_status": "analyzed",
                        "updated_at": now_iso(),
                    }
                )
                if structured_analysis:
                    item["analysis"] = {**analysis, "intelligence": structured_analysis}
                else:
                    item["analysis"] = analysis
                break

        if existing_card:
            existing_card.clear()
            existing_card.update(card)
        else:
            FEED_CARDS.insert(0, card)
        return card

    existing_card = None
    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select {_card_columns()}
                    from public.feed_cards
                    where ingested_article_id::text = %s
                    and coalesce(session_id, %s) = %s
                    order by created_at desc
                    limit 1;
                    """,
                    (ingested_article_id, ANONYMOUS_SESSION_ID, session_id),
                )
                row = cur.fetchone()
                if row:
                    existing_card = _row_to_card(row)

                card = _build_analyzed_ingested_card(
                    ingested_article,
                    article,
                    analysis,
                    session_id=session_id,
                    structured_analysis=structured_analysis,
                    existing_card=existing_card,
                )
                analysis_id = card["article_id"]
                analysis_payload = card["analysis"]

                cur.execute(
                    """
                    insert into public.article_analyses (
                        id,
                        session_id,
                        ingested_article_id,
                        url,
                        final_url,
                        title,
                        source,
                        domain,
                        extracted_text,
                        analysis
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        analysis_id,
                        card["session_id"],
                        ingested_article_id,
                        article.url,
                        article.final_url,
                        article.title,
                        article.source,
                        article.domain,
                        article.text,
                        Json(analysis_payload),
                    ),
                )
                cur.execute(
                    """
                    update public.ingested_articles
                    set
                        article_analysis_id = %s,
                        title = %s,
                        summary = %s,
                        extracted_text = %s,
                        analysis_status = 'analyzed',
                        updated_at = now()
                    where id::text = %s;
                    """,
                    (
                        analysis_id,
                        article.title,
                        analysis.get("summary") or ingested_article.get("summary") or "",
                        article.text,
                        ingested_article_id,
                    ),
                )

                if existing_card:
                    cur.execute(
                        f"""
                        update public.feed_cards
                        set
                            topic_id = %s,
                            article_id = %s,
                            alert_id = %s,
                            report_id = %s,
                            source_id = %s,
                            source_feed_id = %s,
                            ingested_article_id = %s,
                            node_id = %s,
                            comparison_id = %s,
                            title = %s,
                            summary = %s,
                            source = %s,
                            url = %s,
                            topic = %s,
                            card_type = %s,
                            priority = %s,
                            priority_score = %s,
                            personalized_score = %s,
                            narrative_signal = %s,
                            evidence_score = %s,
                            framing = %s,
                            payload = %s,
                            recommendations = %s,
                            explanation = %s,
                            analysis = %s,
                            updated_at = now()
                        where id::text = %s
                        and coalesce(session_id, %s) = %s
                        returning {_card_columns()};
                        """,
                        (
                            card["topic_id"],
                            card["article_id"],
                            card["alert_id"],
                            card["report_id"],
                            card["source_id"],
                            card["source_feed_id"],
                            card["ingested_article_id"],
                            card["node_id"],
                            card["comparison_id"],
                            card["title"],
                            card["summary"],
                            card["source"],
                            card["url"],
                            card["topic"],
                            card["card_type"],
                            card["priority"],
                            card["priority_score"],
                            card["personalized_score"],
                            card["narrative_signal"],
                            card["evidence_score"],
                            card["framing"],
                            Json(card["payload"]),
                            Json(card["recommendations"]),
                            Json(card["explanation"]),
                            Json(card["analysis"]),
                            card["id"],
                            ANONYMOUS_SESSION_ID,
                            session_id,
                        ),
                    )
                    updated = cur.fetchone()
                    if updated:
                        return _row_to_card(updated)

                cur.execute(
                    f"""
                    insert into public.feed_cards (
                        id, session_id, topic_id, article_id, alert_id, report_id,
                        source_id, source_feed_id, ingested_article_id, node_id, comparison_id,
                        title, summary, source, url, topic, card_type, priority,
                        priority_score, personalized_score, narrative_signal, evidence_score,
                        framing, payload, recommendations, explanation, analysis, is_read,
                        is_saved, is_dismissed, created_at, updated_at
                    )
                    values (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, now()
                    )
                    returning {_card_columns()};
                    """,
                    (
                        card["id"],
                        card["session_id"],
                        card["topic_id"],
                        card["article_id"],
                        card["alert_id"],
                        card["report_id"],
                        card["source_id"],
                        card["source_feed_id"],
                        card["ingested_article_id"],
                        card["node_id"],
                        card["comparison_id"],
                        card["title"],
                        card["summary"],
                        card["source"],
                        card["url"],
                        card["topic"],
                        card["card_type"],
                        card["priority"],
                        card["priority_score"],
                        card["personalized_score"],
                        card["narrative_signal"],
                        card["evidence_score"],
                        card["framing"],
                        Json(card["payload"]),
                        Json(card["recommendations"]),
                        Json(card["explanation"]),
                        Json(card["analysis"]),
                        card["is_read"],
                        card["is_saved"],
                        card["is_dismissed"],
                        card["created_at"],
                    ),
                )
                return _row_to_card(cur.fetchone())
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not save ingested article analysis: {exc}") from exc


def save_analysis_card(
    article: ExtractedArticle,
    analysis: dict,
    session_id: str = ANONYMOUS_SESSION_ID,
) -> dict:
    card = build_card(article, analysis, session_id=session_id)

    if not database_enabled():
        FEED_CARDS.insert(0, card)
        return card

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    insert into public.article_analyses (
                        id,
                        session_id,
                        url,
                        final_url,
                        title,
                        source,
                        domain,
                        extracted_text,
                        analysis
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        card["article_id"],
                        card["session_id"],
                        article.url,
                        article.final_url,
                        article.title,
                        article.source,
                        article.domain,
                        article.text,
                        Json(analysis),
                    ),
                )
                cur.execute(
                    f"""
                    insert into public.feed_cards (
                        id,
                        session_id,
                        topic_id,
                        article_id,
                        alert_id,
                        report_id,
                        title,
                        summary,
                        source,
                        url,
                        topic,
                        card_type,
                        priority,
                        priority_score,
                        personalized_score,
                        narrative_signal,
                        evidence_score,
                        framing,
                        payload,
                        recommendations,
                        explanation,
                        analysis,
                        is_read,
                        is_saved,
                        is_dismissed,
                        created_at,
                        updated_at
                    )
                    values (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
                    )
                    returning {_card_columns()};
                    """,
                    (
                        card["id"],
                        card["session_id"],
                        card["topic_id"],
                        card["article_id"],
                        card["alert_id"],
                        card["report_id"],
                        card["title"],
                        card["summary"],
                        card["source"],
                        card["url"],
                        card["topic"],
                        card["card_type"],
                        card["priority"],
                        card["priority_score"],
                        card["personalized_score"],
                        card["narrative_signal"],
                        card["evidence_score"],
                        card["framing"],
                        Json(card["payload"]),
                        Json(card["recommendations"]),
                        Json(card["explanation"]),
                        Json(card["analysis"]),
                        card["is_read"],
                        card["is_saved"],
                        card["is_dismissed"],
                        card["created_at"],
                    ),
                )
                return _row_to_card(cur.fetchone())
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not save analyzed card: {exc}") from exc


def _filtered_in_memory(filter_type: str, session_id: str) -> list[dict]:
    cards = [
        card
        for card in FEED_CARDS
        if not card.get("is_dismissed")
        and (card.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
    ]
    if filter_type == "high":
        return [card for card in cards if card.get("priority_score", 0) >= 0.75]
    if filter_type == "unread":
        return [card for card in cards if not card.get("is_read")]
    if filter_type == "narrative":
        return [
            card
            for card in cards
            if card.get("card_type")
            in {
                "narrative_frame_shift",
                "coverage_change",
                "source_ecosystem_change",
                "divergence_increase",
            }
        ]
    if filter_type == "articles":
        return [card for card in cards if card.get("card_type") in {"article_insight", "ingested_article"}]
    if filter_type == "saved":
        return [card for card in cards if card.get("is_saved")]
    return cards


def list_feed_cards(
    filter_type: str = "all",
    limit: int = 20,
    session_id: str = ANONYMOUS_SESSION_ID,
) -> list[dict]:
    limit = max(1, min(limit, 100))

    if not database_enabled():
        cards = _filtered_in_memory(filter_type, session_id)
        cards = sorted(cards, key=lambda card: card.get("created_at", ""), reverse=True)[:limit]
        return [hydrate_feed_card(card) or card for card in cards]

    _ensure_schema()
    where = ["is_dismissed = false", "coalesce(session_id, %s) = %s"]
    params: list[object] = [ANONYMOUS_SESSION_ID, session_id]

    if filter_type == "high":
        where.append("coalesce(priority_score, 0) >= 0.75")
    elif filter_type == "unread":
        where.append("is_read = false")
    elif filter_type == "narrative":
        where.append(
            "card_type in ('narrative_frame_shift', 'coverage_change', "
            "'source_ecosystem_change', 'divergence_increase')"
        )
    elif filter_type == "articles":
        where.append("card_type in ('article_insight', 'ingested_article')")
    elif filter_type == "saved":
        where.append("is_saved = true")

    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select {_card_columns()}
                    from public.feed_cards
                    where {" and ".join(where)}
                    order by created_at desc
                    limit %s;
                    """,
                    (*params, limit),
                )
                cards = [_row_to_card(row) for row in cur.fetchall()]
                return [hydrate_feed_card(card) or card for card in cards]
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not load feed cards: {exc}") from exc


def _is_alert_card(card: dict) -> bool:
    card_type = card.get("card_type")
    if card_type in _ALERT_CARD_TYPES:
        return True
    return (
        card_type == "article_insight"
        and _as_float(card.get("priority_score"), 0.0) >= _HIGH_PRIORITY_ALERT_THRESHOLD
    )


def _alert_sql_condition() -> str:
    placeholders = ", ".join(["%s"] * len(_ALERT_CARD_TYPES))
    return (
        f"(card_type in ({placeholders}) "
        "or (card_type = %s and coalesce(priority_score, 0) >= %s))"
    )


def _alert_sql_params() -> list[object]:
    return [*_ALERT_CARD_TYPES, "article_insight", _HIGH_PRIORITY_ALERT_THRESHOLD]


def _alert_kind(card: dict) -> str:
    card_type = card.get("card_type")
    if card_type == "topic_monitor":
        return "topic"
    if card_type == "feed_item":
        return "feed"
    if card_type == "ingested_article":
        return "source"
    if card_type == "article_insight":
        return "analysis"
    if card_type in {
        "narrative_frame_shift",
        "coverage_change",
        "source_ecosystem_change",
        "divergence_increase",
    }:
        return "narrative"
    return "signal"


def _alert_href(card: dict) -> str:
    for recommendation in card.get("recommendations") or []:
        href = recommendation.get("href")
        if isinstance(href, str) and href.startswith("/"):
            return href
    if card.get("report_id"):
        return f"/reports/{card['report_id']}"
    if card.get("topic_id"):
        return f"/topics/{card['topic_id']}"
    return f"/feed/{card['id']}"


def _alert_priority_label(card: dict) -> str:
    score = _as_float(card.get("priority_score"), 0.0)
    if score >= 0.85:
        return "High"
    if score >= 0.6:
        return "Medium"
    return "Low"


def build_alert_from_card(card: dict) -> dict:
    payload = card.get("payload") or {}
    return {
        "id": card.get("id"),
        "source_alert_id": card.get("alert_id"),
        "card_id": card.get("id"),
        "session_id": card.get("session_id") or ANONYMOUS_SESSION_ID,
        "type": _alert_kind(card),
        "card_type": card.get("card_type"),
        "title": card.get("title") or "Untitled alert",
        "summary": card.get("summary") or "",
        "source": card.get("source") or payload.get("feed_title") or payload.get("domain"),
        "topic": card.get("topic"),
        "priority_score": _as_float(card.get("priority_score"), 0.0),
        "priority_label": _alert_priority_label(card),
        "is_read": bool(card.get("is_read")),
        "created_at": card.get("created_at"),
        "href": _alert_href(card),
        "external_url": card.get("url") or payload.get("url"),
        "narrative_signal": card.get("narrative_signal"),
        "recommendations": card.get("recommendations") or [],
        "payload": payload,
    }


def list_alerts(
    session_id: str = ANONYMOUS_SESSION_ID,
    limit: int = 50,
    unread_only: bool = False,
) -> list[dict]:
    limit = max(1, min(limit, 100))

    if not database_enabled():
        cards = [
            card
            for card in FEED_CARDS
            if (card.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
            and not card.get("is_dismissed")
            and _is_alert_card(card)
            and (not unread_only or not card.get("is_read"))
        ]
        cards = sorted(cards, key=lambda card: card.get("created_at", ""), reverse=True)
        return [build_alert_from_card(card) for card in cards[:limit]]

    _ensure_schema()
    where = [
        "coalesce(session_id, %s) = %s",
        "is_dismissed = false",
        _alert_sql_condition(),
    ]
    params: list[object] = [ANONYMOUS_SESSION_ID, session_id, *_alert_sql_params()]
    if unread_only:
        where.append("is_read = false")

    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select {_card_columns()}
                    from public.feed_cards
                    where {" and ".join(where)}
                    order by created_at desc
                    limit %s;
                    """,
                    (*params, limit),
                )
                return [build_alert_from_card(_row_to_card(row)) for row in cur.fetchall()]
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not load alerts: {exc}") from exc


def get_unread_alert_count(session_id: str = ANONYMOUS_SESSION_ID) -> int:
    if not database_enabled():
        return sum(
            1
            for card in FEED_CARDS
            if (card.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
            and not card.get("is_dismissed")
            and not card.get("is_read")
            and _is_alert_card(card)
        )

    _ensure_schema()
    where = [
        "coalesce(session_id, %s) = %s",
        "is_dismissed = false",
        "is_read = false",
        _alert_sql_condition(),
    ]
    params: list[object] = [ANONYMOUS_SESSION_ID, session_id, *_alert_sql_params()]

    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select count(*) as unread_count
                    from public.feed_cards
                    where {" and ".join(where)};
                    """,
                    params,
                )
                row = cur.fetchone() or {}
                return int(row.get("unread_count") or 0)
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not count unread alerts: {exc}") from exc


def mark_alert_read(
    alert_id: str,
    session_id: str = ANONYMOUS_SESSION_ID,
) -> dict | None:
    card = update_feed_card(alert_id, session_id=session_id, is_read=True)
    if not card:
        for alert in list_alerts(session_id=session_id, limit=100):
            if alert.get("id") == alert_id and alert.get("card_id"):
                card = update_feed_card(alert["card_id"], session_id=session_id, is_read=True)
                break
    if not card or not _is_alert_card(card):
        return None
    return build_alert_from_card(card)


def mark_all_alerts_read(session_id: str = ANONYMOUS_SESSION_ID) -> int:
    if not database_enabled():
        updated = 0
        for card in FEED_CARDS:
            if (
                (card.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
                and not card.get("is_dismissed")
                and not card.get("is_read")
                and _is_alert_card(card)
            ):
                card["is_read"] = True
                updated += 1
        return updated

    _ensure_schema()
    where = [
        "coalesce(session_id, %s) = %s",
        "is_dismissed = false",
        "is_read = false",
        _alert_sql_condition(),
    ]
    params: list[object] = [ANONYMOUS_SESSION_ID, session_id, *_alert_sql_params()]

    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    update public.feed_cards
                    set is_read = true, updated_at = now()
                    where {" and ".join(where)};
                    """,
                    params,
                )
                return cur.rowcount
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not mark alerts as read: {exc}") from exc


def get_feed_card(card_id: str, session_id: str = ANONYMOUS_SESSION_ID) -> dict | None:
    if not database_enabled():
        for card in FEED_CARDS:
            if card["id"] == card_id and (card.get("session_id") or ANONYMOUS_SESSION_ID) == session_id:
                return hydrate_feed_card(card) or card
        return None

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select {_card_columns()}
                    from public.feed_cards
                    where id = %s and coalesce(session_id, %s) = %s
                    limit 1;
                    """,
                    (card_id, ANONYMOUS_SESSION_ID, session_id),
                )
                row = cur.fetchone()
                return hydrate_feed_card(_row_to_card(row)) if row else None
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not load feed card: {exc}") from exc


def get_report(report_id: str, session_id: str = ANONYMOUS_SESSION_ID) -> dict | None:
    if not database_enabled():
        for card in FEED_CARDS:
            if (
                report_id in {card.get("report_id"), card.get("article_id"), card.get("id")}
                and (card.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
            ):
                return build_report_from_card(card)
        return None

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    select {_card_columns()}
                    from public.feed_cards
                    where (report_id = %s or article_id = %s or id = %s)
                    and coalesce(session_id, %s) = %s
                    order by created_at desc
                    limit 1;
                    """,
                    (report_id, report_id, report_id, ANONYMOUS_SESSION_ID, session_id),
                )
                row = cur.fetchone()
                return build_report_from_card(_row_to_card(row)) if row else None
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not load report: {exc}") from exc


def update_report_saved(
    report_id: str,
    session_id: str = ANONYMOUS_SESSION_ID,
    is_saved: bool = True,
) -> dict | None:
    if not database_enabled():
        for card in FEED_CARDS:
            if (
                report_id in {card.get("report_id"), card.get("article_id"), card.get("id")}
                and (card.get("session_id") or ANONYMOUS_SESSION_ID) == session_id
            ):
                card["is_saved"] = is_saved
                return build_report_from_card(card)
        return None

    _ensure_schema()
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    update public.feed_cards
                    set is_saved = %s, updated_at = now()
                    where (report_id = %s or article_id = %s or id = %s)
                    and coalesce(session_id, %s) = %s
                    returning {_card_columns()};
                    """,
                    (
                        is_saved,
                        report_id,
                        report_id,
                        report_id,
                        ANONYMOUS_SESSION_ID,
                        session_id,
                    ),
                )
                row = cur.fetchone()
                return build_report_from_card(_row_to_card(row)) if row else None
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not update saved report: {exc}") from exc


def list_saved_reports(session_id: str = ANONYMOUS_SESSION_ID, limit: int = 50) -> list[dict]:
    cards = list_feed_cards(filter_type="saved", limit=limit, session_id=session_id)
    return [build_report_from_card(card) for card in cards]


def build_report_from_card(card: dict) -> dict:
    payload = card.get("payload") or {}
    analysis = card.get("analysis") or {}
    claims = analysis.get("key_claims") or payload.get("key_claims") or []
    frames = analysis.get("narrative_framing") or payload.get("narrative_framing") or []
    entities = analysis.get("entities") or payload.get("entities") or []
    topics = analysis.get("topics") or payload.get("topics") or []
    confidence = analysis.get("confidence", payload.get("confidence", card.get("evidence_score")))
    priority = analysis.get("priority", payload.get("priority"))

    return {
        "id": card.get("report_id") or card.get("article_id") or card.get("id"),
        "card_id": card.get("id"),
        "session_id": card.get("session_id") or ANONYMOUS_SESSION_ID,
        "article_id": card.get("article_id"),
        "title": card.get("title") or "Untitled report",
        "summary": card.get("summary") or "",
        "source": card.get("source") or payload.get("source"),
        "domain": payload.get("domain"),
        "url": card.get("url") or payload.get("url"),
        "created_at": card.get("created_at"),
        "priority": priority,
        "priority_score": card.get("priority_score"),
        "confidence": confidence,
        "dominant_frame": card.get("framing") or payload.get("dominant_frame"),
        "is_saved": bool(card.get("is_saved")),
        "key_claims": claims,
        "narrative_framing": frames,
        "entities": entities,
        "topics": topics,
        "article_excerpt": payload.get("article_excerpt"),
        "narrative_signal": card.get("narrative_signal"),
        "explanation": card.get("explanation") or {},
        "card": card,
        "methodology_note": (
            "Parallax extracts article text, identifies claims and framing signals, and reports confidence "
            "as analysis quality rather than truth certainty."
        ),
        "limitations": [
            "Confidence is not truth certainty.",
            "Extracted claims should be checked against primary evidence before conclusions are drawn.",
            "Article extraction can miss text on paywalled, scripted, or heavily blocked pages.",
            "Social and audience signals are not used as verdicts in this report.",
        ],
    }


def update_feed_card(
    card_id: str,
    session_id: str = ANONYMOUS_SESSION_ID,
    **updates,
) -> dict | None:
    allowed = {"is_read", "is_saved", "is_dismissed"}
    updates = {key: value for key, value in updates.items() if key in allowed}
    if not updates:
        return get_feed_card(card_id, session_id=session_id)

    if not database_enabled():
        for card in FEED_CARDS:
            if card["id"] == card_id and (card.get("session_id") or ANONYMOUS_SESSION_ID) == session_id:
                card.update(updates)
                return card
        return None

    _ensure_schema()
    assignments = [f"{key} = %s" for key in updates]
    values = list(updates.values())
    values.append(card_id)

    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    update public.feed_cards
                    set {", ".join(assignments)}, updated_at = now()
                    where id = %s and coalesce(session_id, %s) = %s
                    returning {_card_columns()};
                    """,
                    [*values, ANONYMOUS_SESSION_ID, session_id],
                )
                row = cur.fetchone()
                return _row_to_card(row) if row else None
    except psycopg2.Error as exc:
        raise FeedStoreError(f"Could not update feed card: {exc}") from exc


def seed_initial_cards():
    if FEED_CARDS:
        return

    FEED_CARDS.extend(
        [
            {
                "id": str(uuid4()),
                "session_id": ANONYMOUS_SESSION_ID,
                "topic_id": None,
                "article_id": None,
                "alert_id": None,
                "report_id": None,
                "card_type": "narrative_frame_shift",
                "title": "The story's framing just shifted",
                "summary": "A monitored topic changed its dominant frame. Open the card to inspect what changed.",
                "source": None,
                "url": None,
                "topic": "Demo topic",
                "priority": 0.9,
                "priority_score": 0.9,
                "personalized_score": 0.9,
                "narrative_signal": "Dominant frame changed inside a monitored topic.",
                "evidence_score": 0.72,
                "framing": "diplomatic_process",
                "payload": {"dominant_frame": "diplomatic_process"},
                "is_read": False,
                "is_saved": False,
                "is_dismissed": False,
                "created_at": now_iso(),
                "recommendations": [
                    {
                        "type": "open_topic",
                        "label": "Open topic",
                        "href": "/topics/demo-topic",
                        "reason": "Inspect timeline and source comparison.",
                    }
                ],
                "explanation": {
                    "why_this_matters": "A dominant frame changed inside a monitored topic.",
                    "what_changed": {"from": "security_threat", "to": "diplomatic_process"},
                    "recommended_action": "Open the topic to inspect timeline and sources.",
                },
                "analysis": {},
            },
            {
                "id": str(uuid4()),
                "session_id": ANONYMOUS_SESSION_ID,
                "topic_id": None,
                "article_id": None,
                "alert_id": None,
                "report_id": "demo-report",
                "card_type": "article_insight",
                "title": "New article insight",
                "summary": "This article contains 6 extracted claims, with a dominant frame of institutional_response.",
                "source": "example.com",
                "url": "https://example.com",
                "topic": "Demo article",
                "priority": 0.7,
                "priority_score": 0.7,
                "personalized_score": 0.7,
                "narrative_signal": "Structured claim and framing signals are available.",
                "evidence_score": 0.6,
                "framing": "institutional_response",
                "payload": {
                    "domain": "example.com",
                    "claim_count": 6,
                    "dominant_frame": "institutional_response",
                },
                "is_read": False,
                "is_saved": False,
                "is_dismissed": False,
                "created_at": now_iso(),
                "recommendations": [
                    {
                        "type": "open_report",
                        "label": "Open report",
                        "href": "/reports/demo-report",
                        "reason": "See claim-level evidence and framing analysis.",
                    },
                    {
                        "type": "compare",
                        "label": "Compare with another article",
                        "href": "/compare",
                        "reason": "Check whether other sources frame this story differently.",
                    },
                ],
                "explanation": {
                    "why_this_matters": "This article produced structured claim and framing signals.",
                    "what_changed": None,
                    "recommended_action": "Open the full report for claim-level evidence.",
                },
                "analysis": {},
            },
        ]
    )


seed_initial_cards()
