from datetime import datetime, timezone
from uuid import uuid4

import psycopg2
from psycopg2.extras import Json, RealDictCursor

from app.core.config import settings
from app.core.session import ANONYMOUS_SESSION_ID
from app.services.articles import ExtractedArticle


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

_SCHEMA_READY = False
_HIGH_PRIORITY_ALERT_THRESHOLD = 0.75
_ALERT_CARD_TYPES = (
    "narrative_frame_shift",
    "coverage_change",
    "source_ecosystem_change",
    "divergence_increase",
    "topic_monitor",
    "feed_item",
)


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

                for statement in [
                    "alter table public.feed_cards add column if not exists session_id text;",
                    "alter table public.feed_cards add column if not exists topic_id text;",
                    "alter table public.feed_cards add column if not exists article_id text;",
                    "alter table public.feed_cards add column if not exists alert_id text;",
                    "alter table public.feed_cards add column if not exists report_id text;",
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
        return [card for card in cards if card.get("card_type") == "article_insight"]
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
        return sorted(cards, key=lambda card: card.get("created_at", ""), reverse=True)[:limit]

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
        where.append("card_type = 'article_insight'")
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
                return [_row_to_card(row) for row in cur.fetchall()]
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
                return card
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
                return _row_to_card(row) if row else None
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
