import hashlib
import re
from urllib.parse import urlparse

from app.core.session import ANONYMOUS_SESSION_ID
from app.services.feed.store import build_report_from_card, list_feed_cards


SOURCE_LIMITATIONS = [
    "Source profiles summarize available session data only.",
    "Source activity, framing, and confidence are context signals, not truth verdicts.",
    "A low sample size can overstate patterns; compare against primary evidence.",
    "Social and audience signals are intentionally bounded and never determine credibility.",
]


def _clean_text(value, fallback: str = "") -> str:
    if value is None:
        return fallback
    cleaned = " ".join(str(value).strip().split())
    return cleaned or fallback


def _as_float(value, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _domain_from_url(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlparse(value)
    domain = parsed.netloc.lower().removeprefix("www.")
    return domain or None


def _source_from_card(card: dict) -> dict | None:
    payload = card.get("payload") or {}
    url = card.get("url") or payload.get("url")
    domain = (
        payload.get("domain")
        or _domain_from_url(url)
        or _domain_from_url(payload.get("feed_url"))
    )
    name = (
        card.get("source")
        or payload.get("feed_title")
        or payload.get("source")
        or domain
    )

    name = _clean_text(name)
    if not name and not domain:
        return None

    key = (domain or name).lower()
    return {
        "id": source_id(key),
        "key": key,
        "name": name or domain,
        "domain": domain,
        "url": url,
    }


def source_id(key: str | None) -> str:
    clean_key = _clean_text(key or "unknown-source").lower()
    slug = re.sub(r"[^a-z0-9]+", "-", clean_key).strip("-")[:36]
    digest = hashlib.sha1(clean_key.encode("utf-8")).hexdigest()[:10]
    return f"{slug or 'source'}-{digest}"


def _unique(values: list[str], limit: int = 12) -> list[str]:
    result = []
    seen = set()
    for value in values:
        text = _clean_text(value)
        key = text.lower()
        if text and key not in seen:
            result.append(text)
            seen.add(key)
        if len(result) >= limit:
            break
    return result


def _list_values(value) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item if isinstance(item, str) else _clean_text(item) for item in value]
    return []


def _card_href(card: dict) -> str:
    if card.get("report_id"):
        return f"/reports/{card['report_id']}"
    if card.get("topic_id"):
        return f"/topics/{card['topic_id']}"
    return f"/feed/{card['id']}"


def _signal_from_card(card: dict) -> dict:
    payload = card.get("payload") or {}
    report = build_report_from_card(card)
    return {
        "id": card.get("id"),
        "card_id": card.get("id"),
        "report_id": card.get("report_id"),
        "card_type": card.get("card_type"),
        "title": card.get("title") or "Untitled signal",
        "summary": card.get("summary") or "",
        "url": card.get("url") or payload.get("url"),
        "href": _card_href(card),
        "created_at": card.get("created_at"),
        "priority_score": _as_float(card.get("priority_score"), 0.0),
        "confidence": report.get("confidence"),
        "dominant_frame": report.get("dominant_frame") or card.get("framing"),
        "narrative_signal": card.get("narrative_signal"),
        "key_claims": _list_values(report.get("key_claims"))[:5],
        "narrative_framing": _list_values(report.get("narrative_framing"))[:5],
        "entities": _list_values(report.get("entities"))[:8],
        "topics": _list_values(report.get("topics") or [card.get("topic")])[:8],
        "is_saved": bool(card.get("is_saved")),
    }


def _empty_bucket(source: dict) -> dict:
    return {
        "id": source["id"],
        "key": source["key"],
        "name": source["name"],
        "domain": source.get("domain"),
        "url": source.get("url"),
        "signals": [],
        "article_count": 0,
        "feed_item_count": 0,
        "saved_count": 0,
        "high_priority_count": 0,
        "avg_priority_score": 0.0,
        "avg_confidence": None,
        "dominant_frames": [],
        "topics": [],
        "entities": [],
        "key_claims": [],
    }


def _finalize_source(bucket: dict) -> dict:
    signals = sorted(
        bucket["signals"],
        key=lambda item: (
            _as_float(item.get("priority_score"), 0.0),
            item.get("created_at") or "",
        ),
        reverse=True,
    )
    priorities = [_as_float(item.get("priority_score"), 0.0) for item in signals]
    confidences = [
        _as_float(item.get("confidence"), -1.0)
        for item in signals
        if item.get("confidence") is not None
    ]
    confidences = [value for value in confidences if value >= 0]

    bucket["signals"] = signals
    bucket["signal_count"] = len(signals)
    bucket["avg_priority_score"] = round(sum(priorities) / len(priorities), 3) if priorities else 0.0
    bucket["avg_confidence"] = round(sum(confidences) / len(confidences), 3) if confidences else None
    bucket["dominant_frames"] = _unique(
        [frame for item in signals for frame in item.get("narrative_framing", [])],
        limit=8,
    )
    bucket["topics"] = _unique([topic for item in signals for topic in item.get("topics", [])], limit=10)
    bucket["entities"] = _unique(
        [entity for item in signals for entity in item.get("entities", [])],
        limit=10,
    )
    bucket["key_claims"] = _unique(
        [claim for item in signals for claim in item.get("key_claims", [])],
        limit=10,
    )
    bucket["recent_items"] = signals[:8]
    bucket["bounded_social_signals"] = []
    bucket["profile"] = {
        "name": bucket["name"],
        "domain": bucket.get("domain"),
        "sample_size": bucket["signal_count"],
        "analysis_quality_score": bucket["avg_confidence"],
        "coverage_activity_score": min(1.0, round(bucket["signal_count"] / 10, 3)),
        "high_priority_share": (
            round(bucket["high_priority_count"] / bucket["signal_count"], 3)
            if bucket["signal_count"]
            else 0.0
        ),
    }
    bucket["limitations"] = SOURCE_LIMITATIONS
    return bucket


def list_sources(session_id: str = ANONYMOUS_SESSION_ID, limit: int = 50) -> list[dict]:
    buckets: dict[str, dict] = {}
    cards = list_feed_cards(filter_type="all", limit=100, session_id=session_id)

    for card in cards:
        source = _source_from_card(card)
        if not source:
            continue

        bucket = buckets.setdefault(source["id"], _empty_bucket(source))
        signal = _signal_from_card(card)
        bucket["signals"].append(signal)
        if card.get("card_type") == "article_insight":
            bucket["article_count"] += 1
        if card.get("card_type") == "feed_item":
            bucket["feed_item_count"] += 1
        if card.get("is_saved"):
            bucket["saved_count"] += 1
        if _as_float(card.get("priority_score"), 0.0) >= 0.75:
            bucket["high_priority_count"] += 1

    sources = [_finalize_source(bucket) for bucket in buckets.values()]
    sources.sort(
        key=lambda source: (
            source["high_priority_count"],
            source["signal_count"],
            source["avg_priority_score"],
        ),
        reverse=True,
    )

    summaries = []
    for source in sources[: max(1, min(limit, 100))]:
        summaries.append(
            {
                "id": source["id"],
                "name": source["name"],
                "domain": source.get("domain"),
                "url": source.get("url"),
                "signal_count": source["signal_count"],
                "article_count": source["article_count"],
                "feed_item_count": source["feed_item_count"],
                "saved_count": source["saved_count"],
                "high_priority_count": source["high_priority_count"],
                "avg_priority_score": source["avg_priority_score"],
                "avg_confidence": source["avg_confidence"],
                "dominant_frames": source["dominant_frames"],
                "topics": source["topics"],
                "recent_items": source["recent_items"][:3],
                "profile": source["profile"],
                "limitations": source["limitations"],
            }
        )
    return summaries


def get_source(
    source_id_value: str,
    session_id: str = ANONYMOUS_SESSION_ID,
) -> dict | None:
    cards = list_feed_cards(filter_type="all", limit=100, session_id=session_id)
    bucket = None
    requested = _clean_text(source_id_value).lower()

    for card in cards:
        source = _source_from_card(card)
        if not source:
            continue

        aliases = {
            source["id"].lower(),
            source["key"].lower(),
            _clean_text(source.get("domain")).lower(),
            _clean_text(source.get("name")).lower(),
        }
        if requested not in aliases:
            continue

        bucket = bucket or _empty_bucket(source)
        signal = _signal_from_card(card)
        bucket["signals"].append(signal)
        if card.get("card_type") == "article_insight":
            bucket["article_count"] += 1
        if card.get("card_type") == "feed_item":
            bucket["feed_item_count"] += 1
        if card.get("is_saved"):
            bucket["saved_count"] += 1
        if _as_float(card.get("priority_score"), 0.0) >= 0.75:
            bucket["high_priority_count"] += 1

    if not bucket:
        return None

    source = _finalize_source(bucket)
    return {
        "source": {
            "id": source["id"],
            "name": source["name"],
            "domain": source.get("domain"),
            "url": source.get("url"),
        },
        "profile": source["profile"],
        "signals": source["signals"],
        "recent_items": source["recent_items"],
        "key_claims": source["key_claims"],
        "dominant_frames": source["dominant_frames"],
        "topics": source["topics"],
        "entities": source["entities"],
        "metrics": {
            "signal_count": source["signal_count"],
            "article_count": source["article_count"],
            "feed_item_count": source["feed_item_count"],
            "saved_count": source["saved_count"],
            "high_priority_count": source["high_priority_count"],
            "avg_priority_score": source["avg_priority_score"],
            "avg_confidence": source["avg_confidence"],
        },
        "bounded_social_signals": source["bounded_social_signals"],
        "limitations": source["limitations"],
    }
