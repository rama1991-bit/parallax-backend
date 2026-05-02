import base64
import hashlib
import hmac
import json
from datetime import datetime, timezone

from app.core.config import settings
from app.core.session import ANONYMOUS_SESSION_ID
from app.services.feed.store import build_report_from_card, list_feed_cards
from app.services.sources import source_id


class BriefTokenError(Exception):
    pass


BRIEF_SCOPES = {
    "latest": {
        "title": "Latest narrative brief",
        "label": "Latest",
        "description": "Recent brief-ready signals from this session.",
        "filter": "all",
    },
    "priority": {
        "title": "Priority narrative brief",
        "label": "Priority",
        "description": "High-priority narrative and article analysis signals.",
        "filter": "high",
    },
    "saved": {
        "title": "Saved reports brief",
        "label": "Saved",
        "description": "Reports saved from this session.",
        "filter": "saved",
    },
}

BRIEF_READY_TYPES = {
    "article_insight",
    "feed_item",
    "topic_monitor",
    "narrative_frame_shift",
    "coverage_change",
    "source_ecosystem_change",
    "divergence_increase",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _b64_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64_decode(value: str) -> bytes:
    padded = value + ("=" * (-len(value) % 4))
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii"))
    except (ValueError, TypeError) as exc:
        raise BriefTokenError("Invalid brief token.") from exc


def _signature(payload: str) -> str:
    digest = hmac.new(
        settings.SECRET_KEY.encode("utf-8"),
        payload.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return _b64_encode(digest)


def create_brief_token(
    session_id: str = ANONYMOUS_SESSION_ID,
    scope: str = "latest",
) -> str:
    payload = _b64_encode(
        _json_dumps(
            {
                "version": 1,
                "session_id": session_id,
                "scope": scope,
            }
        ).encode("utf-8")
    )
    return f"{payload}.{_signature(payload)}"


def decode_brief_token(token: str) -> dict:
    parts = token.split(".")
    if len(parts) != 2:
        raise BriefTokenError("Invalid brief token.")

    payload, signature = parts
    expected_signature = _signature(payload)
    if not hmac.compare_digest(signature, expected_signature):
        raise BriefTokenError("Invalid brief token signature.")

    try:
        decoded = json.loads(_b64_decode(payload).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BriefTokenError("Invalid brief token payload.") from exc

    scope = decoded.get("scope")
    if scope not in BRIEF_SCOPES:
        raise BriefTokenError("Unknown brief scope.")

    return {
        "session_id": decoded.get("session_id") or ANONYMOUS_SESSION_ID,
        "scope": scope,
    }


def _as_float(value, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _clean_text(value, fallback: str = "") -> str:
    if value is None:
        return fallback
    text = " ".join(str(value).split())
    return text or fallback


def _text_list(values, limit: int = 10) -> list[str]:
    if not values:
        return []
    if isinstance(values, str):
        values = [values]

    cleaned = []
    seen = set()
    for value in values:
        text = _clean_text(value)
        key = text.lower()
        if text and key not in seen:
            cleaned.append(text[:500])
            seen.add(key)
        if len(cleaned) >= limit:
            break
    return cleaned


def _unique(values: list[str], limit: int = 12) -> list[str]:
    cleaned = []
    seen = set()
    for value in values:
        text = _clean_text(value)
        key = text.lower()
        if text and key not in seen:
            cleaned.append(text)
            seen.add(key)
        if len(cleaned) >= limit:
            break
    return cleaned


def _card_href(card: dict) -> str:
    if card.get("report_id"):
        return f"/reports/{card['report_id']}"
    if card.get("topic_id"):
        return f"/topics/{card['topic_id']}"
    return f"/feed/{card['id']}"


def _is_brief_ready(card: dict) -> bool:
    if card.get("card_type") in BRIEF_READY_TYPES:
        return True
    return _as_float(card.get("priority_score"), 0.0) >= 0.75


def _source_from_item(item: dict) -> dict | None:
    name = item.get("source") or item.get("domain")
    domain = item.get("domain")
    url = item.get("url") or item.get("external_url")
    if not name and not url:
        return None
    return {
        "id": source_id(domain or name or url),
        "name": name or url,
        "domain": domain,
        "url": url,
    }


def _brief_item_from_card(card: dict) -> dict:
    report = build_report_from_card(card)
    payload = card.get("payload") or {}
    source_key = report.get("domain") or card.get("source") or payload.get("feed_title")
    claims = _text_list(report.get("key_claims"), limit=5)
    frames = _text_list(report.get("narrative_framing"), limit=5)
    if not frames and card.get("framing"):
        frames = [card["framing"]]

    return {
        "id": card.get("id"),
        "card_id": card.get("id"),
        "report_id": card.get("report_id"),
        "source_id": source_id(source_key) if source_key else None,
        "card_type": card.get("card_type"),
        "title": card.get("title") or report.get("title") or "Untitled signal",
        "summary": card.get("summary") or report.get("summary") or "",
        "source": card.get("source") or payload.get("feed_title") or payload.get("domain"),
        "domain": report.get("domain") or payload.get("domain"),
        "url": card.get("url") or payload.get("url"),
        "href": _card_href(card),
        "created_at": card.get("created_at"),
        "priority_score": _as_float(card.get("priority_score"), 0.0),
        "confidence": report.get("confidence"),
        "dominant_frame": report.get("dominant_frame") or card.get("framing"),
        "key_claims": claims,
        "narrative_framing": frames,
        "entities": _text_list(report.get("entities"), limit=8),
        "topics": _text_list(report.get("topics") or [card.get("topic")], limit=8),
        "narrative_signal": card.get("narrative_signal"),
    }


def _brief_cards(session_id: str, scope: str) -> list[dict]:
    config = BRIEF_SCOPES[scope]
    cards = list_feed_cards(
        filter_type=config["filter"],
        limit=50,
        session_id=session_id,
    )
    cards = [card for card in cards if _is_brief_ready(card)]

    if scope == "latest":
        cards = sorted(
            cards,
            key=lambda card: (
                _as_float(card.get("priority_score"), 0.0),
                card.get("created_at") or "",
            ),
            reverse=True,
        )

    return cards[:8]


def _brief_summary(scope: str, signal_count: int, source_count: int) -> str:
    if signal_count == 0:
        return "No brief-ready signals are available yet."
    if scope == "saved":
        return f"{signal_count} saved reports across {source_count} sources."
    if scope == "priority":
        return f"{signal_count} high-priority signals across {source_count} sources."
    return f"{signal_count} recent narrative signals across {source_count} sources."


def build_public_brief(
    session_id: str = ANONYMOUS_SESSION_ID,
    scope: str = "latest",
    token: str | None = None,
) -> dict:
    if scope not in BRIEF_SCOPES:
        raise BriefTokenError("Unknown brief scope.")

    token = token or create_brief_token(session_id=session_id, scope=scope)
    config = BRIEF_SCOPES[scope]
    cards = _brief_cards(session_id=session_id, scope=scope)
    items = [_brief_item_from_card(card) for card in cards]
    source_tuples = {
            (
                source["id"],
                source["name"],
                source.get("domain"),
                source.get("url"),
            )
            for source in [_source_from_item(item) for item in items]
            if source
        }
    sources = [
        {
            "id": source_id_value,
            "name": name,
            "domain": domain,
            "url": url,
        }
        for source_id_value, name, domain, url in source_tuples
    ]
    sources = sorted(sources, key=lambda source: source["name"].lower())

    key_claims = _unique(
        [claim for item in items for claim in item.get("key_claims", [])],
        limit=10,
    )
    narrative_frames = _unique(
        [frame for item in items for frame in item.get("narrative_framing", [])],
        limit=10,
    )
    entities = _unique(
        [entity for item in items for entity in item.get("entities", [])],
        limit=12,
    )
    topics = _unique(
        [topic for item in items for topic in item.get("topics", [])],
        limit=12,
    )

    frontend_url = settings.FRONTEND_URL.rstrip("/")
    api_url = "/api/v1/public/briefs"
    return {
        "id": token,
        "token": token,
        "scope": scope,
        "label": config["label"],
        "title": config["title"],
        "description": config["description"],
        "summary": _brief_summary(scope, len(items), len(sources)),
        "generated_at": _now_iso(),
        "session_id": session_id,
        "signal_count": len(items),
        "source_count": len(sources),
        "claim_count": len(key_claims),
        "share_url": f"{frontend_url}/briefs/{token}",
        "api_url": f"{api_url}/{token}",
        "export_urls": {
            "json": f"{api_url}/{token}/export?format=json",
            "markdown": f"{api_url}/{token}/export?format=markdown",
        },
        "key_claims": key_claims,
        "narrative_frames": narrative_frames,
        "entities": entities,
        "topics": topics,
        "sources": sources,
        "items": items,
        "methodology_note": (
            "Parallax builds public briefs from saved reports, high-priority analyses, "
            "topic monitor events, and subscribed feed items. Confidence describes "
            "extraction and analysis quality, not truth certainty."
        ),
        "limitations": [
            "Briefs summarize available session data and are not comprehensive news coverage.",
            "Claims should be checked against primary evidence before decisions are made.",
            "Feed items are ingestion signals until the linked article is analyzed.",
            "Public share links expose the current brief view to anyone with the token.",
        ],
    }


def list_public_briefs(session_id: str = ANONYMOUS_SESSION_ID) -> list[dict]:
    briefs = []
    for scope in BRIEF_SCOPES:
        brief = build_public_brief(session_id=session_id, scope=scope)
        briefs.append(
            {
                "id": brief["id"],
                "token": brief["token"],
                "scope": brief["scope"],
                "label": brief["label"],
                "title": brief["title"],
                "description": brief["description"],
                "summary": brief["summary"],
                "generated_at": brief["generated_at"],
                "signal_count": brief["signal_count"],
                "source_count": brief["source_count"],
                "claim_count": brief["claim_count"],
                "share_url": brief["share_url"],
            }
        )
    return briefs


def get_public_brief(token: str) -> dict:
    decoded = decode_brief_token(token)
    return build_public_brief(
        session_id=decoded["session_id"],
        scope=decoded["scope"],
        token=token,
    )


def public_brief_to_markdown(brief: dict) -> str:
    lines = [
        f"# {brief['title']}",
        "",
        brief.get("summary") or "",
        "",
        f"Generated: {brief.get('generated_at')}",
        "",
        "## Key Claims",
    ]

    claims = brief.get("key_claims") or []
    if claims:
        lines.extend([f"- {claim}" for claim in claims])
    else:
        lines.append("- No claims available in this brief.")

    lines.extend(["", "## Narrative Frames"])
    frames = brief.get("narrative_frames") or []
    if frames:
        lines.extend([f"- {frame}" for frame in frames])
    else:
        lines.append("- No frames available in this brief.")

    lines.extend(["", "## Signals"])
    items = brief.get("items") or []
    if items:
        for item in items:
            url = item.get("url") or item.get("href") or ""
            suffix = f" ({url})" if url else ""
            lines.append(f"- {item.get('title', 'Untitled signal')}{suffix}")
            if item.get("summary"):
                lines.append(f"  {item['summary']}")
    else:
        lines.append("- No brief-ready signals available.")

    lines.extend(["", "## Sources"])
    sources = brief.get("sources") or []
    if sources:
        for source in sources:
            url = source.get("url")
            lines.append(f"- {source.get('name')}{f' ({url})' if url else ''}")
    else:
        lines.append("- No sources available.")

    lines.extend(["", "## Methodology", brief.get("methodology_note") or ""])
    lines.extend(["", "## Limitations"])
    lines.extend([f"- {item}" for item in brief.get("limitations", [])])
    lines.append("")
    return "\n".join(lines)
