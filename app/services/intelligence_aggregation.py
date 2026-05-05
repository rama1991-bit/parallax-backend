from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import re
from typing import Any
from uuid import uuid4

from app.core.session import ANONYMOUS_SESSION_ID
from app.services import intelligence as intelligence_provider
from app.services.feed.store import (
    FeedStoreError,
    get_latest_intelligence_snapshot,
    get_source_record,
    list_ingested_article_records,
    list_intelligence_refresh_runs,
    list_source_records,
    list_topics,
    save_generated_feed_cards,
    save_intelligence_snapshot,
    save_intelligence_refresh_run,
)


STOPWORDS = {
    "about",
    "after",
    "again",
    "also",
    "because",
    "before",
    "being",
    "between",
    "could",
    "from",
    "have",
    "into",
    "more",
    "over",
    "said",
    "says",
    "than",
    "that",
    "their",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "under",
    "were",
    "which",
    "while",
    "with",
    "would",
}


def _tokens(value: Any) -> list[str]:
    text = str(value or "").lower()
    return [
        token
        for token in re.findall(r"[a-zA-Z0-9]+", text)
        if len(token) > 2 and token not in STOPWORDS
    ]


def _dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _analysis(article: dict) -> dict:
    return _dict(article.get("analysis"))


def _intelligence(article: dict) -> dict:
    return _dict(_analysis(article).get("intelligence"))


def _article_source(article: dict) -> dict:
    source = article.get("source")
    if isinstance(source, dict):
        return source
    if source:
        return {"name": str(source)}
    return {}


def _article_claims(article: dict, limit: int = 12) -> list[str]:
    analysis = _analysis(article)
    intelligence = _intelligence(article)
    claims = analysis.get("key_claims") or intelligence.get("key_claims") or []
    return intelligence_provider.safe_list(claims, limit=limit)


def _article_frames(article: dict, limit: int = 12) -> list[str]:
    analysis = _analysis(article)
    intelligence = _intelligence(article)
    narrative = _dict(intelligence.get("narrative"))
    frames = intelligence_provider.safe_list(analysis.get("narrative_framing") or [], limit=limit)
    main_frame = intelligence_provider.clean_text(narrative.get("main_frame"), limit=120)
    if main_frame and main_frame not in frames:
        frames = [main_frame, *frames]
    for frame in intelligence_provider.safe_list(narrative.get("secondary_frames") or [], limit=limit):
        if frame not in frames:
            frames.append(frame)
    return frames[:limit]


def _article_tone(article: dict) -> str:
    narrative = _dict(_intelligence(article).get("narrative"))
    return intelligence_provider.clean_text(narrative.get("tone"), limit=120, fallback="unknown")


def _article_summary(article: dict) -> str:
    analysis = _analysis(article)
    intelligence = _intelligence(article)
    return intelligence_provider.clean_text(
        article.get("summary") or intelligence.get("summary") or analysis.get("summary"),
        limit=900,
    )


def _article_topics(article: dict) -> list[str]:
    analysis = _analysis(article)
    values = []
    values.extend(analysis.get("topics") or [])
    values.extend(article.get("comparison_keywords") or [])
    fingerprint = article.get("event_fingerprint")
    if fingerprint:
        values.append(str(fingerprint).replace("-", " "))
    return intelligence_provider.safe_list(values, limit=16)


def _article_entities(article: dict) -> dict[str, list[str]]:
    analysis = _analysis(article)
    intelligence = _intelligence(article)
    raw_entities = _dict(intelligence.get("entities"))
    grouped = {
        "people": intelligence_provider.safe_list(raw_entities.get("people") or [], limit=10),
        "organizations": intelligence_provider.safe_list(raw_entities.get("organizations") or [], limit=10),
        "locations": intelligence_provider.safe_list(raw_entities.get("locations") or [], limit=10),
        "events": intelligence_provider.safe_list(raw_entities.get("events") or [], limit=10),
    }
    flat_entities = intelligence_provider.safe_list(analysis.get("entities") or [], limit=12)
    for entity in flat_entities:
        if entity not in grouped["organizations"]:
            grouped["organizations"].append(entity)
    return {key: values[:10] for key, values in grouped.items()}


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _label_key(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def _distribution(counter: Counter[str], labels: dict[str, str] | None = None, limit: int = 8) -> list[dict]:
    total = sum(counter.values())
    if not total:
        return []
    labels = labels or {}
    return [
        {
            "label": labels.get(key, key),
            "count": count,
            "share": round(count / total, 3),
        }
        for key, count in counter.most_common(limit)
    ]


def _counter_labels(values: list[str]) -> tuple[Counter[str], dict[str, str]]:
    counter: Counter[str] = Counter()
    labels: dict[str, str] = {}
    for value in values:
        clean = intelligence_provider.clean_text(value, limit=280)
        if not clean:
            continue
        key = _label_key(clean)
        counter[key] += 1
        labels.setdefault(key, clean)
    return counter, labels


def _source_identity(source: dict) -> dict:
    return {
        "id": source.get("id"),
        "name": source.get("name"),
        "website_url": source.get("website_url"),
        "rss_url": source.get("rss_url"),
        "country": source.get("country"),
        "language": source.get("language"),
        "region": source.get("region"),
        "political_context": source.get("political_context"),
        "source_size": source.get("source_size"),
        "source_type": source.get("source_type"),
        "credibility_notes": source.get("credibility_notes"),
        "review_status": source.get("review_status"),
        "quality_score": source.get("quality_score"),
    }


def _topic_identity(topic: dict) -> dict:
    return {
        "id": topic.get("id"),
        "name": topic.get("name"),
        "description": topic.get("description"),
        "keywords": topic.get("keywords") or [],
        "monitor": topic.get("monitor"),
        "article_count": topic.get("article_count", 0),
    }


def _article_excerpt(article: dict) -> dict:
    source = _article_source(article)
    return {
        "id": article.get("id"),
        "title": article.get("title"),
        "url": article.get("url"),
        "source_id": article.get("source_id"),
        "source": source.get("name") or article.get("source_id"),
        "country": article.get("country") or source.get("country"),
        "language": article.get("language") or source.get("language"),
        "published_at": article.get("published_at"),
        "analysis_status": article.get("analysis_status"),
        "summary": _article_summary(article),
        "key_claims": _article_claims(article, limit=6),
        "narrative_frames": _article_frames(article, limit=6),
        "tone": _article_tone(article),
        "entities": _article_entities(article),
        "event_fingerprint": article.get("event_fingerprint"),
        "comparison_keywords": article.get("comparison_keywords") or [],
    }


def _coverage_cadence(articles: list[dict]) -> dict:
    published = [
        parsed
        for parsed in (_parse_datetime(article.get("published_at") or article.get("created_at")) for article in articles)
        if parsed
    ]
    now = datetime.now(timezone.utc)
    if not published:
        return {
            "latest_article_at": None,
            "earliest_article_at": None,
            "published_article_count": 0,
            "articles_24h": 0,
            "articles_7d": 0,
            "span_hours": None,
        }
    latest = max(published)
    earliest = min(published)
    return {
        "latest_article_at": latest.isoformat(),
        "earliest_article_at": earliest.isoformat(),
        "published_article_count": len(published),
        "articles_24h": len([item for item in published if (now - item).total_seconds() <= 86400]),
        "articles_7d": len([item for item in published if (now - item).total_seconds() <= 604800]),
        "span_hours": round((latest - earliest).total_seconds() / 3600, 2),
    }


def _topic_terms(topic: dict) -> set[str]:
    terms = set(_tokens(topic.get("name") or ""))
    for keyword in topic.get("keywords") or []:
        terms.update(_tokens(keyword))
    monitor = _dict(topic.get("monitor"))
    for keyword in monitor.get("keywords") or []:
        terms.update(_tokens(keyword))
    return terms


def _topic_match_score(topic: dict, article: dict) -> float:
    terms = _topic_terms(topic)
    if not terms:
        return 0.0
    text_bits = [
        article.get("title") or "",
        article.get("summary") or "",
        " ".join(_article_claims(article, limit=20)),
        " ".join(_article_frames(article, limit=20)),
        " ".join(_article_topics(article)),
    ]
    article_tokens = set(_tokens(" ".join(text_bits)))
    if not article_tokens:
        return 0.0
    return len(terms & article_tokens) / max(len(terms), 1)


def _articles_for_topic(topic: dict, limit: int) -> list[dict]:
    candidates = list_ingested_article_records(limit=250)
    ranked = [
        (article, _topic_match_score(topic, article))
        for article in candidates
    ]
    matched = [(article, score) for article, score in ranked if score > 0]
    matched.sort(
        key=lambda item: (
            item[1],
            item[0].get("published_at") or item[0].get("created_at") or "",
        ),
        reverse=True,
    )
    return [article for article, _score in matched[: max(1, min(limit, 250))]]


def _aggregate(kind: str, subject: dict, articles: list[dict]) -> dict:
    article_count = len(articles)
    analyzed_articles = [article for article in articles if article.get("analysis_status") == "analyzed"]
    all_frames: list[str] = []
    all_claims: list[str] = []
    all_tones: list[str] = []
    entities = {"people": [], "organizations": [], "locations": [], "events": []}
    source_counter: Counter[str] = Counter()
    source_labels: dict[str, str] = {}
    country_counter: Counter[str] = Counter()
    language_counter: Counter[str] = Counter()
    source_type_counter: Counter[str] = Counter()

    for article in articles:
        source = _article_source(article)
        source_name = intelligence_provider.clean_text(source.get("name") or article.get("source_id"), limit=180)
        if source_name:
            source_key = article.get("source_id") or source_name.lower()
            source_counter[source_key] += 1
            source_labels.setdefault(source_key, source_name)
        country = intelligence_provider.clean_text(article.get("country") or source.get("country"), limit=80)
        language = intelligence_provider.clean_text(article.get("language") or source.get("language"), limit=80)
        source_type = intelligence_provider.clean_text(source.get("source_type"), limit=80)
        if country:
            country_counter[country] += 1
        if language:
            language_counter[language] += 1
        if source_type:
            source_type_counter[source_type] += 1

        all_frames.extend(_article_frames(article))
        all_claims.extend(_article_claims(article))
        tone = _article_tone(article)
        if tone and tone != "unknown":
            all_tones.append(tone)
        grouped = _article_entities(article)
        for key in entities:
            entities[key].extend(grouped.get(key) or [])

    frame_counter, frame_labels = _counter_labels(all_frames)
    claim_counter, claim_labels = _counter_labels(all_claims)
    tone_counter, tone_labels = _counter_labels(all_tones)
    dominant_frames = [item["label"] for item in _distribution(frame_counter, frame_labels, limit=6)]
    dominant_tones = [item["label"] for item in _distribution(tone_counter, tone_labels, limit=4)]
    recurring_claims = [
        claim_labels.get(key, key)
        for key, _count in claim_counter.most_common(10)
    ]
    entity_pattern = {
        key: [
            item["label"]
            for item in _distribution(*_counter_labels(values), limit=10)
        ]
        for key, values in entities.items()
    }
    cadence = _coverage_cadence(articles)
    source_diversity = {
        "source_count": len(source_counter),
        "dominant_sources": _distribution(source_counter, source_labels, limit=8),
        "countries": _distribution(country_counter, limit=8),
        "languages": _distribution(language_counter, limit=8),
        "source_types": _distribution(source_type_counter, limit=8),
    }

    disagreement_zones = []
    if len(frame_counter) > 1:
        disagreement_zones.append("Multiple narrative frames appear in the sampled coverage.")
    if len(tone_counter) > 1:
        disagreement_zones.append("Tone varies across the sampled articles.")
    if kind == "topic" and len(source_counter) > 1:
        disagreement_zones.append("The topic is covered by multiple sources, so compare view can expose source-level differences.")

    weak_spots = []
    if not article_count:
        weak_spots.append("No ingested articles currently match this intelligence subject.")
    if article_count and not analyzed_articles:
        weak_spots.append("No sampled article has completed structured analysis yet.")
    if article_count < 3:
        weak_spots.append("The current sample is small; patterns may change as more articles are ingested.")
    if not recurring_claims:
        weak_spots.append("No recurring claims have been extracted from the current sample.")
    if not dominant_frames:
        weak_spots.append("Narrative frames are sparse until more articles are analyzed.")
    if kind == "topic" and article_count and len(source_counter) < 2:
        weak_spots.append("Topic coverage currently comes from fewer than two sources.")

    subject_name = subject.get("name") or subject.get("id") or "This subject"
    if article_count:
        frame_phrase = dominant_frames[0] if dominant_frames else "unclassified framing"
        summary = (
            f"{subject_name} has {article_count} sampled ingested article"
            f"{'' if article_count == 1 else 's'}, with {len(analyzed_articles)} analyzed. "
            f"The strongest visible frame is {frame_phrase}."
        )
    else:
        summary = f"{subject_name} has no matching ingested articles yet."

    return {
        "status": "ready" if article_count else "empty",
        "snapshot_type": kind,
        "subject": subject,
        "summary": summary,
        "sample": {
            "article_count": article_count,
            "analyzed_count": len(analyzed_articles),
            "source_count": len(source_counter),
            "latest_article_at": cadence.get("latest_article_at"),
        },
        "framing_pattern": {
            "dominant_frames": dominant_frames,
            "frame_distribution": _distribution(frame_counter, frame_labels, limit=8),
        },
        "recurring_claims": recurring_claims,
        "entity_pattern": entity_pattern,
        "tone_pattern": {
            "dominant_tones": dominant_tones,
            "tone_distribution": _distribution(tone_counter, tone_labels, limit=8),
        },
        "coverage_cadence": cadence,
        "source_diversity": source_diversity,
        "disagreement_zones": disagreement_zones,
        "weak_spots": weak_spots,
        "limitations": [
            "Aggregation uses ingested and analyzed article metadata; it is not a factual verdict.",
            "OSINT and comparison views should be used for context before drawing conclusions.",
            "Heuristic matching is lexical and can miss synonyms, translations, and subtle framing shifts.",
        ],
        "sample_articles": [_article_excerpt(article) for article in articles[:12]],
    }


def _sanitize_distribution(value: Any, fallback: list[dict]) -> list[dict]:
    if not isinstance(value, list):
        return fallback
    cleaned = []
    for item in value:
        if isinstance(item, dict):
            label = intelligence_provider.clean_text(
                item.get("label") or item.get("name") or item.get("frame") or item.get("tone"),
                limit=180,
            )
            if not label:
                continue
            cleaned.append(
                {
                    "label": label,
                    "count": int(item.get("count") or 0),
                    "share": round(intelligence_provider.clamp(item.get("share"), 0.0), 3),
                }
            )
        else:
            label = intelligence_provider.clean_text(item, limit=180)
            if label:
                cleaned.append({"label": label, "count": 0, "share": 0.0})
        if len(cleaned) >= 10:
            break
    return cleaned or fallback


def _sanitize_entity_pattern(value: Any, fallback: dict) -> dict:
    raw = _dict(value)
    return {
        "people": intelligence_provider.safe_list(raw.get("people") or fallback.get("people") or [], limit=10),
        "organizations": intelligence_provider.safe_list(
            raw.get("organizations") or fallback.get("organizations") or [],
            limit=10,
        ),
        "locations": intelligence_provider.safe_list(raw.get("locations") or fallback.get("locations") or [], limit=10),
        "events": intelligence_provider.safe_list(raw.get("events") or fallback.get("events") or [], limit=10),
    }


def _sanitize_aggregation(raw: dict, base: dict) -> dict:
    framing = _dict(raw.get("framing_pattern"))
    tone = _dict(raw.get("tone_pattern"))
    source_diversity = _dict(raw.get("source_diversity"))
    base_framing = _dict(base.get("framing_pattern"))
    base_tone = _dict(base.get("tone_pattern"))
    base_source_diversity = _dict(base.get("source_diversity"))
    return {
        **base,
        "summary": intelligence_provider.clean_text(raw.get("summary"), limit=1200, fallback=base.get("summary")),
        "framing_pattern": {
            "dominant_frames": intelligence_provider.safe_list(
                framing.get("dominant_frames") or base_framing.get("dominant_frames") or [],
                limit=8,
            ),
            "frame_distribution": _sanitize_distribution(
                framing.get("frame_distribution"),
                base_framing.get("frame_distribution") or [],
            ),
        },
        "recurring_claims": intelligence_provider.safe_list(
            raw.get("recurring_claims") or base.get("recurring_claims") or [],
            limit=12,
        ),
        "entity_pattern": _sanitize_entity_pattern(raw.get("entity_pattern"), base.get("entity_pattern") or {}),
        "tone_pattern": {
            "dominant_tones": intelligence_provider.safe_list(
                tone.get("dominant_tones") or base_tone.get("dominant_tones") or [],
                limit=6,
            ),
            "tone_distribution": _sanitize_distribution(
                tone.get("tone_distribution"),
                base_tone.get("tone_distribution") or [],
            ),
        },
        "coverage_cadence": _dict(raw.get("coverage_cadence")) or base.get("coverage_cadence") or {},
        "source_diversity": {
            **base_source_diversity,
            **source_diversity,
            "dominant_sources": _sanitize_distribution(
                source_diversity.get("dominant_sources"),
                base_source_diversity.get("dominant_sources") or [],
            ),
        },
        "disagreement_zones": intelligence_provider.safe_list(
            raw.get("disagreement_zones") or base.get("disagreement_zones") or [],
            limit=10,
        ),
        "weak_spots": intelligence_provider.safe_list(raw.get("weak_spots") or base.get("weak_spots") or [], limit=10),
        "limitations": intelligence_provider.safe_list(
            raw.get("limitations") or base.get("limitations") or [],
            limit=10,
        ),
    }


async def _enhance_aggregation(kind: str, base: dict) -> dict:
    task = f"{kind}_intelligence_aggregation"
    if not intelligence_provider.provider_enabled():
        return {
            **base,
            "provider_metadata": intelligence_provider.provider_metadata(task=task, status="heuristic"),
        }

    prompt = (
        "You enhance source/topic intelligence aggregation for a media intelligence feed. "
        "Use only the provided article metadata, claims, entities, frames, and source details. "
        "Preserve the response schema and treat the output as contextual analysis, not truth."
    )
    payload = {
        "aggregation_type": kind,
        "base_aggregation": base,
        "required_schema": {
            "summary": "",
            "framing_pattern": {"dominant_frames": [], "frame_distribution": []},
            "recurring_claims": [],
            "entity_pattern": {"people": [], "organizations": [], "locations": [], "events": []},
            "tone_pattern": {"dominant_tones": [], "tone_distribution": []},
            "coverage_cadence": {},
            "source_diversity": {},
            "disagreement_zones": [],
            "weak_spots": [],
            "limitations": [],
        },
    }
    try:
        raw = await intelligence_provider.openai_json(
            task=task,
            system_prompt=prompt,
            user_payload=payload,
            timeout=50.0,
        )
        enhanced = _sanitize_aggregation(raw, base)
        enhanced["provider_metadata"] = intelligence_provider.provider_metadata(task=task, status="model")
        return enhanced
    except intelligence_provider.IntelligenceProviderError as exc:
        return {
            **base,
            "provider_metadata": intelligence_provider.provider_metadata(
                task=task,
                status="fallback",
                warnings=[str(exc)],
            ),
        }


def _attach_snapshot(payload: dict, snapshot: dict) -> dict:
    return {
        **payload,
        "provider_metadata": payload.get("provider_metadata") or snapshot.get("provider_metadata") or {},
        "snapshot": {
            "id": snapshot.get("id"),
            "snapshot_type": snapshot.get("snapshot_type"),
            "subject_id": snapshot.get("subject_id"),
            "title": snapshot.get("title"),
            "sample_size": snapshot.get("sample_size"),
            "created_at": snapshot.get("created_at"),
        },
    }


def _snapshot_response(snapshot: dict) -> dict:
    payload = _dict(snapshot.get("payload")).copy()
    if "provider_metadata" not in payload:
        payload["provider_metadata"] = snapshot.get("provider_metadata") or {}
    return _attach_snapshot(payload, snapshot)


def _save_snapshot(kind: str, subject_id: str, title: str | None, payload: dict) -> dict:
    provider = payload.get("provider_metadata") or {}
    sample = _dict(payload.get("sample"))
    stored_payload = {key: value for key, value in payload.items() if key != "snapshot"}
    snapshot = save_intelligence_snapshot(
        snapshot_type=kind,
        subject_id=subject_id,
        title=title,
        payload=stored_payload,
        provider_metadata=provider,
        sample_size=int(sample.get("article_count") or 0),
    )
    return _attach_snapshot(payload, snapshot)


def _card_link(kind: str, subject: dict) -> str:
    subject_id = subject.get("id")
    if kind == "source" and subject_id:
        return f"/sources/{subject_id}"
    if kind == "topic" and subject_id:
        return f"/topics/{subject_id}"
    return "/"


def _card_subject_name(subject: dict) -> str:
    return intelligence_provider.clean_text(subject.get("name") or subject.get("id"), limit=160, fallback="Intelligence")


def _card_base_payload(payload: dict, run_id: str | None) -> dict:
    subject = _dict(payload.get("subject"))
    sample = _dict(payload.get("sample"))
    framing = _dict(payload.get("framing_pattern"))
    return {
        "intelligence_card": True,
        "intelligence_refresh_run_id": run_id,
        "intelligence_snapshot_id": (_dict(payload.get("snapshot"))).get("id"),
        "snapshot_type": payload.get("snapshot_type"),
        "subject": subject,
        "sample": sample,
        "provider_metadata": payload.get("provider_metadata") or {},
        "dominant_frame": (framing.get("dominant_frames") or [None])[0],
        "claim_count": len(payload.get("recurring_claims") or []),
        "recurring_claims": payload.get("recurring_claims") or [],
        "weak_spots": payload.get("weak_spots") or [],
    }


def _build_intelligence_feed_cards(
    *,
    kind: str,
    payload: dict,
    session_id: str,
    run_id: str | None,
) -> list[dict]:
    subject = _dict(payload.get("subject"))
    sample = _dict(payload.get("sample"))
    snapshot = _dict(payload.get("snapshot"))
    subject_name = _card_subject_name(subject)
    article_count = int(sample.get("article_count") or 0)
    recurring_claims = intelligence_provider.safe_list(payload.get("recurring_claims") or [], limit=8)
    weak_spots = intelligence_provider.safe_list(payload.get("weak_spots") or [], limit=8)
    status = payload.get("status") or "ready"
    href = _card_link(kind, subject)
    dominant_frame = (_dict(payload.get("framing_pattern")).get("dominant_frames") or [None])[0]
    card_type = "coverage_gap" if status == "empty" or not article_count else (
        "source_pattern" if kind == "source" else "topic_shift"
    )
    priority = 0.74 if recurring_claims else 0.66 if article_count else 0.58
    source_id = subject.get("id") if kind == "source" else None
    topic_id = subject.get("id") if kind == "topic" else None
    topic_label = "Source intelligence" if kind == "source" else subject_name
    summary = intelligence_provider.clean_text(payload.get("summary"), limit=900)
    if card_type == "coverage_gap" and weak_spots:
        summary = weak_spots[0]

    cards = [
        {
            "id": str(uuid4()),
            "session_id": session_id,
            "topic_id": topic_id,
            "source_id": source_id,
            "card_type": card_type,
            "title": (
                f"{subject_name}: coverage gap"
                if card_type == "coverage_gap"
                else f"{subject_name}: intelligence pattern"
            ),
            "summary": summary or "A refreshed intelligence snapshot produced a feed signal.",
            "source": subject_name if kind == "source" else None,
            "url": subject.get("website_url") if kind == "source" else None,
            "topic": topic_label,
            "priority": priority,
            "priority_score": priority,
            "personalized_score": priority,
            "narrative_signal": "Contextual intelligence aggregation, not a truth verdict.",
            "framing": dominant_frame,
            "payload": {
                **_card_base_payload(payload, run_id),
                "signal_reason": "snapshot_refresh",
                "snapshot_created_at": snapshot.get("created_at"),
            },
            "recommendations": [
                {
                    "type": "open_intelligence_subject",
                    "label": "Open intelligence view",
                    "href": href,
                    "reason": "Inspect the source/topic pattern, sample, and limitations.",
                },
                {
                    "type": "open_feed",
                    "label": "Review feed",
                    "href": "/",
                    "reason": "Place this signal alongside recent article and comparison cards.",
                },
            ],
            "explanation": {
                "why_this_matters": "A source/topic aggregation changed enough to be surfaced in the main intelligence feed.",
                "what_changed": {
                    "article_count": article_count,
                    "dominant_frame": dominant_frame,
                    "recurring_claim_count": len(recurring_claims),
                    "weak_spots": weak_spots[:4],
                },
                "recommended_action": "Open the intelligence view, then compare supporting articles before drawing conclusions.",
            },
            "analysis": {
                "intelligence": payload,
                "provider_metadata": payload.get("provider_metadata") or {},
            },
        }
    ]

    if recurring_claims:
        claim_summary = recurring_claims[0]
        cards.append(
            {
                "id": str(uuid4()),
                "session_id": session_id,
                "topic_id": topic_id,
                "source_id": source_id,
                "card_type": "recurring_claim",
                "title": f"{subject_name}: recurring claim cluster",
                "summary": intelligence_provider.clean_text(claim_summary, limit=900),
                "source": subject_name if kind == "source" else None,
                "url": subject.get("website_url") if kind == "source" else None,
                "topic": topic_label,
                "priority": max(priority, 0.7),
                "priority_score": max(priority, 0.7),
                "personalized_score": max(priority, 0.7),
                "narrative_signal": "Repeated claims are comparison leads, not truth judgments.",
                "framing": dominant_frame,
                "payload": {
                    **_card_base_payload(payload, run_id),
                    "signal_reason": "recurring_claim",
                    "primary_claim": claim_summary,
                    "snapshot_created_at": snapshot.get("created_at"),
                },
                "recommendations": [
                    {
                        "type": "open_intelligence_subject",
                        "label": "Open intelligence view",
                        "href": href,
                        "reason": "Review which articles produced the recurring claim signal.",
                    },
                    {
                        "type": "compare",
                        "label": "Compare coverage",
                        "href": "/compare",
                        "reason": "Check whether other sources add, omit, or frame this claim differently.",
                    },
                ],
                "explanation": {
                    "why_this_matters": "The same or similar claim is recurring in the sampled source/topic coverage.",
                    "what_changed": {
                        "primary_claim": claim_summary,
                        "recurring_claims": recurring_claims[:6],
                        "article_count": article_count,
                    },
                    "recommended_action": "Use compare and OSINT context before treating repeated claims as evidence.",
                },
                "analysis": {
                    "intelligence": payload,
                    "provider_metadata": payload.get("provider_metadata") or {},
                },
            }
        )

    return cards


async def build_source_intelligence(source_id: str, refresh: bool = False, limit: int = 100) -> dict:
    source = get_source_record(source_id)
    if not source:
        raise FeedStoreError("Source not found.")
    if not refresh:
        latest = get_latest_intelligence_snapshot("source", source_id)
        if latest:
            return _snapshot_response(latest)

    article_limit = max(1, min(int(limit or 100), 250))
    articles = list_ingested_article_records(source_id=source_id, limit=article_limit)
    base = _aggregate("source", _source_identity(source), articles)
    enhanced = await _enhance_aggregation("source", base)
    return _save_snapshot("source", source_id, source.get("name"), enhanced)


async def build_topic_intelligence(
    topic_id: str,
    session_id: str = ANONYMOUS_SESSION_ID,
    refresh: bool = False,
    limit: int = 100,
) -> dict:
    topic = next((item for item in list_topics(session_id=session_id) if item.get("id") == topic_id), None)
    if not topic:
        raise FeedStoreError("Topic not found.")
    if not refresh:
        latest = get_latest_intelligence_snapshot("topic", topic_id)
        if latest:
            return _snapshot_response(latest)

    article_limit = max(1, min(int(limit or 100), 250))
    articles = _articles_for_topic(topic, article_limit)
    base = _aggregate("topic", _topic_identity(topic), articles)
    enhanced = await _enhance_aggregation("topic", base)
    return _save_snapshot("topic", topic_id, topic.get("name"), enhanced)


def _topic_item(payload: dict) -> dict:
    return {
        "topic": payload.get("subject") or {},
        "status": payload.get("status"),
        "summary": payload.get("summary"),
        "sample": payload.get("sample") or {},
        "framing_pattern": payload.get("framing_pattern") or {},
        "recurring_claims": payload.get("recurring_claims") or [],
        "tone_pattern": payload.get("tone_pattern") or {},
        "source_diversity": payload.get("source_diversity") or {},
        "weak_spots": payload.get("weak_spots") or [],
        "limitations": payload.get("limitations") or [],
        "provider_metadata": payload.get("provider_metadata") or {},
        "snapshot": payload.get("snapshot") or {},
    }


async def list_topic_intelligence(
    session_id: str = ANONYMOUS_SESSION_ID,
    limit: int = 25,
    article_limit: int = 100,
) -> dict:
    topic_limit = max(1, min(int(limit or 25), 100))
    topics = list_topics(session_id=session_id)[:topic_limit]
    items = []
    errors = []
    for topic in topics:
        try:
            snapshot = get_latest_intelligence_snapshot("topic", topic["id"])
            payload = _snapshot_response(snapshot) if snapshot else await build_topic_intelligence(
                topic["id"],
                session_id=session_id,
                refresh=False,
                limit=article_limit,
            )
            items.append(_topic_item(payload))
        except FeedStoreError as exc:
            errors.append({"topic_id": topic.get("id"), "error": str(exc)})
    return {
        "items": items,
        "summary": {
            "topic_count": len(topics),
            "snapshot_count": len(items),
            "error_count": len(errors),
        },
        "errors": errors,
        "provider_metadata": intelligence_provider.provider_metadata(
            task="topic_intelligence_list",
            status="heuristic",
        ),
    }


async def refresh_intelligence_snapshots(
    *,
    session_id: str = ANONYMOUS_SESSION_ID,
    source_limit: int = 50,
    topic_limit: int = 50,
    article_limit: int = 100,
    create_cards: bool = True,
    card_limit: int = 50,
) -> dict:
    run_id = str(uuid4())
    started = datetime.now(timezone.utc)
    source_results = []
    topic_results = []
    errors = []
    generated_cards = []
    saved_cards = []
    card_limit = max(0, min(int(card_limit or 50), 100))

    source_count_limit = max(1, min(int(source_limit or 50), 250))
    sources = sorted(
        list_source_records(limit=250),
        key=lambda item: int(item.get("article_count") or 0),
        reverse=True,
    )[:source_count_limit]
    for source in sources:
        try:
            result = await build_source_intelligence(source["id"], refresh=True, limit=article_limit)
            cards_for_snapshot = []
            if create_cards and len(generated_cards) < card_limit:
                cards_for_snapshot = _build_intelligence_feed_cards(
                    kind="source",
                    payload=result,
                    session_id=session_id,
                    run_id=run_id,
                )[: max(0, card_limit - len(generated_cards))]
                generated_cards.extend(cards_for_snapshot)
            source_results.append(
                {
                    "source_id": source["id"],
                    "name": source.get("name"),
                    "status": result.get("status"),
                    "snapshot_id": (result.get("snapshot") or {}).get("id"),
                    "sample_size": (result.get("sample") or {}).get("article_count", 0),
                    "card_count": len(cards_for_snapshot),
                }
            )
        except FeedStoreError as exc:
            errors.append({"snapshot_type": "source", "subject_id": source.get("id"), "error": str(exc)})

    for topic in list_topics(session_id=session_id)[: max(1, min(int(topic_limit or 50), 100))]:
        try:
            result = await build_topic_intelligence(
                topic["id"],
                session_id=session_id,
                refresh=True,
                limit=article_limit,
            )
            cards_for_snapshot = []
            if create_cards and len(generated_cards) < card_limit:
                cards_for_snapshot = _build_intelligence_feed_cards(
                    kind="topic",
                    payload=result,
                    session_id=session_id,
                    run_id=run_id,
                )[: max(0, card_limit - len(generated_cards))]
                generated_cards.extend(cards_for_snapshot)
            topic_results.append(
                {
                    "topic_id": topic["id"],
                    "name": topic.get("name"),
                    "status": result.get("status"),
                    "snapshot_id": (result.get("snapshot") or {}).get("id"),
                    "sample_size": (result.get("sample") or {}).get("article_count", 0),
                    "card_count": len(cards_for_snapshot),
                }
            )
        except FeedStoreError as exc:
            errors.append({"snapshot_type": "topic", "subject_id": topic.get("id"), "error": str(exc)})

    if generated_cards:
        try:
            saved_cards = save_generated_feed_cards(generated_cards)
        except FeedStoreError as exc:
            errors.append({"snapshot_type": "feed_cards", "subject_id": None, "error": str(exc)})

    completed = len(source_results) + len(topic_results)
    status = "completed" if not errors else "partial" if completed else "failed"
    finished = datetime.now(timezone.utc)
    summary = {
        "source_count": len(source_results),
        "topic_count": len(topic_results),
        "snapshot_count": completed,
        "card_count": len(saved_cards),
        "error_count": len(errors),
        "create_cards": create_cards,
    }
    run = save_intelligence_refresh_run(
        run_id=run_id,
        session_id=session_id,
        status=status,
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        duration_ms=round((finished - started).total_seconds() * 1000),
        source_count=len(source_results),
        topic_count=len(topic_results),
        snapshot_count=completed,
        card_count=len(saved_cards),
        error_count=len(errors),
        limits={
            "source_limit": source_limit,
            "topic_limit": topic_limit,
            "article_limit": article_limit,
            "card_limit": card_limit,
        },
        errors=errors,
        summary=summary,
    )
    return {
        "status": "completed" if not errors else "partial" if completed else "failed",
        "run": run,
        "source_count": len(source_results),
        "topic_count": len(topic_results),
        "snapshot_count": completed,
        "card_count": len(saved_cards),
        "error_count": len(errors),
        "sources": source_results,
        "topics": topic_results,
        "cards": saved_cards,
        "errors": errors,
    }


def list_recent_intelligence_refresh_runs(
    *,
    session_id: str | None = None,
    limit: int = 25,
) -> dict:
    runs = list_intelligence_refresh_runs(session_id=session_id, limit=limit)
    return {
        "runs": runs,
        "summary": {
            "run_count": len(runs),
            "latest_status": runs[0]["status"] if runs else None,
            "latest_started_at": runs[0]["started_at"] if runs else None,
            "latest_card_count": runs[0]["card_count"] if runs else 0,
        },
    }
