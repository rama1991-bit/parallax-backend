from uuid import uuid4
from datetime import datetime, timezone
import re

from app.services.feed.store import (
    FeedStoreError,
    get_ingested_article_record,
    list_ingested_article_records,
    save_article_comparison_record,
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


def _tokens(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-zA-Z0-9]+", value.lower())
        if len(token) > 2 and token not in STOPWORDS
    }


def _similarity(left: str, right: str) -> float:
    left_tokens = _tokens(left)
    right_tokens = _tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _jaccard(left: list[str], right: list[str]) -> float:
    left_set = {item.lower() for item in left}
    right_set = {item.lower() for item in right}
    if not left_set and not right_set:
        return 1.0
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def _ordered_intersection(left: list[str], right: list[str]) -> list[str]:
    right_lookup = {item.lower() for item in right}
    return [item for item in left if item.lower() in right_lookup]


def _ordered_difference(left: list[str], right: list[str]) -> list[str]:
    right_lookup = {item.lower() for item in right}
    return [item for item in left if item.lower() not in right_lookup]


def _report_side(card: dict) -> dict:
    payload = card.get("payload") or {}
    analysis = card.get("analysis") or {}
    return {
        "card_id": card.get("id"),
        "report_id": card.get("report_id"),
        "title": card.get("title"),
        "source": card.get("source") or payload.get("source"),
        "domain": payload.get("domain"),
        "url": card.get("url") or payload.get("url"),
        "summary": card.get("summary"),
        "confidence": analysis.get("confidence", payload.get("confidence")),
        "priority": analysis.get("priority", payload.get("priority")),
        "key_claims": analysis.get("key_claims") or payload.get("key_claims") or [],
        "narrative_framing": analysis.get("narrative_framing")
        or payload.get("narrative_framing")
        or [],
        "entities": analysis.get("entities") or payload.get("entities") or [],
        "topics": analysis.get("topics") or payload.get("topics") or [],
    }


def _claim_overlap(left_claims: list[str], right_claims: list[str]) -> dict:
    shared = []
    matched_right_indexes: set[int] = set()
    left_unique = []

    for left_claim in left_claims:
        best_index = -1
        best_score = 0.0
        best_claim = ""
        for index, right_claim in enumerate(right_claims):
            if index in matched_right_indexes:
                continue
            score = _similarity(left_claim, right_claim)
            if score > best_score:
                best_score = score
                best_index = index
                best_claim = right_claim

        if best_index >= 0 and best_score >= 0.25:
            matched_right_indexes.add(best_index)
            shared.append(
                {
                    "left_claim": left_claim,
                    "right_claim": best_claim,
                    "similarity": round(best_score, 3),
                }
            )
        else:
            left_unique.append(left_claim)

    right_unique = [
        claim for index, claim in enumerate(right_claims) if index not in matched_right_indexes
    ]
    denominator = max(len(left_claims), len(right_claims), 1)

    return {
        "score": round(len(shared) / denominator, 3),
        "shared": shared,
        "left_unique": left_unique,
        "right_unique": right_unique,
    }


def build_compare_result(left_card: dict, right_card: dict, usage: dict | None = None) -> dict:
    left = _report_side(left_card)
    right = _report_side(right_card)

    left_frames = left["narrative_framing"]
    right_frames = right["narrative_framing"]
    frame_similarity = _jaccard(left_frames, right_frames)

    left_entities = left["entities"]
    right_entities = right["entities"]
    left_topics = left["topics"]
    right_topics = right["topics"]

    return {
        "id": str(uuid4()),
        "status": "completed",
        "left": left,
        "right": right,
        "claim_overlap": _claim_overlap(left["key_claims"], right["key_claims"]),
        "framing": {
            "divergence_score": round(1 - frame_similarity, 3),
            "shared": _ordered_intersection(left_frames, right_frames),
            "left_only": _ordered_difference(left_frames, right_frames),
            "right_only": _ordered_difference(right_frames, left_frames),
        },
        "entities": {
            "shared": _ordered_intersection(left_entities, right_entities),
            "left_only": _ordered_difference(left_entities, right_entities),
            "right_only": _ordered_difference(right_entities, left_entities),
        },
        "topics": {
            "shared": _ordered_intersection(left_topics, right_topics),
            "left_only": _ordered_difference(left_topics, right_topics),
            "right_only": _ordered_difference(right_topics, left_topics),
        },
        "usage": usage,
        "limitations": [
            "Claim overlap is lexical and approximate; it is a comparison signal, not a factual verdict.",
            "Framing divergence compares extracted frame labels and can miss subtle rhetorical differences.",
            "Both articles are saved as analyzed report cards for this session.",
        ],
    }


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


def _article_analysis(article: dict) -> dict:
    analysis = article.get("analysis") or {}
    return analysis if isinstance(analysis, dict) else {}


def _article_intelligence(article: dict) -> dict:
    intelligence = _article_analysis(article).get("intelligence") or {}
    return intelligence if isinstance(intelligence, dict) else {}


def _article_source(article: dict) -> dict:
    source = article.get("source") or {}
    if isinstance(source, dict):
        return source
    return {"name": str(source)}


def _article_claims(article: dict) -> list[str]:
    analysis = _article_analysis(article)
    intelligence = _article_intelligence(article)
    claims = analysis.get("key_claims") or intelligence.get("key_claims") or []
    return [str(claim) for claim in claims if claim][:8]


def _article_frames(article: dict) -> list[str]:
    analysis = _article_analysis(article)
    intelligence = _article_intelligence(article)
    narrative = intelligence.get("narrative") if isinstance(intelligence.get("narrative"), dict) else {}
    frames = analysis.get("narrative_framing") or []
    if narrative.get("main_frame") and narrative["main_frame"] not in frames:
        frames = [narrative["main_frame"], *frames]
    for frame in narrative.get("secondary_frames") or []:
        if frame not in frames:
            frames.append(frame)
    return [str(frame) for frame in frames if frame][:8]


def _article_tone(article: dict) -> str:
    intelligence = _article_intelligence(article)
    narrative = intelligence.get("narrative") if isinstance(intelligence.get("narrative"), dict) else {}
    return str(narrative.get("tone") or "unknown")


def _article_missing_context(article: dict) -> list[str]:
    intelligence = _article_intelligence(article)
    narrative = intelligence.get("narrative") if isinstance(intelligence.get("narrative"), dict) else {}
    missing = [str(item) for item in narrative.get("missing_context") or [] if item]
    if article.get("analysis_status") != "analyzed":
        missing.append("Article has not been analyzed yet; comparison relies on ingestion metadata.")
    return missing


def _article_keywords(article: dict) -> list[str]:
    keywords = [str(item).lower() for item in article.get("comparison_keywords") or [] if item]
    keywords.extend(_tokens(article.get("title") or ""))
    keywords.extend(_tokens(article.get("summary") or ""))
    keywords.extend(_tokens(" ".join(_article_claims(article))))
    seen = set()
    ordered = []
    for keyword in keywords:
        if keyword and keyword not in seen:
            seen.add(keyword)
            ordered.append(keyword)
        if len(ordered) >= 40:
            break
    return ordered


def _keyword_score(base: dict, candidate: dict) -> float:
    left = set(_article_keywords(base))
    right = set(_article_keywords(candidate))
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _timeline_difference(base: dict, candidate: dict) -> dict:
    base_time = _parse_datetime(base.get("published_at") or base.get("created_at"))
    candidate_time = _parse_datetime(candidate.get("published_at") or candidate.get("created_at"))
    if not base_time or not candidate_time:
        return {
            "base_published_at": base.get("published_at"),
            "comparison_published_at": candidate.get("published_at"),
            "difference_hours": None,
            "direction": "unknown",
        }
    delta_hours = round((candidate_time - base_time).total_seconds() / 3600, 2)
    return {
        "base_published_at": base_time.isoformat(),
        "comparison_published_at": candidate_time.isoformat(),
        "difference_hours": delta_hours,
        "direction": "after" if delta_hours > 0 else "before" if delta_hours < 0 else "same_time",
    }


def _source_difference(base: dict, candidate: dict) -> dict:
    base_source = _article_source(base)
    candidate_source = _article_source(candidate)
    return {
        "base_source": {
            "id": base.get("source_id"),
            "name": base_source.get("name"),
            "country": base_source.get("country") or base.get("country"),
            "language": base_source.get("language") or base.get("language"),
            "source_type": base_source.get("source_type"),
            "source_size": base_source.get("source_size"),
        },
        "comparison_source": {
            "id": candidate.get("source_id"),
            "name": candidate_source.get("name"),
            "country": candidate_source.get("country") or candidate.get("country"),
            "language": candidate_source.get("language") or candidate.get("language"),
            "source_type": candidate_source.get("source_type"),
            "source_size": candidate_source.get("source_size"),
        },
        "same_source": bool(base.get("source_id") and base.get("source_id") == candidate.get("source_id")),
    }


def _similarity_score(base: dict, candidate: dict) -> dict:
    same_fingerprint = bool(
        base.get("event_fingerprint")
        and candidate.get("event_fingerprint")
        and base.get("event_fingerprint") == candidate.get("event_fingerprint")
    )
    keyword_score = _keyword_score(base, candidate)
    title_score = _similarity(base.get("title") or "", candidate.get("title") or "")
    frame_score = _jaccard(_article_frames(base), _article_frames(candidate))
    source_bonus = 0.08 if base.get("source_id") and base.get("source_id") != candidate.get("source_id") else 0.0
    analysis_bonus = 0.05 if candidate.get("analysis_status") == "analyzed" else 0.0
    score = (
        (0.32 if same_fingerprint else 0.0)
        + keyword_score * 0.32
        + title_score * 0.22
        + frame_score * 0.09
        + source_bonus
        + analysis_bonus
    )
    return {
        "score": round(max(0.0, min(score, 1.0)), 3),
        "same_fingerprint": same_fingerprint,
        "keyword_score": round(keyword_score, 3),
        "title_score": round(title_score, 3),
        "frame_score": round(frame_score, 3),
    }


def _article_summary(article: dict, similarity: dict | None = None) -> dict:
    source = _article_source(article)
    analysis = _article_analysis(article)
    intelligence = _article_intelligence(article)
    return {
        "id": article.get("id"),
        "title": article.get("title"),
        "url": article.get("url"),
        "source": source.get("name") or article.get("source_id"),
        "source_id": article.get("source_id"),
        "source_type": source.get("source_type"),
        "source_size": source.get("source_size"),
        "country": article.get("country") or source.get("country"),
        "language": article.get("language") or source.get("language"),
        "published_at": article.get("published_at"),
        "summary": article.get("summary") or analysis.get("summary"),
        "analysis_status": article.get("analysis_status"),
        "event_fingerprint": article.get("event_fingerprint"),
        "comparison_keywords": article.get("comparison_keywords") or [],
        "key_claims": _article_claims(article),
        "narrative_framing": _article_frames(article),
        "tone": _article_tone(article),
        "confidence": analysis.get("confidence") or (intelligence.get("scores") or {}).get("confidence_score"),
        "similarity": similarity,
    }


def _candidate_comparison(base: dict, candidate: dict, similarity: dict) -> dict:
    claim_overlap = _claim_overlap(_article_claims(base), _article_claims(candidate))
    base_frames = _article_frames(base)
    candidate_frames = _article_frames(candidate)
    source_difference = _source_difference(base, candidate)
    timeline_difference = _timeline_difference(base, candidate)
    base_source = source_difference["base_source"].get("name") or "Base source"
    candidate_source = source_difference["comparison_source"].get("name") or "Comparison source"
    base_tone = _article_tone(base)
    candidate_tone = _article_tone(candidate)

    shared_claims = [
        {
            "base_claim": item["left_claim"],
            "comparison_claim": item["right_claim"],
            "similarity": item["similarity"],
            "comparison_article_id": candidate.get("id"),
            "comparison_source": candidate_source,
        }
        for item in claim_overlap["shared"]
    ]
    unique_claims_by_source = [
        {
            "source": base_source,
            "article_id": base.get("id"),
            "claims": claim_overlap["left_unique"],
        },
        {
            "source": candidate_source,
            "article_id": candidate.get("id"),
            "claims": claim_overlap["right_unique"],
        },
    ]
    framing_differences = [
        {
            "comparison_article_id": candidate.get("id"),
            "comparison_source": candidate_source,
            "shared_frames": _ordered_intersection(base_frames, candidate_frames),
            "base_only": _ordered_difference(base_frames, candidate_frames),
            "comparison_only": _ordered_difference(candidate_frames, base_frames),
        }
    ]
    tone_differences = [
        {
            "comparison_article_id": candidate.get("id"),
            "base_tone": base_tone,
            "comparison_tone": candidate_tone,
            "difference": "same" if base_tone == candidate_tone else "different",
        }
    ]
    missing_context = [*_article_missing_context(base), *_article_missing_context(candidate)]
    confidence = round(max(similarity["score"], claim_overlap["score"]) * 0.7 + similarity["keyword_score"] * 0.3, 3)
    payload = {
        "base_article": _article_summary(base),
        "comparison_article": _article_summary(candidate, similarity=similarity),
        "claim_overlap": claim_overlap,
        "similarity_breakdown": similarity,
    }

    return {
        "shared_claims": shared_claims,
        "unique_claims_by_source": unique_claims_by_source,
        "framing_differences": framing_differences,
        "tone_differences": tone_differences,
        "missing_context": list(dict.fromkeys(missing_context)),
        "timeline_difference": timeline_difference,
        "source_difference": source_difference,
        "confidence": confidence,
        "comparison_payload": payload,
    }


def build_ingested_article_compare_result(article_id: str, limit: int = 8) -> dict:
    base = get_ingested_article_record(article_id)
    if not base:
        raise FeedStoreError("Ingested article not found.")

    candidates = []
    for candidate in list_ingested_article_records(limit=250):
        if candidate.get("id") == article_id:
            continue
        similarity = _similarity_score(base, candidate)
        if similarity["score"] >= 0.12 or similarity["same_fingerprint"]:
            candidates.append((similarity["score"], similarity, candidate))

    candidates.sort(key=lambda item: item[0], reverse=True)
    selected = candidates[: max(1, min(limit, 25))]

    similar_articles = []
    comparisons = []
    for _, similarity, candidate in selected:
        comparison = _candidate_comparison(base, candidate, similarity)
        record = save_article_comparison_record(
            base_article_id=base["id"],
            comparison_article_id=candidate["id"],
            similarity_score=similarity["score"],
            **comparison,
        )
        summary = _article_summary(candidate, similarity=similarity)
        summary["comparison_id"] = record["id"]
        summary["title_difference"] = {
            "base_title": base.get("title"),
            "comparison_title": candidate.get("title"),
            "title_similarity": similarity["title_score"],
        }
        summary["source_difference"] = comparison["source_difference"]
        summary["timeline_difference"] = comparison["timeline_difference"]
        similar_articles.append(summary)
        comparisons.append(comparison)

    shared_claims = []
    unique_claims_by_source = []
    framing_differences = []
    tone_differences = []
    missing_context = []
    timeline_differences = []
    source_differences = []
    for comparison in comparisons:
        shared_claims.extend(comparison["shared_claims"])
        unique_claims_by_source.extend(comparison["unique_claims_by_source"])
        framing_differences.extend(comparison["framing_differences"])
        tone_differences.extend(comparison["tone_differences"])
        missing_context.extend(comparison["missing_context"])
        timeline_differences.append(comparison["timeline_difference"])
        source_differences.append(comparison["source_difference"])

    confidence = round(
        sum(item["similarity"]["score"] for item in similar_articles) / max(len(similar_articles), 1),
        3,
    )

    return {
        "id": str(uuid4()),
        "status": "completed",
        "base_article": _article_summary(base),
        "similar_articles": similar_articles,
        "comparison": {
            "shared_claims": shared_claims[:20],
            "unique_claims_by_source": unique_claims_by_source[:20],
            "framing_differences": framing_differences[:20],
            "tone_differences": tone_differences[:20],
            "missing_context": list(dict.fromkeys(missing_context))[:20],
            "timeline_difference": timeline_differences,
            "source_difference": source_differences,
            "confidence": confidence,
        },
        "limitations": [
            "Similarity is a retrieval signal, not a factual verdict.",
            "Unanalyzed articles can be matched by metadata, but claim and framing comparisons are limited.",
            "OSINT is not used here; this compares only ingested source articles and saved analysis.",
        ],
    }
