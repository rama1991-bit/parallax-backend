from uuid import uuid4
from datetime import datetime, timezone
import re

from app.services.feed.store import (
    FeedStoreError,
    get_event_cluster_for_article,
    get_ingested_article_record,
    list_event_cluster_articles,
    list_ingested_article_records,
    save_article_comparison_record,
)
from app.services.intelligence import enhance_compare_result, provider_metadata


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


def _unique_strings(items: list[str], limit: int | None = None) -> list[str]:
    seen = set()
    ordered = []
    for item in items:
        text = str(item).strip()
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        ordered.append(text)
        if limit and len(ordered) >= limit:
            break
    return ordered


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
        "provider_metadata": provider_metadata(task="url_compare", status="heuristic"),
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


def _article_entities(article: dict) -> list[str]:
    analysis = _article_analysis(article)
    intelligence = _article_intelligence(article)
    collected: list[str] = []

    def collect(value):
        if isinstance(value, str):
            collected.append(value)
            return
        if isinstance(value, dict):
            label = (
                value.get("name")
                or value.get("label")
                or value.get("text")
                or value.get("entity")
                or value.get("title")
            )
            if label:
                collected.append(str(label))
                return
            for nested in value.values():
                collect(nested)
            return
        if isinstance(value, list):
            for item in value:
                collect(item)

    collect(analysis.get("entities"))
    collect(intelligence.get("entities"))
    return _unique_strings(collected, limit=24)


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
    base_profile = {
        "id": base.get("source_id"),
        "name": base_source.get("name"),
        "country": base_source.get("country") or base.get("country"),
        "language": base_source.get("language") or base.get("language"),
        "source_type": base_source.get("source_type"),
        "source_size": base_source.get("source_size"),
    }
    comparison_profile = {
        "id": candidate.get("source_id"),
        "name": candidate_source.get("name"),
        "country": candidate_source.get("country") or candidate.get("country"),
        "language": candidate_source.get("language") or candidate.get("language"),
        "source_type": candidate_source.get("source_type"),
        "source_size": candidate_source.get("source_size"),
    }
    differences = [
        field
        for field in ("country", "language", "source_type", "source_size")
        if base_profile.get(field) and comparison_profile.get(field) and base_profile.get(field) != comparison_profile.get(field)
    ]
    return {
        "base_source": base_profile,
        "comparison_source": comparison_profile,
        "same_source": bool(base.get("source_id") and base.get("source_id") == candidate.get("source_id")),
        "differences": differences,
        "source_contrast": " / ".join(differences) if differences else "same or unknown metadata",
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


def _cluster_summary(cluster: dict | None) -> dict | None:
    if not cluster:
        return None
    metadata = cluster.get("provider_metadata") if isinstance(cluster.get("provider_metadata"), dict) else {}
    return {
        "id": cluster.get("id"),
        "cluster_key": cluster.get("cluster_key"),
        "title": cluster.get("title"),
        "summary": cluster.get("summary"),
        "primary_topic": cluster.get("primary_topic"),
        "languages": cluster.get("languages") or [],
        "countries": cluster.get("countries") or [],
        "source_ids": cluster.get("source_ids") or [],
        "article_count": cluster.get("article_count", 0),
        "analyzed_count": cluster.get("analyzed_count", 0),
        "first_seen_at": cluster.get("first_seen_at"),
        "latest_seen_at": cluster.get("latest_seen_at"),
        "fingerprint_terms": cluster.get("fingerprint_terms") or [],
        "claims": cluster.get("claims") or [],
        "frames": cluster.get("frames") or [],
        "membership": cluster.get("membership") or {},
        "provider_metadata": metadata,
        "cluster_quality": metadata.get("cluster_quality") or {},
        "source_diversity": metadata.get("source_diversity") or {},
        "language_bridge_terms": metadata.get("language_bridge_terms") or [],
        "limitations": [
            "The event cluster is a retrieval signal, not evidence that the articles agree.",
            "Cluster membership should be checked through claim, source, and timeline comparison.",
        ],
    }


def _title_difference(base: dict, candidate: dict, similarity: dict) -> dict:
    base_title = base.get("title") or ""
    candidate_title = candidate.get("title") or ""
    base_terms = sorted(_tokens(base_title) - _tokens(candidate_title))[:10]
    comparison_terms = sorted(_tokens(candidate_title) - _tokens(base_title))[:10]
    return {
        "comparison_article_id": candidate.get("id"),
        "base_title": base_title,
        "comparison_title": candidate_title,
        "title_similarity": similarity["title_score"],
        "base_terms": base_terms,
        "comparison_terms": comparison_terms,
    }


def _entity_overlap(base: dict, candidate: dict) -> dict:
    base_entities = _article_entities(base)
    candidate_entities = _article_entities(candidate)
    return {
        "shared": _ordered_intersection(base_entities, candidate_entities),
        "base_only": _ordered_difference(base_entities, candidate_entities),
        "comparison_only": _ordered_difference(candidate_entities, base_entities),
    }


def _coverage_gaps(
    base: dict,
    candidate: dict,
    claim_overlap: dict,
    source_difference: dict,
    entity_overlap: dict,
) -> list[dict]:
    candidate_source = source_difference["comparison_source"].get("name") or "Comparison source"
    gaps = []
    if claim_overlap["left_unique"]:
        gaps.append(
            {
                "type": "base_claims_missing_from_comparison",
                "comparison_article_id": candidate.get("id"),
                "source": candidate_source,
                "reason": "The comparison article does not surface these extracted base claims.",
                "claims": claim_overlap["left_unique"][:8],
            }
        )
    if claim_overlap["right_unique"]:
        gaps.append(
            {
                "type": "comparison_adds_claims",
                "comparison_article_id": candidate.get("id"),
                "source": candidate_source,
                "reason": "The comparison article adds claims not detected in the base story.",
                "claims": claim_overlap["right_unique"][:8],
            }
        )
    if candidate.get("analysis_status") != "analyzed":
        gaps.append(
            {
                "type": "comparison_unanalyzed",
                "comparison_article_id": candidate.get("id"),
                "source": candidate_source,
                "reason": "This match has not been structurally analyzed yet, so claim and frame coverage is limited.",
                "claims": [],
            }
        )
    if source_difference.get("same_source"):
        gaps.append(
            {
                "type": "same_source_match",
                "comparison_article_id": candidate.get("id"),
                "source": candidate_source,
                "reason": "The closest match comes from the same source; cross-outlet confirmation is still needed.",
                "claims": [],
            }
        )

    base_profile = source_difference["base_source"]
    comparison_profile = source_difference["comparison_source"]
    same_country = (
        base_profile.get("country")
        and comparison_profile.get("country")
        and base_profile.get("country") == comparison_profile.get("country")
    )
    same_language = (
        base_profile.get("language")
        and comparison_profile.get("language")
        and base_profile.get("language") == comparison_profile.get("language")
    )
    if same_country and same_language:
        gaps.append(
            {
                "type": "limited_source_diversity",
                "comparison_article_id": candidate.get("id"),
                "source": candidate_source,
                "reason": "The match shares country and language metadata with the base article; regional or language diversity remains thin.",
                "claims": [],
            }
        )
    if not entity_overlap["shared"] and (_article_entities(base) or _article_entities(candidate)):
        gaps.append(
            {
                "type": "entity_context_gap",
                "comparison_article_id": candidate.get("id"),
                "source": candidate_source,
                "reason": "The compared articles do not share extracted entities, so story alignment may depend mostly on topic keywords.",
                "claims": [],
            }
        )
    return gaps


def _candidate_comparison(base: dict, candidate: dict, similarity: dict) -> dict:
    claim_overlap = _claim_overlap(_article_claims(base), _article_claims(candidate))
    base_frames = _article_frames(base)
    candidate_frames = _article_frames(candidate)
    source_difference = _source_difference(base, candidate)
    timeline_difference = _timeline_difference(base, candidate)
    title_difference = _title_difference(base, candidate, similarity)
    entity_overlap = _entity_overlap(base, candidate)
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
    missing_claims = []
    if claim_overlap["left_unique"]:
        missing_claims.append(
            {
                "comparison_article_id": candidate.get("id"),
                "comparison_source": candidate_source,
                "claims": claim_overlap["left_unique"],
            }
        )
    added_claims = []
    if claim_overlap["right_unique"]:
        added_claims.append(
            {
                "comparison_article_id": candidate.get("id"),
                "comparison_source": candidate_source,
                "claims": claim_overlap["right_unique"],
            }
        )
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
    coverage_gaps = _coverage_gaps(
        base,
        candidate,
        claim_overlap=claim_overlap,
        source_difference=source_difference,
        entity_overlap=entity_overlap,
    )
    confidence = round(max(similarity["score"], claim_overlap["score"]) * 0.7 + similarity["keyword_score"] * 0.3, 3)
    payload = {
        "base_article": _article_summary(base),
        "comparison_article": _article_summary(candidate, similarity=similarity),
        "claim_overlap": claim_overlap,
        "similarity_breakdown": similarity,
        "title_difference": title_difference,
        "missing_claims": missing_claims,
        "added_claims": added_claims,
        "coverage_gaps": coverage_gaps,
        "entities": entity_overlap,
    }

    return {
        "title_difference": title_difference,
        "shared_claims": shared_claims,
        "unique_claims_by_source": unique_claims_by_source,
        "missing_claims": missing_claims,
        "added_claims": added_claims,
        "framing_differences": framing_differences,
        "tone_differences": tone_differences,
        "missing_context": list(dict.fromkeys(missing_context)),
        "timeline_difference": timeline_difference,
        "source_difference": source_difference,
        "coverage_gaps": coverage_gaps,
        "entities": entity_overlap,
        "confidence": confidence,
        "comparison_payload": payload,
    }


def build_ingested_article_compare_result(article_id: str, limit: int = 8) -> dict:
    base = get_ingested_article_record(article_id)
    if not base:
        raise FeedStoreError("Ingested article not found.")

    event_cluster = get_event_cluster_for_article(article_id)
    candidates = []
    seen_candidate_ids = set()
    if event_cluster:
        for membership in list_event_cluster_articles(event_cluster["id"], limit=50):
            candidate = membership.get("article")
            if not candidate or candidate.get("id") == article_id:
                continue
            similarity = _similarity_score(base, candidate)
            cluster_score = _as_float(membership.get("similarity_score"), 0.0)
            similarity = {
                **similarity,
                "score": round(max(similarity["score"], min(cluster_score, 1.0)), 3),
                "event_cluster_match": True,
                "event_cluster_id": event_cluster["id"],
                "cluster_similarity_score": round(cluster_score, 3),
                "matched_terms": membership.get("matched_terms") or [],
            }
            candidates.append((similarity["score"] + 0.08, similarity, candidate))
            seen_candidate_ids.add(candidate.get("id"))

    for candidate in list_ingested_article_records(limit=250):
        if candidate.get("id") == article_id or candidate.get("id") in seen_candidate_ids:
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
            shared_claims=comparison["shared_claims"],
            unique_claims_by_source=comparison["unique_claims_by_source"],
            framing_differences=comparison["framing_differences"],
            tone_differences=comparison["tone_differences"],
            missing_context=comparison["missing_context"],
            timeline_difference=comparison["timeline_difference"],
            source_difference=comparison["source_difference"],
            confidence=comparison["confidence"],
            comparison_payload=comparison["comparison_payload"],
        )
        summary = _article_summary(candidate, similarity=similarity)
        summary["comparison_id"] = record["id"]
        summary["title_difference"] = comparison["title_difference"]
        summary["source_difference"] = comparison["source_difference"]
        summary["timeline_difference"] = comparison["timeline_difference"]
        similar_articles.append(summary)
        comparisons.append(comparison)

    title_differences = []
    shared_claims = []
    unique_claims_by_source = []
    missing_claims = []
    added_claims = []
    framing_differences = []
    tone_differences = []
    missing_context = []
    timeline_differences = []
    source_differences = []
    coverage_gaps = []
    entity_shared = []
    entity_base_only = []
    entity_comparison_only = []
    for comparison in comparisons:
        title_differences.append(comparison["title_difference"])
        shared_claims.extend(comparison["shared_claims"])
        unique_claims_by_source.extend(comparison["unique_claims_by_source"])
        missing_claims.extend(comparison["missing_claims"])
        added_claims.extend(comparison["added_claims"])
        framing_differences.extend(comparison["framing_differences"])
        tone_differences.extend(comparison["tone_differences"])
        missing_context.extend(comparison["missing_context"])
        timeline_differences.append(comparison["timeline_difference"])
        source_differences.append(comparison["source_difference"])
        coverage_gaps.extend(comparison["coverage_gaps"])
        entities = comparison["entities"]
        entity_shared.extend(entities.get("shared") or [])
        entity_base_only.extend(entities.get("base_only") or [])
        entity_comparison_only.extend(entities.get("comparison_only") or [])

    confidence = round(
        sum(item["similarity"]["score"] for item in similar_articles) / max(len(similar_articles), 1),
        3,
    )
    entities = {
        "shared": _unique_strings(entity_shared, limit=24),
        "base_only": _unique_strings(entity_base_only, limit=24),
        "comparison_only": _unique_strings(entity_comparison_only, limit=24),
    }

    return {
        "id": str(uuid4()),
        "status": "completed",
        "base_article": _article_summary(base),
        "event_cluster": _cluster_summary(event_cluster),
        "similar_articles": similar_articles,
        "entities": entities,
        "comparison": {
            "title_differences": title_differences[:20],
            "shared_claims": shared_claims[:20],
            "unique_claims_by_source": unique_claims_by_source[:20],
            "missing_claims": missing_claims[:20],
            "added_claims": added_claims[:20],
            "framing_differences": framing_differences[:20],
            "tone_differences": tone_differences[:20],
            "missing_context": list(dict.fromkeys(missing_context))[:20],
            "timeline_difference": timeline_differences,
            "source_difference": source_differences,
            "coverage_gaps": coverage_gaps[:20],
            "entities": entities,
            "confidence": confidence,
        },
        "limitations": [
            "Similarity is a retrieval signal, not a factual verdict.",
            "Unanalyzed articles can be matched by metadata, but claim and framing comparisons are limited.",
            "OSINT is not used here; this compares only ingested source articles and saved analysis.",
        ],
        "provider_metadata": provider_metadata(task="cross_source_compare", status="heuristic"),
    }


async def build_enhanced_ingested_article_compare_result(article_id: str, limit: int = 8) -> dict:
    result = build_ingested_article_compare_result(article_id, limit=limit)
    return await enhance_compare_result(result)
