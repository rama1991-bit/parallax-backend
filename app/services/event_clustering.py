from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import re
import unicodedata
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from app.core.session import ANONYMOUS_SESSION_ID
from app.services import intelligence as intelligence_provider
from app.services.feed.store import (
    FeedStoreError,
    get_event_cluster,
    list_event_cluster_articles,
    list_event_cluster_refresh_runs,
    list_event_clusters,
    list_ingested_article_records,
    list_topics,
    replace_event_clusters,
    save_event_cluster_refresh_run,
    save_generated_feed_cards,
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


def _dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _list(value: Any, limit: int = 12) -> list[str]:
    if not isinstance(value, list):
        return []
    cleaned = []
    for item in value:
        text = intelligence_provider.clean_text(
            item.get("claim") if isinstance(item, dict) else item,
            limit=280,
        )
        if text and text not in cleaned:
            cleaned.append(text)
        if len(cleaned) >= limit:
            break
    return cleaned


def _normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").lower())
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", text).strip()


def _tokens(value: Any) -> list[str]:
    text = _normalize(value)
    tokens = []
    for token in re.findall(r"[^\W_]+", text, flags=re.UNICODE):
        if token in STOPWORDS:
            continue
        if len(token) <= 2 and not token.isdigit():
            continue
        tokens.append(token)
    return tokens


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


def _article_analysis(article: dict) -> dict:
    return _dict(article.get("analysis"))


def _article_intelligence(article: dict) -> dict:
    return _dict(_article_analysis(article).get("intelligence"))


def _article_source(article: dict) -> dict:
    source = article.get("source")
    if isinstance(source, dict):
        return source
    return {}


def _article_claims(article: dict, limit: int = 8) -> list[str]:
    analysis = _article_analysis(article)
    intelligence = _article_intelligence(article)
    return _list(analysis.get("key_claims") or intelligence.get("key_claims") or [], limit=limit)


def _article_frames(article: dict, limit: int = 8) -> list[str]:
    analysis = _article_analysis(article)
    intelligence = _article_intelligence(article)
    narrative = _dict(intelligence.get("narrative"))
    frames = _list(analysis.get("narrative_framing") or [], limit=limit)
    main_frame = intelligence_provider.clean_text(narrative.get("main_frame"), limit=120)
    if main_frame and main_frame not in frames:
        frames.insert(0, main_frame)
    for frame in _list(narrative.get("secondary_frames") or [], limit=limit):
        if frame not in frames:
            frames.append(frame)
    return frames[:limit]


def _article_tone(article: dict) -> str:
    narrative = _dict(_article_intelligence(article).get("narrative"))
    return intelligence_provider.clean_text(narrative.get("tone"), limit=120, fallback="unknown")


def _article_entities(article: dict) -> list[str]:
    intelligence = _article_intelligence(article)
    raw_entities = _dict(intelligence.get("entities"))
    values: list[str] = []
    for key in ("people", "organizations", "locations", "events"):
        values.extend(_list(raw_entities.get(key) or [], limit=8))
    values.extend(_list(_article_analysis(article).get("entities") or [], limit=12))
    seen = set()
    ordered = []
    for value in values:
        key = _normalize(value)
        if key and key not in seen:
            seen.add(key)
            ordered.append(value)
    return ordered[:20]


def _comparison_hooks(article: dict) -> dict:
    return _dict(_article_intelligence(article).get("comparison_hooks"))


def _article_scores(article: dict) -> dict:
    return _dict(_article_intelligence(article).get("scores"))


def _url_terms(article: dict) -> list[str]:
    url = str(article.get("canonical_url") or article.get("url") or "")
    return _tokens(url.replace("/", " ").replace("-", " ").replace("_", " "))


def _article_terms(article: dict) -> set[str]:
    hooks = _comparison_hooks(article)
    values: list[Any] = [
        article.get("title"),
        article.get("summary"),
        article.get("event_fingerprint"),
        hooks.get("event_fingerprint"),
        " ".join(article.get("comparison_keywords") or []),
        " ".join(hooks.get("similarity_keywords") or []),
        " ".join(hooks.get("search_queries") or []),
        " ".join(_article_claims(article, limit=12)),
        " ".join(_article_frames(article, limit=12)),
        " ".join(_article_entities(article)),
        " ".join(_url_terms(article)),
    ]
    terms: set[str] = set()
    for value in values:
        terms.update(_tokens(value))
    return terms


def _article_topic_terms(article: dict) -> list[str]:
    analysis = _article_analysis(article)
    topics = _list(analysis.get("topics") or [], limit=8)
    topics.extend(_list(article.get("comparison_keywords") or [], limit=8))
    topics.extend(_list(_comparison_hooks(article).get("similarity_keywords") or [], limit=8))
    return topics[:16]


def _article_fingerprint(article: dict) -> str:
    hooks = _comparison_hooks(article)
    return intelligence_provider.clean_text(
        hooks.get("event_fingerprint") or article.get("event_fingerprint"),
        limit=160,
    )


def _date_key(article: dict) -> str:
    return str(article.get("published_at") or article.get("created_at") or "")


def _similarity(article: dict, terms: set[str], cluster: dict) -> tuple[float, list[str]]:
    cluster_terms = cluster.get("terms") or set()
    article_fingerprint = _article_fingerprint(article)
    if article_fingerprint and article_fingerprint in (cluster.get("fingerprints") or set()):
        matched = sorted(list(terms & cluster_terms))[:12]
        return 0.96, matched
    if not terms or not cluster_terms:
        return 0.0, []
    overlap = terms & cluster_terms
    union_score = len(overlap) / max(len(terms | cluster_terms), 1)
    coverage_score = len(overlap) / max(min(len(terms), len(cluster_terms)), 1)
    score = union_score * 0.72 + coverage_score * 0.28
    return round(score, 3), sorted(overlap)[:12]


def _cluster_key(terms: list[str], fingerprint: str | None = None) -> str:
    if fingerprint:
        basis = f"fingerprint:{_normalize(fingerprint)}"
    else:
        basis = "terms:" + "|".join(sorted(terms[:18]))
    digest = hashlib.sha1(basis.encode("utf-8")).hexdigest()[:20]
    return f"event-{digest}"


def _counter_labels(values: list[str]) -> tuple[Counter[str], dict[str, str]]:
    counter: Counter[str] = Counter()
    labels: dict[str, str] = {}
    for value in values:
        label = intelligence_provider.clean_text(value, limit=260)
        if not label:
            continue
        key = _normalize(label)
        counter[key] += 1
        labels.setdefault(key, label)
    return counter, labels


def _top_values(values: list[str], limit: int = 8) -> list[str]:
    counter, labels = _counter_labels(values)
    return [labels.get(key, key) for key, _count in counter.most_common(limit)]


def _article_excerpt(article: dict) -> dict:
    source = _article_source(article)
    return {
        "id": article.get("id"),
        "title": article.get("title"),
        "url": article.get("url"),
        "source": source.get("name") or article.get("source_id"),
        "source_id": article.get("source_id"),
        "country": article.get("country") or source.get("country"),
        "language": article.get("language") or source.get("language"),
        "published_at": article.get("published_at"),
        "analysis_status": article.get("analysis_status"),
        "summary": article.get("summary") or _article_analysis(article).get("summary"),
        "key_claims": _article_claims(article, limit=4),
        "narrative_frames": _article_frames(article, limit=4),
        "tone": _article_tone(article),
        "event_fingerprint": _article_fingerprint(article),
    }


def _build_working_clusters(articles: list[dict]) -> list[dict]:
    working: list[dict] = []
    for article in sorted(articles, key=_date_key):
        terms = _article_terms(article)
        if not terms:
            continue
        best_cluster = None
        best_score = 0.0
        best_terms: list[str] = []
        for cluster in working:
            score, matched = _similarity(article, terms, cluster)
            if score > best_score:
                best_cluster = cluster
                best_score = score
                best_terms = matched

        if best_cluster and best_score >= 0.18:
            best_cluster["articles"].append(article)
            best_cluster["terms"].update(terms)
            best_cluster["matches"][article["id"]] = {
                "similarity_score": best_score,
                "matched_terms": best_terms,
            }
            fingerprint = _article_fingerprint(article)
            if fingerprint:
                best_cluster["fingerprints"].add(fingerprint)
            continue

        fingerprint = _article_fingerprint(article)
        working.append(
            {
                "articles": [article],
                "terms": set(terms),
                "fingerprints": {fingerprint} if fingerprint else set(),
                "matches": {
                    article["id"]: {
                        "similarity_score": 1.0,
                        "matched_terms": sorted(terms)[:12],
                    }
                },
            }
        )
    return working


def _finalize_cluster(working: dict) -> tuple[dict, list[dict]]:
    articles = sorted(working["articles"], key=_date_key, reverse=True)
    article_count = len(articles)
    analyzed_count = len([article for article in articles if article.get("analysis_status") == "analyzed"])
    source_ids = []
    languages = []
    countries = []
    claims = []
    frames = []
    tones = []
    topics = []
    times = []
    for article in articles:
        source = _article_source(article)
        source_id = article.get("source_id")
        if source_id and source_id not in source_ids:
            source_ids.append(source_id)
        language = article.get("language") or source.get("language")
        if language and language not in languages:
            languages.append(language)
        country = article.get("country") or source.get("country")
        if country and country not in countries:
            countries.append(country)
        claims.extend(_article_claims(article, limit=6))
        frames.extend(_article_frames(article, limit=6))
        tone = _article_tone(article)
        if tone and tone != "unknown":
            tones.append(tone)
        topics.extend(_article_topic_terms(article))
        parsed = _parse_datetime(article.get("published_at") or article.get("created_at"))
        if parsed:
            times.append(parsed)

    top_terms = [
        term
        for term, _count in Counter(working["terms"]).most_common(18)
    ]
    fingerprints = sorted([value for value in working.get("fingerprints") or [] if value])
    cluster_key = _cluster_key(top_terms, fingerprints[0] if len(fingerprints) == 1 else None)
    cluster_id = str(uuid5(NAMESPACE_URL, f"parallax:{cluster_key}"))
    primary_topic = (_top_values(topics, limit=1) or top_terms[:1] or [None])[0]
    latest_article = articles[0]
    latest_title = intelligence_provider.clean_text(latest_article.get("title"), limit=240, fallback="Untitled story")
    source_count = len(source_ids)
    language_count = len(languages)
    frame_values = _top_values(frames, limit=8)
    claim_values = _top_values(claims, limit=8)
    summary = (
        f"{article_count} article{'s' if article_count != 1 else ''} across "
        f"{source_count} source{'s' if source_count != 1 else ''}"
    )
    if language_count:
        summary += f" and {language_count} language{'s' if language_count != 1 else ''}"
    summary += f" are clustered around {primary_topic or latest_title}."

    first_seen = min(times).isoformat() if times else None
    latest_seen = max(times).isoformat() if times else None
    cluster = {
        "id": cluster_id,
        "cluster_key": cluster_key,
        "title": latest_title,
        "summary": summary,
        "status": "active",
        "primary_topic": primary_topic,
        "languages": languages,
        "countries": countries,
        "source_ids": source_ids,
        "article_count": article_count,
        "analyzed_count": analyzed_count,
        "first_seen_at": first_seen,
        "latest_seen_at": latest_seen,
        "fingerprint_terms": top_terms,
        "claims": claim_values,
        "frames": frame_values,
        "provider_metadata": intelligence_provider.provider_metadata(
            task="event_clustering",
            status="heuristic",
        ),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "sample_articles": [_article_excerpt(article) for article in articles[:8]],
        "tone_count": len(set(_normalize(tone) for tone in tones)),
    }

    memberships = []
    final_terms = set(top_terms)
    for article in articles:
        article_terms = _article_terms(article)
        match = working["matches"].get(article["id"]) or {}
        matched_terms = sorted((article_terms & final_terms) or set(match.get("matched_terms") or []))[:12]
        source = _article_source(article)
        memberships.append(
            {
                "id": str(uuid4()),
                "cluster_id": cluster_id,
                "article_id": article["id"],
                "similarity_score": max(float(match.get("similarity_score") or 0.0), 0.45 if matched_terms else 0.0),
                "matched_terms": matched_terms,
                "language": article.get("language") or source.get("language"),
                "country": article.get("country") or source.get("country"),
                "source_id": article.get("source_id"),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return cluster, memberships


def _cluster_topic_score(cluster: dict, topic_terms: set[str]) -> float:
    if not topic_terms:
        return 0.0
    cluster_terms = set(_tokens(cluster.get("title")))
    cluster_terms.update(_tokens(cluster.get("summary")))
    cluster_terms.update(_tokens(cluster.get("primary_topic")))
    cluster_terms.update(_tokens(" ".join(cluster.get("fingerprint_terms") or [])))
    cluster_terms.update(_tokens(" ".join(cluster.get("claims") or [])))
    cluster_terms.update(_tokens(" ".join(cluster.get("frames") or [])))
    if not cluster_terms:
        return 0.0
    return len(topic_terms & cluster_terms) / max(len(topic_terms), 1)


def _topic_terms(topic: dict | None) -> set[str]:
    if not topic:
        return set()
    terms = set(_tokens(topic.get("name")))
    for keyword in topic.get("keywords") or []:
        terms.update(_tokens(keyword))
    monitor = _dict(topic.get("monitor"))
    for keyword in monitor.get("keywords") or []:
        terms.update(_tokens(keyword))
    return terms


def _build_cluster_feed_cards(
    clusters: list[dict],
    *,
    session_id: str,
    run_id: str,
    card_limit: int,
) -> list[dict]:
    cards = []
    for cluster in clusters:
        if len(cards) >= card_limit:
            break
        sample_articles = cluster.get("sample_articles") or []
        base_article = sample_articles[0] if sample_articles else {}
        article_count = int(cluster.get("article_count") or 0)
        source_count = len(cluster.get("source_ids") or [])
        language_count = len(cluster.get("languages") or [])
        href = f"/compare?articleId={base_article.get('id')}" if base_article.get("id") else "/compare"
        payload = {
            "event_cluster": True,
            "event_cluster_id": cluster.get("id"),
            "event_cluster_run_id": run_id,
            "cluster_key": cluster.get("cluster_key"),
            "article_count": article_count,
            "source_count": source_count,
            "language_count": language_count,
            "languages": cluster.get("languages") or [],
            "countries": cluster.get("countries") or [],
            "source_ids": cluster.get("source_ids") or [],
            "fingerprint_terms": cluster.get("fingerprint_terms") or [],
            "claims": cluster.get("claims") or [],
            "frames": cluster.get("frames") or [],
            "sample_articles": sample_articles,
            "provider_metadata": cluster.get("provider_metadata") or {},
        }
        if article_count >= 2:
            cards.append(
                {
                    "id": str(uuid4()),
                    "session_id": session_id,
                    "ingested_article_id": base_article.get("id"),
                    "card_type": "event_cluster",
                    "title": f"Event cluster: {cluster.get('title')}",
                    "summary": cluster.get("summary"),
                    "source": None,
                    "url": base_article.get("url"),
                    "topic": cluster.get("primary_topic") or "Event cluster",
                    "priority": 0.78 if source_count > 1 else 0.68,
                    "priority_score": 0.78 if source_count > 1 else 0.68,
                    "personalized_score": 0.78 if source_count > 1 else 0.68,
                    "narrative_signal": "Similar coverage is now grouped by event, source, and language.",
                    "framing": (cluster.get("frames") or [None])[0],
                    "payload": {**payload, "signal_reason": "event_cluster"},
                    "recommendations": [
                        {
                            "type": "compare",
                            "label": "Compare cluster",
                            "href": href,
                            "reason": "Inspect title, framing, source, and timeline differences.",
                        }
                    ],
                    "explanation": {
                        "why_this_matters": "The same story appears across multiple ingested articles.",
                        "what_changed": {
                            "article_count": article_count,
                            "source_count": source_count,
                            "language_count": language_count,
                        },
                        "recommended_action": "Use compare before treating repeated coverage as evidence.",
                    },
                    "analysis": {"event_cluster": cluster},
                }
            )
        if len(cards) >= card_limit:
            break
        if article_count >= 2 and language_count >= 2:
            cards.append(
                {
                    "id": str(uuid4()),
                    "session_id": session_id,
                    "ingested_article_id": base_article.get("id"),
                    "card_type": "cross_language_cluster",
                    "title": f"Cross-language cluster: {cluster.get('title')}",
                    "summary": f"This story appears in {language_count} languages across {source_count} sources.",
                    "url": base_article.get("url"),
                    "topic": cluster.get("primary_topic") or "Cross-language coverage",
                    "priority": 0.82,
                    "priority_score": 0.82,
                    "personalized_score": 0.82,
                    "narrative_signal": "Language coverage differs; compare before inferring consensus.",
                    "framing": (cluster.get("frames") or [None])[0],
                    "payload": {**payload, "signal_reason": "cross_language_cluster"},
                    "recommendations": [
                        {
                            "type": "compare",
                            "label": "Compare languages",
                            "href": href,
                            "reason": "Check whether translated or regional coverage adds or omits claims.",
                        }
                    ],
                    "explanation": {
                        "why_this_matters": "The same event has crossed language boundaries.",
                        "what_changed": {"languages": cluster.get("languages") or []},
                        "recommended_action": "Review framing by language and source type.",
                    },
                    "analysis": {"event_cluster": cluster},
                }
            )
        if len(cards) >= card_limit:
            break
        if article_count >= 2 and source_count >= 2 and (len(cluster.get("frames") or []) > 1 or cluster.get("tone_count", 0) > 1):
            cards.append(
                {
                    "id": str(uuid4()),
                    "session_id": session_id,
                    "ingested_article_id": base_article.get("id"),
                    "card_type": "source_divergence",
                    "title": f"Source divergence: {cluster.get('title')}",
                    "summary": "Sources in this cluster show different frames or tones for the same event.",
                    "url": base_article.get("url"),
                    "topic": cluster.get("primary_topic") or "Source divergence",
                    "priority": 0.8,
                    "priority_score": 0.8,
                    "personalized_score": 0.8,
                    "narrative_signal": "Frame or tone variation is visible inside one event cluster.",
                    "framing": (cluster.get("frames") or [None])[0],
                    "payload": {**payload, "signal_reason": "source_divergence"},
                    "recommendations": [
                        {
                            "type": "compare",
                            "label": "Compare sources",
                            "href": href,
                            "reason": "Inspect source, claim, framing, and timeline differences.",
                        }
                    ],
                    "explanation": {
                        "why_this_matters": "Different outlets may be emphasizing different parts of the same story.",
                        "what_changed": {"frames": cluster.get("frames") or []},
                        "recommended_action": "Open compare and check missing or added claims.",
                    },
                    "analysis": {"event_cluster": cluster},
                }
            )
        if len(cards) >= card_limit:
            break
        if article_count == 1:
            scores = _article_scores((sample_articles or [{}])[0])
            cross_source_need = float(scores.get("cross_source_need") or 0.0)
            if cross_source_need >= 0.6:
                cards.append(
                    {
                        "id": str(uuid4()),
                        "session_id": session_id,
                        "ingested_article_id": base_article.get("id"),
                        "card_type": "missing_coverage",
                        "title": f"Missing coverage lead: {cluster.get('title')}",
                        "summary": "This analyzed story appears to need cross-source checking but has no matching cluster yet.",
                        "url": base_article.get("url"),
                        "topic": cluster.get("primary_topic") or "Coverage gap",
                        "priority": 0.66,
                        "priority_score": 0.66,
                        "personalized_score": 0.66,
                        "narrative_signal": "Cross-source need is high, but matching ingested coverage is missing.",
                        "payload": {**payload, "signal_reason": "missing_coverage"},
                        "recommendations": [
                            {
                                "type": "compare",
                                "label": "Search comparisons",
                                "href": href,
                                "reason": "Look for additional source coverage before drawing conclusions.",
                            }
                        ],
                        "analysis": {"event_cluster": cluster},
                    }
                )
    return cards[:card_limit]


def refresh_event_clusters(
    *,
    session_id: str = ANONYMOUS_SESSION_ID,
    article_limit: int = 250,
    cluster_limit: int = 100,
    card_limit: int = 50,
    create_cards: bool = True,
) -> dict:
    run_id = str(uuid4())
    started = datetime.now(timezone.utc)
    errors = []
    saved_cards = []
    article_limit = max(1, min(int(article_limit or 250), 250))
    cluster_limit = max(1, min(int(cluster_limit or 100), 250))
    card_limit = max(0, min(int(card_limit or 50), 100))

    try:
        articles = list_ingested_article_records(limit=article_limit)
        finalized_clusters = []
        finalized_memberships = []
        for working in _build_working_clusters(articles):
            cluster, memberships = _finalize_cluster(working)
            finalized_clusters.append(cluster)
            finalized_memberships.extend(memberships)
        finalized_clusters.sort(
            key=lambda item: (
                int(item.get("article_count") or 0),
                item.get("latest_seen_at") or item.get("created_at") or "",
            ),
            reverse=True,
        )
        finalized_clusters = finalized_clusters[:cluster_limit]
        allowed_cluster_ids = {cluster["id"] for cluster in finalized_clusters}
        finalized_memberships = [
            membership
            for membership in finalized_memberships
            if membership.get("cluster_id") in allowed_cluster_ids
        ]
        saved = replace_event_clusters(finalized_clusters, finalized_memberships)
        saved_clusters = saved["clusters"]
        for cluster in saved_clusters:
            original = next((item for item in finalized_clusters if item["id"] == cluster["id"]), {})
            cluster["sample_articles"] = original.get("sample_articles") or []
            cluster["tone_count"] = original.get("tone_count", 0)
        generated_cards = (
            _build_cluster_feed_cards(
                saved_clusters,
                session_id=session_id,
                run_id=run_id,
                card_limit=card_limit,
            )
            if create_cards and card_limit
            else []
        )
        saved_cards = save_generated_feed_cards(generated_cards) if generated_cards else []
    except FeedStoreError as exc:
        articles = []
        saved_clusters = []
        errors.append({"scope": "event_clusters", "error": str(exc)})

    finished = datetime.now(timezone.utc)
    status = "completed" if not errors else "partial" if saved_clusters else "failed"
    summary = {
        "cluster_count": len(saved_clusters),
        "article_count": len(articles),
        "card_count": len(saved_cards),
        "create_cards": create_cards,
    }
    run = save_event_cluster_refresh_run(
        run_id=run_id,
        session_id=session_id,
        status=status,
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        duration_ms=round((finished - started).total_seconds() * 1000),
        cluster_count=len(saved_clusters),
        article_count=len(articles),
        card_count=len(saved_cards),
        error_count=len(errors),
        limits={
            "article_limit": article_limit,
            "cluster_limit": cluster_limit,
            "card_limit": card_limit,
        },
        errors=errors,
        summary=summary,
    )
    return {
        "status": status,
        "run": run,
        "cluster_count": len(saved_clusters),
        "article_count": len(articles),
        "card_count": len(saved_cards),
        "error_count": len(errors),
        "clusters": saved_clusters,
        "cards": saved_cards,
        "errors": errors,
        "provider_metadata": intelligence_provider.provider_metadata(
            task="event_cluster_refresh",
            status="heuristic",
        ),
    }


def list_event_cluster_summaries(
    *,
    session_id: str = ANONYMOUS_SESSION_ID,
    topic_id: str | None = None,
    limit: int = 25,
) -> dict:
    limit = max(1, min(int(limit or 25), 100))
    clusters = list_event_clusters(limit=100)
    topic = None
    topic_score = None
    if topic_id:
        topic = next((item for item in list_topics(session_id=session_id) if item.get("id") == topic_id), None)
        if not topic:
            raise FeedStoreError("Topic not found.")
        terms = _topic_terms(topic)
        ranked = [(cluster, _cluster_topic_score(cluster, terms)) for cluster in clusters]
        ranked = [(cluster, score) for cluster, score in ranked if score > 0]
        ranked.sort(key=lambda item: (item[1], item[0].get("latest_seen_at") or ""), reverse=True)
        clusters = [cluster for cluster, _score in ranked]
        topic_score = {cluster["id"]: score for cluster, score in ranked}

    items = []
    for cluster in clusters[:limit]:
        memberships = list_event_cluster_articles(cluster["id"], limit=4)
        items.append(
            {
                **cluster,
                "topic_match_score": round(topic_score.get(cluster["id"], 0.0), 3) if topic_score else None,
                "sample_articles": [
                    _article_excerpt(membership.get("article") or {})
                    for membership in memberships
                    if membership.get("article")
                ],
            }
        )

    return {
        "items": items,
        "summary": {
            "cluster_count": len(items),
            "topic_id": topic_id,
            "topic_name": (topic or {}).get("name"),
        },
        "provider_metadata": intelligence_provider.provider_metadata(
            task="event_cluster_list",
            status="heuristic",
        ),
    }


def get_event_cluster_detail(cluster_id: str) -> dict:
    cluster = get_event_cluster(cluster_id)
    if not cluster:
        raise FeedStoreError("Event cluster not found.")
    memberships = list_event_cluster_articles(cluster_id, limit=100)
    articles = [
        {
            "membership": {
                key: value
                for key, value in membership.items()
                if key != "article"
            },
            "article": _article_excerpt(membership.get("article") or {}),
        }
        for membership in memberships
        if membership.get("article")
    ]
    return {
        "cluster": cluster,
        "articles": articles,
        "comparison_hooks": {
            "base_article_id": (articles[0]["article"] or {}).get("id") if articles else None,
            "compare_url": (
                f"/compare?articleId={(articles[0]['article'] or {}).get('id')}"
                if articles and (articles[0]["article"] or {}).get("id")
                else "/compare"
            ),
        },
        "limitations": [
            "Event clustering is a retrieval signal, not a factual verdict.",
            "Cross-language matching is heuristic and can miss translations, synonyms, and local naming differences.",
            "Use compare and OSINT panels before treating repeated coverage as evidence.",
        ],
        "provider_metadata": intelligence_provider.provider_metadata(
            task="event_cluster_detail",
            status="heuristic",
        ),
    }


def list_recent_event_cluster_refresh_runs(
    *,
    session_id: str | None = None,
    limit: int = 25,
) -> dict:
    runs = list_event_cluster_refresh_runs(session_id=session_id, limit=limit)
    return {
        "runs": runs,
        "summary": {
            "run_count": len(runs),
            "latest_run_id": runs[0]["id"] if runs else None,
        },
    }
