from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import re
import unicodedata
from typing import Any
from urllib.parse import urlencode
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


CROSS_LANGUAGE_PREFIX = "xlang_"
CROSS_LANGUAGE_ALIASES = {
    "grid": (
        "electric grid",
        "power grid",
        "red electrica",
        "\u0634\u0628\u06a9\u0647 \u0628\u0631\u0642",
        "\u0634\u0628\u0643\u0629 \u0627\u0644\u0643\u0647\u0631\u0628\u0627\u0621",
        "\u044d\u043b\u0435\u043a\u0442\u0440\u043e\u0441\u0435\u0442\u044c",
    ),
    "resilience": (
        "resiliencia",
        "resilience",
        "\u062a\u0627\u0628 \u0622\u0648\u0631\u06cc",
        "\u062a\u0627\u0628\u200c\u0622\u0648\u0631\u06cc",
        "\u0645\u0631\u0648\u0646\u0629",
        "\u0443\u0441\u0442\u043e\u0439\u0447\u0438\u0432\u043e\u0441\u0442\u044c",
    ),
    "regulator": (
        "regulator",
        "regulators",
        "regulador",
        "reguladores",
        "autorite",
        "autorites",
        "\u0646\u0647\u0627\u062f \u0646\u0627\u0638\u0631",
        "\u0646\u0647\u0627\u062f\u0647\u0627\u06cc \u0646\u0627\u0638\u0631",
        "\u0627\u0644\u062c\u0647\u0627\u062a \u0627\u0644\u062a\u0646\u0638\u064a\u0645\u064a\u0629",
    ),
    "plan": (
        "plan",
        "programa",
        "strategie",
        "strategy",
        "\u0637\u0631\u062d",
        "\u0628\u0631\u0646\u0627\u0645\u0647",
        "\u062e\u0637\u0629",
        "\u043f\u043b\u0430\u043d",
    ),
    "ceasefire": (
        "ceasefire",
        "alto el fuego",
        "alto fuego",
        "cessez le feu",
        "waffenruhe",
        "\u0622\u062a\u0634 \u0628\u0633",
        "\u0622\u062a\u0634\u200c\u0628\u0633",
        "\u0648\u0642\u0641 \u0625\u0637\u0644\u0627\u0642 \u0627\u0644\u0646\u0627\u0631",
        "\u043f\u0435\u0440\u0435\u043c\u0438\u0440\u0438\u0435",
    ),
    "sanctions": (
        "sanction",
        "sanctions",
        "sanciones",
        "\u062a\u062d\u0631\u06cc\u0645",
        "\u062a\u062d\u0631\u06cc\u0645\u200c\u0647\u0627",
        "\u0639\u0642\u0648\u0628\u0627\u062a",
        "\u0441\u0430\u043d\u043a\u0446\u0438\u0438",
    ),
    "election": (
        "election",
        "elections",
        "eleccion",
        "elecciones",
        "wahl",
        "wahlen",
        "\u0627\u0646\u062a\u062e\u0627\u0628\u0627\u062a",
        "\u0627\u0646\u062a\u062e\u0627\u0628",
        "\u0432\u044b\u0431\u043e\u0440\u044b",
    ),
    "attack": (
        "attack",
        "attacks",
        "ataque",
        "ataques",
        "attaque",
        "angriff",
        "\u062d\u0645\u0644\u0647",
        "\u0647\u062c\u0648\u0645",
        "\u0430\u0442\u0430\u043a\u0430",
    ),
    "inflation": (
        "inflation",
        "inflacion",
        "\u062a\u0648\u0631\u0645",
        "\u062a\u0636\u062e\u0645",
        "\u0438\u043d\u0444\u043b\u044f\u0446\u0438\u044f",
    ),
    "protest": (
        "protest",
        "protests",
        "protesta",
        "manifestation",
        "demonstration",
        "\u0627\u0639\u062a\u0631\u0627\u0636",
        "\u0627\u062d\u062a\u062c\u0627\u062c",
        "\u043f\u0440\u043e\u0442\u0435\u0441\u0442",
    ),
    "government": (
        "government",
        "govt",
        "gobierno",
        "gouvernement",
        "regierung",
        "\u062f\u0648\u0644\u062a",
        "\u062d\u06a9\u0648\u0645\u062a",
        "\u062d\u0643\u0648\u0645\u0629",
        "\u043f\u0440\u0430\u0432\u0438\u0442\u0435\u043b\u044c\u0441\u0442\u0432\u043e",
    ),
    "israel": ("israel", "\u0627\u0633\u0631\u0627\u0626\u06cc\u0644", "\u0625\u0633\u0631\u0627\u0626\u064a\u0644"),
    "palestine": (
        "palestine",
        "palestinian",
        "palestinians",
        "\u0641\u0644\u0633\u0637\u06cc\u0646",
        "\u0641\u0644\u0633\u0637\u064a\u0646",
    ),
    "ukraine": ("ukraine", "\u0627\u0648\u06a9\u0631\u0627\u06cc\u0646", "\u0623\u0648\u0643\u0631\u0627\u0646\u064a\u0627"),
    "russia": ("russia", "russian", "\u0631\u0648\u0633\u06cc\u0647", "\u0631\u0648\u0633\u064a\u0627"),
    "iran": ("iran", "iranian", "\u0627\u06cc\u0631\u0627\u0646", "\u0625\u064a\u0631\u0627\u0646"),
    "gaza": ("gaza", "\u063a\u0632\u0647", "\u063a\u0632\u0629"),
    "china": ("china", "chinese", "\u0686\u06cc\u0646", "\u0627\u0644\u0635\u064a\u0646"),
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


CROSS_LANGUAGE_ALIAS_LOOKUP = {
    _normalize(alias): canonical
    for canonical, aliases in CROSS_LANGUAGE_ALIASES.items()
    for alias in aliases
}


TARGET_COVERAGE_LANGUAGES = ("English", "Spanish", "Arabic", "French", "Persian")
TARGET_SOURCE_TYPES = ("news_agency", "broadcaster", "newspaper", "official", "NGO")


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


def _display_term(term: str) -> str:
    value = str(term or "")
    if value.startswith(CROSS_LANGUAGE_PREFIX):
        value = value.removeprefix(CROSS_LANGUAGE_PREFIX)
    return value.replace("_", " ")


def _semantic_terms(value: Any) -> set[str]:
    text = _normalize(value)
    raw_tokens = set(_tokens(value))
    terms = set(raw_tokens)
    if not text and not raw_tokens:
        return terms

    for alias, canonical in CROSS_LANGUAGE_ALIAS_LOOKUP.items():
        if not alias:
            continue
        if " " in alias:
            if alias in text:
                terms.add(f"{CROSS_LANGUAGE_PREFIX}{canonical}")
            continue
        if alias in raw_tokens:
            terms.add(f"{CROSS_LANGUAGE_PREFIX}{canonical}")
    return terms


def _bridge_terms(terms: set[str]) -> set[str]:
    return {term for term in terms if str(term).startswith(CROSS_LANGUAGE_PREFIX)}


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
        terms.update(_semantic_terms(value))
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


def _rank_matched_terms(terms: set[str], limit: int = 12) -> list[str]:
    bridge = sorted(_display_term(term) for term in _bridge_terms(terms))
    lexical = sorted(_display_term(term) for term in terms if term not in _bridge_terms(terms))
    ordered = []
    for value in [*bridge, *lexical]:
        if value and value not in ordered:
            ordered.append(value)
        if len(ordered) >= limit:
            break
    return ordered


def _similarity(article: dict, terms: set[str], cluster: dict) -> tuple[float, list[str], dict]:
    cluster_terms = cluster.get("terms") or set()
    article_fingerprint = _article_fingerprint(article)
    if article_fingerprint and article_fingerprint in (cluster.get("fingerprints") or set()):
        overlap = terms & cluster_terms
        matched = _rank_matched_terms(overlap or terms, limit=12)
        return 0.96, matched, {
            "match_strategy": "event_fingerprint",
            "semantic_terms": _rank_matched_terms(_bridge_terms(overlap), limit=8),
        }
    if not terms or not cluster_terms:
        return 0.0, [], {"match_strategy": "no_terms", "semantic_terms": []}
    overlap = terms & cluster_terms
    union_score = len(overlap) / max(len(terms | cluster_terms), 1)
    coverage_score = len(overlap) / max(min(len(terms), len(cluster_terms)), 1)
    bridge_overlap = _bridge_terms(overlap)
    bridge_bonus = min(len(bridge_overlap) * 0.035, 0.14)
    score = min(union_score * 0.64 + coverage_score * 0.26 + bridge_bonus, 1.0)
    return round(score, 3), _rank_matched_terms(overlap, limit=12), {
        "match_strategy": "cross_language_alias" if bridge_overlap else "lexical_overlap",
        "semantic_terms": _rank_matched_terms(bridge_overlap, limit=8),
    }


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


def _search_query(*parts: Any) -> str:
    terms = []
    for part in parts:
        if isinstance(part, list):
            terms.extend(str(item) for item in part if item)
        elif part:
            terms.append(str(part))
    return " ".join(dict.fromkeys(term.strip() for term in terms if term and term.strip()))[:240]


def _source_draft_url(draft: dict) -> str:
    params = {
        "draft_name": draft.get("name"),
        "feed_type": draft.get("feed_type"),
        "country": draft.get("country"),
        "language": draft.get("language"),
        "region": draft.get("region"),
        "source_size": draft.get("source_size"),
        "source_type": draft.get("source_type"),
        "credibility_notes": draft.get("credibility_notes"),
        "search_query": draft.get("search_query"),
    }
    return "/sources?" + urlencode({key: value for key, value in params.items() if value})


def _source_candidates_from_automation(
    *,
    title: str,
    primary_topic: str | None,
    countries: list[str],
    tasks: list[dict],
    searches: list[dict],
) -> list[dict]:
    candidates = []
    seen = set()
    topic = primary_topic or title

    def add_candidate(
        *,
        reason: str,
        search_query: str,
        language: str | None = None,
        source_type: str | None = None,
        priority: str = "medium",
    ):
        clean_source_type = source_type if source_type in TARGET_SOURCE_TYPES else None
        name_basis = language or clean_source_type or "Suggested"
        name = intelligence_provider.clean_text(f"{name_basis} source for {topic}", limit=120, fallback="Suggested source")
        key = f"{name}|{search_query}".lower()
        if key in seen:
            return
        seen.add(key)
        draft = {
            "id": hashlib.sha1(key.encode("utf-8")).hexdigest()[:16],
            "name": name,
            "website_url": None,
            "rss_url": None,
            "feed_type": "manual",
            "country": countries[0] if len(countries) == 1 else None,
            "language": language,
            "region": None,
            "source_size": "medium",
            "source_type": clean_source_type or "independent",
            "credibility_notes": intelligence_provider.clean_text(
                f"{reason} Suggested search query: {search_query}",
                limit=500,
            ),
            "search_query": search_query,
            "priority": priority,
            "status": "draft",
        }
        draft["source_manager_url"] = _source_draft_url(draft)
        candidates.append(draft)

    for task in tasks:
        query = task.get("search_query")
        if not query:
            continue
        target_languages = task.get("target_languages") if isinstance(task.get("target_languages"), list) else []
        target_source_types = task.get("target_source_types") if isinstance(task.get("target_source_types"), list) else []
        add_candidate(
            reason=task.get("reason") or "Close this coverage gap.",
            search_query=query,
            language=target_languages[0] if target_languages else None,
            source_type=target_source_types[0] if target_source_types else None,
            priority=task.get("priority") or "medium",
        )

    for search in searches:
        query = search.get("query")
        if not query:
            continue
        add_candidate(
            reason=search.get("reason") or "Find a source candidate for this cluster.",
            search_query=query,
            language=search.get("language"),
            source_type=search.get("source_type"),
            priority="medium",
        )

    return candidates[:8]


def _cluster_automation(
    *,
    title: str,
    primary_topic: str | None,
    languages: list[str],
    countries: list[str],
    source_count: int,
    article_count: int,
    analyzed_count: int,
    claims: list[str],
    frames: list[str],
    bridge_terms: list[str],
    quality_score: float,
) -> dict:
    language_lookup = {str(language).lower() for language in languages}
    missing_languages = [
        language
        for language in TARGET_COVERAGE_LANGUAGES
        if language.lower() not in language_lookup
    ][:3]
    terms = [primary_topic or title, *bridge_terms[:3], *frames[:2]]
    tasks = []
    searches = []

    def add_search(language: str | None, reason: str, source_type: str | None = None):
        query = _search_query(primary_topic or title, bridge_terms[:3], language, source_type, "news", "rss")
        searches.append(
            {
                "query": query,
                "language": language,
                "source_type": source_type,
                "reason": reason,
            }
        )

    if source_count < 3:
        tasks.append(
            {
                "type": "source_diversity_gap",
                "priority": "high" if source_count <= 1 else "medium",
                "label": "Add more independent source coverage",
                "reason": f"This cluster has {source_count} source{'s' if source_count != 1 else ''}; add at least {max(0, 3 - source_count)} more source perspectives before treating the story as broadly covered.",
                "target_source_types": list(TARGET_SOURCE_TYPES[:3]),
                "search_query": _search_query(primary_topic or title, bridge_terms[:3], "independent source coverage"),
            }
        )
        for source_type in TARGET_SOURCE_TYPES[:2]:
            add_search(None, "Find another source type for this event.", source_type)

    if missing_languages:
        tasks.append(
            {
                "type": "language_gap",
                "priority": "medium",
                "label": "Search missing language coverage",
                "reason": "The event has not been observed in several target languages yet.",
                "target_languages": missing_languages,
                "search_query": _search_query(primary_topic or title, bridge_terms[:3], missing_languages[0], "coverage"),
            }
        )
        for language in missing_languages:
            add_search(language, "Find coverage in a missing language.")

    if len(countries) < 2:
        tasks.append(
            {
                "type": "regional_gap",
                "priority": "medium",
                "label": "Add regional contrast",
                "reason": "The cluster has limited country diversity; regional outlets may frame the same event differently.",
                "target_regions": ["regional outlet", "local official source"],
                "search_query": _search_query(primary_topic or title, bridge_terms[:3], "regional coverage"),
            }
        )

    if analyzed_count < article_count:
        tasks.append(
            {
                "type": "analysis_gap",
                "priority": "high",
                "label": "Analyze remaining matched articles",
                "reason": f"{article_count - analyzed_count} matched article{'s' if article_count - analyzed_count != 1 else ''} still need structured analysis before claim and frame gaps are reliable.",
                "target_status": "pending_analysis",
                "search_query": "",
            }
        )

    if claims and article_count >= 2:
        tasks.append(
            {
                "type": "claim_verification_gap",
                "priority": "medium",
                "label": "Compare recurring claims across source types",
                "reason": "Recurring claims should be compared across source types before becoming narrative signals.",
                "claims": claims[:3],
                "search_query": _search_query(claims[:2], "official statement", "NGO report"),
            }
        )
        searches.append(
            {
                "query": _search_query(claims[:2], "official document", "statement"),
                "language": None,
                "source_type": "official",
                "reason": "Look for official-document context around recurring claims.",
            }
        )

    if quality_score < 0.55:
        tasks.append(
            {
                "type": "cluster_quality_gap",
                "priority": "medium",
                "label": "Review weak cluster evidence",
                "reason": "The cluster quality score is low enough that similarity evidence should be manually checked.",
                "quality_score": quality_score,
                "search_query": _search_query(title, bridge_terms[:3], "same event"),
            }
        )

    deduped_searches = []
    seen_queries = set()
    for search in searches:
        query = search.get("query")
        key = str(query).lower()
        if not query or key in seen_queries:
            continue
        seen_queries.add(key)
        deduped_searches.append(search)
        if len(deduped_searches) >= 8:
            break

    priority_order = {"high": 0, "medium": 1, "low": 2}
    tasks.sort(key=lambda task: priority_order.get(task.get("priority"), 3))
    tasks = tasks[:8]
    automation_score = round(
        min(
            len([task for task in tasks if task.get("priority") == "high"]) * 0.28
            + len(tasks) * 0.08
            + (1 - min(quality_score, 1.0)) * 0.24,
            1.0,
        ),
        3,
    )
    source_candidates = _source_candidates_from_automation(
        title=title,
        primary_topic=primary_topic,
        countries=countries,
        tasks=tasks,
        searches=deduped_searches,
    )
    return {
        "coverage_gap_tasks": tasks,
        "suggested_source_searches": deduped_searches,
        "source_candidates": source_candidates,
        "recommended_actions": [
            {
                "type": task.get("type"),
                "label": task.get("label"),
                "priority": task.get("priority"),
                "reason": task.get("reason"),
                "search_query": task.get("search_query"),
                "source_manager_url": (source_candidates[0] or {}).get("source_manager_url") if source_candidates else None,
            }
            for task in tasks[:5]
        ],
        "automation_score": automation_score,
        "terms": list(dict.fromkeys(str(term) for term in terms if term))[:8],
        "limitations": [
            "Automation tasks identify missing context to collect; they are not editorial judgments.",
            "Suggested searches are prompts for source discovery and should be reviewed before adding sources.",
        ],
    }


def _cluster_provider_metadata(
    *,
    article_count: int,
    analyzed_count: int,
    source_count: int,
    language_count: int,
    country_count: int,
    title: str,
    primary_topic: str | None,
    languages: list[str],
    countries: list[str],
    claims: list[str],
    frames: list[str],
    matches: dict,
) -> dict:
    match_scores = [
        float(match.get("similarity_score") or 0.0)
        for match in matches.values()
    ]
    average_similarity = round(sum(match_scores) / max(len(match_scores), 1), 3)
    analysis_ratio = round(analyzed_count / max(article_count, 1), 3)
    source_factor = min(source_count / 3, 1.0)
    language_factor = min(language_count / 2, 1.0)
    quality_score = round(
        min(
            average_similarity * 0.42
            + analysis_ratio * 0.24
            + source_factor * 0.22
            + language_factor * 0.12,
            1.0,
        ),
        3,
    )
    strategies = Counter(
        match.get("match_strategy") or "unknown"
        for match in matches.values()
    )
    bridge_terms = []
    for match in matches.values():
        bridge_terms.extend(match.get("semantic_terms") or [])
    bridge_term_values = _top_values(bridge_terms, limit=10)

    metadata = intelligence_provider.provider_metadata(
        task="event_clustering",
        status="heuristic",
    )
    metadata["cluster_quality"] = {
        "quality_score": quality_score,
        "average_similarity": average_similarity,
        "analysis_ratio": analysis_ratio,
        "match_count": len(matches),
        "strategy_counts": dict(strategies),
    }
    metadata["source_diversity"] = {
        "source_count": source_count,
        "language_count": language_count,
        "country_count": country_count,
        "cross_language": language_count >= 2,
        "cross_source": source_count >= 2,
    }
    metadata["language_bridge_terms"] = bridge_term_values
    metadata["automation"] = _cluster_automation(
        title=title,
        primary_topic=primary_topic,
        languages=languages,
        countries=countries,
        source_count=source_count,
        article_count=article_count,
        analyzed_count=analyzed_count,
        claims=claims,
        frames=frames,
        bridge_terms=bridge_term_values,
        quality_score=quality_score,
    )
    metadata["limitations"] = [
        "Cross-language clustering uses bounded alias matching and article metadata; it is retrieval context, not translation.",
        "High cluster quality means stronger retrieval evidence, not agreement or factual confirmation.",
    ]
    return metadata


def _build_working_clusters(articles: list[dict]) -> list[dict]:
    working: list[dict] = []
    for article in sorted(articles, key=_date_key):
        terms = _article_terms(article)
        if not terms:
            continue
        best_cluster = None
        best_score = 0.0
        best_terms: list[str] = []
        best_evidence: dict = {}
        for cluster in working:
            score, matched, evidence = _similarity(article, terms, cluster)
            if score > best_score:
                best_cluster = cluster
                best_score = score
                best_terms = matched
                best_evidence = evidence

        if best_cluster and best_score >= 0.18:
            best_cluster["articles"].append(article)
            best_cluster["terms"].update(terms)
            best_cluster["matches"][article["id"]] = {
                "similarity_score": best_score,
                "matched_terms": best_terms,
                **best_evidence,
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
                        "matched_terms": _rank_matched_terms(terms, limit=12),
                        "match_strategy": "seed",
                        "semantic_terms": _rank_matched_terms(_bridge_terms(terms), limit=8),
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
    display_top_terms = list(dict.fromkeys(_display_term(term) for term in top_terms if term))[:18]
    fingerprints = sorted([value for value in working.get("fingerprints") or [] if value])
    cluster_key = _cluster_key(top_terms, fingerprints[0] if len(fingerprints) == 1 else None)
    cluster_id = str(uuid5(NAMESPACE_URL, f"parallax:{cluster_key}"))
    primary_topic = (_top_values(topics, limit=1) or top_terms[:1] or [None])[0]
    latest_article = articles[0]
    latest_title = intelligence_provider.clean_text(latest_article.get("title"), limit=240, fallback="Untitled story")
    source_count = len(source_ids)
    language_count = len(languages)
    country_count = len(countries)
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
    provider_metadata = _cluster_provider_metadata(
        article_count=article_count,
        analyzed_count=analyzed_count,
        source_count=source_count,
        language_count=language_count,
        country_count=country_count,
        title=latest_title,
        primary_topic=primary_topic,
        languages=languages,
        countries=countries,
        claims=claim_values,
        frames=frame_values,
        matches=working.get("matches") or {},
    )
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
        "fingerprint_terms": display_top_terms,
        "claims": claim_values,
        "frames": frame_values,
        "provider_metadata": provider_metadata,
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
        term_overlap = article_terms & final_terms
        matched_terms = _rank_matched_terms(term_overlap, limit=12) if term_overlap else (match.get("matched_terms") or [])[:12]
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
        provider_metadata = cluster.get("provider_metadata") or {}
        cluster_quality = provider_metadata.get("cluster_quality") or {}
        source_diversity = provider_metadata.get("source_diversity") or {}
        automation = provider_metadata.get("automation") or {}
        coverage_gap_tasks = automation.get("coverage_gap_tasks") or []
        suggested_source_searches = automation.get("suggested_source_searches") or []
        source_candidates = automation.get("source_candidates") or []
        recommended_actions = automation.get("recommended_actions") or []
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
            "provider_metadata": provider_metadata,
            "cluster_quality": cluster_quality,
            "source_diversity": source_diversity,
            "language_bridge_terms": provider_metadata.get("language_bridge_terms") or [],
            "automation": automation,
            "coverage_gap_tasks": coverage_gap_tasks,
            "suggested_source_searches": suggested_source_searches,
            "source_candidates": source_candidates,
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
                        },
                        *[
                            {
                                "type": "coverage_task",
                                "label": action.get("label"),
                                "href": "/sources",
                                "reason": action.get("reason"),
                                "search_query": action.get("search_query"),
                            }
                            for action in recommended_actions[:2]
                        ],
                    ],
                    "explanation": {
                        "why_this_matters": "The same story appears across multiple ingested articles.",
                        "what_changed": {
                            "article_count": article_count,
                            "source_count": source_count,
                            "language_count": language_count,
                            "quality_score": cluster_quality.get("quality_score"),
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
                        },
                        *[
                            {
                                "type": "source_search",
                                "label": f"Search {search.get('language') or search.get('source_type') or 'source'}",
                                "href": "/sources",
                                "reason": search.get("reason"),
                                "search_query": search.get("query"),
                            }
                            for search in suggested_source_searches[:2]
                        ],
                    ],
                    "explanation": {
                        "why_this_matters": "The same event has crossed language boundaries.",
                        "what_changed": {
                            "languages": cluster.get("languages") or [],
                            "language_bridge_terms": provider_metadata.get("language_bridge_terms") or [],
                        },
                        "recommended_action": "Review framing by language and source type.",
                    },
                    "analysis": {"event_cluster": cluster},
                }
            )
        if len(cards) >= card_limit:
            break
        if coverage_gap_tasks:
            first_task = coverage_gap_tasks[0]
            cards.append(
                {
                    "id": str(uuid4()),
                    "session_id": session_id,
                    "ingested_article_id": base_article.get("id"),
                    "card_type": "coverage_gap",
                    "title": f"Coverage tasks: {cluster.get('title')}",
                    "summary": first_task.get("reason") or "This cluster has source, language, analysis, or claim coverage gaps to close.",
                    "url": base_article.get("url"),
                    "topic": cluster.get("primary_topic") or "Coverage gap",
                    "priority": 0.84 if first_task.get("priority") == "high" else 0.72,
                    "priority_score": 0.84 if first_task.get("priority") == "high" else 0.72,
                    "personalized_score": 0.84 if first_task.get("priority") == "high" else 0.72,
                    "narrative_signal": "The cluster has concrete follow-up tasks before coverage can be treated as robust.",
                    "framing": (cluster.get("frames") or [None])[0],
                    "payload": {**payload, "signal_reason": "coverage_gap_tasks"},
                    "recommendations": [
                        {
                            "type": "compare",
                            "label": "Review compare",
                            "href": href,
                            "reason": "Inspect the current source and claim differences before adding more coverage.",
                        },
                        *[
                            {
                                "type": "coverage_task",
                                "label": task.get("label"),
                                "href": "/sources",
                                "reason": task.get("reason"),
                                "search_query": task.get("search_query"),
                            }
                            for task in coverage_gap_tasks[:3]
                        ],
                    ],
                    "explanation": {
                        "why_this_matters": "The system found specific missing coverage needed to make this event cluster more operational.",
                        "what_changed": {
                            "task_count": len(coverage_gap_tasks),
                            "source_search_count": len(suggested_source_searches),
                            "source_candidate_count": len(source_candidates),
                            "automation_score": automation.get("automation_score"),
                        },
                        "recommended_action": "Use the suggested searches to add or sync missing source perspectives.",
                    },
                    "analysis": {"event_cluster": cluster, "automation": automation},
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
    quality_scores = [
        ((cluster.get("provider_metadata") or {}).get("cluster_quality") or {}).get("quality_score")
        for cluster in saved_clusters
    ]
    quality_scores = [float(score) for score in quality_scores if score is not None]
    automation_payloads = [
        ((cluster.get("provider_metadata") or {}).get("automation") or {})
        for cluster in saved_clusters
    ]
    cross_language_count = len(
        [
            cluster
            for cluster in saved_clusters
            if (((cluster.get("provider_metadata") or {}).get("source_diversity") or {}).get("cross_language"))
        ]
    )
    summary = {
        "cluster_count": len(saved_clusters),
        "article_count": len(articles),
        "card_count": len(saved_cards),
        "create_cards": create_cards,
        "cross_language_cluster_count": cross_language_count,
        "average_cluster_quality": round(sum(quality_scores) / max(len(quality_scores), 1), 3),
        "coverage_gap_task_count": sum(len(item.get("coverage_gap_tasks") or []) for item in automation_payloads),
        "suggested_source_search_count": sum(len(item.get("suggested_source_searches") or []) for item in automation_payloads),
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
    metadata = cluster.get("provider_metadata") or {}
    automation = metadata.get("automation") or {}
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
        "automation": automation,
        "coverage_gap_tasks": automation.get("coverage_gap_tasks") or [],
        "suggested_source_searches": automation.get("suggested_source_searches") or [],
        "source_candidates": automation.get("source_candidates") or [],
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


def build_event_cluster_source_drafts(cluster_id: str, limit: int = 8) -> dict:
    cluster = get_event_cluster(cluster_id)
    if not cluster:
        raise FeedStoreError("Event cluster not found.")
    metadata = cluster.get("provider_metadata") or {}
    automation = metadata.get("automation") or {}
    candidates = automation.get("source_candidates") or []
    limit = max(1, min(int(limit or 8), 25))
    return {
        "cluster": {
            "id": cluster.get("id"),
            "title": cluster.get("title"),
            "primary_topic": cluster.get("primary_topic"),
            "article_count": cluster.get("article_count"),
            "languages": cluster.get("languages") or [],
            "countries": cluster.get("countries") or [],
        },
        "source_candidates": candidates[:limit],
        "coverage_gap_tasks": (automation.get("coverage_gap_tasks") or [])[:limit],
        "suggested_source_searches": (automation.get("suggested_source_searches") or [])[:limit],
        "summary": {
            "candidate_count": min(len(candidates), limit),
            "task_count": len(automation.get("coverage_gap_tasks") or []),
            "search_count": len(automation.get("suggested_source_searches") or []),
        },
        "limitations": [
            "Source candidates are drafts generated from coverage gaps; review names, URLs, and credibility notes before creating a source.",
            "Suggested searches are discovery prompts, not source endorsements.",
        ],
        "provider_metadata": intelligence_provider.provider_metadata(
            task="event_cluster_source_drafts",
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
