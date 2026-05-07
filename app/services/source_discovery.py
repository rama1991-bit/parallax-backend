from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import re
from urllib.parse import urlparse, urlunparse

from app.core.config import settings
from app.services.default_sources import DEFAULT_NEWS_SOURCES
from app.services.feed.store import list_source_records
from app.services.homepage import HomepageSyncError, parse_homepage_feed
from app.services.intelligence import provider_metadata
from app.services.osint import OSINTContextError, fetch_public_search_results
from app.services.rss import RSSSyncError, parse_rss_feed


STOPWORDS = {
    "about",
    "after",
    "also",
    "coverage",
    "find",
    "from",
    "news",
    "official",
    "source",
    "sources",
    "the",
    "this",
    "with",
}


def _clean_text(value: object, limit: int = 240) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def _tokens(value: object) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-zA-Z0-9]+", str(value or "").lower())
        if len(token) > 2 and token not in STOPWORDS
    }


def _origin(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))


def _domain(url: str | None) -> str | None:
    if not url:
        return None
    return urlparse(url).netloc.lower().removeprefix("www.") or None


def _source_name_from_url(url: str | None) -> str:
    domain = _domain(url) or "Discovered source"
    core = domain.split(".")[0]
    return _clean_text(core.replace("-", " ").title(), 120) or "Discovered source"


def _rss_guesses(website_url: str | None) -> list[str]:
    origin = _origin(website_url)
    if not origin:
        return []
    return [f"{origin}/feed", f"{origin}/rss", f"{origin}/rss.xml", f"{origin}/feed.xml"]


def _existing_source_lookup() -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    try:
        for source in list_source_records(limit=250):
            for url in (source.get("website_url"), source.get("rss_url")):
                domain = _domain(url)
                if domain and domain not in lookup:
                    lookup[domain] = source
    except Exception:
        return {}
    return lookup


def _source_payload(candidate: dict) -> dict:
    return {
        "name": candidate.get("name"),
        "website_url": candidate.get("website_url"),
        "rss_url": candidate.get("rss_url"),
        "country": candidate.get("country"),
        "language": candidate.get("language"),
        "region": candidate.get("region"),
        "source_size": candidate.get("source_size"),
        "source_type": candidate.get("source_type"),
        "feed_type": candidate.get("feed_type"),
        "credibility_notes": candidate.get("credibility_notes"),
    }


def _with_source_payload(candidate: dict) -> dict:
    candidate["create_payload"] = _source_payload(candidate)
    return candidate


def _candidate_id(*parts: object) -> str:
    key = "|".join(_clean_text(part, 500).lower() for part in parts if part)
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def _score_default_source(source: dict, query_terms: set[str], hints: dict) -> float:
    searchable = " ".join(
        str(source.get(key) or "")
        for key in (
            "name",
            "country",
            "language",
            "region",
            "source_size",
            "source_type",
            "political_context",
            "notes",
        )
    )
    source_terms = _tokens(searchable)
    overlap = len(query_terms & source_terms)
    score = min(0.4 + overlap * 0.08, 0.85)
    if hints.get("language") and str(source.get("language") or "").lower() == str(hints["language"]).lower():
        score += 0.12
    if hints.get("country") and str(source.get("country") or "").lower() == str(hints["country"]).lower():
        score += 0.1
    if hints.get("source_type") and str(source.get("source_type") or "").lower() == str(hints["source_type"]).lower():
        score += 0.1
    if source.get("rss_url"):
        score += 0.04
    return round(min(score, 0.98), 3)


def _default_source_candidates(query: str, hints: dict, limit: int, existing_by_domain: dict[str, dict]) -> list[dict]:
    query_terms = _tokens(query)
    ranked = [
        (_score_default_source(source, query_terms, hints), source)
        for source in DEFAULT_NEWS_SOURCES
    ]
    ranked.sort(key=lambda item: item[0], reverse=True)

    candidates = []
    for score, source in ranked:
        if score < 0.42 and len(candidates) >= 3:
            continue
        website_url = source.get("website_url")
        rss_url = source.get("rss_url")
        existing = existing_by_domain.get(_domain(website_url)) or existing_by_domain.get(_domain(rss_url))
        candidate = {
            "id": _candidate_id("default", source.get("name"), website_url, rss_url),
            "name": source.get("name"),
            "website_url": website_url,
            "rss_url": rss_url,
            "rss_url_candidates": [rss_url] if rss_url else _rss_guesses(website_url),
            "feed_type": "rss" if rss_url else "homepage",
            "country": source.get("country"),
            "language": source.get("language"),
            "region": source.get("region"),
            "source_size": source.get("source_size"),
            "source_type": source.get("source_type"),
            "credibility_notes": _clean_text(
                f"Discovered from Parallax default source database. {source.get('notes') or ''}",
                700,
            ),
            "discovery_method": "default_source_database_match",
            "provider": "default_source_database",
            "confidence": score,
            "relevance": score,
            "status": "candidate",
            "existing_source_id": (existing or {}).get("id"),
            "risks": [
                "Default source metadata can become stale; verify URL, RSS availability, and terms before scaled ingestion.",
                "A matched source is a discovery candidate, not an endorsement.",
            ],
        }
        candidates.append(_with_source_payload(candidate))
        if len(candidates) >= limit:
            break
    return candidates


def _search_result_candidates(results: list[dict], hints: dict, limit: int, existing_by_domain: dict[str, dict]) -> list[dict]:
    candidates = []
    seen_domains = set()
    blocked_domains = {"duckduckgo.com", "google.com", "bing.com", "search.yahoo.com"}
    for index, result in enumerate(results):
        url = result.get("url")
        domain = _domain(url)
        if not domain or domain in blocked_domains or domain in seen_domains:
            continue
        seen_domains.add(domain)
        website_url = _origin(url)
        if not website_url:
            continue
        existing = existing_by_domain.get(domain)
        candidate = {
            "id": _candidate_id("public_web", domain, result.get("title")),
            "name": _clean_text(result.get("title"), 120) or _source_name_from_url(website_url),
            "website_url": website_url,
            "rss_url": None,
            "rss_url_candidates": _rss_guesses(website_url),
            "feed_type": "homepage",
            "country": hints.get("country"),
            "language": hints.get("language"),
            "region": hints.get("region"),
            "source_size": "medium",
            "source_type": hints.get("source_type") or "independent",
            "credibility_notes": _clean_text(
                f"Discovered from public search result for source discovery query. Result title: {result.get('title') or domain}",
                700,
            ),
            "discovery_method": "public_web_search_result",
            "provider": result.get("provider") or settings.RETRIEVAL_PROVIDER,
            "confidence": round(max(0.45, 0.72 - index * 0.06), 3),
            "relevance": round(max(0.45, 0.72 - index * 0.06), 3),
            "status": "candidate",
            "existing_source_id": (existing or {}).get("id"),
            "evidence_url": url,
            "risks": [
                "Public search results require manual source review.",
                "A domain surfaced by search may be an article page, mirror, or SEO result rather than a stable source homepage.",
            ],
        }
        candidates.append(_with_source_payload(candidate))
        if len(candidates) >= limit:
            break
    return candidates


def _dedupe_candidates(candidates: list[dict], limit: int) -> list[dict]:
    deduped = []
    seen = set()
    for candidate in sorted(candidates, key=lambda item: item.get("confidence") or 0, reverse=True):
        key = _domain(candidate.get("website_url")) or _domain(candidate.get("rss_url")) or candidate.get("name")
        if not key or key in seen:
            continue
        seen.add(key)
        candidate["rank"] = len(deduped) + 1
        deduped.append(candidate)
        if len(deduped) >= limit:
            break
    return deduped


async def discover_source_candidates(
    *,
    query: str,
    cluster_id: str | None = None,
    candidate_id: str | None = None,
    country: str | None = None,
    language: str | None = None,
    region: str | None = None,
    source_type: str | None = None,
    include_external: bool = False,
    limit: int = 8,
) -> dict:
    clean_query = _clean_text(query, 240)
    limit = max(1, min(int(limit or 8), 20))
    hints = {
        "country": _clean_text(country, 80) or None,
        "language": _clean_text(language, 80) or None,
        "region": _clean_text(region, 80) or None,
        "source_type": _clean_text(source_type, 40) or None,
    }
    errors: list[str] = []
    external_results: list[dict] = []
    existing_by_domain = _existing_source_lookup()

    if include_external:
        if settings.EXTERNAL_RETRIEVAL_ENABLED:
            try:
                external_results = await fetch_public_search_results(clean_query, limit=limit)
            except OSINTContextError as exc:
                errors.append(str(exc))
        else:
            errors.append(
                "External source discovery is disabled. Set EXTERNAL_RETRIEVAL_ENABLED=true with RETRIEVAL_PROVIDER=web to fetch public search results."
            )

    default_candidates = _default_source_candidates(clean_query, hints, limit=limit, existing_by_domain=existing_by_domain)
    web_candidates = _search_result_candidates(external_results, hints, limit=limit, existing_by_domain=existing_by_domain)
    candidates = _dedupe_candidates([*web_candidates, *default_candidates], limit=limit)

    return {
        "query": clean_query,
        "cluster_id": cluster_id,
        "candidate_id": candidate_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candidates": candidates,
        "external_results": external_results,
        "summary": {
            "candidate_count": len(candidates),
            "default_candidate_count": len(default_candidates),
            "web_candidate_count": len(web_candidates),
            "existing_source_match_count": len([item for item in candidates if item.get("existing_source_id")]),
        },
        "retrieval_mode": {
            "provider": settings.RETRIEVAL_PROVIDER,
            "external_requested": include_external,
            "external_enabled": bool(settings.EXTERNAL_RETRIEVAL_ENABLED),
            "errors": errors,
        },
        "limitations": [
            "Source discovery returns candidate sources, not source credibility judgments.",
            "Review ownership, editorial profile, RSS terms, and ingestion suitability before production-scale use.",
            "Public web search is used only when external retrieval is explicitly enabled.",
        ],
        "provider_metadata": provider_metadata(task="source_discovery", status="heuristic"),
    }


def _candidate_url_list(candidate: dict, key: str) -> list[str]:
    values = []
    value = candidate.get(key)
    if isinstance(value, str) and value.strip():
        values.append(value.strip())
    if key == "rss_url":
        for item in candidate.get("rss_url_candidates") or []:
            if isinstance(item, str) and item.strip():
                values.append(item.strip())
    deduped = []
    seen = set()
    for url in values:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped[:6]


def _validation_attempt(feed_type: str, url: str, status: str, parsed: dict | None = None, error: str | None = None) -> dict:
    parsed = parsed or {}
    return {
        "feed_type": feed_type,
        "url": url,
        "status": status,
        "title": parsed.get("title"),
        "item_count": len(parsed.get("items") or []),
        "error": error,
    }


def _candidate_with_validation(candidate: dict, validation: dict) -> dict:
    enriched = {
        **candidate,
        "validation_status": validation.get("status"),
        "validation": validation,
    }
    if validation.get("selected_feed_type"):
        enriched["feed_type"] = validation["selected_feed_type"]
    if validation.get("selected_feed_type") == "rss":
        enriched["rss_url"] = validation.get("selected_feed_url") or enriched.get("rss_url")
    if validation.get("selected_feed_type") == "homepage":
        enriched["website_url"] = validation.get("selected_feed_url") or enriched.get("website_url")
    if validation.get("title") and not enriched.get("name"):
        enriched["name"] = validation["title"]
    return _with_source_payload(enriched)


async def validate_source_candidate(
    *,
    candidate: dict,
    allow_homepage_fallback: bool = True,
    limit: int = 5,
) -> dict:
    limit = max(1, min(int(limit or 5), 10))
    candidate = dict(candidate or {})
    attempts = []

    for rss_url in _candidate_url_list(candidate, "rss_url"):
        try:
            parsed = await parse_rss_feed(rss_url, limit=limit)
            attempts.append(_validation_attempt("rss", rss_url, "validated", parsed=parsed))
            if parsed.get("items"):
                validation = {
                    "status": "validated",
                    "selected_feed_type": "rss",
                    "selected_feed_url": parsed.get("url") or rss_url,
                    "title": parsed.get("title"),
                    "description": parsed.get("description"),
                    "item_count": len(parsed.get("items") or []),
                    "attempts": attempts,
                    "risks": [
                        "Validation confirms the feed is readable now; it does not validate editorial credibility or future availability.",
                    ],
                }
                return {
                    "candidate": _candidate_with_validation(candidate, validation),
                    "validation": validation,
                    "provider_metadata": provider_metadata(task="source_candidate_validation", status="heuristic"),
                }
        except RSSSyncError as exc:
            attempts.append(_validation_attempt("rss", rss_url, "failed", error=str(exc)))

    website_urls = _candidate_url_list(candidate, "website_url")
    if allow_homepage_fallback:
        for website_url in website_urls:
            try:
                parsed = await parse_homepage_feed(website_url, limit=limit)
                status = "validated" if parsed.get("items") else "needs_review"
                attempts.append(_validation_attempt("homepage", website_url, status, parsed=parsed))
                validation = {
                    "status": status,
                    "selected_feed_type": "homepage",
                    "selected_feed_url": parsed.get("url") or website_url,
                    "title": parsed.get("title"),
                    "description": parsed.get("description"),
                    "item_count": len(parsed.get("items") or []),
                    "attempts": attempts,
                    "risks": [
                        "Homepage validation is heuristic; article links can be sparse, blocked, or layout-dependent.",
                        "Validation does not assess editorial reliability.",
                    ],
                }
                return {
                    "candidate": _candidate_with_validation(candidate, validation),
                    "validation": validation,
                    "provider_metadata": provider_metadata(task="source_candidate_validation", status="heuristic"),
                }
            except HomepageSyncError as exc:
                attempts.append(_validation_attempt("homepage", website_url, "failed", error=str(exc)))

    status = "needs_review" if not attempts and candidate.get("feed_type") == "manual" else "failed"
    validation = {
        "status": status,
        "selected_feed_type": "manual" if status == "needs_review" else None,
        "selected_feed_url": None,
        "title": candidate.get("name"),
        "description": None,
        "item_count": 0,
        "attempts": attempts,
        "risks": [
            "No readable RSS or homepage feed was validated.",
            "Use admin override only after manual source review.",
        ],
    }
    return {
        "candidate": _candidate_with_validation(candidate, validation),
        "validation": validation,
        "provider_metadata": provider_metadata(task="source_candidate_validation", status="heuristic"),
    }
