from __future__ import annotations

import re
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import httpx

from app.core.config import settings


class OSINTContextError(Exception):
    pass


class _SearchResultParser(HTMLParser):
    def __init__(self, limit: int):
        super().__init__(convert_charrefs=True)
        self.limit = limit
        self._capture_link = False
        self._current_href = ""
        self._current_text: list[str] = []
        self.results: list[dict] = []

    def handle_starttag(self, tag: str, attrs):
        if tag.lower() != "a" or len(self.results) >= self.limit:
            return
        attr_map = {key.lower(): value or "" for key, value in attrs}
        class_name = attr_map.get("class", "")
        if "result__a" not in class_name:
            return
        self._capture_link = True
        self._current_href = attr_map.get("href", "")
        self._current_text = []

    def handle_data(self, data: str):
        if self._capture_link:
            cleaned = _clean_text(data, 240)
            if cleaned:
                self._current_text.append(cleaned)

    def handle_endtag(self, tag: str):
        if tag.lower() != "a" or not self._capture_link:
            return
        title = _clean_text(" ".join(self._current_text), 240)
        url = _clean_result_url(self._current_href)
        if title and url and not any(item["url"] == url for item in self.results):
            self.results.append({"title": title, "url": url})
        self._capture_link = False
        self._current_href = ""
        self._current_text = []


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(value: object, limit: int = 300) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def _safe_list(value: object, limit: int = 12) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    seen = set()
    for item in value:
        if isinstance(item, dict):
            text = item.get("text") or item.get("claim") or item.get("label") or item.get("name")
        else:
            text = item
        text = _clean_text(text, 260)
        key = text.lower()
        if text and key not in seen:
            items.append(text)
            seen.add(key)
        if len(items) >= limit:
            break
    return items


def _domain(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    return parsed.netloc.lower().removeprefix("www.") or None


def _clean_result_url(href: str) -> str | None:
    if not href:
        return None
    if href.startswith("//"):
        href = f"https:{href}"
    parsed = urlparse(href)
    if parsed.path.startswith("/l/"):
        target = parse_qs(parsed.query).get("uddg", [None])[0]
        if target:
            return unquote(target)
    if parsed.scheme in {"http", "https"}:
        return href
    return None


def _analysis(article: dict) -> dict:
    analysis = article.get("analysis") if isinstance(article.get("analysis"), dict) else {}
    return analysis


def _intelligence(article: dict) -> dict:
    analysis = _analysis(article)
    intelligence = analysis.get("intelligence") if isinstance(analysis.get("intelligence"), dict) else {}
    return intelligence


def _source(article: dict) -> dict:
    return article.get("source") if isinstance(article.get("source"), dict) else {}


def _source_feed(article: dict) -> dict:
    return article.get("source_feed") if isinstance(article.get("source_feed"), dict) else {}


def _comparison_hooks(article: dict) -> dict:
    intelligence = _intelligence(article)
    hooks = intelligence.get("comparison_hooks") if isinstance(intelligence.get("comparison_hooks"), dict) else {}
    if hooks:
        return hooks
    return {
        "search_queries": [article.get("title") or article.get("url")],
        "similarity_keywords": article.get("comparison_keywords") or [],
        "event_fingerprint": article.get("event_fingerprint") or "",
    }


def _claims(article: dict) -> list[str]:
    intelligence = _intelligence(article)
    return _safe_list(_analysis(article).get("key_claims") or intelligence.get("key_claims") or [], limit=8)


def _entities(article: dict) -> list[str]:
    intelligence = _intelligence(article)
    entities = intelligence.get("entities") if isinstance(intelligence.get("entities"), dict) else {}
    values: list[str] = []
    for key in ("people", "organizations", "locations", "events"):
        values.extend(_safe_list(entities.get(key) or [], limit=5))
    return _safe_list(values, limit=12)


def _reference(
    title: str,
    url: str,
    source_type: str,
    reliability_level: str,
    relevance: float,
    role: str,
    risks: list[str] | None = None,
    citation_label: str | None = None,
) -> dict:
    return {
        "title": _clean_text(title, 220),
        "url": url,
        "domain": _domain(url),
        "source_type": source_type,
        "reliability_level": reliability_level,
        "relevance": round(max(0.0, min(float(relevance or 0.0), 1.0)), 3),
        "role": role,
        "risks": _safe_list(risks or [], limit=6),
        "citation_label": citation_label or _clean_text(title, 80),
    }


def _search_url(query: str) -> str:
    return f"https://duckduckgo.com/?q={quote_plus(query)}"


def _archive_url(url: str) -> str:
    return f"https://web.archive.org/web/*/{url}"


def _append_reference(references: list[dict], reference: dict | None) -> None:
    if not reference or not reference.get("url"):
        return
    if any(item.get("url") == reference.get("url") and item.get("role") == reference.get("role") for item in references):
        return
    references.append(reference)


def _search_probe_references(article: dict) -> list[dict]:
    hooks = _comparison_hooks(article)
    source = _source(article)
    title = article.get("title") or "Article"
    claims = _claims(article)
    entities = _entities(article)
    queries = _safe_list(hooks.get("search_queries") or [], limit=4)
    if title:
        queries.insert(0, title)
    if claims:
        queries.append(claims[0])
    probes: list[dict] = []
    seen = set()
    for query in queries:
        clean_query = _clean_text(query, 180)
        key = clean_query.lower()
        if not clean_query or key in seen:
            continue
        seen.add(key)
        probes.append(
            _reference(
                title=f"Public web search: {clean_query}",
                url=_search_url(clean_query),
                source_type="public_web_search",
                reliability_level="context_probe",
                relevance=0.64,
                role="query_probe",
                risks=[
                    "Search results are leads, not verification.",
                    "Ranking can reflect popularity, SEO, or personalization.",
                ],
            )
        )
    official_terms = [title, source.get("country"), *entities[:3]]
    official_query = " ".join(item for item in official_terms if item)
    if official_query:
        probes.append(
            _reference(
                title="Official statements and documents query",
                url=_search_url(f"{official_query} official statement government document"),
                source_type="official_documents",
                reliability_level="official_context_probe",
                relevance=0.56,
                role="official_context_probe",
                risks=[
                    "Official sources can be incomplete or strategically framed.",
                    "A query URL is a retrieval lead, not a discovered document.",
                ],
            )
        )
        probes.append(
            _reference(
                title="NGO and civil-society reports query",
                url=_search_url(f"{official_query} NGO report civil society analysis"),
                source_type="NGO_reports",
                reliability_level="civil_context_probe",
                relevance=0.5,
                role="ngo_context_probe",
                risks=[
                    "NGO reports vary by mandate, method, and advocacy position.",
                    "A query URL is a retrieval lead, not a discovered report.",
                ],
            )
        )
    return probes[:8]


def build_bounded_osint_context(article: dict, external_results: list[dict] | None = None) -> dict:
    source = _source(article)
    source_feed = _source_feed(article)
    hooks = _comparison_hooks(article)
    intelligence = _intelligence(article)
    narrative = intelligence.get("narrative") if isinstance(intelligence.get("narrative"), dict) else {}
    claims = _claims(article)
    references: list[dict] = []

    if article.get("url"):
        _append_reference(
            references,
            _reference(
                title=article.get("title") or "Source article",
                url=article["url"],
                source_type="source_article",
                reliability_level="source_claim",
                relevance=0.95,
                role="original_publication",
                risks=[
                    "This is the article being analyzed; it is evidence of publication, not independent verification.",
                    "Source framing and omissions must be compared with other references.",
                ],
            ),
        )
        _append_reference(
            references,
            _reference(
                title="Archived copies for source article",
                url=_archive_url(article["url"]),
                source_type="archived_pages",
                reliability_level="archival_probe",
                relevance=0.62,
                role="archive_probe",
                risks=[
                    "Archive availability is not guaranteed.",
                    "Archived pages can preserve wording, but not validate claims.",
                ],
            ),
        )

    if source.get("website_url"):
        _append_reference(
            references,
            _reference(
                title=f"{source.get('name') or 'Source'} website",
                url=source["website_url"],
                source_type="source_profile",
                reliability_level="source_metadata_context",
                relevance=0.58,
                role="source_context",
                risks=[
                    "Publisher self-description may omit ownership or editorial constraints.",
                    "Use independent source profiles when available.",
                ],
            ),
        )

    if source_feed.get("feed_url"):
        _append_reference(
            references,
            _reference(
                title=source_feed.get("title") or "Source feed",
                url=source_feed["feed_url"],
                source_type="public_feed",
                reliability_level="publication_metadata",
                relevance=0.52,
                role="feed_context",
                risks=["Feed metadata can be partial or syndicated."],
            ),
        )

    for probe in _search_probe_references(article):
        _append_reference(references, probe)

    for index, result in enumerate(external_results or []):
        _append_reference(
            references,
            _reference(
                title=result.get("title") or result.get("url") or "Public search result",
                url=result.get("url"),
                source_type="public_web_search_result",
                reliability_level="unverified_public_reference",
                relevance=max(0.25, 0.7 - index * 0.08),
                role="external_search_result",
                risks=[
                    "Public search results require source-by-source validation.",
                    "Snippet/title matching does not imply factual confirmation.",
                ],
            ),
        )

    missing_context = _safe_list(narrative.get("missing_context") or [], limit=8)
    contradictions = []
    if missing_context:
        contradictions.append(
            {
                "type": "missing_context",
                "description": "Analysis identified missing context that OSINT should investigate.",
                "items": missing_context,
            }
        )
    if not external_results:
        contradictions.append(
            {
                "type": "not_assessed",
                "description": "No external public search results were fetched for contradiction checks in this response.",
                "items": [],
            }
        )

    source_types = sorted({item["source_type"] for item in references})
    reliability_levels = sorted({item["reliability_level"] for item in references})
    citation_refs = [
        {
            "label": item["citation_label"],
            "url": item["url"],
            "source_type": item["source_type"],
            "reliability_level": item["reliability_level"],
        }
        for item in references
    ]

    relevance_basis = [
        "Original article and publisher context are high relevance for provenance.",
        "Search, official, NGO, and archive links are retrieval leads for context gathering.",
    ]
    if claims:
        relevance_basis.append(f"{len(claims)} extracted claims are available for future reference matching.")

    return {
        "article_id": article.get("id"),
        "status": "bounded_context_ready",
        "generated_at": _now_iso(),
        "retrieval_mode": {
            "provider": settings.RETRIEVAL_PROVIDER,
            "external_enabled": bool(settings.EXTERNAL_RETRIEVAL_ENABLED),
            "external_results_included": len(external_results or []),
        },
        "discovered_references": references,
        "source_type": source_types,
        "reliability_level": reliability_levels,
        "relevance": {
            "overall": round(sum(item["relevance"] for item in references) / max(len(references), 1), 3),
            "basis": relevance_basis,
        },
        "risks": [
            "OSINT provides context and retrieval leads, not final judgment.",
            "Public references can be incomplete, outdated, mirrored, partisan, or strategically framed.",
            "Leaked or document references must be public, legal, sourceable, and handled as context only.",
            "Contradictions require explicit source-by-source review before being interpreted.",
        ],
        "contradictions": contradictions,
        "citations": citation_refs,
        "claims_to_check": claims,
        "entities_to_check": _entities(article),
        "search_queries": _safe_list(hooks.get("search_queries") or [], limit=8),
        "limitations": [
            "This layer does not mark claims true or false.",
            "Query probes are included so the UI can guide retrieval without overstating evidence.",
            "External search is disabled unless explicitly enabled by backend configuration.",
        ],
    }


async def fetch_public_search_results(query: str, limit: int = 5) -> list[dict]:
    clean_query = _clean_text(query, 220)
    if not clean_query:
        return []
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=min(settings.ARTICLE_FETCH_TIMEOUT_SECONDS, 10),
            headers={"User-Agent": "ParallaxOSINTBot/0.1"},
        ) as client:
            response = await client.get(f"https://duckduckgo.com/html/?q={quote_plus(clean_query)}")
            response.raise_for_status()
    except httpx.HTTPError as exc:
        raise OSINTContextError("Public web search failed before OSINT context could be expanded.") from exc

    parser = _SearchResultParser(limit=max(1, min(limit, 10)))
    parser.feed(response.text)
    return parser.results[:limit]


async def build_article_osint_context(
    article: dict,
    include_external: bool = False,
    limit: int = 5,
) -> dict:
    external_results: list[dict] = []
    errors: list[str] = []
    hooks = _comparison_hooks(article)
    queries = _safe_list(hooks.get("search_queries") or [article.get("title")], limit=2)

    if include_external and settings.EXTERNAL_RETRIEVAL_ENABLED:
        for query in queries:
            try:
                external_results.extend(await fetch_public_search_results(query, limit=limit))
            except OSINTContextError as exc:
                errors.append(str(exc))
            if len(external_results) >= limit:
                break

    context = build_bounded_osint_context(article, external_results=external_results[:limit])
    context["retrieval_mode"]["requested_external"] = bool(include_external)
    context["retrieval_mode"]["errors"] = errors
    if include_external and not settings.EXTERNAL_RETRIEVAL_ENABLED:
        context["retrieval_mode"]["errors"].append(
            "External retrieval is disabled. Set EXTERNAL_RETRIEVAL_ENABLED=true to fetch public search results."
        )
    return context
