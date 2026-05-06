from __future__ import annotations

import re
from urllib.parse import urlparse

from app.core.session import ANONYMOUS_SESSION_ID
from app.services.analysis import AIAnalysisError, analyze_article as run_article_analysis
from app.services.articles import ArticleFetchError, ExtractedArticle, fetch_article
from app.services.feed.store import (
    FeedStoreError,
    get_ingested_article_record,
    list_ingested_article_records,
    mark_ingested_article_analysis_failed,
    save_analysis_card,
    save_ingested_article_analysis_card,
)
from app.services.intelligence import enhance_article_intelligence


def _clamp(value: int | None, default: int, minimum: int, maximum: int) -> int:
    try:
        raw = int(value if value is not None else default)
    except (TypeError, ValueError):
        raw = default
    return max(minimum, min(raw, maximum))


def _normalize_space(value: object, fallback: str = "") -> str:
    if value is None:
        return fallback
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned or fallback


def _domain_from_url(url: str | None) -> str:
    if not url:
        return "unknown-source"
    return urlparse(url).netloc.lower().removeprefix("www.") or "unknown-source"


def _metadata_article_from_ingested(record: dict, fetch_error: str | None = None) -> ExtractedArticle:
    url = record.get("canonical_url") or record.get("url") or ""
    source = record.get("source") or {}
    source_name = source.get("name") if isinstance(source, dict) else None
    title = _normalize_space(record.get("title"), _domain_from_url(url))
    summary = _normalize_space(record.get("summary"))
    raw_metadata = record.get("raw_metadata") or {}
    raw_title = _normalize_space(raw_metadata.get("title"))
    raw_summary = _normalize_space(raw_metadata.get("summary") or raw_metadata.get("description"))
    text_parts = [title, summary, raw_title, raw_summary]
    text = "\n\n".join(part for part in text_parts if part)
    if len(text) < 120:
        text = (text + "\n\n" + "This analysis is based on source metadata because full article text was unavailable. ")[:300]
    domain = _domain_from_url(url)
    return ExtractedArticle(
        url=url,
        final_url=url,
        title=title,
        source=_normalize_space(source_name, domain),
        domain=domain,
        text=text[:12000],
        excerpt=text[:1200],
    )


async def article_from_ingested(record: dict) -> tuple[ExtractedArticle, str | None]:
    url = record.get("canonical_url") or record.get("url")
    if not url:
        return _metadata_article_from_ingested(record, fetch_error="Ingested article has no URL."), "Ingested article has no URL."

    try:
        return await fetch_article(url), None
    except ArticleFetchError as exc:
        return _metadata_article_from_ingested(record, fetch_error=str(exc)), str(exc)


def _entity_groups(entities: list[str]) -> dict:
    people = []
    organizations = []
    locations = []
    events = []
    org_markers = ("agency", "bank", "council", "court", "department", "ministry", "party", "university")
    event_markers = ("summit", "election", "war", "strike", "trial", "crisis", "plan", "deal")
    location_markers = ("city", "province", "state", "region", "border")

    for entity in entities:
        lowered = entity.lower()
        if any(marker in lowered for marker in org_markers):
            organizations.append(entity)
        elif any(marker in lowered for marker in event_markers):
            events.append(entity)
        elif any(marker in lowered for marker in location_markers):
            locations.append(entity)
        else:
            people.append(entity)

    return {
        "people": people[:8],
        "organizations": organizations[:8],
        "locations": locations[:8],
        "events": events[:8],
    }


def build_structured_intelligence(
    article: ExtractedArticle,
    analysis: dict,
    ingested_article: dict | None = None,
    fetch_warning: str | None = None,
) -> dict:
    frames = analysis.get("narrative_framing") or []
    claims = analysis.get("key_claims") or []
    topics = analysis.get("topics") or []
    confidence = float(analysis.get("confidence") or 0.5)
    source = (ingested_article or {}).get("source") or {}
    if not isinstance(source, dict):
        source = {}

    missing_context = []
    if fetch_warning:
        missing_context.append(f"Full article extraction warning: {fetch_warning}")
    if len(claims) < 3:
        missing_context.append("Few explicit claims were available in the extracted text or feed metadata.")

    return {
        "article": {
            "title": article.title,
            "url": article.final_url,
            "source": article.source,
            "author": (ingested_article or {}).get("author"),
            "published_at": (ingested_article or {}).get("published_at"),
            "language": (ingested_article or {}).get("language") or source.get("language"),
            "country": (ingested_article or {}).get("country") or source.get("country"),
        },
        "summary": analysis.get("summary") or article.excerpt,
        "key_claims": claims,
        "entities": _entity_groups(analysis.get("entities") or []),
        "narrative": {
            "main_frame": frames[0] if frames else "general_news_frame",
            "secondary_frames": frames[1:],
            "tone": "measured" if confidence >= 0.65 else "uncertain",
            "implied_causality": claims[0] if claims else "",
            "missing_context": missing_context,
        },
        "source_analysis": {
            "source_profile": source.get("name") or article.source,
            "known_angle": source.get("source_type") or "unknown",
            "reliability_notes": source.get("credibility_notes") or "No credibility profile has been added yet.",
            "limitations": [
                "Source metadata is descriptive context, not a truth verdict.",
                "Analysis confidence reflects extraction and structure quality, not factual certainty.",
            ],
        },
        "comparison_hooks": {
            "search_queries": [
                " ".join([article.title, source.get("name") or article.source]).strip(),
                " ".join((claims[:1] or [article.title])).strip(),
            ],
            "similarity_keywords": (ingested_article or {}).get("comparison_keywords") or topics[:8],
            "event_fingerprint": (ingested_article or {}).get("event_fingerprint") or "",
        },
        "scores": {
            "importance_score": round(min(1.0, 0.35 + len(claims) * 0.07 + confidence * 0.25), 3),
            "confidence_score": round(confidence, 3),
            "controversy_score": round(min(1.0, len(frames) * 0.16 + len(claims) * 0.04), 3),
            "cross_source_need": round(0.7 if len(claims) >= 3 else 0.45, 3),
        },
    }


async def analyze_extracted_article(
    article: ExtractedArticle,
    *,
    session_id: str = ANONYMOUS_SESSION_ID,
    ingested_article: dict | None = None,
    fetch_warning: str | None = None,
) -> dict:
    analysis = await run_article_analysis(article)
    intelligence = build_structured_intelligence(
        article,
        analysis,
        ingested_article=ingested_article,
        fetch_warning=fetch_warning,
    )
    intelligence = await enhance_article_intelligence(
        base=intelligence,
        article=article,
        analysis=analysis,
        ingested_article=ingested_article,
    )
    if ingested_article:
        card = save_ingested_article_analysis_card(
            ingested_article,
            article,
            analysis,
            session_id=session_id,
            structured_analysis=intelligence,
        )
    else:
        analysis = {**analysis, "intelligence": intelligence}
        card = save_analysis_card(article, analysis, session_id=session_id)

    return {
        "card": card,
        "analysis": analysis,
        "intelligence": card.get("analysis", {}).get("intelligence"),
        "fetch_warning": fetch_warning,
    }


async def analyze_ingested_article(
    article_or_id: dict | str,
    *,
    session_id: str = ANONYMOUS_SESSION_ID,
) -> dict:
    ingested_article = (
        get_ingested_article_record(article_or_id)
        if isinstance(article_or_id, str)
        else article_or_id
    )
    if not ingested_article:
        raise FeedStoreError("Ingested article not found.")

    article_id = ingested_article.get("id")
    try:
        article, fetch_warning = await article_from_ingested(ingested_article)
        result = await analyze_extracted_article(
            article,
            session_id=session_id,
            ingested_article=ingested_article,
            fetch_warning=fetch_warning,
        )
    except AIAnalysisError as exc:
        if article_id:
            mark_ingested_article_analysis_failed(article_id, str(exc))
        raise

    card = result["card"]
    return {
        "article_id": article_id,
        "status": "completed",
        "analysis_status": "analyzed",
        "card_id": card.get("id"),
        "report_id": card.get("report_id"),
        "title": card.get("title") or ingested_article.get("title"),
        "source_id": ingested_article.get("source_id"),
        "fetch_warning": result.get("fetch_warning"),
        "provider_metadata": (card.get("analysis") or {}).get("provider_metadata"),
        "card": card,
        "intelligence": result.get("intelligence"),
    }


def _batch_status(candidate_count: int, analyzed_count: int, error_count: int) -> str:
    if not candidate_count:
        return "skipped"
    if error_count and analyzed_count:
        return "partial"
    if error_count:
        return "failed"
    return "completed"


async def analyze_pending_ingested_articles(
    *,
    session_id: str = ANONYMOUS_SESSION_ID,
    source_id: str | None = None,
    limit: int = 25,
    include_failed: bool = False,
) -> dict:
    max_articles = _clamp(limit, default=25, minimum=1, maximum=100)
    candidate_statuses = {"pending"}
    if include_failed:
        candidate_statuses.add("failed")

    records = list_ingested_article_records(source_id=source_id, limit=250)
    candidates = [
        record
        for record in records
        if (record.get("analysis_status") or "pending") in candidate_statuses
    ][:max_articles]

    results = []
    errors = []
    for record in candidates:
        try:
            analyzed = await analyze_ingested_article(record, session_id=session_id)
            results.append(
                {
                    "article_id": analyzed.get("article_id"),
                    "status": analyzed.get("status"),
                    "report_id": analyzed.get("report_id"),
                    "card_id": analyzed.get("card_id"),
                    "title": analyzed.get("title"),
                    "fetch_warning": analyzed.get("fetch_warning"),
                    "provider_metadata": analyzed.get("provider_metadata"),
                }
            )
        except (AIAnalysisError, FeedStoreError) as exc:
            article_id = record.get("id")
            if article_id:
                mark_ingested_article_analysis_failed(article_id, str(exc))
            errors.append(
                {
                    "article_id": article_id,
                    "title": record.get("title"),
                    "source_id": record.get("source_id"),
                    "error": str(exc),
                }
            )

    status = _batch_status(len(candidates), len(results), len(errors))
    return {
        "status": status,
        "source_id": source_id,
        "candidate_count": len(candidates),
        "analyzed_count": len(results),
        "failed_count": len(errors),
        "skipped_count": max(0, len(records) - len(candidates)),
        "limit": max_articles,
        "include_failed": include_failed,
        "results": results,
        "errors": errors,
        "summary": {
            "pending_before_limit": len(
                [
                    record
                    for record in records
                    if (record.get("analysis_status") or "pending") in candidate_statuses
                ]
            ),
            "analyzed_count": len(results),
            "failed_count": len(errors),
        },
        "limitations": [
            "Batch analysis converts article metadata and extractable text into intelligence; it does not establish factual truth.",
            "Fetch failures fall back to saved source metadata so operators can still compare and triage the item.",
        ],
    }
