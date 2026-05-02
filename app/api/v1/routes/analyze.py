import re
from urllib.parse import urlparse
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, HttpUrl, model_validator
from uuid import uuid4

from app.services.analysis import AIAnalysisError, analyze_article as run_article_analysis
from app.services.articles import ArticleFetchError, ExtractedArticle, fetch_article
from app.core.session import ANONYMOUS_SESSION_ID, get_session_id
from app.services.feed.store import (
    FeedStoreError,
    QuotaExceededError,
    enforce_analyze_quota,
    get_analyze_usage,
    get_ingested_article_record,
    mark_ingested_article_analysis_failed,
    save_analysis_card,
    save_ingested_article_analysis_card,
)

router = APIRouter()


class AnalyzeRequest(BaseModel):
    url: HttpUrl | None = None
    ingested_article_id: str | None = None

    @model_validator(mode="after")
    def require_url_or_ingested_article(self):
        if not self.url and not self.ingested_article_id:
            raise ValueError("Provide either url or ingested_article_id.")
        return self


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


async def _article_from_ingested(record: dict) -> tuple[ExtractedArticle, str | None]:
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


def _build_structured_intelligence(
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


async def analyze_url(payload: AnalyzeRequest, session_id: str = ANONYMOUS_SESSION_ID):
    try:
        enforce_analyze_quota(session_id)
        ingested_article = None
        fetch_warning = None

        if payload.ingested_article_id:
            ingested_article = get_ingested_article_record(payload.ingested_article_id)
            if not ingested_article:
                raise HTTPException(status_code=404, detail="Ingested article not found")
            article, fetch_warning = await _article_from_ingested(ingested_article)
        else:
            article = await fetch_article(str(payload.url))

        analysis = await run_article_analysis(article)
        intelligence = _build_structured_intelligence(
            article,
            analysis,
            ingested_article=ingested_article,
            fetch_warning=fetch_warning,
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
        usage = get_analyze_usage(session_id)
    except ArticleFetchError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except AIAnalysisError as exc:
        if payload.ingested_article_id:
            mark_ingested_article_analysis_failed(payload.ingested_article_id, str(exc))
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except QuotaExceededError as exc:
        raise HTTPException(
            status_code=429,
            detail={
                "message": str(exc),
                "usage": exc.usage,
            },
        ) from exc
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "job_id": str(uuid4()),
        "report_id": card.get("report_id"),
        "card_id": card["id"],
        "status": "completed",
        "ingested_article_id": payload.ingested_article_id,
        "card": card,
        "intelligence": card.get("analysis", {}).get("intelligence"),
        "usage": usage,
    }


@router.post("")
async def analyze(payload: AnalyzeRequest, session_id: str = Depends(get_session_id)):
    return await analyze_url(payload, session_id=session_id)
