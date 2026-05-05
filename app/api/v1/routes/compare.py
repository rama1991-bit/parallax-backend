import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, HttpUrl

from app.core.session import get_session_id
from app.services.analysis import AIAnalysisError, analyze_article as run_article_analysis
from app.services.articles import ArticleFetchError, fetch_article
from app.services.compare import build_compare_result, build_enhanced_ingested_article_compare_result
from app.services.feed.store import (
    FeedStoreError,
    QuotaExceededError,
    get_analyze_usage,
    save_analysis_card,
)

router = APIRouter()


class CompareRequest(BaseModel):
    left_url: HttpUrl
    right_url: HttpUrl


def _enforce_compare_quota(session_id: str, required_analyses: int = 2):
    usage = get_analyze_usage(session_id)
    if not usage.get("quota_enabled"):
        return usage

    if usage.get("cooldown_remaining_seconds", 0) > 0:
        seconds = usage["cooldown_remaining_seconds"]
        raise QuotaExceededError(
            f"Please wait {seconds} seconds before comparing articles.",
            usage,
        )

    if usage.get("remaining", 0) < required_analyses:
        raise QuotaExceededError(
            f"Compare needs {required_analyses} remaining analyses.",
            usage,
        )

    return usage


@router.post("")
async def compare_articles(payload: CompareRequest, session_id: str = Depends(get_session_id)):
    try:
        _enforce_compare_quota(session_id, required_analyses=2)

        left_article, right_article = await asyncio.gather(
            fetch_article(str(payload.left_url)),
            fetch_article(str(payload.right_url)),
        )
        left_analysis, right_analysis = await asyncio.gather(
            run_article_analysis(left_article),
            run_article_analysis(right_article),
        )
        left_card = save_analysis_card(left_article, left_analysis, session_id=session_id)
        right_card = save_analysis_card(right_article, right_analysis, session_id=session_id)
        usage = get_analyze_usage(session_id)
    except ArticleFetchError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except AIAnalysisError as exc:
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

    return build_compare_result(left_card, right_card, usage=usage)


@router.get("/{article_id}")
async def compare_ingested_article(
    article_id: str,
    limit: int = Query(default=8, ge=1, le=25),
):
    try:
        return await build_enhanced_ingested_article_compare_result(article_id, limit=limit)
    except FeedStoreError as exc:
        message = str(exc)
        if "not found" in message.lower():
            raise HTTPException(status_code=404, detail=message) from exc
        raise HTTPException(status_code=503, detail=message) from exc
