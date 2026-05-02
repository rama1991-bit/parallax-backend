from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, HttpUrl
from uuid import uuid4

from app.services.analysis import AIAnalysisError, analyze_article as run_article_analysis
from app.services.articles import ArticleFetchError, fetch_article
from app.core.session import ANONYMOUS_SESSION_ID, get_session_id
from app.services.feed.store import (
    FeedStoreError,
    QuotaExceededError,
    enforce_analyze_quota,
    get_analyze_usage,
    save_analysis_card,
)

router = APIRouter()


class AnalyzeRequest(BaseModel):
    url: HttpUrl


async def analyze_url(payload: AnalyzeRequest, session_id: str = ANONYMOUS_SESSION_ID):
    try:
        enforce_analyze_quota(session_id)
        article = await fetch_article(str(payload.url))
        analysis = await run_article_analysis(article)
        card = save_analysis_card(article, analysis, session_id=session_id)
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

    return {
        "job_id": str(uuid4()),
        "report_id": card.get("report_id"),
        "card_id": card["id"],
        "status": "completed",
        "card": card,
        "usage": usage,
    }


@router.post("")
async def analyze(payload: AnalyzeRequest, session_id: str = Depends(get_session_id)):
    return await analyze_url(payload, session_id=session_id)
