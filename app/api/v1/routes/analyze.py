from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, HttpUrl, model_validator

from app.core.session import ANONYMOUS_SESSION_ID, get_session_id
from app.services.analysis import AIAnalysisError
from app.services.articles import ArticleFetchError, fetch_article
from app.services.feed.store import (
    FeedStoreError,
    QuotaExceededError,
    enforce_analyze_quota,
    get_analyze_usage,
)
from app.services.ingested_analysis import (
    analyze_extracted_article,
    analyze_ingested_article,
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


async def analyze_url(payload: AnalyzeRequest, session_id: str = ANONYMOUS_SESSION_ID):
    try:
        enforce_analyze_quota(session_id)
        if payload.ingested_article_id:
            analyzed = await analyze_ingested_article(payload.ingested_article_id, session_id=session_id)
            card = analyzed["card"]
        else:
            article = await fetch_article(str(payload.url))
            analyzed = await analyze_extracted_article(article, session_id=session_id)
            card = analyzed["card"]

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
        if str(exc) == "Ingested article not found.":
            raise HTTPException(status_code=404, detail="Ingested article not found") from exc
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
