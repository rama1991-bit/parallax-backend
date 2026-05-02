from fastapi import APIRouter, Depends, HTTPException, Response
import json

from app.core.session import get_session_id
from app.services.briefs import (
    BriefTokenError,
    get_public_brief,
    list_public_briefs,
    public_brief_to_markdown,
)
from app.services.feed.store import FeedStoreError

router = APIRouter()


@router.get("/briefs")
async def list_briefs(session_id: str = Depends(get_session_id)):
    try:
        briefs = list_public_briefs(session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {"briefs": briefs}


@router.get("/briefs/{token}")
async def get_brief(token: str):
    try:
        return get_public_brief(token)
    except BriefTokenError as exc:
        raise HTTPException(status_code=404, detail="Brief not found") from exc
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/briefs/{token}/export")
async def export_brief(token: str, format: str = "json"):
    try:
        brief = get_public_brief(token)
    except BriefTokenError as exc:
        raise HTTPException(status_code=404, detail="Brief not found") from exc
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if format == "markdown":
        return Response(public_brief_to_markdown(brief), media_type="text/markdown")

    return Response(json.dumps(brief, indent=2), media_type="application/json")
