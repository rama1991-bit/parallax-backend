from fastapi import APIRouter, HTTPException, Response
from uuid import uuid4
import json

from app.services.feed.store import BRIEFS

router = APIRouter()


@router.get("/briefs")
async def list_briefs():
    return BRIEFS


@router.get("/briefs/{token}")
async def get_brief(token: str):
    for brief in BRIEFS:
        if brief["token"] == token:
            return brief
    raise HTTPException(status_code=404, detail="Brief not found")


@router.get("/briefs/{token}/export")
async def export_brief(token: str, format: str = "json"):
    for brief in BRIEFS:
        if brief["token"] == token:
            if format == "markdown":
                md = f"# {brief['title']}\n\n{brief.get('brief', {}).get('methodology_note', '')}\n"
                return Response(md, media_type="text/markdown")
            return Response(json.dumps(brief, indent=2), media_type="application/json")
    raise HTTPException(status_code=404, detail="Brief not found")
