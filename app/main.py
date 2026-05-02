import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.api import api_router
from app.api.v1.routes.analyze import AnalyzeRequest, analyze_url
from app.core.config import settings
from app.services.feed.store import FeedStoreError, list_feed_cards


app = FastAPI(
    title="Parallax Narrative Intelligence",
    version="0.1.0",
)

allowed_origins = [
    "https://parallax-frontend.vercel.app",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

if getattr(settings, "FRONTEND_URL", None):
    allowed_origins.append(settings.FRONTEND_URL)

if settings.BACKEND_CORS_ORIGINS:
    allowed_origins.extend(
        origin.strip()
        for origin in settings.BACKEND_CORS_ORIGINS.split(",")
        if origin.strip()
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=list(set(allowed_origins)),
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api/v1")


@app.get("/")
async def root():
    return {
        "name": "Parallax",
        "status": "running",
        "message": "Narrative intelligence backend is alive.",
    }


@app.get("/debug/env")
def debug_env():
    if settings.ENV == "production" and not settings.DEBUG:
        raise HTTPException(status_code=404, detail="Not found")

    return {
        "database_url_exists": bool(os.getenv("DATABASE_URL")),
        "ai_provider": settings.AI_PROVIDER,
        "openai_key_exists": bool(settings.OPENAI_API_KEY),
    }


@app.get("/api/feed")
def get_feed():
    try:
        return {"items": list_feed_cards(limit=20), "next_cursor": None}
    except FeedStoreError as exc:
        return {"items": [], "next_cursor": None, "error": str(exc)}


@app.get("/feed")
def get_feed_alias():
    return get_feed()


@app.post("/analyze")
async def analyze_article(payload: AnalyzeRequest):
    return await analyze_url(payload)


@app.post("/api/analyze")
async def analyze_article_api(payload: AnalyzeRequest):
    return await analyze_url(payload)
