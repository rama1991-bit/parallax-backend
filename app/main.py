from fastapi.middleware.cors import CORSMiddleware
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.api.v1.api import api_router

app = FastAPI(
    title="Parallax Narrative Intelligence",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://parallax-frontend.vercel.app",
        "http://localhost:3000",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

origins = [
    settings.FRONTEND_URL,
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins + ["*"] if settings.ENV == "development" else origins,
    allow_credentials=True,
    allow_methods=["*"],
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
@app.get("/api/feed")
def get_feed():
    database_url = os.getenv("DATABASE_URL")

    conn = psycopg2.connect(database_url)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        select
            id,
            title,
            summary,
            source,
            url,
            topic,
            card_type,
            priority,
            narrative_signal,
            evidence_score,
            framing,
            is_read,
            is_saved,
            is_dismissed,
            created_at
        from public.feed_cards
        where is_dismissed = false
        order by priority desc, created_at desc
        limit 20;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return {
        "items": rows,
        "next_cursor": None
    }

@app.get("/debug/env")
def debug_env():
    return {"database_url_exists": bool(os.getenv("DATABASE_URL"))}

@app.get("/feed")
def get_feed_alias():
    return get_feed()

@app.post("/analyze")
def analyze_article():
    return {
        "title": "Analyzed article",
        "summary": "This article contains extracted claims and narrative signals.",
        "source": "External",
        "url": "input-url",
        "topic": "Analysis",
        "card_type": "article_insight",
        "priority": 0.8,
        "narrative_signal": "Narrative signal only — not a truth verdict.",
        "evidence_score": 0.6,
        "framing": "Institutional response",
        "is_read": False,
        "is_saved": False,
        "is_dismissed": False
    }

from pydantic import BaseModel

class AnalyzeRequest(BaseModel):
    url: str | None = None


def mock_analyze_response(url: str | None = None):
    return {
        "id": "mock-analyze-card",
        "title": "New article insight",
        "summary": "This article contains extracted claims, with a dominant frame of institutional_response.",
        "source": "example.com",
        "url": url or "https://example.com",
        "topic": "Analysis",
        "card_type": "article_insight",
        "priority": 0.8,
        "narrative_signal": "Narrative signal only — not a truth verdict.",
        "evidence_score": 0.6,
        "framing": "institutional_response",
        "is_read": False,
        "is_saved": False,
        "is_dismissed": False,
    }


@app.post("/analyze")
def analyze_article(payload: AnalyzeRequest):
    return mock_analyze_response(payload.url)


@app.post("/api/analyze")
def analyze_article_api(payload: AnalyzeRequest):
    return mock_analyze_response(payload.url)


@app.post("/api/v1/analyze")
def analyze_article_v1(payload: AnalyzeRequest):
    return mock_analyze_response(payload.url)

@app.options("/api/v1/analyze")
def analyze_options():
    return {}