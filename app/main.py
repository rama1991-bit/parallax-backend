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
    allow_origins=["*"],
    allow_credentials=True,
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