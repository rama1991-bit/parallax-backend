from fastapi import APIRouter

from app.core.config import settings
from app.services.feed.store import FeedStoreError, check_database

router = APIRouter()


@router.get("")
async def health():
    return {
        "status": "ok",
        "environment": settings.ENV,
        "ai_provider": settings.AI_PROVIDER,
    }


@router.get("/db")
async def db_health():
    try:
        return check_database()
    except FeedStoreError as exc:
        return {"status": "error", "enabled": True, "message": str(exc)}


@router.get("/redis")
async def redis_health():
    return {"status": "stub", "message": "Redis check not wired in scaffold."}
