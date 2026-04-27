from fastapi import APIRouter
from app.services.feed.store import ALERTS

router = APIRouter()


@router.get("")
async def list_alerts():
    return ALERTS


@router.get("/unread-count")
async def unread_count():
    return {"unread_count": len([a for a in ALERTS if not a.get("is_read")])}
