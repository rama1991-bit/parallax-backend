from fastapi import APIRouter, HTTPException
from uuid import uuid4

from app.services.feed.store import FEED_CARDS, now_iso

router = APIRouter()


def apply_filter(cards, filter_type: str):
    if filter_type == "high":
        return [c for c in cards if c.get("priority_score", 0) >= 0.75]
    if filter_type == "unread":
        return [c for c in cards if not c.get("is_read")]
    if filter_type == "narrative":
        return [c for c in cards if c.get("card_type") in [
            "narrative_frame_shift",
            "coverage_change",
            "source_ecosystem_change",
            "divergence_increase",
        ]]
    if filter_type == "articles":
        return [c for c in cards if c.get("card_type") == "article_insight"]
    if filter_type == "saved":
        return [c for c in cards if c.get("is_saved")]
    return cards


@router.get("")
async def get_feed(filter_type: str = "all", limit: int = 20, cursor: str | None = None):
    cards = [c for c in FEED_CARDS if not c.get("is_dismissed")]
    cards = apply_filter(cards, filter_type)
    cards = sorted(cards, key=lambda c: c.get("created_at", ""), reverse=True)
    return {
        "cards": cards[:limit],
        "next_cursor": None,
        "has_more": False,
    }


@router.get("/{card_id}")
async def get_card(card_id: str):
    for card in FEED_CARDS:
        if card["id"] == card_id:
            return card
    raise HTTPException(status_code=404, detail="Feed card not found")


@router.post("/{card_id}/read")
async def read_card(card_id: str):
    for card in FEED_CARDS:
        if card["id"] == card_id:
            card["is_read"] = True
            return card
    raise HTTPException(status_code=404, detail="Feed card not found")


@router.post("/{card_id}/save")
async def save_card(card_id: str):
    for card in FEED_CARDS:
        if card["id"] == card_id:
            card["is_saved"] = True
            return card
    raise HTTPException(status_code=404, detail="Feed card not found")


@router.post("/{card_id}/unsave")
async def unsave_card(card_id: str):
    for card in FEED_CARDS:
        if card["id"] == card_id:
            card["is_saved"] = False
            return card
    raise HTTPException(status_code=404, detail="Feed card not found")


@router.post("/{card_id}/dismiss")
async def dismiss_card(card_id: str):
    for card in FEED_CARDS:
        if card["id"] == card_id:
            card["is_dismissed"] = True
            return {"id": card_id, "is_dismissed": True}
    raise HTTPException(status_code=404, detail="Feed card not found")


@router.post("/{card_id}/track")
async def track_card(card_id: str, payload: dict):
    return {"ok": True, "card_id": card_id, "interaction_type": payload.get("interaction_type")}
