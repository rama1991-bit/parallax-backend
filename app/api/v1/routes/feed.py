from fastapi import APIRouter, Depends, HTTPException

from app.core.session import get_session_id
from app.services.feed.store import (
    FeedStoreError,
    get_feed_card,
    list_feed_cards,
    update_feed_card,
)

router = APIRouter()


@router.get("")
async def get_feed(
    filter_type: str = "all",
    limit: int = 20,
    cursor: str | None = None,
    session_id: str = Depends(get_session_id),
):
    try:
        cards = list_feed_cards(filter_type=filter_type, limit=limit, session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "cards": cards,
        "next_cursor": None,
        "has_more": False,
    }


@router.get("/{card_id}")
async def get_card(card_id: str, session_id: str = Depends(get_session_id)):
    try:
        card = get_feed_card(card_id, session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if card:
        return card
    raise HTTPException(status_code=404, detail="Feed card not found")


@router.post("/{card_id}/read")
async def read_card(card_id: str, session_id: str = Depends(get_session_id)):
    try:
        card = update_feed_card(card_id, session_id=session_id, is_read=True)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if card:
        return card
    raise HTTPException(status_code=404, detail="Feed card not found")


@router.post("/{card_id}/save")
async def save_card(card_id: str, session_id: str = Depends(get_session_id)):
    try:
        card = update_feed_card(card_id, session_id=session_id, is_saved=True)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if card:
        return card
    raise HTTPException(status_code=404, detail="Feed card not found")


@router.post("/{card_id}/unsave")
async def unsave_card(card_id: str, session_id: str = Depends(get_session_id)):
    try:
        card = update_feed_card(card_id, session_id=session_id, is_saved=False)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if card:
        return card
    raise HTTPException(status_code=404, detail="Feed card not found")


@router.post("/{card_id}/dismiss")
async def dismiss_card(card_id: str, session_id: str = Depends(get_session_id)):
    try:
        card = update_feed_card(card_id, session_id=session_id, is_dismissed=True)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if card:
        return {"id": card_id, "is_dismissed": True}
    raise HTTPException(status_code=404, detail="Feed card not found")


@router.post("/{card_id}/track")
async def track_card(card_id: str, payload: dict):
    return {"ok": True, "card_id": card_id, "interaction_type": payload.get("interaction_type")}
