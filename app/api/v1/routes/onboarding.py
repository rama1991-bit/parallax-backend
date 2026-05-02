from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, HttpUrl

from app.core.session import get_session_id
from app.services.feed.store import (
    FeedStoreError,
    create_feed_subscription,
    create_topic_monitor,
    list_feed_subscriptions,
    list_topics,
)

router = APIRouter()


class OnboardingTopic(BaseModel):
    name: str
    description: str | None = None
    keywords: list[str] = Field(default_factory=list)


class OnboardingFeed(BaseModel):
    url: HttpUrl
    title: str | None = None
    description: str | None = None
    topic_id: str | None = None


class OnboardingRequest(BaseModel):
    topics: list[OnboardingTopic] = Field(default_factory=list)
    feeds: list[OnboardingFeed] = Field(default_factory=list)


@router.get("/state")
async def onboarding_state(session_id: str = Depends(get_session_id)):
    try:
        topics = list_topics(session_id=session_id)
        feeds = list_feed_subscriptions(session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "session_id": session_id,
        "topic_count": len(topics),
        "feed_count": len(feeds),
        "is_configured": bool(topics or feeds),
        "topics": topics,
        "feeds": feeds,
    }


@router.post("")
async def complete_onboarding(
    payload: OnboardingRequest,
    session_id: str = Depends(get_session_id),
):
    if not payload.topics and not payload.feeds:
        raise HTTPException(
            status_code=422,
            detail="Add at least one topic or source feed to complete onboarding.",
        )

    created_topics = []
    created_feeds = []
    cards = []

    try:
        for topic in payload.topics[:5]:
            if not topic.name.strip():
                continue
            result = create_topic_monitor(
                name=topic.name,
                description=topic.description,
                keywords=topic.keywords,
                session_id=session_id,
            )
            created_topics.append(result["topic"])
            if result.get("card"):
                cards.append(result["card"])

        for feed in payload.feeds[:5]:
            created_feeds.append(
                create_feed_subscription(
                    url=str(feed.url),
                    title=feed.title,
                    description=feed.description,
                    topic_id=feed.topic_id,
                    session_id=session_id,
                )
            )
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "session_id": session_id,
        "topics": created_topics,
        "feeds": created_feeds,
        "cards": cards,
        "topic_count": len(created_topics),
        "feed_count": len(created_feeds),
        "next_href": "/",
    }
