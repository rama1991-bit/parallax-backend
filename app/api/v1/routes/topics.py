from fastapi import APIRouter
from pydantic import BaseModel
from uuid import uuid4

from app.services.feed.store import TOPICS

router = APIRouter()


class TopicCreate(BaseModel):
    name: str
    description: str | None = None
    keywords: list[str] = []


@router.get("")
async def list_topics():
    return TOPICS


@router.post("")
async def create_topic(payload: TopicCreate):
    topic = {
        "id": str(uuid4()),
        "name": payload.name,
        "description": payload.description,
        "keywords": payload.keywords,
    }
    TOPICS.append(topic)
    return topic


@router.get("/{topic_id}")
async def get_topic(topic_id: str):
    return {
        "topic": {
            "id": topic_id,
            "name": "Demo topic",
            "description": "Scaffold topic detail.",
        },
        "ecosystem_summary": {
            "article_count": 2,
            "claim_count": 12,
            "source_count": 3,
            "dominant_frame": "diplomatic_process",
            "source_diversity_score": 0.72,
            "dominant_sources": [],
            "frame_distribution": [],
        },
        "latest_articles": [],
        "narrative_clusters": [],
        "narrative_timeline": [],
    }
