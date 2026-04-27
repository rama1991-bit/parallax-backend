from fastapi import APIRouter
from pydantic import BaseModel, HttpUrl
from uuid import uuid4

from app.services.feed.store import FEED_CARDS, now_iso

router = APIRouter()


class AnalyzeRequest(BaseModel):
    url: HttpUrl


@router.post("")
async def analyze(payload: AnalyzeRequest):
    report_id = str(uuid4())
    card_id = str(uuid4())

    FEED_CARDS.insert(0, {
        "id": card_id,
        "topic_id": None,
        "article_id": None,
        "alert_id": None,
        "report_id": report_id,
        "card_type": "article_insight",
        "title": "New article insight",
        "summary": f"Queued analysis for {payload.url}. This scaffold creates a demo card immediately.",
        "priority_score": 0.65,
        "personalized_score": 0.65,
        "payload": {"url": str(payload.url), "report_id": report_id},
        "is_read": False,
        "is_saved": False,
        "created_at": now_iso(),
        "recommendations": [
            {
                "type": "open_report",
                "label": "Open report",
                "href": f"/reports/{report_id}",
                "reason": "See the article analysis report.",
            }
        ],
        "explanation": {
            "why_this_matters": "A new article was submitted for analysis.",
            "what_changed": None,
            "recommended_action": "Open the report when processing is complete.",
        },
    })

    return {"job_id": str(uuid4()), "report_id": report_id, "card_id": card_id, "status": "queued"}
