from datetime import datetime, timezone
from uuid import uuid4

# In-memory store for first deploy smoke tests.
# Replace with PostgreSQL models/migrations when moving to production data.
FEED_CARDS = []
TOPICS = []
ALERTS = []
BRIEFS = []
AUTHORS = []


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def seed_initial_cards():
    if FEED_CARDS:
        return

    FEED_CARDS.extend([
        {
            "id": str(uuid4()),
            "topic_id": None,
            "article_id": None,
            "alert_id": None,
            "report_id": None,
            "card_type": "narrative_frame_shift",
            "title": "The story’s framing just shifted",
            "summary": "A monitored topic changed its dominant frame. Open the card to inspect what changed.",
            "priority_score": 0.9,
            "personalized_score": 0.9,
            "payload": {"dominant_frame": "diplomatic_process"},
            "is_read": False,
            "is_saved": False,
            "created_at": now_iso(),
            "recommendations": [
                {
                    "type": "open_topic",
                    "label": "Open topic",
                    "href": "/topics/demo-topic",
                    "reason": "Inspect timeline and source comparison.",
                }
            ],
            "explanation": {
                "why_this_matters": "A dominant frame changed inside a monitored topic.",
                "what_changed": {"from": "security_threat", "to": "diplomatic_process"},
                "recommended_action": "Open the topic to inspect timeline and sources.",
            },
        },
        {
            "id": str(uuid4()),
            "topic_id": None,
            "article_id": None,
            "alert_id": None,
            "report_id": "demo-report",
            "card_type": "article_insight",
            "title": "New article insight",
            "summary": "This article contains 6 extracted claims, with a dominant frame of institutional_response.",
            "priority_score": 0.7,
            "personalized_score": 0.7,
            "payload": {"domain": "example.com", "claim_count": 6, "dominant_frame": "institutional_response"},
            "is_read": False,
            "is_saved": False,
            "created_at": now_iso(),
            "recommendations": [
                {
                    "type": "open_report",
                    "label": "Open report",
                    "href": "/reports/demo-report",
                    "reason": "See claim-level evidence and framing analysis.",
                },
                {
                    "type": "compare",
                    "label": "Compare with another article",
                    "href": "/compare",
                    "reason": "Check whether other sources frame this story differently.",
                },
            ],
            "explanation": {
                "why_this_matters": "This article produced structured claim and framing signals.",
                "what_changed": None,
                "recommended_action": "Open the full report for claim-level evidence.",
            },
        },
    ])


seed_initial_cards()
