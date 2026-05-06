from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from app.core.session import ANONYMOUS_SESSION_ID
from app.services.event_clustering import refresh_event_clusters
from app.services.feed.store import FeedStoreError
from app.services.intelligence_aggregation import refresh_intelligence_snapshots
from app.services.source_sync import sync_active_source_feeds


def _status_from_phases(phases: list[dict]) -> str:
    statuses = {phase.get("status") for phase in phases}
    if "failed" in statuses:
        return "partial" if any(status in statuses for status in {"completed", "partial", "skipped"}) else "failed"
    if "partial" in statuses:
        return "partial"
    if statuses == {"skipped"}:
        return "skipped"
    return "completed"


async def run_intelligence_pipeline(
    *,
    session_id: str = ANONYMOUS_SESSION_ID,
    source_limit: int = 50,
    feed_limit: int = 100,
    sync_article_limit: int = 10,
    sync_card_limit: int = 25,
    intelligence_source_limit: int = 50,
    topic_limit: int = 50,
    intelligence_article_limit: int = 100,
    intelligence_card_limit: int = 50,
    cluster_article_limit: int = 250,
    cluster_limit: int = 100,
    cluster_card_limit: int = 50,
    skip_sync: bool = False,
    skip_intelligence: bool = False,
    skip_clusters: bool = False,
) -> dict:
    pipeline_id = str(uuid4())
    started = datetime.now(timezone.utc)
    phases = []
    errors = []

    if not skip_sync:
        try:
            sync_result = await sync_active_source_feeds(
                session_id=session_id,
                source_limit=source_limit,
                feed_limit=feed_limit,
                article_limit=sync_article_limit,
                card_limit=sync_card_limit,
            )
            phases.append(
                {
                    "name": "source_sync",
                    "status": sync_result.get("status"),
                    "run_id": sync_result.get("sync_run_id"),
                    "source_count": sync_result.get("source_count", 0),
                    "feed_count": sync_result.get("feed_count", 0),
                    "syncable_feed_count": sync_result.get("syncable_feed_count", 0),
                    "skipped_feed_count": sync_result.get("skipped_feed_count", 0),
                    "article_count": sync_result.get("article_count", 0),
                    "card_count": sync_result.get("card_count", 0),
                    "error_count": sync_result.get("error_count", 0),
                    "summary": {
                        "synced_feed_count": sync_result.get("synced_feed_count", 0),
                        "ops_alerts": (sync_result.get("ops_alerts") or {}).get("summary"),
                        "ops_alert_delivery": (sync_result.get("ops_alert_delivery") or {}).get("summary"),
                    },
                }
            )
            errors.extend(sync_result.get("errors") or [])
        except FeedStoreError as exc:
            errors.append({"phase": "source_sync", "error": str(exc)})
            phases.append({"name": "source_sync", "status": "failed", "error_count": 1})
    else:
        phases.append({"name": "source_sync", "status": "skipped", "error_count": 0})

    if not skip_intelligence:
        try:
            intelligence_result = await refresh_intelligence_snapshots(
                session_id=session_id,
                source_limit=intelligence_source_limit,
                topic_limit=topic_limit,
                article_limit=intelligence_article_limit,
                card_limit=intelligence_card_limit,
                create_cards=True,
            )
            phases.append(
                {
                    "name": "intelligence_refresh",
                    "status": intelligence_result.get("status"),
                    "run_id": (intelligence_result.get("run") or {}).get("id"),
                    "source_count": intelligence_result.get("source_count", 0),
                    "topic_count": intelligence_result.get("topic_count", 0),
                    "snapshot_count": intelligence_result.get("snapshot_count", 0),
                    "card_count": intelligence_result.get("card_count", 0),
                    "error_count": intelligence_result.get("error_count", 0),
                }
            )
            errors.extend(intelligence_result.get("errors") or [])
        except FeedStoreError as exc:
            errors.append({"phase": "intelligence_refresh", "error": str(exc)})
            phases.append({"name": "intelligence_refresh", "status": "failed", "error_count": 1})
    else:
        phases.append({"name": "intelligence_refresh", "status": "skipped", "error_count": 0})

    if not skip_clusters:
        try:
            cluster_result = refresh_event_clusters(
                session_id=session_id,
                article_limit=cluster_article_limit,
                cluster_limit=cluster_limit,
                card_limit=cluster_card_limit,
                create_cards=True,
            )
            phases.append(
                {
                    "name": "event_clusters",
                    "status": cluster_result.get("status"),
                    "run_id": (cluster_result.get("run") or {}).get("id"),
                    "cluster_count": cluster_result.get("cluster_count", 0),
                    "article_count": cluster_result.get("article_count", 0),
                    "card_count": cluster_result.get("card_count", 0),
                    "error_count": cluster_result.get("error_count", 0),
                }
            )
            errors.extend(cluster_result.get("errors") or [])
        except FeedStoreError as exc:
            errors.append({"phase": "event_clusters", "error": str(exc)})
            phases.append({"name": "event_clusters", "status": "failed", "error_count": 1})
    else:
        phases.append({"name": "event_clusters", "status": "skipped", "error_count": 0})

    finished = datetime.now(timezone.utc)
    status = _status_from_phases(phases)
    return {
        "pipeline_id": pipeline_id,
        "status": status,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "duration_ms": round((finished - started).total_seconds() * 1000),
        "phases": phases,
        "summary": {
            "phase_count": len(phases),
            "completed_phase_count": len([phase for phase in phases if phase.get("status") == "completed"]),
            "skipped_phase_count": len([phase for phase in phases if phase.get("status") == "skipped"]),
            "error_count": len(errors),
            "feed_card_count": sum(int(phase.get("card_count") or 0) for phase in phases),
            "article_count": sum(
                int(phase.get("article_count") or 0)
                for phase in phases
                if phase.get("name") in {"source_sync", "event_clusters"}
            ),
            "cluster_count": next(
                (int(phase.get("cluster_count") or 0) for phase in phases if phase.get("name") == "event_clusters"),
                0,
            ),
        },
        "limits": {
            "source_limit": source_limit,
            "feed_limit": feed_limit,
            "sync_article_limit": sync_article_limit,
            "sync_card_limit": sync_card_limit,
            "intelligence_source_limit": intelligence_source_limit,
            "topic_limit": topic_limit,
            "intelligence_article_limit": intelligence_article_limit,
            "intelligence_card_limit": intelligence_card_limit,
            "cluster_article_limit": cluster_article_limit,
            "cluster_limit": cluster_limit,
            "cluster_card_limit": cluster_card_limit,
        },
        "errors": errors,
        "limitations": [
            "The pipeline orchestrates existing bounded jobs; it does not turn OSINT or repeated coverage into truth.",
            "Source ingestion depends on RSS availability and source review status.",
            "Event clustering is heuristic and should be reviewed through compare and OSINT panels.",
        ],
    }
