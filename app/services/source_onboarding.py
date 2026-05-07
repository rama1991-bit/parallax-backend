from __future__ import annotations

from datetime import datetime, timezone

from app.core.session import ANONYMOUS_SESSION_ID
from app.services.event_clustering import refresh_event_clusters, resolve_event_cluster_source_draft
from app.services.feed.store import (
    FeedStoreError,
    create_source_feed_record,
    create_source_record,
    get_source_record,
    save_source_onboarding_run,
)
from app.services.ingested_analysis import analyze_pending_ingested_articles
from app.services.intelligence_aggregation import build_source_intelligence
from app.services.source_discovery import validate_source_candidate
from app.services.source_sync import sync_source_feeds


def _phase(name: str, status: str, **kwargs) -> dict:
    return {"name": name, "status": status, **{key: value for key, value in kwargs.items() if value is not None}}


def _workflow_status(phases: list[dict], errors: list[dict]) -> str:
    if not phases:
        return "skipped"
    statuses = {phase.get("status") for phase in phases}
    if errors and any(status in statuses for status in {"completed", "partial"}):
        return "partial"
    if "failed" in statuses:
        return "partial" if any(status in statuses for status in {"completed", "partial", "skipped"}) else "failed"
    if "partial" in statuses or "needs_review" in statuses:
        return "partial"
    if statuses == {"skipped"}:
        return "skipped"
    return "completed"


def _compact_error(phase: str, exc: Exception) -> dict:
    return {"phase": phase, "error": str(exc)}


def _clean_text(value: object) -> str | None:
    cleaned = " ".join(str(value or "").strip().split())
    return cleaned or None


def _has_value(value: object) -> bool:
    return value is not None and value != ""


def _candidate_payload(candidate: dict, validation: dict | None, override: dict | None) -> dict:
    validation = validation or {}
    payload = {
        **(candidate.get("create_payload") or {}),
        "name": candidate.get("name"),
        "website_url": candidate.get("website_url"),
        "rss_url": candidate.get("rss_url"),
        "country": candidate.get("country"),
        "language": candidate.get("language"),
        "region": candidate.get("region"),
        "source_size": candidate.get("source_size"),
        "source_type": candidate.get("source_type"),
        "feed_type": candidate.get("feed_type"),
        "credibility_notes": candidate.get("credibility_notes"),
    }
    payload = {key: value for key, value in payload.items() if _has_value(value)}

    selected_feed_type = validation.get("selected_feed_type")
    selected_feed_url = validation.get("selected_feed_url")
    if selected_feed_type == "rss" and selected_feed_url:
        payload["feed_type"] = "rss"
        payload["rss_url"] = selected_feed_url
    elif selected_feed_type == "homepage" and selected_feed_url:
        payload["feed_type"] = "homepage"
        payload["website_url"] = selected_feed_url
    elif selected_feed_type == "manual":
        payload["feed_type"] = "manual"

    if validation.get("title") and not payload.get("name"):
        payload["name"] = validation["title"]

    if override:
        payload.update({key: value for key, value in override.items() if _has_value(value)})

    if payload.get("rss_url") and payload.get("feed_type") != "homepage":
        payload["feed_type"] = "rss"
    elif payload.get("website_url") and not payload.get("feed_type"):
        payload["feed_type"] = "homepage"
    else:
        payload["feed_type"] = payload.get("feed_type") or "manual"
    return payload


def _create_source_from_payload(payload: dict) -> tuple[dict, dict | None]:
    source = create_source_record(
        name=payload.get("name"),
        website_url=payload.get("website_url"),
        rss_url=payload.get("rss_url"),
        country=payload.get("country"),
        language=payload.get("language"),
        region=payload.get("region"),
        political_context=payload.get("political_context"),
        source_size=payload.get("source_size"),
        source_type=payload.get("source_type"),
        credibility_notes=payload.get("credibility_notes"),
        notes=payload.get("notes"),
    )

    feed_type = payload.get("feed_type")
    feed_url = None
    if feed_type == "rss" and payload.get("rss_url"):
        feed_url = payload.get("rss_url")
    elif feed_type in {"homepage", "manual"} and payload.get("website_url"):
        feed_url = payload.get("website_url")
    elif payload.get("rss_url"):
        feed_type = "rss"
        feed_url = payload.get("rss_url")
    elif payload.get("website_url"):
        feed_type = "homepage"
        feed_url = payload.get("website_url")

    feed = None
    if feed_url:
        feed = create_source_feed_record(
            source_id=source["id"],
            feed_url=feed_url,
            feed_type=feed_type or "homepage",
            title=source.get("name"),
            language=source.get("language"),
            country=source.get("country"),
        )
    return get_source_record(source["id"]) or source, feed


def _compact_sync_result(result: dict | None) -> dict:
    result = result or {}
    return {
        "status": result.get("status"),
        "sync_run_id": result.get("sync_run_id"),
        "feed_count": result.get("feed_count", 0),
        "synced_feed_count": result.get("synced_feed_count", 0),
        "article_count": result.get("article_count", 0),
        "card_count": result.get("card_count", 0),
        "error_count": result.get("error_count", 0),
    }


async def onboard_source_candidate(
    *,
    candidate: dict,
    session_id: str = ANONYMOUS_SESSION_ID,
    source_payload: dict | None = None,
    draft_cluster_id: str | None = None,
    draft_candidate_id: str | None = None,
    draft_resolution_notes: str | None = None,
    allow_unvalidated: bool = False,
    allow_homepage_fallback: bool = True,
    validate_limit: int = 5,
    sync_after_create: bool = True,
    sync_article_limit: int = 5,
    sync_card_limit: int = 5,
    analyze_after_sync: bool = True,
    analysis_article_limit: int = 10,
    refresh_intelligence: bool = True,
    intelligence_article_limit: int = 50,
    refresh_clusters: bool = True,
    cluster_article_limit: int = 100,
    cluster_limit: int = 50,
    cluster_card_limit: int = 20,
) -> dict:
    started = datetime.now(timezone.utc)
    phases: list[dict] = []
    errors: list[dict] = []
    result_payload: dict = {}
    source = None
    feed = None
    validation_result = None

    request_payload = {
        "candidate": candidate or {},
        "source_payload": source_payload or {},
        "draft_cluster_id": draft_cluster_id,
        "draft_candidate_id": draft_candidate_id,
        "allow_unvalidated": allow_unvalidated,
        "allow_homepage_fallback": allow_homepage_fallback,
        "sync_after_create": sync_after_create,
        "analyze_after_sync": analyze_after_sync,
        "refresh_intelligence": refresh_intelligence,
        "refresh_clusters": refresh_clusters,
    }

    try:
        validation_result = await validate_source_candidate(
            candidate=candidate or {},
            session_id=session_id,
            allow_homepage_fallback=allow_homepage_fallback,
            limit=validate_limit,
        )
        validation = validation_result.get("validation") or {}
        candidate = validation_result.get("candidate") or candidate or {}
        result_payload["validation"] = validation_result
        phases.append(
            _phase(
                "candidate_validation",
                validation.get("status") or "failed",
                validation_run_id=(validation_result.get("validation_run") or {}).get("id"),
                item_count=validation.get("item_count", 0),
                selected_feed_type=validation.get("selected_feed_type"),
            )
        )
    except Exception as exc:
        errors.append(_compact_error("candidate_validation", exc))
        phases.append(_phase("candidate_validation", "failed", error=str(exc)))
        validation = {}

    validation_status = (validation_result or {}).get("validation", {}).get("status")
    if validation_status != "validated" and not allow_unvalidated:
        errors.append(
            {
                "phase": "candidate_validation",
                "error": "Candidate was not validated. Enable allow_unvalidated only after manual review.",
            }
        )
        finished = datetime.now(timezone.utc)
        summary = {
            "source_created": False,
            "validation_status": validation_status or "failed",
            "coverage_delta": {"new_articles": 0, "new_cards": 0, "analyzed_articles": 0},
        }
        run = save_source_onboarding_run(
            session_id=session_id,
            status="failed",
            cluster_id=draft_cluster_id or candidate.get("cluster_id"),
            candidate_id=draft_candidate_id or candidate.get("id"),
            source_name=candidate.get("name"),
            started_at=started,
            finished_at=finished,
            phases=phases,
            errors=errors,
            request_payload=request_payload,
            result_payload=result_payload,
            summary=summary,
        )
        return {
            "status": "failed",
            "run": run,
            "phases": phases,
            "errors": errors,
            "summary": summary,
            "candidate": candidate,
            "validation": (validation_result or {}).get("validation"),
        }

    try:
        create_payload = _candidate_payload(candidate, (validation_result or {}).get("validation"), source_payload)
        source, feed = _create_source_from_payload(create_payload)
        result_payload["source"] = source
        result_payload["feed"] = feed
        phases.append(
            _phase(
                "source_creation",
                "completed",
                source_id=source.get("id"),
                source_feed_id=(feed or {}).get("id"),
                feed_type=(feed or {}).get("feed_type"),
            )
        )
    except Exception as exc:
        errors.append(_compact_error("source_creation", exc))
        phases.append(_phase("source_creation", "failed", error=str(exc)))

    before_article_count = int((source or {}).get("article_count") or 0)

    if source and draft_cluster_id and draft_candidate_id:
        try:
            draft_resolution = resolve_event_cluster_source_draft(
                cluster_id=draft_cluster_id,
                candidate_id=draft_candidate_id,
                status="created",
                source_id=source["id"],
                resolution_notes=draft_resolution_notes or "Source onboarded from validated discovery candidate.",
                draft_payload={"candidate": candidate, "source_payload": source_payload or {}},
            )
            result_payload["draft_resolution"] = draft_resolution
            phases.append(_phase("source_draft_resolution", "completed"))
        except Exception as exc:
            errors.append(_compact_error("source_draft_resolution", exc))
            phases.append(_phase("source_draft_resolution", "failed", error=str(exc)))

    sync_result = None
    if source and sync_after_create and feed and feed.get("feed_type") != "manual":
        try:
            sync_result = await sync_source_feeds(
                source["id"],
                session_id=session_id,
                article_limit=sync_article_limit,
                card_limit=sync_card_limit,
            )
            result_payload["sync"] = _compact_sync_result(sync_result)
            phases.append(
                _phase(
                    "source_sync",
                    sync_result.get("status") or "failed",
                    sync_run_id=sync_result.get("sync_run_id"),
                    article_count=sync_result.get("article_count", 0),
                    card_count=sync_result.get("card_count", 0),
                    error_count=sync_result.get("error_count", 0),
                )
            )
            errors.extend({"phase": "source_sync", **item} for item in (sync_result.get("errors") or []))
        except Exception as exc:
            errors.append(_compact_error("source_sync", exc))
            phases.append(_phase("source_sync", "failed", error=str(exc)))
    else:
        phases.append(_phase("source_sync", "skipped"))

    analysis_result = None
    if source and analyze_after_sync:
        try:
            analysis_result = await analyze_pending_ingested_articles(
                session_id=session_id,
                source_id=source["id"],
                limit=analysis_article_limit,
            )
            result_payload["analysis"] = {
                "status": analysis_result.get("status"),
                "candidate_count": analysis_result.get("candidate_count", 0),
                "analyzed_count": analysis_result.get("analyzed_count", 0),
                "failed_count": analysis_result.get("failed_count", 0),
            }
            phases.append(
                _phase(
                    "article_analysis",
                    analysis_result.get("status") or "completed",
                    candidate_count=analysis_result.get("candidate_count", 0),
                    analyzed_count=analysis_result.get("analyzed_count", 0),
                    failed_count=analysis_result.get("failed_count", 0),
                )
            )
        except Exception as exc:
            errors.append(_compact_error("article_analysis", exc))
            phases.append(_phase("article_analysis", "failed", error=str(exc)))
    else:
        phases.append(_phase("article_analysis", "skipped"))

    intelligence_result = None
    if source and refresh_intelligence:
        try:
            intelligence_result = await build_source_intelligence(
                source["id"],
                refresh=True,
                limit=intelligence_article_limit,
            )
            result_payload["intelligence"] = {
                "status": intelligence_result.get("status"),
                "snapshot_id": (intelligence_result.get("snapshot") or {}).get("id"),
                "sample_size": (intelligence_result.get("sample") or {}).get("article_count", 0),
            }
            phases.append(
                _phase(
                    "source_intelligence",
                    intelligence_result.get("status") or "completed",
                    snapshot_id=(intelligence_result.get("snapshot") or {}).get("id"),
                    sample_size=(intelligence_result.get("sample") or {}).get("article_count", 0),
                )
            )
        except Exception as exc:
            errors.append(_compact_error("source_intelligence", exc))
            phases.append(_phase("source_intelligence", "failed", error=str(exc)))
    else:
        phases.append(_phase("source_intelligence", "skipped"))

    cluster_result = None
    if source and refresh_clusters:
        try:
            cluster_result = refresh_event_clusters(
                session_id=session_id,
                article_limit=cluster_article_limit,
                cluster_limit=cluster_limit,
                card_limit=cluster_card_limit,
            )
            result_payload["clusters"] = {
                "status": cluster_result.get("status"),
                "run_id": (cluster_result.get("run") or {}).get("id"),
                "cluster_count": cluster_result.get("cluster_count", 0),
                "card_count": cluster_result.get("card_count", 0),
                "summary": (cluster_result.get("run") or {}).get("summary") or {},
            }
            phases.append(
                _phase(
                    "cluster_refresh",
                    cluster_result.get("status") or "completed",
                    run_id=(cluster_result.get("run") or {}).get("id"),
                    cluster_count=cluster_result.get("cluster_count", 0),
                    card_count=cluster_result.get("card_count", 0),
                )
            )
        except Exception as exc:
            errors.append(_compact_error("cluster_refresh", exc))
            phases.append(_phase("cluster_refresh", "failed", error=str(exc)))
    else:
        phases.append(_phase("cluster_refresh", "skipped"))

    refreshed_source = get_source_record(source["id"]) if source else None
    after_article_count = int((refreshed_source or source or {}).get("article_count") or 0)
    coverage_delta = {
        "new_articles": max(0, after_article_count - before_article_count),
        "synced_articles": (sync_result or {}).get("article_count", 0),
        "new_cards": (sync_result or {}).get("card_count", 0),
        "analyzed_articles": (analysis_result or {}).get("analyzed_count", 0),
        "cluster_count": (cluster_result or {}).get("cluster_count", 0),
        "coverage_gap_task_count": ((cluster_result or {}).get("run") or {}).get("summary", {}).get("coverage_gap_task_count", 0),
        "suggested_source_search_count": ((cluster_result or {}).get("run") or {}).get("summary", {}).get("suggested_source_search_count", 0),
    }
    summary = {
        "source_created": bool(source),
        "source_id": (source or {}).get("id"),
        "source_name": (source or {}).get("name") or _clean_text(candidate.get("name")),
        "validation_status": validation_status,
        "coverage_delta": coverage_delta,
        "phase_count": len(phases),
        "error_count": len(errors),
    }
    status = _workflow_status(phases, errors)
    finished = datetime.now(timezone.utc)
    run = save_source_onboarding_run(
        session_id=session_id,
        status=status,
        source_id=(source or {}).get("id"),
        source_feed_id=(feed or {}).get("id"),
        cluster_id=draft_cluster_id or candidate.get("cluster_id"),
        candidate_id=draft_candidate_id or candidate.get("id"),
        source_name=(source or {}).get("name") or candidate.get("name"),
        started_at=started,
        finished_at=finished,
        phases=phases,
        errors=errors,
        request_payload=request_payload,
        result_payload=result_payload,
        summary=summary,
    )
    return {
        "status": status,
        "run": run,
        "source": refreshed_source or source,
        "feed": feed,
        "candidate": candidate,
        "validation": (validation_result or {}).get("validation"),
        "phases": phases,
        "errors": errors,
        "summary": summary,
        "results": result_payload,
    }
