from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx

from app.core.config import settings
from app.services.feed.store import (
    FeedStoreError,
    get_source_ops_alert,
    latest_source_ops_alert_delivery,
    list_source_ops_alerts,
    record_source_ops_alert_delivery,
)


SEVERITY_RANK = {"info": 1, "warning": 2, "critical": 3}
MAX_DELIVERY_ATTEMPTS = 3


def _clean(value: str | None) -> str:
    return str(value or "").strip()


def _severity_rank(value: str | None) -> int:
    return SEVERITY_RANK.get(_clean(value).lower(), 0)


def _min_severity() -> str:
    value = _clean(settings.OPS_NOTIFICATION_MIN_SEVERITY).lower() or "warning"
    return value if value in SEVERITY_RANK else "warning"


def _webhook_url() -> str | None:
    value = _clean(settings.OPS_WEBHOOK_URL)
    if not value:
        return None
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return value


def _json_body(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str).encode("utf-8")


def _signature(secret: str | None, body: bytes) -> str | None:
    if not _clean(secret):
        return None
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def _timeout_seconds() -> float:
    try:
        timeout = float(settings.OPS_NOTIFICATION_TIMEOUT_SECONDS)
    except (TypeError, ValueError):
        timeout = 5.0
    return max(1.0, min(timeout, 20.0))


def _notification_payload(alert: dict, *, sync_run_id: str | None = None) -> dict:
    evidence = alert.get("evidence") or {}
    return {
        "event": "source_ops_alert",
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "alert": {
            "id": alert.get("id"),
            "alert_key": alert.get("alert_key"),
            "alert_type": alert.get("alert_type"),
            "severity": alert.get("severity"),
            "status": alert.get("status"),
            "title": alert.get("title"),
            "message": alert.get("message"),
            "source_id": alert.get("source_id"),
            "source_feed_id": alert.get("source_feed_id"),
            "sync_run_id": sync_run_id or alert.get("sync_run_id"),
            "updated_at": alert.get("updated_at"),
        },
        "context": {
            "source": evidence.get("source"),
            "health": evidence.get("health"),
            "quality": evidence.get("quality"),
            "feed": evidence.get("feed"),
        },
        "policy": {
            "min_severity": _min_severity(),
            "truth_status": "operational context only",
        },
    }


async def _post_webhook(url: str, payload: dict) -> dict:
    body = _json_body(payload)
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Parallax-Ops-Alerts/1.0",
    }
    signature = _signature(settings.OPS_WEBHOOK_SECRET, body)
    if signature:
        headers["X-Parallax-Signature"] = signature
    async with httpx.AsyncClient(timeout=_timeout_seconds()) as client:
        response = await client.post(url, content=body, headers=headers)
    if response.status_code >= 400:
        return {
            "status": "failed",
            "response_status": response.status_code,
            "error": response.text[:1000],
        }
    return {
        "status": "delivered",
        "response_status": response.status_code,
        "error": None,
    }


def _already_delivered(alert: dict, latest: dict | None, *, force: bool) -> bool:
    if force or not latest:
        return False
    return (
        latest.get("status") == "delivered"
        and latest.get("alert_updated_at") == alert.get("updated_at")
    )


def _attempt_count(alert: dict, latest: dict | None, *, force: bool) -> int | None:
    if force or not latest or latest.get("alert_updated_at") != alert.get("updated_at"):
        return 1
    previous = int(latest.get("attempt_count") or 1)
    if latest.get("status") == "failed" and previous >= MAX_DELIVERY_ATTEMPTS:
        return None
    return previous + 1


async def deliver_source_ops_alerts(
    *,
    alert_id: str | None = None,
    source_id: str | None = None,
    sync_run_id: str | None = None,
    limit: int = 50,
    force: bool = False,
) -> dict:
    limit = max(1, min(int(limit or 50), 250))

    if alert_id:
        alert = get_source_ops_alert(alert_id)
        alerts = [alert] if alert else []
    else:
        alerts = list_source_ops_alerts(status="active", source_id=source_id, limit=limit)

    webhook_url = _webhook_url()
    if not settings.OPS_NOTIFICATIONS_ENABLED:
        return {
            "status": "disabled",
            "delivery_count": 0,
            "deliveries": [],
            "summary": {"delivered": 0, "failed": 0, "skipped": len(alerts)},
            "message": "OPS_NOTIFICATIONS_ENABLED is false.",
        }
    if not webhook_url:
        return {
            "status": "not_configured",
            "delivery_count": 0,
            "deliveries": [],
            "summary": {"delivered": 0, "failed": 0, "skipped": len(alerts)},
            "message": "OPS_WEBHOOK_URL is missing or invalid.",
        }

    min_rank = _severity_rank(_min_severity())
    deliveries: list[dict] = []
    skipped = 0

    for alert in alerts:
        if not alert or alert.get("status") != "active":
            skipped += 1
            continue
        if _severity_rank(alert.get("severity")) < min_rank:
            skipped += 1
            continue

        latest = latest_source_ops_alert_delivery(alert["id"])
        if _already_delivered(alert, latest, force=force):
            skipped += 1
            continue
        attempt_count = _attempt_count(alert, latest, force=force)
        if attempt_count is None:
            skipped += 1
            continue

        payload = _notification_payload(alert, sync_run_id=sync_run_id)
        try:
            result = await _post_webhook(webhook_url, payload)
        except Exception as exc:  # noqa: BLE001 - delivery failures must not break schedulers.
            result = {"status": "failed", "response_status": None, "error": str(exc)}

        deliveries.append(
            record_source_ops_alert_delivery(
                alert_id=alert["id"],
                alert_updated_at=alert.get("updated_at"),
                destination_type="webhook",
                destination_url=webhook_url,
                status=result["status"],
                attempt_count=attempt_count,
                response_status=result.get("response_status"),
                error=result.get("error"),
                payload=payload,
            )
        )

    summary = {
        "delivered": len([item for item in deliveries if item.get("status") == "delivered"]),
        "failed": len([item for item in deliveries if item.get("status") == "failed"]),
        "skipped": skipped,
    }
    return {
        "status": "delivered" if deliveries and not summary["failed"] else "completed",
        "delivery_count": len(deliveries),
        "deliveries": deliveries,
        "summary": summary,
    }


async def safely_deliver_source_ops_alerts(**kwargs) -> dict | None:
    try:
        return await deliver_source_ops_alerts(**kwargs)
    except FeedStoreError:
        return None
    except Exception:  # noqa: BLE001 - scheduler notification delivery is best-effort.
        return None
