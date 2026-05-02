"""
Read-only smoke checks against a deployed Parallax backend.

Usage:
    PARALLAX_LIVE_API_URL=https://your-backend.example.com python scripts/smoke_live.py
"""

from __future__ import annotations

import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4


TIMEOUT_SECONDS = 20


def _base_url() -> str:
    value = os.getenv("PARALLAX_LIVE_API_URL", "").strip().rstrip("/")
    if not value:
        print("PARALLAX_LIVE_API_URL is required.", file=sys.stderr)
        raise SystemExit(2)
    return value


def _session_id() -> str:
    return os.getenv("PARALLAX_LIVE_SESSION_ID", "").strip() or f"live-smoke-{uuid4()}"


def _request_json(base_url: str, path: str, session_id: str, query: dict | None = None) -> dict:
    url = f"{base_url}{path}"
    if query:
        url = f"{url}?{urlencode(query)}"

    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "X-Parallax-Session-Id": session_id,
            "User-Agent": "ParallaxLiveSmoke/1.0",
        },
        method="GET",
    )

    try:
        with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            body = response.read().decode("utf-8")
            status = response.status
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise AssertionError(f"{path} returned HTTP {exc.code}: {body[:500]}") from exc
    except URLError as exc:
        raise AssertionError(f"{path} failed before response: {exc}") from exc

    if status < 200 or status >= 300:
        raise AssertionError(f"{path} returned HTTP {status}: {body[:500]}")

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise AssertionError(f"{path} did not return JSON: {body[:500]}") from exc


def main() -> int:
    base_url = _base_url()
    session_id = _session_id()

    health = _request_json(base_url, "/api/v1/health", session_id)
    assert health.get("status") in {"ok", "healthy"}, health

    db = _request_json(base_url, "/api/v1/health/db", session_id)
    assert db.get("status") in {"ok", "fallback"}, db

    me = _request_json(base_url, "/api/v1/auth/me", session_id)
    assert me.get("session", {}).get("id") == session_id, me

    feed = _request_json(base_url, "/api/v1/feed", session_id, {"limit": 5})
    assert isinstance(feed.get("cards"), list), feed

    alerts = _request_json(base_url, "/api/v1/alerts/unread-count", session_id)
    assert "unread_count" in alerts, alerts

    briefs = _request_json(base_url, "/api/v1/public/briefs", session_id)
    brief_list = briefs.get("briefs")
    assert isinstance(brief_list, list) and len(brief_list) >= 3, briefs

    print(
        "live smoke passed",
        {
            "base_url": base_url,
            "session_id": session_id,
            "db_status": db.get("status"),
            "feed_cards": len(feed.get("cards", [])),
            "briefs": len(brief_list),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
