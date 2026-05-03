"""
Run scheduled source ingestion for active RSS source feeds.

Intended for cron, platform schedulers, or one-off maintenance runs. The script
uses the configured DATABASE_URL and is safe to run repeatedly because article
persistence deduplicates by source URL.
"""

from __future__ import annotations

import argparse
import asyncio
import json

from app.core.session import ANONYMOUS_SESSION_ID
from app.services.source_sync import sync_active_source_feeds


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync active Parallax source feeds.")
    parser.add_argument("--session-id", default=ANONYMOUS_SESSION_ID, help="Session id for created feed cards.")
    parser.add_argument("--source-limit", type=int, default=50, help="Maximum sources to inspect.")
    parser.add_argument("--feed-limit", type=int, default=100, help="Maximum RSS feeds to fetch.")
    parser.add_argument("--article-limit", type=int, default=10, help="Maximum articles per RSS feed.")
    parser.add_argument("--card-limit", type=int, default=25, help="Maximum feed cards to create.")
    args = parser.parse_args()

    result = asyncio.run(
        sync_active_source_feeds(
            session_id=args.session_id,
            source_limit=args.source_limit,
            feed_limit=args.feed_limit,
            article_limit=args.article_limit,
            card_limit=args.card_limit,
        )
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "source_count": result["source_count"],
                "feed_count": result["feed_count"],
                "synced_feed_count": result["synced_feed_count"],
                "article_count": result["article_count"],
                "card_count": result["card_count"],
                "error_count": result["error_count"],
                "sync_run_id": result.get("sync_run_id"),
                "limits": result["limits"],
            },
            indent=2,
        )
    )
    return 1 if result["error_count"] and not result["article_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
