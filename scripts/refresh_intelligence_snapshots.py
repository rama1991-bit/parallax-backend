"""
Refresh source and topic intelligence aggregation snapshots.

Intended for cron, platform schedulers, or one-off maintenance runs after source
ingestion. The command stores compact snapshots so source and topic pages can
load intelligence summaries without recomputing every request.
"""

from __future__ import annotations

import argparse
import asyncio
import json

from app.core.session import ANONYMOUS_SESSION_ID
from app.services.intelligence_aggregation import refresh_intelligence_snapshots


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh Parallax intelligence snapshots.")
    parser.add_argument("--session-id", default=ANONYMOUS_SESSION_ID, help="Session id for topic monitors.")
    parser.add_argument("--source-limit", type=int, default=50, help="Maximum sources to refresh.")
    parser.add_argument("--topic-limit", type=int, default=50, help="Maximum topics to refresh.")
    parser.add_argument("--article-limit", type=int, default=100, help="Maximum articles per subject sample.")
    args = parser.parse_args()

    result = asyncio.run(
        refresh_intelligence_snapshots(
            session_id=args.session_id,
            source_limit=args.source_limit,
            topic_limit=args.topic_limit,
            article_limit=args.article_limit,
        )
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "source_count": result["source_count"],
                "topic_count": result["topic_count"],
                "error_count": result["error_count"],
                "errors": result["errors"],
            },
            indent=2,
        )
    )
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
