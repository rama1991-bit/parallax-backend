"""
Analyze pending ingested source articles.

This is scheduler-friendly glue for turning freshly ingested RSS/homepage
articles into structured intelligence before aggregation and clustering.
"""

from __future__ import annotations

import argparse
import asyncio
import json

from app.core.session import ANONYMOUS_SESSION_ID
from app.services.ingested_analysis import analyze_pending_ingested_articles


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze pending Parallax source articles.")
    parser.add_argument("--session-id", default=ANONYMOUS_SESSION_ID, help="Session id for generated feed cards.")
    parser.add_argument("--source-id", default=None, help="Optional source id to limit analysis.")
    parser.add_argument("--limit", type=int, default=25, help="Maximum pending articles to analyze.")
    parser.add_argument("--include-failed", action="store_true", help="Retry articles currently marked failed.")
    args = parser.parse_args()

    result = asyncio.run(
        analyze_pending_ingested_articles(
            session_id=args.session_id,
            source_id=args.source_id,
            limit=args.limit,
            include_failed=args.include_failed,
        )
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "source_id": result["source_id"],
                "candidate_count": result["candidate_count"],
                "analyzed_count": result["analyzed_count"],
                "failed_count": result["failed_count"],
                "skipped_count": result["skipped_count"],
                "summary": result["summary"],
                "errors": result["errors"],
            },
            indent=2,
        )
    )
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
