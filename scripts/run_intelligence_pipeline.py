"""
Run the full scheduled Parallax intelligence pipeline.

This orchestrates active source ingestion, source/topic intelligence snapshots,
and event clustering in the same order an external scheduler should use.
"""

from __future__ import annotations

import argparse
import asyncio
import json

from app.core.session import ANONYMOUS_SESSION_ID
from app.services.intelligence_pipeline import run_intelligence_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Parallax intelligence automation pipeline.")
    parser.add_argument("--session-id", default=ANONYMOUS_SESSION_ID, help="Session id for generated feed cards.")
    parser.add_argument("--source-limit", type=int, default=50, help="Maximum sources to inspect.")
    parser.add_argument("--feed-limit", type=int, default=100, help="Maximum RSS feeds to fetch.")
    parser.add_argument("--sync-article-limit", type=int, default=10, help="Maximum articles per RSS feed.")
    parser.add_argument("--sync-card-limit", type=int, default=25, help="Maximum ingestion feed cards.")
    parser.add_argument("--intelligence-source-limit", type=int, default=50, help="Maximum source snapshots.")
    parser.add_argument("--topic-limit", type=int, default=50, help="Maximum topic snapshots.")
    parser.add_argument("--intelligence-article-limit", type=int, default=100, help="Maximum articles per snapshot.")
    parser.add_argument("--intelligence-card-limit", type=int, default=50, help="Maximum intelligence feed cards.")
    parser.add_argument("--cluster-article-limit", type=int, default=250, help="Maximum articles for event clustering.")
    parser.add_argument("--cluster-limit", type=int, default=100, help="Maximum event clusters.")
    parser.add_argument("--cluster-card-limit", type=int, default=50, help="Maximum event-cluster feed cards.")
    parser.add_argument("--skip-sync", action="store_true", help="Skip source ingestion.")
    parser.add_argument("--skip-intelligence", action="store_true", help="Skip source/topic intelligence refresh.")
    parser.add_argument("--skip-clusters", action="store_true", help="Skip event clustering.")
    args = parser.parse_args()

    result = asyncio.run(
        run_intelligence_pipeline(
            session_id=args.session_id,
            source_limit=args.source_limit,
            feed_limit=args.feed_limit,
            sync_article_limit=args.sync_article_limit,
            sync_card_limit=args.sync_card_limit,
            intelligence_source_limit=args.intelligence_source_limit,
            topic_limit=args.topic_limit,
            intelligence_article_limit=args.intelligence_article_limit,
            intelligence_card_limit=args.intelligence_card_limit,
            cluster_article_limit=args.cluster_article_limit,
            cluster_limit=args.cluster_limit,
            cluster_card_limit=args.cluster_card_limit,
            skip_sync=args.skip_sync,
            skip_intelligence=args.skip_intelligence,
            skip_clusters=args.skip_clusters,
        )
    )
    print(
        json.dumps(
            {
                "pipeline_id": result["pipeline_id"],
                "status": result["status"],
                "duration_ms": result["duration_ms"],
                "summary": result["summary"],
                "phases": result["phases"],
                "errors": result["errors"],
            },
            indent=2,
        )
    )
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
