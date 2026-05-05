"""
Refresh event clusters from ingested articles.

Intended for cron, platform schedulers, or a one-off maintenance run after RSS
ingestion and source/topic intelligence refreshes. The command groups related
articles into event clusters and can create feed cards for cross-source,
cross-language, divergence, and missing-coverage signals.
"""

from __future__ import annotations

import argparse
import json

from app.core.session import ANONYMOUS_SESSION_ID
from app.services.event_clustering import refresh_event_clusters


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh Parallax event clusters.")
    parser.add_argument("--session-id", default=ANONYMOUS_SESSION_ID, help="Session id for feed cards.")
    parser.add_argument("--article-limit", type=int, default=250, help="Maximum ingested articles to cluster.")
    parser.add_argument("--cluster-limit", type=int, default=100, help="Maximum clusters to persist.")
    parser.add_argument("--card-limit", type=int, default=50, help="Maximum event-cluster feed cards to create.")
    parser.add_argument("--no-cards", action="store_true", help="Refresh clusters without creating feed cards.")
    args = parser.parse_args()

    result = refresh_event_clusters(
        session_id=args.session_id,
        article_limit=args.article_limit,
        cluster_limit=args.cluster_limit,
        card_limit=args.card_limit,
        create_cards=not args.no_cards,
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "cluster_count": result["cluster_count"],
                "article_count": result["article_count"],
                "card_count": result["card_count"],
                "error_count": result["error_count"],
                "run_id": result.get("run", {}).get("id"),
                "errors": result["errors"],
            },
            indent=2,
        )
    )
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
