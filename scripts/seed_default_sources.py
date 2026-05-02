"""
Seed the Phase 2 default news source database.

This script is idempotent. It uses the configured DATABASE_URL when Postgres is
enabled, otherwise it seeds the in-memory fallback for local inspection.
"""

from __future__ import annotations

import argparse
import json

from app.services.default_sources import preview_default_sources, seed_default_sources


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed default Parallax news sources.")
    parser.add_argument("--preview", action="store_true", help="Print the catalog without writing records.")
    parser.add_argument("--limit", type=int, default=None, help="Seed only the first N default sources.")
    args = parser.parse_args()

    result = preview_default_sources(limit=args.limit) if args.preview else seed_default_sources(limit=args.limit)
    print(json.dumps({"summary": result["summary"], "limitations": result["limitations"]}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
