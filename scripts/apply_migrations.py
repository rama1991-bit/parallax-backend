from pathlib import Path
import sys

import psycopg2

from app.core.config import settings


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "migrations"


def database_url() -> str:
    url = settings.DATABASE_URL or ""
    if url.startswith("postgres://"):
        return "postgresql://" + url.removeprefix("postgres://")
    if url.startswith("postgresql+asyncpg://"):
        return "postgresql://" + url.removeprefix("postgresql+asyncpg://")
    if url.startswith("postgresql+psycopg2://"):
        return "postgresql://" + url.removeprefix("postgresql+psycopg2://")
    return url


def connect():
    kwargs = {"connect_timeout": 10}
    if settings.DATABASE_SSLMODE:
        kwargs["sslmode"] = settings.DATABASE_SSLMODE
    return psycopg2.connect(database_url(), **kwargs)


def main() -> int:
    url = database_url()
    if not url.startswith("postgresql://"):
        print("DATABASE_URL must be a Postgres/Supabase URL to apply migrations.")
        return 1

    migration_paths = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_paths:
        print("No SQL migrations found.")
        return 0

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists public.schema_migrations (
                    version text primary key,
                    applied_at timestamptz not null default now()
                );
                """
            )

            for path in migration_paths:
                version = path.stem
                cur.execute(
                    "select 1 from public.schema_migrations where version = %s;",
                    (version,),
                )
                if cur.fetchone():
                    print(f"Skipping {path.name}; already applied.")
                    continue

                print(f"Applying {path.name}...")
                cur.execute(path.read_text(encoding="utf-8"))
                cur.execute(
                    "insert into public.schema_migrations (version) values (%s);",
                    (version,),
                )

    print("Migrations complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
