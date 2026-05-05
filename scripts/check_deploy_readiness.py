from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

from app.core.config import settings


SCHEDULER_COMMAND = (
    "python scripts/sync_active_sources.py --source-limit 50 --feed-limit 100 "
    "--article-limit 10 --card-limit 25"
)
INTELLIGENCE_SCHEDULER_COMMAND = (
    "python scripts/refresh_intelligence_snapshots.py --source-limit 50 "
    "--topic-limit 50 --article-limit 100 --card-limit 50"
)
EVENT_CLUSTER_SCHEDULER_COMMAND = (
    "python scripts/refresh_event_clusters.py --article-limit 250 "
    "--cluster-limit 100 --card-limit 50"
)
PIPELINE_SCHEDULER_COMMAND = (
    "python scripts/run_intelligence_pipeline.py --source-limit 50 --feed-limit 100 "
    "--sync-article-limit 10 --sync-card-limit 25 --intelligence-source-limit 50 "
    "--topic-limit 50 --intelligence-article-limit 100 --intelligence-card-limit 50 "
    "--cluster-article-limit 250 --cluster-limit 100 --cluster-card-limit 50"
)

PRODUCTION_ENV_VARS = [
    "ENV=production",
    "DEBUG=false",
    "SECRET_KEY=<long-random-secret>",
    "ADMIN_API_KEY=<long-random-admin-key>",
    "DATABASE_URL=postgresql://...",
    "DATABASE_SSLMODE=require",
    "FRONTEND_URL=https://your-frontend.example.com",
    "AI_PROVIDER=heuristic|openai",
    "OPS_NOTIFICATIONS_ENABLED=false|true",
    "OPS_WEBHOOK_URL=https://hooks.example.com/parallax",
    "OPS_NOTIFICATION_MIN_SEVERITY=warning",
]


@dataclass
class Issue:
    severity: str
    key: str
    message: str
    recommendation: str

    def as_dict(self) -> dict[str, str]:
        return {
            "severity": self.severity,
            "key": self.key,
            "message": self.message,
            "recommendation": self.recommendation,
        }


def _compact(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _is_truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _is_local_host(hostname: str | None) -> bool:
    if not hostname:
        return False
    host = hostname.lower()
    return host in {"localhost", "127.0.0.1", "::1"} or host.endswith(".local")


def _has_sslmode(database_url: str) -> bool:
    parsed = urlparse(database_url)
    query = parse_qs(parsed.query)
    sslmode = query.get("sslmode", [""])[0].lower()
    return sslmode in {"require", "verify-ca", "verify-full"}


def _origin_is_valid(origin: str) -> bool:
    parsed = urlparse(origin)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _webhook_url_is_valid(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _issue(
    issues: list[Issue],
    severity: str,
    key: str,
    message: str,
    recommendation: str,
) -> None:
    issues.append(
        Issue(
            severity=severity,
            key=key,
            message=message,
            recommendation=recommendation,
        )
    )


def _fail_or_warn(
    errors: list[Issue],
    warnings: list[Issue],
    production_like: bool,
    key: str,
    message: str,
    recommendation: str,
) -> None:
    target = errors if production_like else warnings
    severity = "error" if production_like else "warning"
    _issue(target, severity, key, message, recommendation)


def build_report(strict: bool = False) -> dict[str, Any]:
    errors: list[Issue] = []
    warnings: list[Issue] = []

    env = _compact(settings.ENV).lower() or "development"
    production_like = strict or env == "production"
    database_url = _compact(settings.DATABASE_URL)
    database_scheme = urlparse(database_url).scheme.lower()
    frontend_url = _compact(settings.FRONTEND_URL)
    frontend = urlparse(frontend_url)
    admin_api_key = _compact(settings.ADMIN_API_KEY)
    secret_key = _compact(settings.SECRET_KEY)
    cors_origins = [
        origin.strip()
        for origin in _compact(settings.BACKEND_CORS_ORIGINS).split(",")
        if origin.strip()
    ]
    ai_provider = _compact(settings.AI_PROVIDER).lower() or "heuristic"
    retrieval_provider = _compact(settings.RETRIEVAL_PROVIDER).lower() or "mock"
    ops_notifications_enabled = _is_truthy(settings.OPS_NOTIFICATIONS_ENABLED)
    ops_webhook_url = _compact(settings.OPS_WEBHOOK_URL)
    ops_webhook = urlparse(ops_webhook_url)
    ops_min_severity = _compact(settings.OPS_NOTIFICATION_MIN_SEVERITY).lower() or "warning"

    if production_like and env != "production":
        _issue(
            warnings,
            "warning",
            "ENV",
            f"Strict mode is running with ENV={settings.ENV!r}.",
            "Set ENV=production in deployed backend environments.",
        )

    if production_like and _is_truthy(settings.DEBUG):
        _issue(
            errors,
            "error",
            "DEBUG",
            "DEBUG is enabled for a production-like readiness check.",
            "Set DEBUG=false before deploying.",
        )

    if secret_key in {"", "change-me"} or len(secret_key) < 32:
        _fail_or_warn(
            errors,
            warnings,
            production_like,
            "SECRET_KEY",
            "SECRET_KEY is missing, still the default, or shorter than 32 characters.",
            "Use a long random value because it signs public brief tokens.",
        )

    if not admin_api_key or len(admin_api_key) < 24:
        _fail_or_warn(
            errors,
            warnings,
            production_like,
            "ADMIN_API_KEY",
            "ADMIN_API_KEY is missing or shorter than 24 characters.",
            "Set a long random admin key for protected source seed and sync routes.",
        )

    if database_scheme not in {"postgres", "postgresql"}:
        _fail_or_warn(
            errors,
            warnings,
            production_like,
            "DATABASE_URL",
            f"DATABASE_URL uses {database_scheme or 'no'} scheme.",
            "Use a Postgres/Supabase DATABASE_URL for persisted production data.",
        )

    sslmode = _compact(settings.DATABASE_SSLMODE).lower()
    if database_scheme in {"postgres", "postgresql"} and sslmode not in {
        "require",
        "verify-ca",
        "verify-full",
    } and not _has_sslmode(database_url):
        _fail_or_warn(
            errors,
            warnings,
            production_like,
            "DATABASE_SSLMODE",
            "Postgres SSL mode is not set to require/verify-ca/verify-full.",
            "Set DATABASE_SSLMODE=require for Supabase unless sslmode is already in DATABASE_URL.",
        )

    if not _origin_is_valid(frontend_url):
        _fail_or_warn(
            errors,
            warnings,
            production_like,
            "FRONTEND_URL",
            f"FRONTEND_URL is not a valid origin: {frontend_url!r}.",
            "Set FRONTEND_URL to the deployed frontend origin.",
        )
    elif production_like and (
        frontend.scheme != "https" or _is_local_host(frontend.hostname)
    ):
        _issue(
            errors,
            "error",
            "FRONTEND_URL",
            "FRONTEND_URL is local or not HTTPS for a production-like readiness check.",
            "Use the public HTTPS frontend origin in production.",
        )

    for origin in cors_origins:
        parsed = urlparse(origin)
        if not _origin_is_valid(origin):
            _issue(
                errors if production_like else warnings,
                "error" if production_like else "warning",
                "BACKEND_CORS_ORIGINS",
                f"Invalid CORS origin: {origin!r}.",
                "Use comma-separated HTTP/HTTPS origins without paths.",
            )
        elif production_like and (parsed.scheme != "https" or _is_local_host(parsed.hostname)):
            _issue(
                warnings,
                "warning",
                "BACKEND_CORS_ORIGINS",
                f"CORS origin is local or not HTTPS: {origin!r}.",
                "Keep only production HTTPS origins in production unless this is a controlled staging deploy.",
            )

    if ai_provider not in {"heuristic", "openai"}:
        _issue(
            errors,
            "error",
            "AI_PROVIDER",
            f"Unsupported AI_PROVIDER: {settings.AI_PROVIDER!r}.",
            "Use AI_PROVIDER=heuristic or AI_PROVIDER=openai.",
        )
    elif ai_provider == "openai":
        if not _compact(settings.OPENAI_API_KEY):
            _fail_or_warn(
                errors,
                warnings,
                production_like,
                "OPENAI_API_KEY",
                "AI_PROVIDER=openai but OPENAI_API_KEY is missing.",
                "Set OPENAI_API_KEY before enabling OpenAI analysis.",
            )
        if not _compact(settings.OPENAI_MODEL):
            _fail_or_warn(
                errors,
                warnings,
                production_like,
                "OPENAI_MODEL",
                "AI_PROVIDER=openai but OPENAI_MODEL is missing.",
                "Set OPENAI_MODEL to the deployed analysis model.",
            )
    elif production_like:
        _issue(
            warnings,
            "warning",
            "AI_PROVIDER",
            "AI_PROVIDER=heuristic is deterministic and MVP-grade.",
            "Use heuristic only intentionally; set AI_PROVIDER=openai for model-backed production analysis.",
        )

    if settings.ARTICLE_FETCH_TIMEOUT_SECONDS <= 0:
        _issue(
            errors,
            "error",
            "ARTICLE_FETCH_TIMEOUT_SECONDS",
            "ARTICLE_FETCH_TIMEOUT_SECONDS must be greater than zero.",
            "Use a small positive timeout such as 15.",
        )

    if settings.ARTICLE_MAX_CHARS < 2000:
        _issue(
            errors,
            "error",
            "ARTICLE_MAX_CHARS",
            "ARTICLE_MAX_CHARS is too low for useful article analysis.",
            "Use at least 2000; the default is 12000.",
        )

    if settings.ANALYZE_DAILY_LIMIT <= 0:
        _issue(
            errors,
            "error",
            "ANALYZE_DAILY_LIMIT",
            "ANALYZE_DAILY_LIMIT must be greater than zero.",
            "Use a positive per-session quota.",
        )

    if settings.ANALYZE_QUOTA_WINDOW_SECONDS <= 0:
        _issue(
            errors,
            "error",
            "ANALYZE_QUOTA_WINDOW_SECONDS",
            "ANALYZE_QUOTA_WINDOW_SECONDS must be greater than zero.",
            "Use a positive quota window such as 86400.",
        )

    if settings.ANALYZE_COOLDOWN_SECONDS < 0:
        _issue(
            errors,
            "error",
            "ANALYZE_COOLDOWN_SECONDS",
            "ANALYZE_COOLDOWN_SECONDS cannot be negative.",
            "Use zero to disable cooldown or a positive wait interval.",
        )

    if settings.EXTERNAL_RETRIEVAL_ENABLED and retrieval_provider == "mock":
        _issue(
            warnings,
            "warning",
            "RETRIEVAL_PROVIDER",
            "EXTERNAL_RETRIEVAL_ENABLED=true but RETRIEVAL_PROVIDER=mock.",
            "Configure a real retrieval provider before relying on external OSINT enrichment.",
        )
    if retrieval_provider not in {"mock", "none", "web", "duckduckgo", "public_web"}:
        _fail_or_warn(
            errors,
            warnings,
            production_like,
            "RETRIEVAL_PROVIDER",
            f"Unsupported RETRIEVAL_PROVIDER: {settings.RETRIEVAL_PROVIDER!r}.",
            "Use RETRIEVAL_PROVIDER=mock for deterministic probes or RETRIEVAL_PROVIDER=web for public web search.",
        )

    if ops_min_severity not in {"info", "warning", "critical"}:
        _issue(
            errors,
            "error",
            "OPS_NOTIFICATION_MIN_SEVERITY",
            f"Unsupported OPS_NOTIFICATION_MIN_SEVERITY: {settings.OPS_NOTIFICATION_MIN_SEVERITY!r}.",
            "Use info, warning, or critical.",
        )

    if ops_notifications_enabled:
        if not ops_webhook_url or not _webhook_url_is_valid(ops_webhook_url):
            _fail_or_warn(
                errors,
                warnings,
                production_like,
                "OPS_WEBHOOK_URL",
                "OPS_NOTIFICATIONS_ENABLED=true but OPS_WEBHOOK_URL is missing or invalid.",
                "Set OPS_WEBHOOK_URL to an HTTP/HTTPS endpoint that can receive source operational alerts.",
            )
        elif production_like and (
            ops_webhook.scheme != "https" or _is_local_host(ops_webhook.hostname)
        ):
            _issue(
                errors,
                "error",
                "OPS_WEBHOOK_URL",
                "OPS_WEBHOOK_URL is local or not HTTPS for a production-like readiness check.",
                "Use a public HTTPS webhook endpoint for production alert delivery.",
            )
        if not _compact(settings.OPS_WEBHOOK_SECRET):
            _issue(
                warnings,
                "warning",
                "OPS_WEBHOOK_SECRET",
                "OPS notifications are enabled without a webhook signing secret.",
                "Set OPS_WEBHOOK_SECRET so receivers can verify X-Parallax-Signature.",
            )
    elif production_like:
        _issue(
            warnings,
            "warning",
            "OPS_NOTIFICATIONS_ENABLED",
            "Source operational alerts will remain internal only.",
            "Set OPS_NOTIFICATIONS_ENABLED=true and OPS_WEBHOOK_URL when production alert delivery is required.",
        )

    if settings.OPS_NOTIFICATION_TIMEOUT_SECONDS <= 0:
        _issue(
            errors,
            "error",
            "OPS_NOTIFICATION_TIMEOUT_SECONDS",
            "OPS_NOTIFICATION_TIMEOUT_SECONDS must be greater than zero.",
            "Use a small positive timeout such as 5.",
        )

    status = "ready" if not errors else "blocked"
    return {
        "status": status,
        "environment": env,
        "strict": strict,
        "checks": {
            "error_count": len(errors),
            "warning_count": len(warnings),
            "production_like": production_like,
        },
        "errors": [issue.as_dict() for issue in errors],
        "warnings": [issue.as_dict() for issue in warnings],
        "scheduler": {
            "purpose": "Run active RSS source ingestion on a recurring worker or platform scheduler.",
            "recommended_interval": "15 minutes",
            "command": SCHEDULER_COMMAND,
            "pipeline_purpose": "Run ingestion, source/topic snapshots, and event clusters in one scheduler job.",
            "pipeline_interval": "15-60 minutes, depending on feed volume",
            "pipeline_command": PIPELINE_SCHEDULER_COMMAND,
            "pipeline_api_alternative": "POST /api/v1/intelligence/pipeline/run with X-Parallax-Admin-Key",
            "follow_up_purpose": "Refresh source/topic intelligence snapshots after ingestion batches.",
            "follow_up_interval": "15-60 minutes after ingestion",
            "follow_up_command": INTELLIGENCE_SCHEDULER_COMMAND,
            "follow_up_api_alternative": "POST /api/v1/intelligence/refresh with X-Parallax-Admin-Key",
            "cluster_follow_up_purpose": "Refresh source/topic/node event clusters after intelligence snapshots.",
            "cluster_follow_up_interval": "15-60 minutes after intelligence refresh",
            "cluster_follow_up_command": EVENT_CLUSTER_SCHEDULER_COMMAND,
            "cluster_follow_up_api_alternative": "POST /api/v1/intelligence/clusters/refresh with X-Parallax-Admin-Key",
            "admin_api_alternative": "POST /api/v1/sources/sync-active with X-Parallax-Admin-Key",
        },
        "required_env": PRODUCTION_ENV_VARS,
    }


def print_text(report: dict[str, Any]) -> None:
    print(f"Deploy readiness: {report['status']}")
    print(f"Environment: {report['environment']} (strict={report['strict']})")
    print(
        "Checks: "
        f"{report['checks']['error_count']} error(s), "
        f"{report['checks']['warning_count']} warning(s)"
    )

    if report["errors"]:
        print("\nErrors:")
        for issue in report["errors"]:
            print(f"- [{issue['key']}] {issue['message']}")
            print(f"  Recommendation: {issue['recommendation']}")

    if report["warnings"]:
        print("\nWarnings:")
        for issue in report["warnings"]:
            print(f"- [{issue['key']}] {issue['message']}")
            print(f"  Recommendation: {issue['recommendation']}")

    print("\nScheduler:")
    print(f"- Interval: {report['scheduler']['recommended_interval']}")
    print(f"- Command: {report['scheduler']['command']}")
    print(f"- Pipeline interval: {report['scheduler']['pipeline_interval']}")
    print(f"- Pipeline command: {report['scheduler']['pipeline_command']}")
    print(f"- Pipeline API alternative: {report['scheduler']['pipeline_api_alternative']}")
    print(f"- Follow-up interval: {report['scheduler']['follow_up_interval']}")
    print(f"- Follow-up command: {report['scheduler']['follow_up_command']}")
    print(f"- Follow-up API alternative: {report['scheduler']['follow_up_api_alternative']}")
    print(f"- Cluster follow-up interval: {report['scheduler']['cluster_follow_up_interval']}")
    print(f"- Cluster follow-up command: {report['scheduler']['cluster_follow_up_command']}")
    print(f"- Cluster follow-up API alternative: {report['scheduler']['cluster_follow_up_api_alternative']}")
    print(f"- API alternative: {report['scheduler']['admin_api_alternative']}")

    print("\nProduction env:")
    for env_var in report["required_env"]:
        print(f"- {env_var}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Parallax backend deploy readiness.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat production-only requirements as blocking even when ENV is not production.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(strict=args.strict)

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_text(report)

    return 1 if report["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
