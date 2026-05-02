from fastapi import APIRouter

from app.api.v1.routes import (
    health,
    feed,
    feeds,
    analyze,
    topics,
    alerts,
    briefs,
    authors,
    reports,
    auth,
    onboarding,
    saved_reports,
    compare,
)

api_router = APIRouter()

api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(feed.router, prefix="/feed", tags=["feed"])
api_router.include_router(feeds.router, prefix="/feeds", tags=["feeds"])
api_router.include_router(analyze.router, prefix="/analyze", tags=["analyze"])
api_router.include_router(compare.router, prefix="/compare", tags=["compare"])
api_router.include_router(reports.router, prefix="/reports", tags=["reports"])
api_router.include_router(saved_reports.router, prefix="/saved-reports", tags=["reports"])
api_router.include_router(onboarding.router, prefix="/onboarding", tags=["onboarding"])
api_router.include_router(topics.router, prefix="/topics", tags=["topics"])
api_router.include_router(alerts.router, prefix="/alerts", tags=["alerts"])
api_router.include_router(briefs.router, prefix="/public", tags=["public"])
api_router.include_router(authors.router, prefix="/authors", tags=["authors"])
