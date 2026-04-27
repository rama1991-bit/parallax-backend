from fastapi import APIRouter

from app.api.v1.routes import health, feed, analyze, topics, alerts, briefs, authors

api_router = APIRouter()

api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(feed.router, prefix="/feed", tags=["feed"])
api_router.include_router(analyze.router, prefix="/analyze", tags=["analyze"])
api_router.include_router(topics.router, prefix="/topics", tags=["topics"])
api_router.include_router(alerts.router, prefix="/alerts", tags=["alerts"])
api_router.include_router(briefs.router, prefix="/public", tags=["public"])
api_router.include_router(authors.router, prefix="/authors", tags=["authors"])
