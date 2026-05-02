from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import urlparse
import re

import feedparser
import httpx

from app.core.config import settings


class RSSSyncError(Exception):
    pass


class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.chunks: list[str] = []

    def handle_data(self, data: str):
        cleaned = re.sub(r"\s+", " ", data).strip()
        if cleaned:
            self.chunks.append(cleaned)


def _strip_html(value: str | None) -> str:
    if not value:
        return ""
    stripper = _HTMLStripper()
    stripper.feed(value)
    return " ".join(stripper.chunks).strip()[:900]


def _parse_published(value: str | None):
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        if not parsed.tzinfo:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.isoformat()
    except (TypeError, ValueError):
        return None


def _domain(url: str | None) -> str | None:
    if not url:
        return None
    return urlparse(url).netloc.lower().removeprefix("www.") or None


async def parse_rss_feed(url: str, limit: int = 20) -> dict:
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=settings.ARTICLE_FETCH_TIMEOUT_SECONDS,
            headers={"User-Agent": "ParallaxRSSBot/0.1"},
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise RSSSyncError(f"RSS fetch failed with HTTP {exc.response.status_code}.") from exc
    except httpx.HTTPError as exc:
        raise RSSSyncError("RSS fetch failed before the feed could be read.") from exc

    parsed = feedparser.parse(response.content)
    if parsed.bozo and not parsed.entries:
        raise RSSSyncError("RSS feed could not be parsed.")

    feed_title = parsed.feed.get("title") or _domain(str(response.url)) or str(response.url)
    feed_description = _strip_html(parsed.feed.get("subtitle") or parsed.feed.get("description"))
    items = []

    for entry in parsed.entries[: max(1, min(limit, 50))]:
        link = entry.get("link")
        title = re.sub(r"\s+", " ", entry.get("title", "")).strip() or link or "Untitled feed item"
        summary = _strip_html(entry.get("summary") or entry.get("description"))
        published = _parse_published(entry.get("published") or entry.get("updated"))
        external_id = entry.get("id") or entry.get("guid") or link

        items.append(
            {
                "external_id": str(external_id)[:500] if external_id else None,
                "title": title[:300],
                "summary": summary,
                "url": link,
                "source": feed_title,
                "published_at": published,
                "raw": {
                    "title": title,
                    "link": link,
                    "published": entry.get("published"),
                    "updated": entry.get("updated"),
                    "id": entry.get("id"),
                },
            }
        )

    return {
        "url": str(response.url),
        "title": feed_title[:180],
        "description": feed_description,
        "items": items,
    }
