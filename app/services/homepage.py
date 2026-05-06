from __future__ import annotations

from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, urlunparse
import re

import httpx

from app.core.config import settings


class HomepageSyncError(Exception):
    pass


class _HomepageLinkParser(HTMLParser):
    _ignored_tags = {"script", "style", "noscript", "svg", "canvas", "iframe"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._ignore_depth = 0
        self._in_title = False
        self._active_link: dict | None = None
        self.title_chunks: list[str] = []
        self.links: list[dict] = []

    def handle_starttag(self, tag: str, attrs):
        tag = tag.lower()
        attrs_map = {key.lower(): value or "" for key, value in attrs}
        if tag in self._ignored_tags:
            self._ignore_depth += 1
            return
        if tag == "title":
            self._in_title = True
        if tag == "a":
            href = attrs_map.get("href", "").strip()
            if href:
                self._active_link = {
                    "href": href,
                    "text": attrs_map.get("title") or attrs_map.get("aria-label") or "",
                }

    def handle_endtag(self, tag: str):
        tag = tag.lower()
        if tag in self._ignored_tags and self._ignore_depth > 0:
            self._ignore_depth -= 1
            return
        if tag == "title":
            self._in_title = False
        if tag == "a" and self._active_link:
            self.links.append(self._active_link)
            self._active_link = None

    def handle_data(self, data: str):
        if self._ignore_depth:
            return
        cleaned = _normalize_space(data)
        if not cleaned:
            return
        if self._in_title:
            self.title_chunks.append(cleaned)
        if self._active_link:
            current = self._active_link.get("text") or ""
            self._active_link["text"] = _normalize_space(f"{current} {cleaned}")


def _normalize_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _domain(url: str) -> str:
    return urlparse(url).netloc.lower().removeprefix("www.")


def _canonical_url(base_url: str, href: str) -> str | None:
    if not href or href.startswith(("mailto:", "tel:", "javascript:")):
        return None
    absolute = urljoin(base_url, href)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    parsed = parsed._replace(fragment="")
    return urlunparse(parsed)


def _looks_like_article(homepage_url: str, url: str, text: str) -> bool:
    parsed = urlparse(url)
    homepage_domain = _domain(homepage_url)
    link_domain = _domain(url)
    if link_domain != homepage_domain:
        return False

    path = parsed.path.lower().rstrip("/")
    if not path or path == "/":
        return False
    if re.search(r"\.(jpg|jpeg|png|gif|webp|svg|pdf|zip|mp3|mp4|mov)$", path):
        return False
    blocked = (
        "/about",
        "/account",
        "/advert",
        "/author",
        "/category",
        "/contact",
        "/login",
        "/newsletter",
        "/privacy",
        "/search",
        "/signin",
        "/subscribe",
        "/tag",
        "/terms",
        "/topics",
    )
    if any(part in path for part in blocked):
        return False

    slug = path.rsplit("/", 1)[-1]
    score = 0
    if re.search(r"/20\d{2}([/-]\d{1,2})?", path):
        score += 2
    if len(_normalize_space(text)) >= 20:
        score += 2
    if "-" in slug and len(slug) >= 14:
        score += 2
    if path.count("/") >= 2:
        score += 1
    if len(slug) >= 18:
        score += 1
    return score >= 2


def _title_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    slug = path.rsplit("/", 1)[-1] if path else _domain(url)
    return _normalize_space(slug.replace("-", " ").replace("_", " ")).title() or _domain(url)


async def parse_homepage_feed(url: str, limit: int = 20) -> dict:
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=settings.ARTICLE_FETCH_TIMEOUT_SECONDS,
            headers={"User-Agent": "ParallaxHomepageBot/0.1"},
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise HomepageSyncError(f"Homepage fetch failed with HTTP {exc.response.status_code}.") from exc
    except httpx.HTTPError as exc:
        raise HomepageSyncError("Homepage fetch failed before links could be read.") from exc

    content_type = response.headers.get("content-type", "").lower()
    if "html" not in content_type and not response.text.lstrip().startswith("<"):
        raise HomepageSyncError("Homepage did not return readable HTML.")

    final_url = str(response.url)
    parser = _HomepageLinkParser()
    parser.feed(response.text)
    feed_title = _normalize_space(" ".join(parser.title_chunks)) or _domain(final_url)

    items = []
    seen = set()
    for link in parser.links:
        absolute = _canonical_url(final_url, link.get("href") or "")
        if not absolute or absolute in seen:
            continue
        link_text = _normalize_space(link.get("text") or "")
        if not _looks_like_article(final_url, absolute, link_text):
            continue
        seen.add(absolute)
        title = link_text[:300] if link_text else _title_from_url(absolute)[:300]
        items.append(
            {
                "external_id": absolute[:500],
                "title": title or _title_from_url(absolute)[:300],
                "summary": f"Discovered from homepage: {feed_title}"[:900],
                "url": absolute,
                "source": feed_title,
                "published_at": None,
                "raw": {
                    "homepage_url": final_url,
                    "link_text": link_text,
                    "discovery_method": "homepage_link_heuristic",
                },
            }
        )
        if len(items) >= max(1, min(limit, 50)):
            break

    return {
        "url": final_url,
        "title": feed_title[:180],
        "description": "Article links discovered from a news homepage.",
        "items": items,
    }
