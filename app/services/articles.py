from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urlparse
import re

import httpx

from app.core.config import settings


class ArticleFetchError(Exception):
    pass


@dataclass
class ExtractedArticle:
    url: str
    final_url: str
    title: str
    source: str
    domain: str
    text: str
    excerpt: str


class _ArticleHTMLParser(HTMLParser):
    _ignored_tags = {"script", "style", "noscript", "svg", "canvas", "iframe"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._ignore_depth = 0
        self._in_title = False
        self.title_chunks: list[str] = []
        self.text_chunks: list[str] = []
        self.meta: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs):
        tag = tag.lower()
        attrs_map = {key.lower(): value or "" for key, value in attrs}

        if tag in self._ignored_tags:
            self._ignore_depth += 1
            return

        if tag == "title":
            self._in_title = True

        if tag == "meta":
            name = attrs_map.get("property") or attrs_map.get("name")
            content = attrs_map.get("content", "").strip()
            if name and content:
                self.meta[name.lower()] = content

    def handle_endtag(self, tag: str):
        tag = tag.lower()
        if tag in self._ignored_tags and self._ignore_depth > 0:
            self._ignore_depth -= 1
            return

        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str):
        if self._ignore_depth:
            return

        cleaned = _normalize_space(data)
        if len(cleaned) < 2:
            return

        if self._in_title:
            self.title_chunks.append(cleaned)
            return

        self.text_chunks.append(cleaned)


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _domain_from_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc.lower().removeprefix("www.")


def _extract_from_html(url: str, html: str) -> ExtractedArticle:
    parser = _ArticleHTMLParser()
    parser.feed(html)

    final_domain = _domain_from_url(url)
    title = (
        parser.meta.get("og:title")
        or parser.meta.get("twitter:title")
        or " ".join(parser.title_chunks)
        or final_domain
    )
    source = parser.meta.get("og:site_name") or final_domain

    chunks = [_normalize_space(chunk) for chunk in parser.text_chunks]
    meaningful = [chunk for chunk in chunks if len(chunk) >= 45]
    if sum(len(chunk) for chunk in meaningful) < 600:
        meaningful = [chunk for chunk in chunks if len(chunk) >= 20]

    text = "\n\n".join(meaningful)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()

    if len(text) < 120:
        raise ArticleFetchError("Article text could not be extracted from the URL.")

    max_chars = max(settings.ARTICLE_MAX_CHARS, 2000)
    text = text[:max_chars]

    return ExtractedArticle(
        url=url,
        final_url=url,
        title=_normalize_space(title),
        source=_normalize_space(source),
        domain=final_domain,
        text=text,
        excerpt=text[:1200],
    )


async def fetch_article(url: str) -> ExtractedArticle:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; ParallaxNarrativeBot/0.1; "
            "+https://github.com/rama1991-bit/parallax-backend)"
        ),
        "Accept": "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.8",
    }

    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=settings.ARTICLE_FETCH_TIMEOUT_SECONDS,
            headers=headers,
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code
        raise ArticleFetchError(f"Article fetch failed with HTTP {status_code}.") from exc
    except httpx.HTTPError as exc:
        raise ArticleFetchError("Article fetch failed before the page could be read.") from exc

    content_type = response.headers.get("content-type", "").lower()
    final_url = str(response.url)

    if "html" in content_type or response.text.lstrip().startswith("<"):
        article = _extract_from_html(final_url, response.text)
        return ExtractedArticle(
            url=url,
            final_url=final_url,
            title=article.title,
            source=article.source,
            domain=article.domain,
            text=article.text,
            excerpt=article.excerpt,
        )

    text = _normalize_space(response.text)
    if len(text) < 120:
        raise ArticleFetchError("The URL did not return enough readable article text.")

    domain = _domain_from_url(final_url)
    text = text[: max(settings.ARTICLE_MAX_CHARS, 2000)]
    return ExtractedArticle(
        url=url,
        final_url=final_url,
        title=domain,
        source=domain,
        domain=domain,
        text=text,
        excerpt=text[:1200],
    )
