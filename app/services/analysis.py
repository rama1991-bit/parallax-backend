import json
import re

import httpx

from app.core.config import settings
from app.services.articles import ExtractedArticle


class AIAnalysisError(Exception):
    pass


PRIORITY_VALUES = {"low", "medium", "high"}


def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def _clean_text(value: object, fallback: str = "") -> str:
    if value is None:
        return fallback
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned or fallback


def _as_list(value: object, limit: int = 8) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        parts = [value]
    elif isinstance(value, list):
        parts = value
    else:
        parts = list(value) if isinstance(value, tuple) else []

    cleaned = []
    for item in parts:
        text = _clean_text(item)
        if text and text not in cleaned:
            cleaned.append(text[:300])
        if len(cleaned) >= limit:
            break
    return cleaned


def _split_sentences(text: str) -> list[str]:
    sentences = re.split(r"(?<=[.!?])\s+", text)
    return [_clean_text(sentence) for sentence in sentences if len(sentence.strip()) > 35]


def _fallback_summary(article: ExtractedArticle) -> str:
    sentences = _split_sentences(article.text)
    if not sentences:
        return article.excerpt[:450]
    return " ".join(sentences[:2])[:700]


def _extract_claims(article: ExtractedArticle) -> list[str]:
    claim_markers = re.compile(
        r"\b(said|says|will|would|could|because|according|reported|found|shows|"
        r"announced|warned|claimed|expected|percent|million|billion)\b|\d",
        re.IGNORECASE,
    )
    sentences = _split_sentences(article.text)
    claims = [sentence for sentence in sentences if claim_markers.search(sentence)]
    if len(claims) < 3:
        claims.extend(sentence for sentence in sentences if sentence not in claims)
    return claims[:6]


def _extract_frames(text: str) -> list[str]:
    frame_keywords = {
        "institutional_response": [
            "government",
            "official",
            "agency",
            "regulator",
            "policy",
            "minister",
            "court",
        ],
        "security_or_conflict": [
            "security",
            "threat",
            "war",
            "attack",
            "military",
            "police",
            "border",
        ],
        "economic_consequence": [
            "market",
            "prices",
            "inflation",
            "jobs",
            "budget",
            "cost",
            "economic",
        ],
        "human_impact": [
            "families",
            "workers",
            "patients",
            "residents",
            "children",
            "community",
            "people",
        ],
        "political_strategy": [
            "election",
            "campaign",
            "party",
            "vote",
            "poll",
            "candidate",
            "parliament",
        ],
    }
    lowered = text.lower()
    scored = []
    for frame, keywords in frame_keywords.items():
        score = sum(lowered.count(keyword) for keyword in keywords)
        if score:
            scored.append((score, frame))
    scored.sort(reverse=True)
    return [frame for _, frame in scored[:3]] or ["general_news_frame"]


def _extract_entities(text: str) -> list[str]:
    candidates = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3}\b", text)
    ignored = {
        "The",
        "This",
        "That",
        "There",
        "A",
        "An",
        "But",
        "It",
        "In",
        "On",
        "As",
    }
    counts: dict[str, int] = {}
    for candidate in candidates:
        if candidate in ignored or len(candidate) < 3:
            continue
        counts[candidate] = counts.get(candidate, 0) + 1
    return [item for item, _ in sorted(counts.items(), key=lambda pair: pair[1], reverse=True)[:8]]


def _extract_topics(text: str, frames: list[str]) -> list[str]:
    topics = [frame.replace("_", " ") for frame in frames]
    common_terms = re.findall(r"\b[a-zA-Z]{5,}\b", text.lower())
    stopwords = {
        "about",
        "after",
        "again",
        "article",
        "because",
        "before",
        "could",
        "first",
        "from",
        "their",
        "there",
        "these",
        "those",
        "which",
        "would",
    }
    counts: dict[str, int] = {}
    for term in common_terms:
        if term in stopwords:
            continue
        counts[term] = counts.get(term, 0) + 1
    for term, _ in sorted(counts.items(), key=lambda pair: pair[1], reverse=True):
        label = term.replace("_", " ")
        if label not in topics:
            topics.append(label)
        if len(topics) >= 6:
            break
    return topics[:6]


def _heuristic_analysis(article: ExtractedArticle) -> dict:
    claims = _extract_claims(article)
    frames = _extract_frames(article.text)
    entities = _extract_entities(article.text)
    topics = _extract_topics(article.text, frames)
    confidence = _clamp(0.45 + min(len(article.text), 9000) / 30000 + len(claims) * 0.03)
    priority = "high" if len(claims) >= 5 and confidence > 0.65 else "medium"

    return {
        "title": article.title,
        "source": article.source,
        "url": article.final_url,
        "summary": _fallback_summary(article),
        "key_claims": claims,
        "narrative_framing": frames,
        "entities": entities,
        "topics": topics,
        "confidence": confidence,
        "priority": priority,
        "card_type": "article",
    }


def _strip_json_fence(content: str) -> str:
    cleaned = content.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?", "", cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()
    return cleaned


async def _openai_analysis(article: ExtractedArticle) -> dict:
    if not settings.OPENAI_API_KEY:
        raise AIAnalysisError("OPENAI_API_KEY is not configured.")

    endpoint = settings.OPENAI_BASE_URL.rstrip("/") + "/chat/completions"
    prompt = (
        "Analyze this article for a narrative intelligence product. "
        "Return only valid JSON with these keys: title, source, url, summary, "
        "key_claims, narrative_framing, entities, topics, confidence, priority, card_type. "
        "Priority must be low, medium, or high. Confidence must be 0 to 1. "
        "Explain narrative framing without declaring truth certainty."
    )
    article_input = (
        f"URL: {article.final_url}\n"
        f"Source: {article.source}\n"
        f"Title: {article.title}\n\n"
        f"Article text:\n{article.text}"
    )

    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            response = await client.post(
                endpoint,
                headers={
                    "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": settings.OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": prompt},
                        {"role": "user", "content": article_input},
                    ],
                    "temperature": 0.2,
                    "response_format": {"type": "json_object"},
                },
            )
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:300]
        raise AIAnalysisError(f"AI analysis failed with HTTP {exc.response.status_code}: {detail}") from exc
    except httpx.HTTPError as exc:
        raise AIAnalysisError("AI analysis request failed.") from exc

    data = response.json()
    try:
        content = data["choices"][0]["message"]["content"]
        return json.loads(_strip_json_fence(content))
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        raise AIAnalysisError("AI analysis returned an invalid JSON payload.") from exc


def sanitize_analysis(raw: dict, article: ExtractedArticle) -> dict:
    fallback = _heuristic_analysis(article)

    confidence = raw.get("confidence", fallback["confidence"])
    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        confidence = fallback["confidence"]

    priority = _clean_text(raw.get("priority"), fallback["priority"]).lower()
    if priority not in PRIORITY_VALUES:
        priority = fallback["priority"]

    card_type = _clean_text(raw.get("card_type"), "article").lower()
    if card_type not in {"article", "narrative", "alert"}:
        card_type = "article"

    analysis = {
        "title": _clean_text(raw.get("title"), article.title)[:220],
        "source": _clean_text(raw.get("source"), article.source)[:160],
        "url": _clean_text(raw.get("url"), article.final_url),
        "summary": _clean_text(raw.get("summary"), fallback["summary"])[:900],
        "key_claims": _as_list(raw.get("key_claims"), limit=8) or fallback["key_claims"],
        "narrative_framing": _as_list(raw.get("narrative_framing"), limit=6)
        or fallback["narrative_framing"],
        "entities": _as_list(raw.get("entities"), limit=10) or fallback["entities"],
        "topics": _as_list(raw.get("topics"), limit=8) or fallback["topics"],
        "confidence": round(_clamp(confidence), 3),
        "priority": priority,
        "card_type": card_type,
    }
    return analysis


async def analyze_article(article: ExtractedArticle) -> dict:
    provider = settings.AI_PROVIDER.lower().strip()
    if provider in {"openai", "openai-compatible"}:
        raw = await _openai_analysis(article)
    else:
        raw = _heuristic_analysis(article)

    return sanitize_analysis(raw, article)
