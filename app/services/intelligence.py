from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

import httpx

from app.core.config import settings
from app.services.articles import ExtractedArticle


class IntelligenceProviderError(Exception):
    pass


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(value: Any, limit: int = 600, fallback: str = "") -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip() or str(fallback or "")
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def safe_list(value: Any, limit: int = 8) -> list[str]:
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        values = []
    items: list[str] = []
    seen = set()
    for item in values:
        if isinstance(item, dict):
            text = item.get("text") or item.get("claim") or item.get("label") or item.get("name")
        else:
            text = item
        text = clean_text(text, limit=280)
        key = text.lower()
        if text and key not in seen:
            items.append(text)
            seen.add(key)
        if len(items) >= limit:
            break
    return items


def clamp(value: Any, fallback: float = 0.0) -> float:
    try:
        return max(0.0, min(float(value), 1.0))
    except (TypeError, ValueError):
        return max(0.0, min(fallback, 1.0))


def provider_enabled() -> bool:
    return (settings.AI_PROVIDER or "heuristic").strip().lower() in {"openai", "openai-compatible"}


def provider_metadata(
    *,
    task: str,
    provider: str | None = None,
    status: str = "heuristic",
    warnings: list[str] | None = None,
    model: str | None = None,
) -> dict:
    provider_name = provider or ((settings.AI_PROVIDER or "heuristic").strip().lower() or "heuristic")
    return {
        "task": task,
        "provider": provider_name,
        "model": model or (settings.OPENAI_MODEL if provider_name in {"openai", "openai-compatible"} else None),
        "status": status,
        "generated_at": now_iso(),
        "truth_status": "contextual analysis, not factual verdict",
        "warnings": safe_list(warnings or [], limit=8),
    }


def strip_json_fence(content: str) -> str:
    cleaned = content.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?", "", cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()
    return cleaned


async def openai_json(
    *,
    task: str,
    system_prompt: str,
    user_payload: dict,
    timeout: float = 45.0,
    max_chars: int = 18000,
) -> dict:
    if not settings.OPENAI_API_KEY:
        raise IntelligenceProviderError("OPENAI_API_KEY is not configured.")

    endpoint = settings.OPENAI_BASE_URL.rstrip("/") + "/chat/completions"
    payload_text = json.dumps(user_payload, ensure_ascii=False, default=str)
    if len(payload_text) > max_chars:
        payload_text = payload_text[:max_chars] + "\n...truncated..."

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                endpoint,
                headers={
                    "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": settings.OPENAI_MODEL,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                f"{system_prompt}\n\n"
                                "Return only valid JSON. Do not declare truth certainty. "
                                "Use cautious language and label uncertainty."
                            ),
                        },
                        {"role": "user", "content": payload_text},
                    ],
                    "temperature": 0.15,
                    "response_format": {"type": "json_object"},
                },
            )
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:300]
        raise IntelligenceProviderError(
            f"{task} failed with HTTP {exc.response.status_code}: {detail}"
        ) from exc
    except httpx.HTTPError as exc:
        raise IntelligenceProviderError(f"{task} request failed.") from exc

    data = response.json()
    try:
        content = data["choices"][0]["message"]["content"]
        parsed = json.loads(strip_json_fence(content))
        if not isinstance(parsed, dict):
            raise TypeError("Provider JSON was not an object.")
        return parsed
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        raise IntelligenceProviderError(f"{task} returned invalid JSON.") from exc


def _entity_groups(raw: Any, fallback: dict | None = None) -> dict:
    fallback = fallback or {}
    raw = raw if isinstance(raw, dict) else {}
    return {
        "people": safe_list(raw.get("people") or fallback.get("people") or [], limit=10),
        "organizations": safe_list(raw.get("organizations") or fallback.get("organizations") or [], limit=10),
        "locations": safe_list(raw.get("locations") or fallback.get("locations") or [], limit=10),
        "events": safe_list(raw.get("events") or fallback.get("events") or [], limit=10),
    }


def _node_hints(raw: Any) -> dict:
    if not isinstance(raw, dict):
        return {}
    hints = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            continue
        hints[str(key)] = {
            "summary": clean_text(value.get("summary"), limit=420),
            "signals": safe_list(value.get("signals") or [], limit=8),
            "limitations": safe_list(value.get("limitations") or [], limit=8),
        }
    return hints


def sanitize_article_intelligence(raw: dict, base: dict) -> dict:
    article = raw.get("article") if isinstance(raw.get("article"), dict) else {}
    narrative = raw.get("narrative") if isinstance(raw.get("narrative"), dict) else {}
    source_analysis = raw.get("source_analysis") if isinstance(raw.get("source_analysis"), dict) else {}
    comparison_hooks = raw.get("comparison_hooks") if isinstance(raw.get("comparison_hooks"), dict) else {}
    scores = raw.get("scores") if isinstance(raw.get("scores"), dict) else {}
    base_narrative = base.get("narrative") if isinstance(base.get("narrative"), dict) else {}
    base_source = base.get("source_analysis") if isinstance(base.get("source_analysis"), dict) else {}
    base_hooks = base.get("comparison_hooks") if isinstance(base.get("comparison_hooks"), dict) else {}
    base_scores = base.get("scores") if isinstance(base.get("scores"), dict) else {}

    sanitized = {
        **base,
        "article": {
            **(base.get("article") or {}),
            "title": clean_text(article.get("title"), limit=240, fallback=(base.get("article") or {}).get("title", "")),
            "url": clean_text(article.get("url"), limit=1000, fallback=(base.get("article") or {}).get("url", "")),
            "source": clean_text(article.get("source"), limit=180, fallback=(base.get("article") or {}).get("source", "")),
            "author": clean_text(article.get("author"), limit=180, fallback=(base.get("article") or {}).get("author", "")),
            "published_at": article.get("published_at") or (base.get("article") or {}).get("published_at"),
            "language": clean_text(article.get("language"), limit=80, fallback=(base.get("article") or {}).get("language", "")),
            "country": clean_text(article.get("country"), limit=120, fallback=(base.get("article") or {}).get("country", "")),
        },
        "summary": clean_text(raw.get("summary"), limit=1200, fallback=base.get("summary", "")),
        "key_claims": safe_list(raw.get("key_claims") or base.get("key_claims") or [], limit=10),
        "entities": _entity_groups(raw.get("entities"), fallback=base.get("entities") or {}),
        "narrative": {
            "main_frame": clean_text(
                narrative.get("main_frame"),
                limit=180,
                fallback=base_narrative.get("main_frame", "general_news_frame"),
            ),
            "secondary_frames": safe_list(
                narrative.get("secondary_frames") or base_narrative.get("secondary_frames") or [],
                limit=8,
            ),
            "tone": clean_text(narrative.get("tone"), limit=120, fallback=base_narrative.get("tone", "uncertain")),
            "implied_causality": clean_text(
                narrative.get("implied_causality"),
                limit=500,
                fallback=base_narrative.get("implied_causality", ""),
            ),
            "missing_context": safe_list(
                narrative.get("missing_context") or base_narrative.get("missing_context") or [],
                limit=10,
            ),
        },
        "source_analysis": {
            "source_profile": clean_text(
                source_analysis.get("source_profile"),
                limit=500,
                fallback=base_source.get("source_profile", ""),
            ),
            "known_angle": clean_text(
                source_analysis.get("known_angle"),
                limit=280,
                fallback=base_source.get("known_angle", "unknown"),
            ),
            "reliability_notes": clean_text(
                source_analysis.get("reliability_notes"),
                limit=500,
                fallback=base_source.get("reliability_notes", ""),
            ),
            "limitations": safe_list(
                source_analysis.get("limitations") or base_source.get("limitations") or [],
                limit=8,
            ),
        },
        "comparison_hooks": {
            "search_queries": safe_list(
                comparison_hooks.get("search_queries") or base_hooks.get("search_queries") or [],
                limit=8,
            ),
            "similarity_keywords": safe_list(
                comparison_hooks.get("similarity_keywords") or base_hooks.get("similarity_keywords") or [],
                limit=12,
            ),
            "event_fingerprint": clean_text(
                comparison_hooks.get("event_fingerprint"),
                limit=160,
                fallback=base_hooks.get("event_fingerprint", ""),
            ),
        },
        "scores": {
            "importance_score": round(clamp(scores.get("importance_score"), base_scores.get("importance_score", 0.5)), 3),
            "confidence_score": round(clamp(scores.get("confidence_score"), base_scores.get("confidence_score", 0.5)), 3),
            "controversy_score": round(clamp(scores.get("controversy_score"), base_scores.get("controversy_score", 0.4)), 3),
            "cross_source_need": round(clamp(scores.get("cross_source_need"), base_scores.get("cross_source_need", 0.5)), 3),
        },
    }
    sanitized["node_perspectives"] = _node_hints(raw.get("node_perspectives"))
    return sanitized


async def enhance_article_intelligence(
    *,
    base: dict,
    article: ExtractedArticle,
    analysis: dict,
    ingested_article: dict | None = None,
) -> dict:
    if not provider_enabled():
        return {
            **base,
            "provider_metadata": provider_metadata(task="article_intelligence", status="heuristic"),
        }

    prompt = (
        "You are the analysis layer for a source/topic/node-based media intelligence feed. "
        "Improve the provided structured intelligence JSON. Preserve the schema. "
        "Focus on claims, entities, framing, source context, comparison hooks, and node perspective hints. "
        "Do not decide whether claims are true."
    )
    payload = {
        "base_intelligence": base,
        "analysis": analysis,
        "article": {
            "url": article.final_url,
            "title": article.title,
            "source": article.source,
            "excerpt": article.excerpt,
            "text": article.text[:12000],
        },
        "ingested_article": ingested_article or {},
        "required_schema": {
            "article": {},
            "summary": "",
            "key_claims": [],
            "entities": {"people": [], "organizations": [], "locations": [], "events": []},
            "narrative": {
                "main_frame": "",
                "secondary_frames": [],
                "tone": "",
                "implied_causality": "",
                "missing_context": [],
            },
            "source_analysis": {
                "source_profile": "",
                "known_angle": "",
                "reliability_notes": "",
                "limitations": [],
            },
            "comparison_hooks": {
                "search_queries": [],
                "similarity_keywords": [],
                "event_fingerprint": "",
            },
            "scores": {
                "importance_score": 0.0,
                "confidence_score": 0.0,
                "controversy_score": 0.0,
                "cross_source_need": 0.0,
            },
            "node_perspectives": {
                "article": {"summary": "", "signals": [], "limitations": []},
                "source": {"summary": "", "signals": [], "limitations": []},
                "author": {"summary": "", "signals": [], "limitations": []},
                "topic": {"summary": "", "signals": [], "limitations": []},
                "event": {"summary": "", "signals": [], "limitations": []},
                "claim": {"summary": "", "signals": [], "limitations": []},
                "narrative": {"summary": "", "signals": [], "limitations": []},
            },
        },
    }

    try:
        raw = await openai_json(
            task="article_intelligence",
            system_prompt=prompt,
            user_payload=payload,
            timeout=50.0,
        )
        enhanced = sanitize_article_intelligence(raw, base)
        enhanced["provider_metadata"] = provider_metadata(task="article_intelligence", status="model")
        return enhanced
    except IntelligenceProviderError as exc:
        return {
            **base,
            "provider_metadata": provider_metadata(
                task="article_intelligence",
                status="fallback",
                warnings=[str(exc)],
            ),
        }


def sanitize_compare_result(raw: dict, base: dict) -> dict:
    comparison = raw.get("comparison") if isinstance(raw.get("comparison"), dict) else {}
    base_comparison = base.get("comparison") if isinstance(base.get("comparison"), dict) else {}
    enhanced = {
        **base,
        "comparison": {
            **base_comparison,
            "title_differences": comparison.get("title_differences") if isinstance(comparison.get("title_differences"), list) else base_comparison.get("title_differences", []),
            "shared_claims": comparison.get("shared_claims") if isinstance(comparison.get("shared_claims"), list) else base_comparison.get("shared_claims", []),
            "unique_claims_by_source": comparison.get("unique_claims_by_source") if isinstance(comparison.get("unique_claims_by_source"), list) else base_comparison.get("unique_claims_by_source", []),
            "missing_claims": comparison.get("missing_claims") if isinstance(comparison.get("missing_claims"), list) else base_comparison.get("missing_claims", []),
            "added_claims": comparison.get("added_claims") if isinstance(comparison.get("added_claims"), list) else base_comparison.get("added_claims", []),
            "framing_differences": comparison.get("framing_differences") if isinstance(comparison.get("framing_differences"), list) else base_comparison.get("framing_differences", []),
            "tone_differences": comparison.get("tone_differences") if isinstance(comparison.get("tone_differences"), list) else base_comparison.get("tone_differences", []),
            "missing_context": safe_list(
                comparison.get("missing_context") or base_comparison.get("missing_context") or [],
                limit=20,
            ),
            "timeline_difference": comparison.get("timeline_difference") if isinstance(comparison.get("timeline_difference"), list) else base_comparison.get("timeline_difference", []),
            "source_difference": comparison.get("source_difference") if isinstance(comparison.get("source_difference"), list) else base_comparison.get("source_difference", []),
            "coverage_gaps": comparison.get("coverage_gaps") if isinstance(comparison.get("coverage_gaps"), list) else base_comparison.get("coverage_gaps", []),
            "entities": comparison.get("entities") if isinstance(comparison.get("entities"), dict) else base_comparison.get("entities", {}),
            "confidence": round(clamp(comparison.get("confidence"), base_comparison.get("confidence", 0.0)), 3),
        },
    }
    if isinstance(enhanced["comparison"].get("entities"), dict):
        enhanced["entities"] = enhanced["comparison"]["entities"]
    return enhanced


async def enhance_compare_result(result: dict) -> dict:
    if not provider_enabled():
        return {
            **result,
            "provider_metadata": provider_metadata(task="cross_source_compare", status="heuristic"),
        }

    prompt = (
        "You enhance cross-source comparison for a media intelligence product. "
        "Use only the provided articles and comparison fields. Preserve JSON schema. "
        "Improve title, framing, missing-claim, tone, source, and timeline differences. "
        "Do not make truth judgments."
    )
    payload = {
        "compare_result": result,
        "required_comparison_schema": {
            "title_differences": [],
            "shared_claims": [],
            "unique_claims_by_source": [],
            "missing_claims": [],
            "added_claims": [],
            "framing_differences": [],
            "tone_differences": [],
            "missing_context": [],
            "timeline_difference": [],
            "source_difference": [],
            "coverage_gaps": [],
            "entities": {"shared": [], "base_only": [], "comparison_only": []},
            "confidence": 0.0,
        },
    }
    try:
        raw = await openai_json(
            task="cross_source_compare",
            system_prompt=prompt,
            user_payload=payload,
            timeout=45.0,
        )
        enhanced = sanitize_compare_result(raw, result)
        enhanced["provider_metadata"] = provider_metadata(task="cross_source_compare", status="model")
        return enhanced
    except IntelligenceProviderError as exc:
        return {
            **result,
            "provider_metadata": provider_metadata(
                task="cross_source_compare",
                status="fallback",
                warnings=[str(exc)],
            ),
        }


def sanitize_osint_context(raw: dict, base: dict) -> dict:
    contradictions = raw.get("contradictions") if isinstance(raw.get("contradictions"), list) else base.get("contradictions", [])
    relevance = raw.get("relevance") if isinstance(raw.get("relevance"), dict) else {}
    base_relevance = base.get("relevance") if isinstance(base.get("relevance"), dict) else {}
    return {
        **base,
        "risks": safe_list(raw.get("risks") or base.get("risks") or [], limit=12),
        "contradictions": contradictions,
        "relevance": {
            **base_relevance,
            "overall": round(clamp(relevance.get("overall"), base_relevance.get("overall", 0.0)), 3),
            "basis": safe_list(relevance.get("basis") or base_relevance.get("basis") or [], limit=10),
        },
        "limitations": safe_list(raw.get("limitations") or base.get("limitations") or [], limit=10),
    }


async def enhance_osint_context(context: dict, article: dict) -> dict:
    if not provider_enabled():
        return {
            **context,
            "provider_metadata": provider_metadata(task="osint_context_synthesis", status="heuristic"),
        }

    prompt = (
        "You synthesize a bounded OSINT context panel. OSINT is context, not truth. "
        "Use the provided references, risks, citations, claims, and entities only. "
        "Improve risks, contradictions, relevance basis, and limitations without declaring claims true or false."
    )
    payload = {
        "article": {
            "id": article.get("id"),
            "title": article.get("title"),
            "url": article.get("url"),
            "source": article.get("source"),
            "analysis_status": article.get("analysis_status"),
        },
        "context": context,
        "required_schema": {
            "risks": [],
            "contradictions": [],
            "relevance": {"overall": 0.0, "basis": []},
            "limitations": [],
        },
    }
    try:
        raw = await openai_json(
            task="osint_context_synthesis",
            system_prompt=prompt,
            user_payload=payload,
            timeout=45.0,
        )
        enhanced = sanitize_osint_context(raw, context)
        enhanced["provider_metadata"] = provider_metadata(task="osint_context_synthesis", status="model")
        return enhanced
    except IntelligenceProviderError as exc:
        return {
            **context,
            "provider_metadata": provider_metadata(
                task="osint_context_synthesis",
                status="fallback",
                warnings=[str(exc)],
            ),
        }
