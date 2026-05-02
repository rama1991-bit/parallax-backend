from uuid import uuid4
import re


STOPWORDS = {
    "about",
    "after",
    "again",
    "also",
    "because",
    "before",
    "being",
    "between",
    "could",
    "from",
    "have",
    "into",
    "more",
    "over",
    "said",
    "says",
    "than",
    "that",
    "their",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "under",
    "were",
    "which",
    "while",
    "with",
    "would",
}


def _tokens(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-zA-Z0-9]+", value.lower())
        if len(token) > 2 and token not in STOPWORDS
    }


def _similarity(left: str, right: str) -> float:
    left_tokens = _tokens(left)
    right_tokens = _tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _jaccard(left: list[str], right: list[str]) -> float:
    left_set = {item.lower() for item in left}
    right_set = {item.lower() for item in right}
    if not left_set and not right_set:
        return 1.0
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def _ordered_intersection(left: list[str], right: list[str]) -> list[str]:
    right_lookup = {item.lower() for item in right}
    return [item for item in left if item.lower() in right_lookup]


def _ordered_difference(left: list[str], right: list[str]) -> list[str]:
    right_lookup = {item.lower() for item in right}
    return [item for item in left if item.lower() not in right_lookup]


def _report_side(card: dict) -> dict:
    payload = card.get("payload") or {}
    analysis = card.get("analysis") or {}
    return {
        "card_id": card.get("id"),
        "report_id": card.get("report_id"),
        "title": card.get("title"),
        "source": card.get("source") or payload.get("source"),
        "domain": payload.get("domain"),
        "url": card.get("url") or payload.get("url"),
        "summary": card.get("summary"),
        "confidence": analysis.get("confidence", payload.get("confidence")),
        "priority": analysis.get("priority", payload.get("priority")),
        "key_claims": analysis.get("key_claims") or payload.get("key_claims") or [],
        "narrative_framing": analysis.get("narrative_framing")
        or payload.get("narrative_framing")
        or [],
        "entities": analysis.get("entities") or payload.get("entities") or [],
        "topics": analysis.get("topics") or payload.get("topics") or [],
    }


def _claim_overlap(left_claims: list[str], right_claims: list[str]) -> dict:
    shared = []
    matched_right_indexes: set[int] = set()
    left_unique = []

    for left_claim in left_claims:
        best_index = -1
        best_score = 0.0
        best_claim = ""
        for index, right_claim in enumerate(right_claims):
            if index in matched_right_indexes:
                continue
            score = _similarity(left_claim, right_claim)
            if score > best_score:
                best_score = score
                best_index = index
                best_claim = right_claim

        if best_index >= 0 and best_score >= 0.25:
            matched_right_indexes.add(best_index)
            shared.append(
                {
                    "left_claim": left_claim,
                    "right_claim": best_claim,
                    "similarity": round(best_score, 3),
                }
            )
        else:
            left_unique.append(left_claim)

    right_unique = [
        claim for index, claim in enumerate(right_claims) if index not in matched_right_indexes
    ]
    denominator = max(len(left_claims), len(right_claims), 1)

    return {
        "score": round(len(shared) / denominator, 3),
        "shared": shared,
        "left_unique": left_unique,
        "right_unique": right_unique,
    }


def build_compare_result(left_card: dict, right_card: dict, usage: dict | None = None) -> dict:
    left = _report_side(left_card)
    right = _report_side(right_card)

    left_frames = left["narrative_framing"]
    right_frames = right["narrative_framing"]
    frame_similarity = _jaccard(left_frames, right_frames)

    left_entities = left["entities"]
    right_entities = right["entities"]
    left_topics = left["topics"]
    right_topics = right["topics"]

    return {
        "id": str(uuid4()),
        "status": "completed",
        "left": left,
        "right": right,
        "claim_overlap": _claim_overlap(left["key_claims"], right["key_claims"]),
        "framing": {
            "divergence_score": round(1 - frame_similarity, 3),
            "shared": _ordered_intersection(left_frames, right_frames),
            "left_only": _ordered_difference(left_frames, right_frames),
            "right_only": _ordered_difference(right_frames, left_frames),
        },
        "entities": {
            "shared": _ordered_intersection(left_entities, right_entities),
            "left_only": _ordered_difference(left_entities, right_entities),
            "right_only": _ordered_difference(right_entities, left_entities),
        },
        "topics": {
            "shared": _ordered_intersection(left_topics, right_topics),
            "left_only": _ordered_difference(left_topics, right_topics),
            "right_only": _ordered_difference(right_topics, left_topics),
        },
        "usage": usage,
        "limitations": [
            "Claim overlap is lexical and approximate; it is a comparison signal, not a factual verdict.",
            "Framing divergence compares extracted frame labels and can miss subtle rhetorical differences.",
            "Both articles are saved as analyzed report cards for this session.",
        ],
    }
