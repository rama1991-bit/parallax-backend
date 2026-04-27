from fastapi import APIRouter

router = APIRouter()


@router.get("/{author_id}")
async def get_author(author_id: str):
    return {
        "author": {"id": author_id, "name": "Demo Author", "domain": "example.com"},
        "profile": None,
        "articles": [],
        "social_candidates": [],
        "limitations": [
            "Author profiles are context only.",
            "Social signals never determine truth.",
        ],
    }
