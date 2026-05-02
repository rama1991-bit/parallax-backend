import re
from uuid import uuid4

from fastapi import Header


ANONYMOUS_SESSION_ID = "anonymous"
SESSION_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")


def normalize_session_id(value: str | None) -> str:
    if not value:
        return ANONYMOUS_SESSION_ID

    session_id = value.strip()
    if SESSION_PATTERN.match(session_id):
        return session_id

    return f"invalid-{uuid4()}"


async def get_session_id(
    x_parallax_session_id: str | None = Header(default=None, alias="X-Parallax-Session-Id"),
) -> str:
    return normalize_session_id(x_parallax_session_id)
