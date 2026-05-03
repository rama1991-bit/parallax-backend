import hmac

from fastapi import Header, HTTPException, status

from app.core.config import settings


async def require_admin_key(
    x_parallax_admin_key: str | None = Header(default=None, alias="X-Parallax-Admin-Key"),
) -> None:
    expected = (settings.ADMIN_API_KEY or "").strip()
    provided = (x_parallax_admin_key or "").strip()
    if not expected or not provided or not hmac.compare_digest(provided, expected):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin API key required.",
        )
