import hmac

from fastapi import Header

from app.core.config import get_settings
from app.core.exceptions import AuthenticationError


async def verify_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    settings = get_settings()

    expected = settings.api_key or ""
    provided = x_api_key or ""

    if not expected:
        raise AuthenticationError("API key is not configured on the server")

    if not provided:
        raise AuthenticationError("Missing X-API-Key header")

    if not hmac.compare_digest(provided, expected):
        raise AuthenticationError("Invalid API key")