from fastapi import APIRouter
from redis import Redis

from app.core.config import get_settings
from app.models.common import ApiResponse

router = APIRouter()


@router.get("/health", response_model=ApiResponse[dict])
def healthcheck():
    settings = get_settings()
    redis_ok = False
    try:
        client = Redis.from_url(settings.redis_url, decode_responses=True)
        redis_ok = bool(client.ping())
    except Exception:
        redis_ok = False

    return ApiResponse.success_response(
        data={
            "service": settings.app_name,
            "status": "ok" if redis_ok else "degraded",
            "redis": redis_ok,
        }
    )