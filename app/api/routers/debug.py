from fastapi import APIRouter, Depends, HTTPException

from app.core.config import get_settings
from app.core.redis import get_async_redis
from app.services.event_bus import AsyncEventBus

router = APIRouter()


@router.get("/jobs/{job_id}")
async def get_job_snapshot(job_id: str):
    settings = get_settings()
    redis = get_async_redis(settings)
    try:
        bus = AsyncEventBus(redis=redis, settings=settings)
        snapshot = await bus.get_snapshot(job_id)
        if snapshot is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return {
            "success": True,
            "message": None,
            "data": snapshot,
        }
    finally:
        await redis.aclose()


@router.get("/jobs/{job_id}/events")
async def get_job_events(job_id: str):
    settings = get_settings()
    redis = get_async_redis(settings)
    try:
        bus = AsyncEventBus(redis=redis, settings=settings)
        events = await bus.replay_events(job_id)
        return {
            "success": True,
            "message": None,
            "data": events,
        }
    finally:
        await redis.aclose()