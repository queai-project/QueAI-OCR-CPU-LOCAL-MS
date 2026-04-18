from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from rq import Queue

from app.core.config import Settings


def get_sync_redis(settings: Settings) -> Redis:
    return Redis.from_url(
        settings.redis_url,
        decode_responses=False,
    )


def get_async_redis(settings: Settings) -> AsyncRedis:
    return AsyncRedis.from_url(
        settings.redis_url,
        decode_responses=True,
    )


def get_queue(settings: Settings) -> Queue:
    return Queue(
        name=settings.rq_queue_name,
        connection=get_sync_redis(settings),
        default_timeout=settings.job_timeout_seconds,
    )