import json
from datetime import datetime, timezone
from typing import Any

from redis import Redis
from redis.asyncio import Redis as AsyncRedis

from app.core.config import Settings
from app.models.process import ProcessEvent


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class BaseEventBus:
    def __init__(self, settings: Settings):
        self.settings = settings

    def channel_key(self, job_id: str) -> str:
        return f"ocr:channel:{job_id}"

    def events_key(self, job_id: str) -> str:
        return f"ocr:events:{job_id}"

    def snapshot_key(self, job_id: str) -> str:
        return f"ocr:snapshot:{job_id}"

    def seq_key(self, job_id: str) -> str:
        return f"ocr:seq:{job_id}"

    def build_event(
        self,
        *,
        event: str,
        job_id: str,
        status: str,
        stage: str | None = None,
        progress: int | None = None,
        current_page: int | None = None,
        total_pages: int | None = None,
        warnings: list[str] | None = None,
        error: str | None = None,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> dict[str, Any]:
        payload = ProcessEvent(
            event=event,
            job_id=job_id,
            status=status,
            stage=stage,
            progress=progress,
            current_page=current_page,
            total_pages=total_pages,
            warnings=warnings or [],
            error=error,
            result=result,
            message=message,
            created_at=utcnow_iso(),
        )
        return payload.model_dump()


class SyncEventBus(BaseEventBus):
    def __init__(self, redis: Redis, settings: Settings):
        super().__init__(settings)
        self.redis = redis

    def publish(self, payload: dict[str, Any]) -> dict[str, Any]:
        job_id = payload["job_id"]
        seq = self.redis.incr(self.seq_key(job_id))
        payload["seq"] = int(seq)
        raw = json.dumps(payload, ensure_ascii=False)

        pipe = self.redis.pipeline()
        pipe.rpush(self.events_key(job_id), raw)
        pipe.expire(self.events_key(job_id), self.settings.event_ttl_seconds)
        pipe.set(self.snapshot_key(job_id), raw, ex=self.settings.event_ttl_seconds)
        pipe.expire(self.seq_key(job_id), self.settings.event_ttl_seconds)
        pipe.publish(self.channel_key(job_id), raw)
        pipe.execute()
        return payload


class AsyncEventBus(BaseEventBus):
    def __init__(self, redis: AsyncRedis, settings: Settings):
        super().__init__(settings)
        self.redis = redis

    async def publish(self, payload: dict[str, Any]) -> dict[str, Any]:
        job_id = payload["job_id"]
        seq = await self.redis.incr(self.seq_key(job_id))
        payload["seq"] = int(seq)
        raw = json.dumps(payload, ensure_ascii=False)

        pipe = self.redis.pipeline()
        pipe.rpush(self.events_key(job_id), raw)
        pipe.expire(self.events_key(job_id), self.settings.event_ttl_seconds)
        pipe.set(self.snapshot_key(job_id), raw, ex=self.settings.event_ttl_seconds)
        pipe.expire(self.seq_key(job_id), self.settings.event_ttl_seconds)
        pipe.publish(self.channel_key(job_id), raw)
        await pipe.execute()
        return payload

    async def replay_events(self, job_id: str) -> list[dict[str, Any]]:
        items = await self.redis.lrange(self.events_key(job_id), 0, -1)
        return [json.loads(item) for item in items]

    async def get_snapshot(self, job_id: str) -> dict[str, Any] | None:
        raw = await self.redis.get(self.snapshot_key(job_id))
        return json.loads(raw) if raw else None