import asyncio
import json
import uuid
from typing import AsyncGenerator

from fastapi import Request, UploadFile
from fastapi.responses import StreamingResponse

from app.core.config import Settings
from app.core.logger import get_logger
from app.core.redis import get_async_redis
from app.core.sse import encode_sse, encode_sse_comment
from app.services.event_bus import AsyncEventBus
from app.services.queue_service import QueueService
from app.services.validation import FileValidationService
from app.storage.temp_store import TempStore

logger = get_logger(__name__)


class ProcessService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.validator = FileValidationService(settings)
        self.temp_store = TempStore(settings)
        self.queue_service = QueueService(settings)

    async def start_stream(self, *, request: Request, file: UploadFile) -> StreamingResponse:
        job_id = str(uuid.uuid4())
        workspace_created = False
        redis = None

        try:
            extension = self.validator.get_extension(file.filename)

            workspace = self.temp_store.create_workspace(job_id)
            workspace_created = True

            input_path = self.temp_store.build_input_path(job_id, extension)
            size_bytes = await self.validator.save_upload_streaming(file, input_path)

            doc_info = self.validator.validate_saved_document(input_path, extension)

            redis = get_async_redis(self.settings)
            bus = AsyncEventBus(redis=redis, settings=self.settings)

            self.queue_service.enqueue_process_job(
                job_id=job_id,
                workspace_dir=str(workspace),
            )

            await bus.publish(
                bus.build_event(
                    event="accepted",
                    job_id=job_id,
                    status="accepted",
                    stage="validated",
                    progress=0,
                    current_page=None,
                    total_pages=doc_info.get("page_count"),
                    message=f"Upload accepted ({size_bytes} bytes)",
                )
            )

            await bus.publish(
                bus.build_event(
                    event="queued",
                    job_id=job_id,
                    status="queued",
                    stage="queued",
                    progress=0,
                    current_page=None,
                    total_pages=doc_info.get("page_count"),
                    message="Job queued",
                )
            )

        except Exception:
            try:
                await file.close()
            except Exception:
                pass

            if redis is not None:
                await redis.aclose()

            if workspace_created:
                self.temp_store.cleanup_workspace(job_id)

            raise

        async def event_stream() -> AsyncGenerator[bytes, None]:
            pubsub = redis.pubsub()
            sent_sequences: set[int] = set()
            last_ping = asyncio.get_running_loop().time()

            try:
                replay = await bus.replay_events(job_id)
                for item in replay:
                    seq = item.get("seq")
                    if seq is not None:
                        sent_sequences.add(seq)

                    yield encode_sse(
                        event=item["event"],
                        data=item,
                        event_id=str(seq) if seq is not None else None,
                    )

                    if item["event"] in {"completed", "failed"}:
                        return

                await pubsub.subscribe(bus.channel_key(job_id))

                while True:
                    if await request.is_disconnected():
                        logger.info("client_disconnected", extra={"job_id": job_id})
                        break

                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=1.0,
                    )

                    if message and message.get("type") == "message":
                        payload = json.loads(message["data"])
                        seq = payload.get("seq")

                        if seq in sent_sequences:
                            continue

                        if seq is not None:
                            sent_sequences.add(seq)

                        yield encode_sse(
                            event=payload["event"],
                            data=payload,
                            event_id=str(seq) if seq is not None else None,
                        )

                        if payload["event"] in {"completed", "failed"}:
                            break
                    else:
                        now = asyncio.get_running_loop().time()
                        if now - last_ping >= self.settings.sse_ping_seconds:
                            yield encode_sse_comment()
                            last_ping = now

                    await asyncio.sleep(0.05)

            except Exception as exc:
                logger.exception("stream_failed", extra={"job_id": job_id})
                failed_payload = {
                    "seq": None,
                    "event": "failed",
                    "job_id": job_id,
                    "status": "failed",
                    "stage": "stream_error",
                    "progress": 100,
                    "current_page": None,
                    "total_pages": None,
                    "warnings": [],
                    "error": str(exc),
                    "result": None,
                    "message": "Streaming failed after response started",
                    "created_at": None,
                }
                yield encode_sse(
                    event="failed",
                    data=failed_payload,
                    event_id=None,
                )
            finally:
                try:
                    await pubsub.unsubscribe(bus.channel_key(job_id))
                except Exception:
                    pass
                try:
                    await pubsub.close()
                except Exception:
                    pass
                try:
                    await redis.aclose()
                except Exception:
                    pass

        headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }

        logger.info("stream_started", extra={"job_id": job_id})

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers=headers,
        )