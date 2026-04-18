from pathlib import Path
from time import monotonic

from redis import Redis
from rq import get_current_job

from app.core.config import get_settings
from app.core.logger import get_logger
from app.pipeline.factory import get_ocr_pipeline
from app.services.event_bus import SyncEventBus
from app.storage.temp_store import TempStore

logger = get_logger(__name__)


def process_document_task(job_id: str, workspace_dir: str) -> dict:
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    bus = SyncEventBus(redis=redis, settings=settings)
    temp_store = TempStore(settings)
    job = get_current_job()
    task_start = monotonic()

    def push_progress(
        *,
        stage: str,
        progress: int,
        current_page: int | None = None,
        total_pages: int | None = None,
        message: str | None = None,
        warnings: list[str] | None = None,
    ) -> None:
        logger.info(
            f"stage={stage} progress={progress} current_page={current_page} total_pages={total_pages}",
            extra={"job_id": job_id},
        )

        payload = bus.build_event(
            event="progress",
            job_id=job_id,
            status="processing",
            stage=stage,
            progress=progress,
            current_page=current_page,
            total_pages=total_pages,
            warnings=warnings or [],
            message=message,
        )
        bus.publish(payload)

        if job:
            job.meta.update(
                {
                    "status": "processing",
                    "stage": stage,
                    "progress": progress,
                    "current_page": current_page,
                    "total_pages": total_pages,
                    "warnings": warnings or [],
                    "error": None,
                }
            )
            job.save_meta()

    input_files = list(Path(workspace_dir).glob("input.*"))
    if not input_files:
        raise FileNotFoundError("Temporary input file not found")

    document_path = input_files[0]

    try:
        bus.publish(
            bus.build_event(
                event="processing",
                job_id=job_id,
                status="processing",
                stage="worker_started",
                progress=1,
                message="Worker started",
            )
        )

        if job:
            job.meta.update(
                {
                    "status": "processing",
                    "stage": "worker_started",
                    "progress": 1,
                    "error": None,
                }
            )
            job.save_meta()

        pipeline = get_ocr_pipeline(settings)
        result = pipeline.process_document(
            document_path=document_path,
            job_id=job_id,
            report_progress=push_progress,
        )

        elapsed = round(monotonic() - task_start, 3)
        logger.info(
            f"stage=done elapsed_seconds={elapsed}",
            extra={"job_id": job_id},
        )

        completed = bus.build_event(
            event="completed",
            job_id=job_id,
            status="completed",
            stage="done",
            progress=100,
            current_page=None,
            total_pages=None,
            result=result,
            message="Processing completed",
        )
        bus.publish(completed)

        if job:
            job.meta.update(
                {
                    "status": "completed",
                    "stage": "done",
                    "progress": 100,
                    "result": result,
                    "error": None,
                }
            )
            job.save_meta()

        logger.info("job_completed", extra={"job_id": job_id})
        return result

    except Exception as exc:
        failed = bus.build_event(
            event="failed",
            job_id=job_id,
            status="failed",
            stage="failed",
            progress=100,
            error=str(exc),
            message="Processing failed",
        )
        bus.publish(failed)

        if job:
            job.meta.update(
                {
                    "status": "failed",
                    "stage": "failed",
                    "progress": 100,
                    "error": str(exc),
                }
            )
            job.save_meta()

        logger.exception("job_failed", extra={"job_id": job_id})
        raise

    finally:
        if not settings.debug_keep_workspace:
            temp_store.cleanup_workspace(job_id)
            logger.info("workspace_cleaned", extra={"job_id": job_id})