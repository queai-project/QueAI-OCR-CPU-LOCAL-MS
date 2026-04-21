from rq import Queue, Retry

from app.core.config import Settings
from app.core.exceptions import QueueEnqueueError
from app.core.redis import get_queue
from app.workers.tasks import process_document_task


class QueueService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.queue: Queue = get_queue(settings)

    def enqueue_process_job(self, *, job_id: str, workspace_dir: str, lang: str) -> None:
        try:
            retry = Retry(
                max=self.settings.job_max_retries,
                interval=[self.settings.job_retry_interval_seconds],
            )

            self.queue.enqueue(
                process_document_task,
                kwargs={
                    "job_id": job_id,
                    "workspace_dir": workspace_dir,
                    "lang": lang,
                },
                job_id=job_id,
                job_timeout=self.settings.job_timeout_seconds,
                result_ttl=self.settings.job_result_ttl_seconds,
                failure_ttl=self.settings.job_failure_ttl_seconds,
                ttl=self.settings.job_ttl_seconds,
                retry=retry,
            )
        except Exception as exc:
            raise QueueEnqueueError(str(exc)) from exc