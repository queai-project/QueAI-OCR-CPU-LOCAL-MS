from rq import Worker

from app.core.config import get_settings
from app.core.logger import configure_logging, get_logger
from app.core.redis import get_sync_redis
from app.storage.temp_store import TempStore


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger(__name__)

    removed = TempStore(settings).cleanup_stale_workspaces()
    logger.info(
        f"stale_workspaces_removed={removed}",
        extra={"job_id": "-"},
    )

    redis = get_sync_redis(settings)

    worker = Worker(
        queues=[settings.rq_queue_name],
        connection=redis,
    )
    worker.work(with_scheduler=False)


if __name__ == "__main__":
    main()