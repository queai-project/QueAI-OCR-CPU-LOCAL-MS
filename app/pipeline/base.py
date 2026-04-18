from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable

ProgressCallback = Callable[..., None]


class OCRPipeline(ABC):
    @abstractmethod
    def process_document(
        self,
        *,
        document_path: Path,
        job_id: str,
        report_progress: ProgressCallback,
    ) -> dict[str, Any]:
        raise NotImplementedError