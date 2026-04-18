import time
from pathlib import Path
from typing import Any

from app.pipeline.base import OCRPipeline


class PlaceholderOCRPipeline(OCRPipeline):
    def process_document(
        self,
        *,
        document_path: Path,
        job_id: str,
        report_progress,
    ) -> dict[str, Any]:
        total_pages = 1

        report_progress(
            stage="prepare_document",
            progress=10,
            current_page=1,
            total_pages=total_pages,
            message="Preparing document",
        )
        time.sleep(0.5)

        report_progress(
            stage="analyze_layout",
            progress=45,
            current_page=1,
            total_pages=total_pages,
            message="Analyzing layout",
        )
        time.sleep(0.8)

        report_progress(
            stage="extract_content",
            progress=80,
            current_page=1,
            total_pages=total_pages,
            message="Extracting content",
        )
        time.sleep(0.8)

        result = {
            "pipeline": "placeholder",
            "job_id": job_id,
            "filename": document_path.name,
            "pages": total_pages,
            "ocr": None,
            "message": "Placeholder pipeline ready for PP-StructureV3 integration",
        }

        report_progress(
            stage="finalize",
            progress=95,
            current_page=1,
            total_pages=total_pages,
            message="Finalizing result",
        )
        time.sleep(0.3)

        return result