from app.core.config import Settings
from app.pipeline.base import OCRPipeline
from app.pipeline.tesseract_ocr import TesseractOCRPipeline


def get_ocr_pipeline(settings: Settings) -> OCRPipeline:
    engine = settings.ocr_engine.strip().lower()

    if engine in {"tesseract", "tesseract_ocr"}:
        return TesseractOCRPipeline(settings)

    raise ValueError(f"Unsupported OCR_ENGINE: {settings.ocr_engine}")