import subprocess
from pathlib import Path

from fastapi import UploadFile
from PIL import Image, UnidentifiedImageError

from app.core.config import Settings
from app.core.exceptions import (
    CorruptImageError,
    CorruptPdfError,
    EmptyFileError,
    FileTooLargeError,
    InvalidFileTypeError,
    PdfPageLimitExceededError,
    TemporaryStorageError,
)


class FileValidationService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def get_extension(self, filename: str | None) -> str:
        if not filename or "." not in filename:
            raise InvalidFileTypeError("File must have a valid extension")

        extension = filename.rsplit(".", 1)[-1].lower()
        if extension not in self.settings.allowed_extensions:
            raise InvalidFileTypeError("Unsupported file extension")

        return extension

    def validate_content_type(self, upload_file: UploadFile) -> None:
        content_type = (upload_file.content_type or "").lower()
        if content_type not in self.settings.allowed_mime_types:
            raise InvalidFileTypeError("Unsupported content type")

    async def save_upload_streaming(self, upload_file: UploadFile, target_path: Path) -> int:
        self.validate_content_type(upload_file)
        total_size = 0
        chunk_size = 1024 * 1024

        try:
            with target_path.open("wb") as output:
                while True:
                    chunk = await upload_file.read(chunk_size)
                    if not chunk:
                        break

                    total_size += len(chunk)

                    if total_size > self.settings.max_upload_size_bytes:
                        output.close()
                        target_path.unlink(missing_ok=True)
                        raise FileTooLargeError()

                    output.write(chunk)
        except OSError as exc:
            target_path.unlink(missing_ok=True)
            raise TemporaryStorageError(f"Could not write temporary file: {exc}") from exc
        finally:
            await upload_file.close()

        if total_size == 0:
            target_path.unlink(missing_ok=True)
            raise EmptyFileError()

        return total_size

    def validate_saved_document(self, file_path: Path, extension: str) -> dict:
        if extension == "pdf":
            return self._validate_pdf(file_path)
        return self._validate_image(file_path)

    def _validate_pdf(self, file_path: Path) -> dict:
        try:
            proc = subprocess.run(
                ["pdfinfo", str(file_path)],
                check=True,
                capture_output=True,
                text=True,
                timeout=self.settings.pdfinfo_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise CorruptPdfError("PDF validation timed out") from exc
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            raise CorruptPdfError("PDF is corrupt, unreadable, or pdfinfo is unavailable") from exc

        page_count = None
        for line in proc.stdout.splitlines():
            if line.lower().startswith("pages:"):
                value = line.split(":", 1)[1].strip()
                if value.isdigit():
                    page_count = int(value)
                    break

        if page_count is None or page_count <= 0:
            raise CorruptPdfError("Could not determine PDF page count")

        if self.settings.max_pdf_pages is not None and page_count > self.settings.max_pdf_pages:
            raise PdfPageLimitExceededError(
                f"PDF has {page_count} pages, limit is {self.settings.max_pdf_pages}"
            )

        return {
            "document_type": "pdf",
            "page_count": page_count,
        }

    def _validate_image(self, file_path: Path) -> dict:
        try:
            with Image.open(file_path) as img:
                img.verify()

            with Image.open(file_path) as img2:
                width, height = img2.size
                image_format = img2.format
        except (UnidentifiedImageError, OSError) as exc:
            raise CorruptImageError("Image is corrupt or unreadable") from exc

        if width <= 0 or height <= 0:
            raise CorruptImageError("Image has invalid dimensions")

        return {
            "document_type": "image",
            "page_count": 1,
            "image_format": image_format,
            "width": width,
            "height": height,
        }