from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import AliasChoices, Field, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ==========================================
    # PROJECT
    # ==========================================
    project_name: str = Field(
        default="OCR Local CPU",
        validation_alias=AliasChoices("PROJECT_NAME", "APP_NAME"),
    )
    project_slug: str = Field(
        default="ocr_local_cpu",
        validation_alias=AliasChoices("PROJECT_SLUG"),
    )
    version: str = Field(
        default="1.1.4",
        validation_alias=AliasChoices("VERSION", "APP_VERSION"),
    )
    environment: str = Field(
        default="development",
        validation_alias=AliasChoices("ENVIRONMENT", "APP_ENV"),
    )
    cors_origins: list[str] = Field(
        default_factory=lambda: ["*"],
        validation_alias=AliasChoices("CORS_ORIGINS"),
    )
    base_path: str = Field(
        default="/api/ocr_local_cpu",
        validation_alias=AliasChoices("BASE_PATH"),
    )

    # ==========================================
    # SERVER
    # ==========================================
    api_host: str = Field(default="0.0.0.0", validation_alias=AliasChoices("API_HOST"))
    api_port: int = Field(default=8000, validation_alias=AliasChoices("API_PORT"))

    # ==========================================
    # LOGGING
    # ==========================================
    log_level: str = Field(default="INFO", validation_alias=AliasChoices("LOG_LEVEL"))
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        validation_alias=AliasChoices("LOG_FORMAT"),
    )
    log_datetime_format: str = Field(
        default="%Y-%m-%d %H:%M:%S",
        validation_alias=AliasChoices("LOG_DATETIME_FORMAT"),
    )
    log_dir: str = Field(default="logs", validation_alias=AliasChoices("LOG_DIR"))
    log_filename: str = Field(
        default="ocr_local_cpu.log",
        validation_alias=AliasChoices("LOG_FILENAME"),
    )

    # ==========================================
    # PLUGIN META
    # ==========================================
    display_name: str = Field(
        default="OCR Local CPU",
        validation_alias=AliasChoices("DISPLAY_NAME"),
    )
    description: str = Field(
        default="Módulo OCR oficial para QueAI. Imagen a texto de alta fidelidad basado en Tesseract. Ejecución local en CPU.",
        validation_alias=AliasChoices("DESCRIPTION"),
    )
    author: str = Field(
        default="Alejandro Fonseca && Juana Iris",
        validation_alias=AliasChoices("AUTHOR"),
    )
    license: str = Field(default="MIT", validation_alias=AliasChoices("LICENSE"))
    logo: str = Field(default="ocr_logo.png", validation_alias=AliasChoices("LOGO"))

    # ==========================================
    # REDIS / JOBS
    # ==========================================
    redis_url: str = Field(
        default="redis://redis:6379/0",
        validation_alias=AliasChoices("REDIS_URL"),
    )
    rq_queue_name: str = Field(
        default="ocr",
        validation_alias=AliasChoices("RQ_QUEUE_NAME"),
    )
    job_timeout_seconds: int = Field(
        default=1800,
        validation_alias=AliasChoices("JOB_TIMEOUT_SECONDS"),
    )
    job_result_ttl_seconds: int = Field(
        default=3600,
        validation_alias=AliasChoices("JOB_RESULT_TTL_SECONDS"),
    )
    job_failure_ttl_seconds: int = Field(
        default=3600,
        validation_alias=AliasChoices("JOB_FAILURE_TTL_SECONDS"),
    )
    job_ttl_seconds: int = Field(
        default=3600,
        validation_alias=AliasChoices("JOB_TTL_SECONDS"),
    )
    job_max_retries: int = Field(
        default=1,
        validation_alias=AliasChoices("JOB_MAX_RETRIES"),
    )
    job_retry_interval_seconds: int = Field(
        default=10,
        validation_alias=AliasChoices("JOB_RETRY_INTERVAL_SECONDS"),
    )
    event_ttl_seconds: int = Field(
        default=3600,
        validation_alias=AliasChoices("EVENT_TTL_SECONDS"),
    )

    # ==========================================
    # WORKSPACE / FILES
    # ==========================================
    workspace_root: Path = Field(
        default=Path("/tmp/ocr-jobs"),
        validation_alias=AliasChoices("WORKSPACE_ROOT"),
    )
    temp_workspace_ttl_hours: int = Field(
        default=24,
        validation_alias=AliasChoices("TEMP_WORKSPACE_TTL_HOURS"),
    )
    max_upload_size_mb: int = Field(
        default=25,
        validation_alias=AliasChoices("MAX_UPLOAD_SIZE_MB"),
    )
    max_pdf_pages: int | None = Field(
        default=50,
        validation_alias=AliasChoices("MAX_PDF_PAGES"),
    )
    allowed_extensions: list[str] = Field(
        default_factory=lambda: ["pdf", "png", "jpg", "jpeg", "tif", "tiff", "webp"],
        validation_alias=AliasChoices("ALLOWED_EXTENSIONS"),
    )
    allowed_mime_types: list[str] = Field(
        default_factory=lambda: [
            "application/pdf",
            "image/png",
            "image/jpeg",
            "image/tiff",
            "image/webp",
        ],
        validation_alias=AliasChoices("ALLOWED_MIME_TYPES"),
    )

    # ==========================================
    # SSE / DEBUG
    # ==========================================
    sse_ping_seconds: int = Field(
        default=10,
        validation_alias=AliasChoices("SSE_PING_SECONDS"),
    )
    debug_keep_workspace: bool = Field(
        default=False,
        validation_alias=AliasChoices("DEBUG_KEEP_WORKSPACE"),
    )
    debug_save_ocr_artifacts: bool = Field(
        default=False,
        validation_alias=AliasChoices("DEBUG_SAVE_OCR_ARTIFACTS"),
    )

    # ==========================================
    # OCR
    # ==========================================
    ocr_engine: str = Field(
        default="tesseract",
        validation_alias=AliasChoices("OCR_ENGINE"),
    )
    ocr_lang: str = Field(
        default="spa",
        validation_alias=AliasChoices("OCR_LANG"),
    )
    tesseract_default_lang: str = Field(
        default="spa",
        validation_alias=AliasChoices("TESSERACT_DEFAULT_LANG"),
    )
    tesseract_tessdata_dir: str = Field(
        default="/data",
        validation_alias=AliasChoices("TESSERACT_TESSDATA_DIR"),
    )
    tesseract_catalog_api: str = Field(
        default="https://api.github.com/repos/tesseract-ocr/tessdata/contents",
        validation_alias=AliasChoices("TESSERACT_CATALOG_API"),
    )
    tesseract_catalog_raw_base: str = Field(
        default="https://raw.githubusercontent.com/tesseract-ocr/tessdata/main",
        validation_alias=AliasChoices("TESSERACT_CATALOG_RAW_BASE"),
    )
    pdf_render_dpi: int = Field(
        default=200,
        validation_alias=AliasChoices("PDF_RENDER_DPI"),
    )
    pdfinfo_timeout_seconds: int = Field(
        default=15,
        validation_alias=AliasChoices("PDFINFO_TIMEOUT_SECONDS"),
    )
    pdftoppm_timeout_seconds: int = Field(
        default=180,
        validation_alias=AliasChoices("PDFTOPPM_TIMEOUT_SECONDS"),
    )
    tesseract_timeout_seconds: int = Field(
        default=180,
        validation_alias=AliasChoices("TESSERACT_TIMEOUT_SECONDS"),
    )
    tesseract_psm: int = Field(
        default=3,
        validation_alias=AliasChoices("TESSERACT_PSM"),
    )
    tesseract_oem: int = Field(
        default=1,
        validation_alias=AliasChoices("TESSERACT_OEM"),
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, value: Any):
        if isinstance(value, str):
            value = value.strip()
            if value.startswith("[") and value.endswith("]"):
                import json
                return json.loads(value)
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("allowed_extensions", mode="before")
    @classmethod
    def parse_extensions(cls, value: Any):
        if isinstance(value, str):
            value = value.strip()
            if value.startswith("[") and value.endswith("]"):
                import json
                parsed = json.loads(value)
                return [str(item).strip().lower() for item in parsed if str(item).strip()]
            return [item.strip().lower() for item in value.split(",") if item.strip()]
        return value

    @field_validator("allowed_mime_types", mode="before")
    @classmethod
    def parse_mime_types(cls, value: Any):
        if isinstance(value, str):
            value = value.strip()
            if value.startswith("[") and value.endswith("]"):
                import json
                parsed = json.loads(value)
                return [str(item).strip().lower() for item in parsed if str(item).strip()]
            return [item.strip().lower() for item in value.split(",") if item.strip()]
        return value

    @field_validator("max_pdf_pages", mode="before")
    @classmethod
    def parse_max_pdf_pages(cls, value: Any):
        if value in ("", None):
            return None
        return int(value)

    @property
    def is_dev(self) -> bool:
        return self.environment.lower() == "development"

    @property
    def max_upload_size_bytes(self) -> int:
        return self.max_upload_size_mb * 1024 * 1024

    @computed_field
    @property
    def openapi_path(self) -> str:
        return f"{self.base_path}/openapi.json"

    @computed_field
    @property
    def docs_path(self) -> str:
        return f"{self.base_path}/docs"

    @computed_field
    @property
    def redoc_path(self) -> str:
        return f"{self.base_path}/redoc"

    @computed_field
    @property
    def ui_path(self) -> str:
        return f"{self.base_path}/ui"

    @computed_field
    @property
    def health_path(self) -> str:
        return f"{self.base_path}/health"

    @computed_field
    @property
    def config_path(self) -> str:
        return f"{self.base_path}/config"


@lru_cache
def get_settings() -> Settings:
    return Settings()