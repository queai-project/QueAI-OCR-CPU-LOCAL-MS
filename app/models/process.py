from typing import Any

from pydantic import BaseModel, Field


class ProcessEvent(BaseModel):
    seq: int | None = None
    event: str
    job_id: str
    status: str
    stage: str | None = None
    progress: int | None = Field(default=None, ge=0, le=100)
    current_page: int | None = None
    total_pages: int | None = None
    warnings: list[str] = Field(default_factory=list)
    error: str | None = None
    result: dict[str, Any] | None = None
    message: str | None = None
    created_at: str