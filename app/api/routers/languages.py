from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.core.config import Settings
from app.services.language_service import LanguageService

router = APIRouter()


class InstallLanguagesRequest(BaseModel):
    codes: list[str]


def _service() -> LanguageService:
    settings = Settings()
    return LanguageService(settings)


@router.get("/catalog")
def get_language_catalog():
    service = _service()
    return {
        "default": service.settings.tesseract_default_lang,
        "installed": service.list_installed(),
        "catalog": service.fetch_catalog(),
        "processing_options": service.processing_options(),
    }


@router.get("/installed")
def get_installed_languages():
    service = _service()
    return {
        "default": service.settings.tesseract_default_lang,
        "installed": service.list_installed(),
        "processing_options": service.processing_options(),
    }


@router.post("/install")
def install_languages(payload: InstallLanguagesRequest):
    service = _service()

    try:
        return service.install_languages(payload.codes)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc