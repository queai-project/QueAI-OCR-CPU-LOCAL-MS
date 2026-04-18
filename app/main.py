"""
Main application entry point.
Features:
- Environment-based docs
- Static frontend serving
- Health endpoint
- CORS
- Request logging
"""

import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.api.routers.api import api_router
from app.core.config import get_settings
from app.core.exceptions import AppError, to_error_payload
from app.core.logger import configure_logging, get_logger
from app.storage.temp_store import TempStore


BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIST_DIR = BASE_DIR.parent / "frontend_dist"


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger(__name__)

    temp_store = TempStore(settings)
    temp_store.ensure_root()
    removed = temp_store.cleanup_stale_workspaces()

    logger.info(
        f"application_startup stale_workspaces_removed={removed}",
        extra={"job_id": "-"},
    )

    try:
        yield
    finally:
        logger.info("application_shutdown", extra={"job_id": "-"})


def create_app() -> FastAPI:
    settings = get_settings()
    logger = get_logger(__name__)

    app = FastAPI(
        title=settings.project_name,
        version=settings.version,
        openapi_url=settings.openapi_path if settings.is_dev else None,
        docs_url=settings.docs_path if settings.is_dev else None,
        redoc_url=settings.redoc_path if settings.is_dev else None,
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = (time.time() - start_time) * 1000

        logger.info(
            f"{request.method} {request.url.path} "
            f"- {response.status_code} "
            f"- {process_time:.2f}ms",
            extra={"job_id": "-"},
        )
        return response

    @app.exception_handler(AppError)
    async def app_error_handler(_: Request, exc: AppError):
        payload = to_error_payload(exc)
        return JSONResponse(status_code=payload["status_code"], content=payload["body"])

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_: Request, exc: Exception):
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "message": "Internal server error",
                "data": {"detail": str(exc)},
            },
        )

    @app.get(settings.health_path, tags=["System"])
    async def health_check():
        return {
            "status": "online",
            "environment": settings.environment,
            "version": settings.version,
            "name": settings.project_name,
        }

    @app.get("/", include_in_schema=False)
    async def root():
        return RedirectResponse(url=settings.ui_path)

    app.include_router(api_router, prefix=settings.base_path)

    if FRONTEND_DIST_DIR.exists():
        app.mount(
            settings.ui_path,
            StaticFiles(directory=str(FRONTEND_DIST_DIR), html=True),
            name="ui",
        )
        logger.info(
            f"Frontend mounted at {settings.ui_path} from: {FRONTEND_DIST_DIR}",
            extra={"job_id": "-"},
        )
    else:
        logger.warning(
            f"Frontend dist directory not found: {FRONTEND_DIST_DIR}",
            extra={"job_id": "-"},
        )

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.is_dev,
    )