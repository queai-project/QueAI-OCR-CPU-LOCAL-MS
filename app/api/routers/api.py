from fastapi import APIRouter

from app.api.routers.debug import router as debug_router
from app.api.routers.health import router as health_router
from app.api.routers.process import router as process_router

api_router = APIRouter()
api_router.include_router(health_router, tags=["health"])
api_router.include_router(process_router, prefix="/process", tags=["process"])
api_router.include_router(debug_router, prefix="/debug", tags=["debug"])