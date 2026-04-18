from app.core.config import get_settings
from app.services.process_service import ProcessService


def get_process_service() -> ProcessService:
    return ProcessService(get_settings())