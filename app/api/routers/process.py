from fastapi import APIRouter, Depends, File, Request, UploadFile
from fastapi.responses import StreamingResponse

from app.api.dependencies import get_process_service
from app.services.process_service import ProcessService

router = APIRouter()


@router.post(
    "/stream",
    response_class=StreamingResponse,
    responses={
        200: {"content": {"text/event-stream": {}}},
    },
)
async def process_stream(
    request: Request,
    file: UploadFile = File(...),
    service: ProcessService = Depends(get_process_service),
):
    return await service.start_stream(request=request, file=file)