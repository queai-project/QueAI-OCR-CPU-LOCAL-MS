class AppError(Exception):
    status_code = 400
    message = "Application error"

    def __init__(self, message: str | None = None):
        if message:
            self.message = message
        super().__init__(self.message)


class InvalidFileTypeError(AppError):
    status_code = 400
    message = "Invalid file type"


class FileTooLargeError(AppError):
    status_code = 413
    message = "File too large"


class EmptyFileError(AppError):
    status_code = 400
    message = "Uploaded file is empty"


class CorruptPdfError(AppError):
    status_code = 400
    message = "PDF file is corrupt or unreadable"


class CorruptImageError(AppError):
    status_code = 400
    message = "Image file is corrupt or unreadable"


class PdfPageLimitExceededError(AppError):
    status_code = 400
    message = "PDF page limit exceeded"


class TemporaryStorageError(AppError):
    status_code = 500
    message = "Temporary storage error"


class QueueEnqueueError(AppError):
    status_code = 500
    message = "Failed to enqueue job"


class RedisConnectionAppError(AppError):
    status_code = 500
    message = "Redis connection error"


class PdfRenderError(AppError):
    status_code = 500
    message = "Failed to render PDF pages"


class OCRExecutionError(AppError):
    status_code = 500
    message = "OCR execution failed"


class AuthenticationError(AppError):
    status_code = 401
    message = "Invalid or missing API key"


def to_error_payload(exc: AppError) -> dict:
    return {
        "status_code": exc.status_code,
        "body": {
            "success": False,
            "message": exc.message,
            "data": None,
        },
    }