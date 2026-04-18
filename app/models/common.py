from typing import Generic, Optional, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class ApiResponse(BaseModel, Generic[T]):
    success: bool
    message: Optional[str] = None
    data: Optional[T] = None

    @classmethod
    def success_response(cls, data: Optional[T] = None, message: Optional[str] = None):
        return cls(success=True, message=message, data=data)