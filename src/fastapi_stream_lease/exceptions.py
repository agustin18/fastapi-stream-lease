from __future__ import annotations

from typing import Any, Literal


class StreamLeaseError(Exception):
    """Base exception for all stream lease errors."""


class StreamLeaseRejected(StreamLeaseError):
    """Raised when a stream lease request is rejected due to concurrency limits."""

    def __init__(
        self,
        reason: Literal["user_limit", "global_limit"],
        retry_after: int = 5,
        detail: str | None = None,
    ) -> None:
        self.reason: Literal["user_limit", "global_limit"] = reason
        self.retry_after: int = retry_after
        self.detail: str = detail or (
            "User stream limit reached" if reason == "user_limit" else "Global stream limit reached"
        )
        super().__init__(f"Stream lease rejected: {self.reason} - {self.detail}")

    def as_response(self) -> Any:
        """Convert this rejection into a JSONResponse (HTTP 429)."""
        try:
            from starlette.responses import JSONResponse
        except ImportError as err:
            raise RuntimeError(
                "Starlette or FastAPI must be installed to use as_response()"
            ) from err

        return JSONResponse(
            status_code=429,
            content={
                "code": "stream_connection_limit",
                "reason": self.reason,
                "detail": self.detail,
            },
            headers={"Retry-After": str(self.retry_after)},
        )

    def as_http_exception(self) -> Any:
        """Convert this rejection into a FastAPI/Starlette HTTPException (HTTP 429) to be raised."""
        try:
            from starlette.exceptions import HTTPException
        except ImportError as err:
            raise RuntimeError(
                "FastAPI or Starlette must be installed to use as_http_exception()"
            ) from err

        return HTTPException(
            status_code=429,
            detail=self.detail,
            headers={"Retry-After": str(self.retry_after)},
        )
