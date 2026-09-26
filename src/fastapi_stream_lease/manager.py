from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import redis.exceptions

from fastapi_stream_lease.config import LeaseConfig
from fastapi_stream_lease.exceptions import (
    StreamLeaseLost,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)
from fastapi_stream_lease.lease import StreamLease, _safe_uncancel, _trigger_hook
from fastapi_stream_lease.lua import (
    ACQUIRE_SCRIPT,
    COUNT_SCRIPT,
    RELEASE_SCRIPT,
    RENEW_SCRIPT,
)

logger = logging.getLogger(__name__)


def is_network_error(exc: BaseException) -> bool:
    """Return True for transient network, timeout, or failover conditions."""
    if isinstance(
        exc,
        (
            redis.exceptions.AuthenticationError,
            getattr(redis.exceptions, "AuthorizationError", ()),
        ),
    ):
        return False
    if isinstance(
        exc,
        (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
            getattr(redis.exceptions, "ReadOnlyError", ()),
        ),
    ):
        return True
    if isinstance(exc, (ConnectionError, TimeoutError, asyncio.TimeoutError, OSError)):
        return True
    return False


class StreamLeaseManager:
    """
    Coordinates distributed stream concurrency leases backed by atomic Redis Lua scripts.
    """

    def __init__(self, redis: Any, config: LeaseConfig | None = None) -> None:
        self.redis = redis
        self.config: LeaseConfig = config or LeaseConfig()

    async def acquire(self, user_id: str | int) -> StreamLease:
        """
        Acquire a new stream lease for the given user.

        Raises:
            StreamLeaseRejected: If user or global concurrency limits are exceeded.
            RuntimeError: If Redis evaluation fails unexpectedly.
        """
        lease_id = uuid4().hex
        user_key = self.config.user_key(user_id)
        global_key = self.config.global_key
        start_monotonic = time.monotonic()
        try:
            result = await self.redis.eval(
                ACQUIRE_SCRIPT,
                2,
                user_key,
                global_key,
                self.config.lease_seconds,
                lease_id,
                self.config.max_per_user,
                self.config.max_global,
                self.config.redis_ttl,
            )
        except Exception as exc:
            if is_network_error(exc):
                await _trigger_hook(self.config.on_backend_error, exc)
                if self.config.fail_open:
                    logger.warning(
                        "Redis backend unavailable during acquire; "
                        "fail_open=True allows fallback lease %s: %s",
                        lease_id,
                        exc,
                    )
                    lease = StreamLease(
                        lease_id=lease_id,
                        user_id=user_id,
                        user_key=user_key,
                        global_key=global_key,
                        manager=self,
                        created_at=time.time(),
                        created_monotonic=start_monotonic,
                    )
                    lease._is_fallback = True
                    await _trigger_hook(self.config.on_acquired, lease)
                    return lease
                logger.warning(
                    "Redis backend unavailable during acquire for user %s: %s",
                    user_id,
                    exc,
                )
                raise StreamLeaseUnavailable(
                    detail="Stream lease coordination backend is temporarily unavailable",
                    retry_after=self.config.retry_after_seconds,
                ) from exc
            logger.error(
                "Execution error during stream lease acquire for user %s: %s",
                user_id,
                exc,
                exc_info=True,
            )
            raise

        code = int(result)
        if code == 2:
            await _trigger_hook(self.config.on_rejected, user_id, "user_limit")
            raise StreamLeaseRejected(reason="user_limit")
        if code == 3:
            await _trigger_hook(self.config.on_rejected, user_id, "global_limit")
            raise StreamLeaseRejected(reason="global_limit")
        if code != 1:
            raise RuntimeError(f"Unexpected stream lease acquisition return code: {code}")

        lease = StreamLease(
            lease_id=lease_id,
            user_id=user_id,
            user_key=user_key,
            global_key=global_key,
            manager=self,
            created_at=time.time(),
            created_monotonic=start_monotonic,
        )
        await _trigger_hook(self.config.on_acquired, lease)
        return lease

    async def renew(self, lease: StreamLease) -> bool:
        """
        Renew an active lease, extending its TTL in Redis.

        Returns:
            bool: True if successfully extended, False if the lease expired or was evicted in Redis.
        Raises:
            StreamLeaseUnavailable: If the Redis backend is unreachable.
        """
        check_global = 1 if self.config.max_global > 0 else 0
        try:
            result = await self.redis.eval(
                RENEW_SCRIPT,
                2,
                lease.user_key,
                lease.global_key,
                lease.lease_id,
                self.config.lease_seconds,
                self.config.redis_ttl,
                check_global,
            )
            return int(result) == 1
        except Exception as exc:
            if is_network_error(exc):
                await _trigger_hook(self.config.on_backend_error, exc)
                logger.warning(
                    "Network error renewing stream lease %s: %s",
                    lease.lease_id,
                    exc,
                )
                raise StreamLeaseUnavailable(
                    detail="Stream lease coordination backend is temporarily unavailable",
                    retry_after=self.config.retry_after_seconds,
                ) from exc
            logger.error(
                "Execution error renewing stream lease %s: %s",
                lease.lease_id,
                exc,
                exc_info=True,
            )
            raise

    async def release(self, lease: StreamLease) -> None:
        """
        Release an active lease immediately from Redis.
        """
        try:
            await self.redis.eval(
                RELEASE_SCRIPT,
                2,
                lease.user_key,
                lease.global_key,
                lease.lease_id,
            )
        except Exception as exc:
            if is_network_error(exc):
                await _trigger_hook(self.config.on_backend_error, exc)
                logger.warning("Network error releasing stream lease %s: %s", lease.lease_id, exc)
            else:
                logger.error(
                    "Execution error releasing stream lease %s: %s",
                    lease.lease_id,
                    exc,
                    exc_info=True,
                )

    async def get_active_count(self, user_id: str | int | None = None) -> int:
        """
        Return the current number of active (non-expired) streams for a user or globally.
        """
        target_key = (
            self.config.user_key(user_id) if user_id is not None else self.config.global_key
        )
        try:
            count = await self.redis.eval(COUNT_SCRIPT, 1, target_key)
            return int(count)
        except Exception as exc:
            if is_network_error(exc):
                await _trigger_hook(self.config.on_backend_error, exc)
                logger.warning(
                    "Network error querying active stream count for %s: %s",
                    target_key,
                    exc,
                )
                raise StreamLeaseUnavailable(
                    detail="Stream lease coordination backend is temporarily unavailable",
                    retry_after=self.config.retry_after_seconds,
                ) from exc
            logger.error(
                "Execution error querying active stream count for %s: %s",
                target_key,
                exc,
                exc_info=True,
            )
            raise

    @asynccontextmanager
    async def lease(
        self, user_id: str | int, renew_interval: float | None = None
    ) -> AsyncIterator[StreamLease]:
        """
        Context manager for acquiring and safely releasing a stream lease for scoped
        executions (such as WebSockets, background tasks, or pub/sub loops).

        For HTTP StreamingResponse (SSE / LLM tokens), use `lease = await acquire()`
        and `return StreamingResponse(lease.wrap(...))` instead.

        Example:
            async with lease_manager.lease(user_id=42) as lease:
                while True:
                    msg = await websocket.receive_text()
                    ...
        """
        stream_lease = await self.acquire(user_id)
        renew_task: asyncio.Task[None] | None = None
        lease_lost = asyncio.Event()
        reason = "completed"
        try:
            renew_task, lease_lost = stream_lease._start_auto_renew(renew_interval)
            yield stream_lease
        except asyncio.CancelledError:
            if lease_lost.is_set():
                reason = "lost"
                _safe_uncancel()
                raise StreamLeaseLost(stream_lease.lease_id) from None
            reason = "cancelled"
            raise
        except Exception:
            reason = "error"
            raise
        finally:
            if renew_task is not None:
                await stream_lease._stop_auto_renew(renew_task)
            await stream_lease.release(reason=reason)

    async def stream(
        self,
        user_id: str | int,
        stream: AsyncIterable[Any],
        media_type: str = "text/event-stream",
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        auto_renew: bool = True,
        renew_interval: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Acquire a lease and return a protected Starlette/FastAPI StreamingResponse.

        Provides 1-line streaming integration:
            @app.get("/stream")
            async def stream_view(user_id: str = Depends(auth)):
                return await manager.stream(user_id, token_generator())

        If lease acquisition fails (e.g. 429 Too Many Requests or 503 Unavailable),
        an exception is raised immediately. If stream setup fails before returning,
        the acquired lease is safely released to prevent lingering ghost leases.
        """
        lease = await self.acquire(user_id)
        try:
            return lease.as_streaming_response(
                stream,
                media_type=media_type,
                status_code=status_code,
                headers=headers,
                auto_renew=auto_renew,
                renew_interval=renew_interval,
                **kwargs,
            )
        except Exception:
            await lease.release()
            raise
