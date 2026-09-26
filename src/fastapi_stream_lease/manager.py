from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

from fastapi_stream_lease.config import LeaseConfig
from fastapi_stream_lease.exceptions import (
    StreamLeaseLost,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)
from fastapi_stream_lease.lease import StreamLease
from fastapi_stream_lease.lua import (
    ACQUIRE_SCRIPT,
    COUNT_SCRIPT,
    RELEASE_SCRIPT,
    RENEW_SCRIPT,
)

logger = logging.getLogger(__name__)


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
            if self.config.fail_open:
                logger.warning(
                    "Redis backend unavailable during acquire; fail_open=True allows lease %s: %s",
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
                )
                lease._is_fallback = True
                return lease
            raise StreamLeaseUnavailable(detail=f"Redis backend unavailable: {exc}") from exc

        code = int(result)
        if code == 2:
            raise StreamLeaseRejected(reason="user_limit")
        if code == 3:
            raise StreamLeaseRejected(reason="global_limit")
        if code != 1:
            raise RuntimeError(f"Unexpected stream lease acquisition return code: {code}")

        return StreamLease(
            lease_id=lease_id,
            user_id=user_id,
            user_key=user_key,
            global_key=global_key,
            manager=self,
            created_at=time.time(),
        )

    async def renew(self, lease: StreamLease) -> bool:
        """
        Renew an active lease, extending its TTL in Redis.

        Returns:
            bool: True if successfully extended, False if the lease expired, was evicted,
                  or if a Redis error occurred.
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
            logger.warning("Failed to renew stream lease %s: %s", lease.lease_id, exc)
            return False

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
            logger.warning("Failed to release stream lease %s: %s", lease.lease_id, exc)

    async def get_active_count(self, user_id: str | int | None = None) -> int:
        """
        Return the current number of active (non-expired) streams for a user or globally.
        """
        target_key = (
            self.config.user_key(user_id) if user_id is not None else self.config.global_key
        )
        count = await self.redis.eval(COUNT_SCRIPT, 1, target_key)
        return int(count)

    @asynccontextmanager
    async def lease(self, user_id: str | int) -> AsyncIterator[StreamLease]:
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
        try:
            renew_task, lease_lost = stream_lease._start_auto_renew()
            yield stream_lease
        except asyncio.CancelledError:
            if lease_lost.is_set():
                raise StreamLeaseLost(stream_lease.lease_id) from None
            raise
        finally:
            if renew_task is not None:
                await stream_lease._stop_auto_renew(renew_task)
            await stream_lease.release()
