from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from fastapi_stream_lease.manager import StreamLeaseManager

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class StreamLease:
    """Represents an active, acquired stream lease."""

    lease_id: str
    user_id: str | int
    user_key: str
    global_key: str
    manager: StreamLeaseManager
    created_at: float = field(default_factory=time.time)
    _is_released: bool = field(default=False, init=False)

    async def __aenter__(self) -> StreamLease:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.release()

    async def renew(self) -> bool:
        """Manually renew this lease, extending its TTL in Redis."""
        if self._is_released:
            return False
        return await self.manager.renew(self)

    async def release(self) -> None:
        """Explicitly release this lease from Redis."""
        if self._is_released:
            return
        self._is_released = True
        await self.manager.release(self)

    async def wrap(
        self,
        stream: AsyncIterable[T],
        auto_renew: bool = True,
        renew_interval: float | None = None,
    ) -> AsyncIterator[T]:
        """
        Wrap an async stream (e.g. SSE event generator or LLM token stream).

        Guarantees that:
        1. A background renewal worker keeps the lease alive during long pauses
           (such as slow LLM time-to-first-token or client idle periods).
        2. When the stream terminates, is cancelled, or the client disconnects,
           the lease is guaranteed to be released in the `finally` block.
        """
        if renew_interval is not None and renew_interval >= self.manager.config.lease_seconds:
            logger.warning(
                "renew_interval (%.1fs) >= lease_seconds (%.1fs) on lease %s",
                renew_interval,
                self.manager.config.lease_seconds,
                self.lease_id,
            )

        interval = renew_interval or (self.manager.config.lease_seconds / 2.0)
        renew_task: asyncio.Task[None] | None = None

        async def _auto_renew_worker() -> None:
            while not self._is_released:
                await asyncio.sleep(interval)
                success = await self.renew()
                if not success:
                    logger.warning("Stream lease %s was lost during auto-renewal", self.lease_id)
                    break

        if auto_renew:
            renew_task = asyncio.create_task(_auto_renew_worker())

        try:
            async for chunk in stream:
                yield chunk
        finally:
            if renew_task is not None:
                renew_task.cancel()
                with suppress(asyncio.CancelledError):
                    await renew_task
            await self.release()
