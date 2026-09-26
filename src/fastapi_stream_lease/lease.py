from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

from fastapi_stream_lease.exceptions import StreamLeaseLost, StreamLeaseUnavailable

if TYPE_CHECKING:
    from fastapi_stream_lease.manager import StreamLeaseManager

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _safe_uncancel() -> None:
    task = asyncio.current_task()
    if task is not None:
        uncancel = getattr(task, "uncancel", None)
        if callable(uncancel):
            uncancel()


async def _trigger_hook(hook: Any, *args: Any) -> None:
    """Safely trigger an optional sync or async lifecycle callback without raising."""
    if hook is None:
        return
    try:
        res = hook(*args)
        if asyncio.iscoroutine(res):
            await res
    except Exception as exc:
        logger.warning("Error executing lease lifecycle callback %s: %s", hook, exc)


@dataclass
class StreamLease:
    """Represents an active, acquired stream lease."""

    lease_id: str
    user_id: str | int
    user_key: str
    global_key: str
    manager: StreamLeaseManager
    created_at: float = field(default_factory=time.time)
    created_monotonic: float = field(default_factory=time.monotonic)
    expires_at: float = field(init=False)
    _is_released: bool = field(default=False, init=False)
    _is_fallback: bool = field(default=False, init=False)
    _context_renew_task: asyncio.Task[None] | None = field(default=None, init=False, repr=False)
    _context_lease_lost: asyncio.Event = field(
        default_factory=asyncio.Event, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self.expires_at = self.created_monotonic + self.manager.config.lease_seconds

    async def __aenter__(self) -> StreamLease:
        if self._is_released:
            raise StreamLeaseLost(self.lease_id)
        if self._context_renew_task is not None:
            raise RuntimeError("A stream lease cannot enter the same context twice")
        self._context_renew_task, self._context_lease_lost = self._start_auto_renew()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            if self._context_lease_lost.is_set():
                _safe_uncancel()
                raise StreamLeaseLost(self.lease_id) from None
        finally:
            if self._context_renew_task is not None:
                await self._stop_auto_renew(self._context_renew_task)
                self._context_renew_task = None
            await self.release()

    async def renew(self) -> bool:
        """
        Manually renew this lease, extending its TTL in Redis.

        Returns:
            bool: True if successfully extended, False if the lease expired or was evicted.
        Raises:
            StreamLeaseUnavailable: If the Redis backend is unreachable.
        """
        if self._is_released:
            return False
        if self._is_fallback:
            self.expires_at = time.monotonic() + self.manager.config.lease_seconds
            return True
        start_monotonic = time.monotonic()
        success = await self.manager.renew(self)
        if success:
            self.expires_at = start_monotonic + self.manager.config.lease_seconds
        return success

    async def release(self) -> None:
        """Explicitly release this lease from Redis."""
        if self._is_released:
            return
        self._is_released = True
        if not self._is_fallback:
            await self.manager.release(self)

    def _start_auto_renew(
        self, interval: float | None = None
    ) -> tuple[asyncio.Task[None], asyncio.Event]:
        nominal_interval = self.manager.config.lease_seconds / 2
        interval = nominal_interval if interval is None else interval
        if not 0 < interval < self.manager.config.lease_seconds:
            raise ValueError("renew_interval must be positive and shorter than lease_seconds")

        try:
            owner = asyncio.current_task()
        except RuntimeError:
            owner = None
        if owner is None:
            raise RuntimeError("Auto-renewal requires a running asyncio task")
        lease_lost = asyncio.Event()

        async def worker() -> None:
            current_interval = interval
            while not self._is_released:
                await asyncio.sleep(current_interval)
                if self._is_released:
                    return

                try:
                    renewed = await self.renew()
                except StreamLeaseUnavailable:
                    # Backend outage: lease might still be active in Redis;
                    # retry during remaining TTL
                    now = time.monotonic()
                    remaining = self.expires_at - now
                    min_window = max(0.05, interval / 4)
                    if remaining > min_window:
                        retry_interval = max(0.05, min(remaining / 3, 2.0))
                        logger.warning(
                            "Stream lease %s renewal attempt failed due to backend outage; "
                            "retrying in %.2fs (%.2fs remaining)",
                            self.lease_id,
                            retry_interval,
                            remaining,
                        )
                        current_interval = retry_interval
                        continue
                    else:
                        logger.error(
                            "Stream lease %s expired during backend outage; cancelling stream",
                            self.lease_id,
                        )
                        lease_lost.set()
                        await _trigger_hook(self.manager.config.on_lost, self, "backend_timeout")
                        owner.cancel()
                        return
                except Exception as exc:
                    logger.error(
                        "Stream lease %s unexpected failure during auto-renewal: %s; "
                        "cancelling stream",
                        self.lease_id,
                        exc,
                        exc_info=True,
                    )
                    lease_lost.set()
                    await _trigger_hook(self.manager.config.on_lost, self, "unexpected_error")
                    owner.cancel()
                    return

                if renewed:
                    current_interval = interval
                    continue

                # Redis confirmed lease is lost/expired (renew() returned False).
                # Terminate immediately without retries to enforce concurrency limits.
                logger.error(
                    "Stream lease %s was revoked or expired in Redis; "
                    "cancelling stream immediately",
                    self.lease_id,
                )
                lease_lost.set()
                await _trigger_hook(self.manager.config.on_lost, self, "redis_revoked")
                owner.cancel()
                return

        return asyncio.create_task(worker()), lease_lost

    @staticmethod
    async def _stop_auto_renew(task: asyncio.Task[None]) -> None:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def wrap(
        self,
        stream: AsyncIterable[T],
        auto_renew: bool = True,
        renew_interval: float | None = None,
    ) -> AsyncIterator[T]:
        """
        Wrap an async stream (e.g. SSE event generator or LLM token stream).

        Renew during long pauses and release when the iterator closes. If renewal
        fails, interrupt the stream with StreamLeaseLost. Consumers that stop early
        must close the iterator (for example, with contextlib.aclosing).
        """
        renew_task: asyncio.Task[None] | None = None
        lease_lost = asyncio.Event()

        try:
            if auto_renew:
                renew_task, lease_lost = self._start_auto_renew(renew_interval)
            async for chunk in stream:
                yield chunk
        except asyncio.CancelledError:
            if lease_lost.is_set():
                _safe_uncancel()
                raise StreamLeaseLost(self.lease_id) from None
            raise
        finally:
            if renew_task is not None:
                await self._stop_auto_renew(renew_task)
            await self.release()
