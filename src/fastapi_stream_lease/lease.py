from __future__ import annotations

import asyncio
import inspect
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


try:
    import anyio

    _has_anyio = True
except ImportError:  # pragma: no cover
    _has_anyio = False

try:
    from starlette.responses import StreamingResponse as _StarletteStreamingResponse
except ImportError:  # pragma: no cover
    _StarletteStreamingResponse = object  # type: ignore[misc,assignment]


class ProtectedStreamingResponse(_StarletteStreamingResponse):
    """
    FastAPI/Starlette StreamingResponse subclass ensuring deterministic body_iterator
    aclose() cleanup on ASGI client disconnect, cancellation, or error.
    """

    lease: StreamLease | None = None

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            if _has_anyio:
                with anyio.CancelScope(shield=True):
                    await self._cleanup_response()
            else:  # pragma: no cover
                await self._cleanup_response()

    async def _cleanup_response(self) -> None:
        aclose = getattr(self.body_iterator, "aclose", None)
        if callable(aclose):
            cleanup = aclose()
            if inspect.isawaitable(cleanup):
                with suppress(Exception, asyncio.CancelledError):
                    await cleanup
        if self.lease is not None:
            if not self.lease._is_released:
                with suppress(Exception, asyncio.CancelledError):
                    await self.lease.release(reason="cancelled")
            elif self.lease._release_task is not None and not self.lease._release_task.done():
                with suppress(Exception, asyncio.CancelledError):
                    await self.lease._release_task


async def _close_single_target(target: Any, timeout: float) -> None:  # noqa: ASYNC109
    # 1. Look for aclose
    aclose = getattr(target, "aclose", None)
    if callable(aclose):
        try:

            async def _run_aclose() -> None:
                if inspect.iscoroutinefunction(aclose):
                    await asyncio.wait_for(aclose(), timeout=timeout)
                else:
                    res = aclose()
                    if inspect.isawaitable(res):
                        await asyncio.wait_for(res, timeout=timeout)

            if _has_anyio:
                with anyio.CancelScope(shield=True):
                    await _run_aclose()
            else:  # pragma: no cover
                await _run_aclose()
            return
        except (Exception, asyncio.TimeoutError, asyncio.CancelledError):
            return

    # 2. Look for close (async or sync)
    close = getattr(target, "close", None)
    if callable(close):
        try:

            async def _run_close() -> None:
                if inspect.iscoroutinefunction(close):
                    await asyncio.wait_for(close(), timeout=timeout)
                else:
                    # Sync close must be offloaded to worker thread to avoid event-loop starvation
                    res = await asyncio.wait_for(asyncio.to_thread(close), timeout=timeout)
                    if inspect.isawaitable(res):
                        await asyncio.wait_for(res, timeout=timeout)

            if _has_anyio:
                with anyio.CancelScope(shield=True):
                    await _run_close()
            else:  # pragma: no cover
                await _run_close()
            return
        except (Exception, asyncio.TimeoutError, asyncio.CancelledError):
            return


async def _close_stream_source(
    source: Any,
    iterator: Any,
    timeout: float = 2.0,  # noqa: ASYNC109
) -> None:
    """
    Deterministically close upstream stream iterator and source objects within
    a global timeout budget.
    """
    targets: list[Any] = [iterator]
    if source is not iterator:
        targets.append(source)

    deadline = time.monotonic() + timeout
    for target in targets:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            logger.warning(
                "Upstream cleanup timeout budget exhausted (%.2fs); skipping remaining targets",
                timeout,
            )
            break
        await _close_single_target(target, timeout=remaining)


def _safe_uncancel() -> None:
    task = asyncio.current_task()
    if task is not None:
        uncancel = getattr(task, "uncancel", None)
        if callable(uncancel):
            uncancel()


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
    _release_task: asyncio.Task[None] | None = field(default=None, init=False, repr=False)

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
        reason = "completed"
        try:
            if self._context_lease_lost.is_set():
                reason = "lost"
                _safe_uncancel()
                raise StreamLeaseLost(self.lease_id) from None
            if exc_type is asyncio.CancelledError:
                reason = "cancelled"
            elif exc_type is not None:
                reason = "error"
        finally:
            if self._context_renew_task is not None:
                await self._stop_auto_renew(self._context_renew_task)
                self._context_renew_task = None
            await self.release(reason=reason)

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

    async def release(self, reason: str = "manual") -> None:
        """Explicitly release this lease from Redis."""
        if self._is_released:
            if self._release_task is not None and not self._release_task.done():
                await self._await_release_task(self._release_task)
            return

        self._is_released = True
        if self._is_fallback:
            self.manager.dispatcher.dispatch(self.manager.config.on_released, self, reason)
            return

        async def _do_release() -> None:
            try:
                await self.manager.release(self)
            finally:
                self.manager.dispatcher.dispatch(self.manager.config.on_released, self, reason)

        task = asyncio.create_task(_do_release())
        self._release_task = task
        await self._await_release_task(task)

    async def _await_release_task(self, task: asyncio.Task[None]) -> None:
        if task.done():
            return
        cancelled = False
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
            await asyncio.wait({task})
        finally:
            if cancelled:
                raise asyncio.CancelledError()

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
                        owner.cancel()
                        self.manager._safe_record_lost("backend_timeout")
                        self.manager.dispatcher.dispatch(
                            self.manager.config.on_lost, self, "backend_timeout"
                        )
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
                    owner.cancel()
                    self.manager._safe_record_lost("unexpected_error")
                    self.manager.dispatcher.dispatch(
                        self.manager.config.on_lost, self, "unexpected_error"
                    )
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
                owner.cancel()
                self.manager._safe_record_lost("redis_revoked")
                self.manager.dispatcher.dispatch(self.manager.config.on_lost, self, "redis_revoked")
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
        close_source: bool = True,
    ) -> AsyncIterator[T]:
        """
        Wrap an async stream (e.g. SSE event generator or LLM token stream).

        Renew during long pauses and release when the iterator closes. If renewal
        fails, interrupt the stream with StreamLeaseLost. When close_source=True,
        deterministically invokes aclose() (or close()) on the underlying stream
        upon completion, early termination, cancellation, or error.
        """
        renew_task: asyncio.Task[None] | None = None
        lease_lost = asyncio.Event()
        reason = "completed"
        iterator = aiter(stream)

        try:
            if auto_renew:
                renew_task, lease_lost = self._start_auto_renew(renew_interval)
            async for chunk in iterator:
                yield chunk
        except asyncio.CancelledError:
            if auto_renew and lease_lost.is_set():
                reason = "lost"
                _safe_uncancel()
                raise StreamLeaseLost(self.lease_id) from None
            reason = "cancelled"
            raise
        except Exception:
            reason = "error"
            raise
        finally:
            if renew_task is not None:
                await self._stop_auto_renew(renew_task)
            try:
                if close_source:
                    timeout = self.manager.config.upstream_cleanup_timeout
                    await _close_stream_source(source=stream, iterator=iterator, timeout=timeout)
            finally:
                await self.release(reason=reason)

    def as_streaming_response(
        self,
        stream: AsyncIterable[Any],
        media_type: str = "text/event-stream",
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        auto_renew: bool = True,
        renew_interval: float | None = None,
        close_source: bool = True,
        **kwargs: Any,
    ) -> Any:
        """
        Wrap an async stream and return a Starlette/FastAPI StreamingResponse.

        Automatically manages lease renewal during iteration and releases Redis
        resources when the stream closes or client disconnects.
        """
        try:
            from starlette.responses import StreamingResponse  # noqa: F401
        except ImportError:
            raise RuntimeError(
                "Starlette or FastAPI must be installed to use as_streaming_response()."
            ) from None

        resp = ProtectedStreamingResponse(
            self.wrap(
                stream,
                auto_renew=auto_renew,
                renew_interval=renew_interval,
                close_source=close_source,
            ),
            media_type=media_type,
            status_code=status_code,
            headers=headers,
            **kwargs,
        )
        resp.lease = self
        return resp
