from __future__ import annotations

import asyncio
import inspect
import logging
import time
from contextlib import suppress
from typing import Any

logger = logging.getLogger(__name__)


class HookDispatcher:
    """Bounded-queue asynchronous and threadpool lifecycle hook dispatcher.

    Provides strict FIFO execution order for telemetry/observability callbacks,
    offloads asynchronous and blocking synchronous callbacks off the stream critical path,
    and enforces bounded queue backpressure to prevent unbounded task accumulation.
    """

    def __init__(
        self,
        max_queue_size: int = 1024,
        sync_inline: bool = False,
        on_drop: Any | None = None,
        on_error: Any | None = None,
    ) -> None:
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be greater than 0")
        self._max_queue_size = max_queue_size
        self._sync_inline = sync_inline
        self._on_drop = on_drop
        self._on_error = on_error
        self._queue: asyncio.Queue[tuple[Any, tuple[Any, ...]] | None] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._closed = False
        self._queued_count = 0
        self._dropped_count = 0
        self._error_count = 0
        self._last_drop_log_time = 0.0

    @property
    def queued_count(self) -> int:
        """Total number of callback invocations successfully enqueued."""
        return self._queued_count

    @property
    def dropped_count(self) -> int:
        """Total number of callback invocations dropped due to queue backpressure or closure."""
        return self._dropped_count

    @property
    def error_count(self) -> int:
        """Total number of exceptions raised during callback execution."""
        return self._error_count

    @property
    def queue_depth(self) -> int:
        """Current number of pending callbacks in the queue."""
        return self._queue.qsize() if self._queue is not None else 0

    def _ensure_worker(self) -> None:
        """Lazily initialize the queue and consumer worker task on the running loop."""
        if self._queue is None:
            self._queue = asyncio.Queue(maxsize=self._max_queue_size)
        if self._worker_task is None or self._worker_task.done():
            loop = asyncio.get_running_loop()
            self._worker_task = loop.create_task(self._worker())

    def _notify_drop(self) -> None:
        if self._on_drop is not None:
            try:
                self._on_drop()
            except Exception:
                pass

    def _notify_error(self) -> None:
        if self._on_error is not None:
            try:
                self._on_error()
            except Exception:
                pass

    async def _worker(self) -> None:
        """Background consumer executing queued callbacks in strict FIFO order."""
        assert self._queue is not None
        while True:
            item = await self._queue.get()
            if item is None:
                self._queue.task_done()
                break
            hook_or_coro, args = item
            try:
                if asyncio.iscoroutine(hook_or_coro):
                    await hook_or_coro
                elif inspect.iscoroutinefunction(hook_or_coro):
                    await hook_or_coro(*args)
                else:
                    res = await asyncio.to_thread(hook_or_coro, *args)
                    if inspect.isawaitable(res):
                        await res
            except Exception as exc:
                self._error_count += 1
                self._notify_error()
                logger.warning(
                    "Error executing lifecycle callback %s: %s",
                    hook_or_coro,
                    exc,
                )
            finally:
                self._queue.task_done()

    def _enqueue(self, hook_or_coro: Any, args: tuple[Any, ...]) -> bool:
        try:
            self._ensure_worker()
            assert self._queue is not None
            self._queue.put_nowait((hook_or_coro, args))
            self._queued_count += 1
            return True
        except asyncio.QueueFull:
            self._dropped_count += 1
            self._notify_drop()
            now = time.monotonic()
            if now - self._last_drop_log_time >= 2.0:
                self._last_drop_log_time = now
                logger.warning(
                    "Lifecycle hook queue full (capacity=%d, dropped_total=%d); "
                    "dropping callback %s",
                    self._max_queue_size,
                    self._dropped_count,
                    hook_or_coro,
                )
            return False
        except RuntimeError as exc:
            # Event loop is closed or shutting down
            self._dropped_count += 1
            self._notify_drop()
            logger.warning("Could not dispatch lifecycle hook %s: %s", hook_or_coro, exc)
            return False

    def dispatch(self, hook: Any, *args: Any) -> bool:
        """Dispatch a callback hook.

        If sync_inline is True and hook is a synchronous callable, it executes inline.
        Otherwise (or if hook is asynchronous), it is enqueued into the bounded FIFO queue.

        Returns:
            bool: True if executed or queued, False if dropped or closed.
        """
        if hook is None:
            return True
        if self._closed:
            self._dropped_count += 1
            self._notify_drop()
            logger.warning("Attempted to dispatch hook %s on a closed dispatcher", hook)
            return False

        if self._sync_inline and not inspect.iscoroutinefunction(hook):
            try:
                res = hook(*args)
                if asyncio.iscoroutine(res):
                    return self._enqueue(res, ())
                return True
            except Exception as exc:
                self._error_count += 1
                self._notify_error()
                logger.warning("Error executing lifecycle callback %s: %s", hook, exc)
                return False

        return self._enqueue(hook, args)

    async def drain(self, timeout: float = 5.0) -> None:
        """Wait until all currently queued callbacks have finished executing."""
        if self._queue is not None:
            try:
                await asyncio.wait_for(self._queue.join(), timeout=timeout)
            except (TimeoutError, asyncio.TimeoutError):
                logger.warning(
                    "Timed out waiting for lifecycle hook queue to drain (%d remaining)",
                    self._queue.qsize(),
                )

    async def close(self, drain: bool = True, timeout: float = 5.0) -> None:
        """Shut down the dispatcher and terminate the consumer worker task."""
        if self._closed:
            return
        self._closed = True

        if drain and self._queue is not None:
            await self.drain(timeout=timeout)

        has_active_worker = (
            self._queue is not None
            and self._worker_task is not None
            and not self._worker_task.done()
        )
        if has_active_worker:
            assert self._queue is not None
            assert self._worker_task is not None
            try:
                self._queue.put_nowait(None)
                await asyncio.wait_for(self._worker_task, timeout=timeout)
            except (asyncio.QueueFull, TimeoutError, asyncio.TimeoutError):
                self._worker_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._worker_task
