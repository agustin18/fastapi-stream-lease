import asyncio
import time

import pytest

from fastapi_stream_lease.config import LeaseConfig
from fastapi_stream_lease.dispatcher import HookDispatcher
from fastapi_stream_lease.manager import StreamLeaseManager


def test_dispatcher_invalid_max_size():
    """Verify that non-positive max_queue_size raises ValueError."""
    with pytest.raises(ValueError, match="max_queue_size must be greater than 0"):
        HookDispatcher(max_queue_size=0)
    with pytest.raises(ValueError, match="max_queue_size must be greater than 0"):
        HookDispatcher(max_queue_size=-5)


@pytest.mark.asyncio
async def test_dispatch_none_hook():
    """Verify that dispatching None is a no-op returning True."""
    dispatcher = HookDispatcher()
    assert dispatcher.dispatch(None) is True
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatch_sync_and_async_fifo_ordering():
    """Verify that sync and async hooks are processed in strict FIFO order when queued."""
    dispatcher = HookDispatcher(max_queue_size=100, sync_inline=False)
    execution_order = []

    def sync_hook(idx: int):
        execution_order.append(f"sync_{idx}")

    async def async_hook(idx: int):
        await asyncio.sleep(0.01)
        execution_order.append(f"async_{idx}")

    dispatcher.dispatch(sync_hook, 1)
    dispatcher.dispatch(async_hook, 2)
    dispatcher.dispatch(sync_hook, 3)
    dispatcher.dispatch(async_hook, 4)

    await dispatcher.drain(timeout=2.0)
    assert execution_order == ["sync_1", "async_2", "sync_3", "async_4"]
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatch_sync_blocking_does_not_block_loop():
    """Verify that blocking sync hooks are offloaded to a thread when sync_inline=False."""
    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=False)
    hook_ran = False

    def blocking_sync_hook():
        nonlocal hook_ran
        time.sleep(0.1)
        hook_ran = True

    start = time.monotonic()
    dispatcher.dispatch(blocking_sync_hook)
    dispatch_elapsed = time.monotonic() - start

    # dispatch() itself must be instantaneous (O(1))
    assert dispatch_elapsed < 0.05

    # Concurrently, the event loop should remain responsive
    loop_ticks = 0
    while not hook_ran:
        await asyncio.sleep(0.01)
        loop_ticks += 1

    assert loop_ticks >= 3, f"Loop was blocked; only ticked {loop_ticks} times"
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatch_sync_inline_execution_and_exception():
    """Verify that sync_inline=True executes synchronous callbacks immediately inline."""
    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=True)
    events = []

    def sync_hook(x):
        events.append(x)

    def failing_sync_hook(x):
        raise RuntimeError("inline failure")

    assert dispatcher.dispatch(sync_hook, "inline_immediate") is True
    assert events == ["inline_immediate"]

    # Failing hook logs warning and returns False without raising out
    assert dispatcher.dispatch(failing_sync_hook, "bad") is False
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatch_sync_returning_coroutine():
    """Verify that a regular function returning a coroutine is enqueued and awaited."""
    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=True)
    awaited = []

    def factory_hook(val):
        async def inner():
            await asyncio.sleep(0.01)
            awaited.append(val)

        return inner()

    assert dispatcher.dispatch(factory_hook, "from_coro") is True
    await dispatcher.drain(timeout=2.0)
    assert awaited == ["from_coro"]
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatch_bounded_queue_backpressure():
    """Verify that events exceeding max_queue_size are rejected with backpressure."""
    dispatcher = HookDispatcher(max_queue_size=2, sync_inline=False)
    blocked_event = asyncio.Event()

    async def blocking_hook():
        await blocked_event.wait()

    # Worker picks up item 1 immediately
    assert dispatcher.dispatch(blocking_hook) is True
    await asyncio.sleep(0.01)

    # Fill queue to capacity (max_queue_size=2)
    assert dispatcher.dispatch(lambda: None) is True
    assert dispatcher.dispatch(lambda: None) is True

    # 4th item should exceed queue capacity and be dropped
    assert dispatcher.dispatch(lambda: None) is False

    # Unblock worker and clean up
    blocked_event.set()
    await dispatcher.drain(timeout=2.0)
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatcher_drain_timeout():
    """Verify that drain timeout is handled gracefully when worker is blocked."""
    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=False)
    never_finish = asyncio.Event()

    async def hung_hook():
        await never_finish.wait()

    dispatcher.dispatch(hung_hook)
    await asyncio.sleep(0.01)

    # drain should timeout and not raise
    await dispatcher.drain(timeout=0.05)

    never_finish.set()
    await dispatcher.close(drain=True, timeout=1.0)


@pytest.mark.asyncio
async def test_dispatcher_drain_and_close():
    """Verify clean drain and close lifecycle."""
    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=False)
    processed = []

    def task(n):
        processed.append(n)

    for i in range(5):
        dispatcher.dispatch(task, i)

    await dispatcher.drain(timeout=2.0)
    assert processed == [0, 1, 2, 3, 4]

    await dispatcher.close(drain=True)
    # Subsequent dispatch returns False after close
    assert dispatcher.dispatch(task, 99) is False
    # Multiple close calls are idempotent
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatcher_exception_handling():
    """Verify that an exception in a hook does not crash the dispatcher worker."""
    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=False)
    results = []

    def buggy_hook():
        raise RuntimeError("Telemetry failure")

    def good_hook(val):
        results.append(val)

    dispatcher.dispatch(buggy_hook)
    dispatcher.dispatch(good_hook, "recovered")

    await dispatcher.drain(timeout=2.0)
    assert results == ["recovered"]
    await dispatcher.close()


@pytest.mark.asyncio
async def test_manager_lifecycle_context_and_drain(fake_redis):
    """Verify StreamLeaseManager __aenter__, __aexit__, drain, and close."""
    events = []

    async def async_hook(lease):
        await asyncio.sleep(0.01)
        events.append("acq")

    config = LeaseConfig(on_acquired=async_hook, hook_queue_size=100)
    async with StreamLeaseManager(redis=fake_redis, config=config) as mgr:
        lease = await mgr.acquire("u_ctx")
        await mgr.drain(timeout=2.0)
        assert events == ["acq"]
        await lease.release()


@pytest.mark.asyncio
async def test_dispatcher_metrics_properties():
    """Verify queued_count, dropped_count, error_count, and queue_depth counters."""
    dispatcher = HookDispatcher(max_queue_size=2, sync_inline=False)
    block_worker = asyncio.Event()

    async def blocking_hook():
        await block_worker.wait()

    def failing_hook():
        raise RuntimeError("boom")

    assert dispatcher.queued_count == 0
    assert dispatcher.dropped_count == 0
    assert dispatcher.error_count == 0
    assert dispatcher.queue_depth == 0

    # 1. Enqueue blocking hook (will be picked up by worker immediately)
    assert dispatcher.dispatch(blocking_hook) is True
    await asyncio.sleep(0.01)

    # 2. Fill queue to max capacity (2 items)
    assert dispatcher.dispatch(lambda: None) is True
    assert dispatcher.dispatch(failing_hook) is True
    assert dispatcher.queue_depth == 2

    # 3. Exceed capacity -> should drop
    assert dispatcher.dispatch(lambda: None) is False
    assert dispatcher.dropped_count == 1

    # Unblock worker and let items process
    block_worker.set()
    await dispatcher.drain(timeout=2.0)

    assert dispatcher.queued_count == 3
    assert dispatcher.dropped_count == 1
    assert dispatcher.error_count == 1
    assert dispatcher.queue_depth == 0

    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatcher_default_sync_inline_is_false():
    """Verify that default HookDispatcher sets sync_inline=False for out-of-band execution."""
    dispatcher = HookDispatcher()
    assert dispatcher._sync_inline is False
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatcher_close_worker_cancellation_timeout():
    """Verify that close() forcibly cancels hung worker task on timeout without uncaught error."""
    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=False)
    hung_forever = asyncio.Event()

    async def hang():
        await hung_forever.wait()

    dispatcher.dispatch(hang)
    await asyncio.sleep(0.01)

    # close without drain, with tiny timeout so worker is cancelled while hanging
    await dispatcher.close(drain=False, timeout=0.01)
    assert dispatcher._closed is True
    assert dispatcher._worker_task is not None
    assert dispatcher._worker_task.done()


@pytest.mark.asyncio
async def test_dispatcher_sync_hook_returning_awaitable():
    """Verify that a sync hook returning an awaitable coroutine is awaited in the consumer."""
    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=False)
    executed = []

    async def async_inner():
        executed.append("done")

    def sync_returning_coro():
        return async_inner()

    dispatcher.dispatch(sync_returning_coro)
    await dispatcher.drain(timeout=2.0)
    assert executed == ["done"]
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatcher_rate_limited_drop_logging():
    """Verify rate-limited logging branch when multiple drops occur in rapid succession."""
    dispatcher = HookDispatcher(max_queue_size=1, sync_inline=False)
    block_worker = asyncio.Event()

    async def blocker():
        await block_worker.wait()

    dispatcher.dispatch(blocker)
    await asyncio.sleep(0.01)

    # Fill queue to capacity (1 item)
    dispatcher.dispatch(lambda: None)
    # First drop (triggers log)
    assert dispatcher.dispatch(lambda: None) is False
    # Second drop within 2s (hits rate-limit branch without re-logging)
    assert dispatcher.dispatch(lambda: None) is False
    assert dispatcher.dropped_count == 2

    block_worker.set()
    await dispatcher.drain(timeout=2.0)
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatcher_enqueue_runtime_error():
    """Verify that RuntimeError during _enqueue (e.g. shutdown) drops and returns False."""
    from unittest.mock import patch

    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=False)
    with patch.object(dispatcher, "_ensure_worker", side_effect=RuntimeError("Loop closed")):
        assert dispatcher.dispatch(lambda: None) is False
        assert dispatcher.dropped_count == 1
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatcher_drain_when_queue_none():
    """Verify drain() returns immediately when no worker/queue was instantiated."""
    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=False)
    assert dispatcher._queue is None
    await dispatcher.drain(timeout=1.0)
    await dispatcher.close()


@pytest.mark.asyncio
async def test_dispatcher_callbacks_and_resilience():
    """Verify on_drop/on_error callbacks are triggered and exceptions are safely suppressed."""
    drop_called = []
    error_called = []

    def on_drop():
        drop_called.append(True)
        raise RuntimeError("failing drop callback")

    def on_error():
        error_called.append(True)
        raise RuntimeError("failing error callback")

    dispatcher = HookDispatcher(
        max_queue_size=1,
        sync_inline=True,
        on_drop=on_drop,
        on_error=on_error,
    )

    # 1. Sync inline error triggers on_error
    def bad_sync():
        raise ValueError("sync err")

    dispatcher.dispatch(bad_sync)
    assert len(error_called) == 1

    # 2. Async error in queue triggers on_error
    async def bad_async():
        raise ValueError("async err")

    dispatcher.dispatch(bad_async)
    await dispatcher.drain(timeout=1.0)
    assert len(error_called) == 2

    # 3. Queue full triggers on_drop
    blocker = asyncio.Event()

    async def blocking():
        await blocker.wait()

    async def dummy():
        pass

    dispatcher.dispatch(blocking)
    assert dispatcher.dispatch(dummy) is False  # dropped!
    assert len(drop_called) == 1

    # 4. Dispatch after close triggers on_drop
    blocker.set()
    await dispatcher.close()
    assert dispatcher.dispatch(dummy) is False
    assert len(drop_called) == 2


@pytest.mark.asyncio
async def test_dispatcher_on_queue_change_lifecycle():
    """Verify on_queue_change receives +1 on enqueue and -1 on dequeue."""
    changes = []

    def on_change(delta: int) -> None:
        changes.append(delta)

    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=False, on_queue_change=on_change)

    executed = asyncio.Event()

    async def sample_hook():
        executed.set()

    dispatcher.dispatch(sample_hook)
    assert 1 in changes

    await executed.wait()
    await dispatcher.drain(timeout=1.0)
    await dispatcher.close()

    assert sum(changes) == 0
    assert changes.count(1) == 1
    assert changes.count(-1) == 1


@pytest.mark.asyncio
async def test_dispatcher_on_queue_change_close_without_drain():
    """Verify on_queue_change purges remaining queue items with negative delta
    on un-drained close."""
    changes = []
    blocker = asyncio.Event()

    def on_change(delta: int) -> None:
        changes.append(delta)

    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=False, on_queue_change=on_change)

    async def blocking_hook():
        await blocker.wait()

    async def pending_hook_1():
        pass

    async def pending_hook_2():
        pass

    dispatcher.dispatch(blocking_hook)
    dispatcher.dispatch(pending_hook_1)
    dispatcher.dispatch(pending_hook_2)

    # Let the worker pick up blocking_hook
    await asyncio.sleep(0.02)

    # Close with drain=False while items are still in queue
    await dispatcher.close(drain=False)
    blocker.set()

    # The sum of all changes must balance out to 0 (no leaked gauge)
    assert sum(changes) == 0


@pytest.mark.asyncio
async def test_dispatcher_on_queue_change_exception_resilience():
    """Verify that exceptions in on_queue_change do not disrupt dispatcher operation."""

    def broken_change(delta: int) -> None:
        raise RuntimeError("queue change notification failure")

    dispatcher = HookDispatcher(max_queue_size=10, sync_inline=False, on_queue_change=broken_change)

    executed = asyncio.Event()

    async def hook():
        executed.set()

    assert dispatcher.dispatch(hook) is True
    await executed.wait()
    await dispatcher.drain(timeout=1.0)
    await dispatcher.close()
