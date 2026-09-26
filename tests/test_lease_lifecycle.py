from __future__ import annotations

import asyncio
import json
import time
from contextlib import aclosing
from unittest.mock import AsyncMock

import pytest

from fastapi_stream_lease import (
    LeaseConfig,
    StreamLease,
    StreamLeaseLost,
    StreamLeaseManager,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)


@pytest.mark.asyncio
async def test_context_manager_lifecycle(lease_manager):
    async with lease_manager.lease("user_1") as lease:
        assert lease.lease_id is not None
        assert await lease_manager.get_active_count("user_1") == 1

    # After block exit, lease is automatically released
    assert await lease_manager.get_active_count("user_1") == 0


@pytest.mark.asyncio
async def test_wrap_stream_successful(lease_manager):
    async def sample_generator():
        for i in range(5):
            yield f"token_{i}"
            await asyncio.sleep(0.05)

    lease = await lease_manager.acquire("user_1")
    assert await lease_manager.get_active_count("user_1") == 1

    consumed = []
    async for item in lease.wrap(sample_generator()):
        consumed.append(item)

    assert consumed == ["token_0", "token_1", "token_2", "token_3", "token_4"]
    # Guaranteed release after completion
    assert await lease_manager.get_active_count("user_1") == 0


@pytest.mark.asyncio
async def test_wrap_stream_with_exception(lease_manager):
    async def faulty_generator():
        yield "chunk_1"
        raise ValueError("Boom upstream!")

    lease = await lease_manager.acquire("user_1")
    assert await lease_manager.get_active_count("user_1") == 1

    consumed = []
    with pytest.raises(ValueError, match="Boom upstream!"):
        async for chunk in lease.wrap(faulty_generator()):
            consumed.append(chunk)

    assert consumed == ["chunk_1"]
    # Lease must be cleanly released even if generator errors
    assert await lease_manager.get_active_count("user_1") == 0


@pytest.mark.asyncio
async def test_wrap_stream_consumer_break(lease_manager):
    async def infinite_generator():
        i = 0
        while True:
            yield i
            i += 1
            await asyncio.sleep(0.01)

    lease = await lease_manager.acquire("user_1")
    assert await lease_manager.get_active_count("user_1") == 1

    async with aclosing(lease.wrap(infinite_generator())) as stream:
        async for item in stream:
            if item == 3:
                break

    # Consumer broke early with aclosing (standard asyncgen lifecycle) - lease is cleanly released
    assert await lease_manager.get_active_count("user_1") == 0


@pytest.mark.asyncio
async def test_wrap_stream_auto_renew_keeps_lease_alive(lease_manager):
    # lease expires in 2.0s, renew_interval will be set to 0.4s
    async def slow_generator():
        yield "first"
        # Sleep longer than the 2.0s lease timeout
        await asyncio.sleep(2.5)
        yield "second"

    lease = await lease_manager.acquire("user_1")
    assert await lease_manager.get_active_count("user_1") == 1

    chunks = []
    async for chunk in lease.wrap(slow_generator(), auto_renew=True, renew_interval=0.4):
        chunks.append(chunk)

    assert chunks == ["first", "second"]
    # Released at the end
    assert await lease_manager.get_active_count("user_1") == 0


@pytest.mark.asyncio
async def test_lease_release_idempotent(lease_manager):
    lease = await lease_manager.acquire("user_1")
    assert await lease_manager.get_active_count("user_1") == 1

    # First release
    await lease.release()
    assert await lease_manager.get_active_count("user_1") == 0

    # Second release should be a no-op
    await lease.release()
    assert await lease_manager.get_active_count("user_1") == 0


@pytest.mark.asyncio
async def test_wrap_stream_auto_renew_lost_lease(lease_manager, fake_redis):
    async def lingering_generator():
        yield "start"
        # Wait while auto-renew worker attempts to renew
        await asyncio.sleep(0.2)
        yield "end"

    lease = await lease_manager.acquire("user_1")

    # Wipe redis so renewal returns 0 (lease expired/evicted) during stream
    await fake_redis.flushall()

    chunks = []
    with pytest.raises(StreamLeaseLost):
        async for chunk in lease.wrap(lingering_generator(), auto_renew=True, renew_interval=0.04):
            chunks.append(chunk)

    assert chunks == ["start"]


@pytest.mark.asyncio
async def test_stream_lease_as_context_manager(lease_manager):
    lease = await lease_manager.acquire("user_cm")
    assert await lease_manager.get_active_count("user_cm") == 1

    async with lease:
        assert await lease_manager.get_active_count("user_cm") == 1

    # Guaranteed release after block exit
    assert await lease_manager.get_active_count("user_cm") == 0


@pytest.mark.asyncio
async def test_direct_lease_context_renews(lease_manager):
    lease = await lease_manager.acquire("direct_context")
    async with lease:
        await asyncio.sleep(2.1)
        assert await lease_manager.get_active_count("direct_context") == 1

    assert await lease_manager.get_active_count("direct_context") == 0


@pytest.mark.asyncio
async def test_direct_lease_context_stops_when_lost(lease_manager, fake_redis):
    lease = await lease_manager.acquire("direct_context")
    with pytest.raises(StreamLeaseLost):
        async with lease:
            await fake_redis.flushall()
            await asyncio.sleep(2)

    assert await lease_manager.get_active_count("direct_context") == 0


@pytest.mark.asyncio
async def test_direct_lease_context_rejects_reentry(lease_manager):
    lease = await lease_manager.acquire("direct_context")
    async with lease:
        with pytest.raises(RuntimeError, match="cannot enter"):
            async with lease:
                pass

    with pytest.raises(StreamLeaseLost):
        async with lease:
            pass


@pytest.mark.asyncio
async def test_direct_lease_context_releases_on_cancellation(lease_manager):
    entered = asyncio.Event()

    async def wait_inside_context():
        lease = await lease_manager.acquire("direct_context")
        async with lease:
            entered.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(wait_inside_context())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await lease_manager.get_active_count("direct_context") == 0


@pytest.mark.asyncio
async def test_wrap_rejects_invalid_renew_interval_and_releases(lease_manager):
    async def quick_generator():
        yield "data"

    lease = await lease_manager.acquire("user_warn")
    with pytest.raises(ValueError, match="renew_interval"):
        async for _chunk in lease.wrap(quick_generator(), renew_interval=10.0):
            pass

    assert await lease_manager.get_active_count("user_warn") == 0


@pytest.mark.asyncio
async def test_context_manager_renews_long_lived_connection(lease_manager):
    async with lease_manager.lease("websocket"):
        await asyncio.sleep(2.4)
        assert await lease_manager.get_active_count("websocket") == 1

    assert await lease_manager.get_active_count("websocket") == 0


@pytest.mark.asyncio
async def test_context_manager_interrupts_after_lease_loss(lease_manager, fake_redis):
    with pytest.raises(StreamLeaseLost):
        async with lease_manager.lease("websocket"):
            await fake_redis.flushall()
            await asyncio.sleep(2.0)

    assert await lease_manager.get_active_count("websocket") == 0


@pytest.mark.asyncio
async def test_auto_renew_grace_period_recovers_from_transient_redis_outage(
    lease_manager, fake_redis
):
    async def long_stream():
        yield "chunk_1"
        await asyncio.sleep(0.3)
        yield "chunk_2"

    lease = await lease_manager.acquire("resilient_user")
    original_eval = lease_manager.redis.eval
    eval_call_count = 0

    async def flaky_eval(*args, **kwargs):
        nonlocal eval_call_count
        eval_call_count += 1
        if eval_call_count == 1:
            raise ConnectionError("Transient network hiccup")
        return await original_eval(*args, **kwargs)

    lease_manager.redis.eval = flaky_eval

    chunks = []
    async for chunk in lease.wrap(long_stream(), auto_renew=True, renew_interval=0.1):
        chunks.append(chunk)

    assert chunks == ["chunk_1", "chunk_2"]
    assert eval_call_count >= 2
    assert await lease_manager.get_active_count("resilient_user") == 0


@pytest.mark.asyncio
async def test_acquire_fail_open_and_closed(fake_redis):
    from unittest.mock import AsyncMock

    import redis.exceptions

    from fastapi_stream_lease import LeaseConfig, StreamLeaseManager, StreamLeaseUnavailable

    # Fail-closed (default) with network error
    mgr_closed = StreamLeaseManager(fake_redis, LeaseConfig(fail_open=False))
    mgr_closed.redis.eval = AsyncMock(side_effect=ConnectionError("Redis down"))
    with pytest.raises(StreamLeaseUnavailable) as exc_info:
        await mgr_closed.acquire("user_err")
    assert "temporarily unavailable" in exc_info.value.detail
    # Verify no raw socket exception leakage in detail
    assert "Redis down" not in exc_info.value.detail

    # Fail-open with network error
    mgr_open = StreamLeaseManager(fake_redis, LeaseConfig(fail_open=True))
    mgr_open.redis.eval = AsyncMock(side_effect=ConnectionError("Redis down"))
    fallback_lease = await mgr_open.acquire("user_fallback")
    assert fallback_lease._is_fallback is True
    assert await fallback_lease.renew() is True
    await fallback_lease.release()

    # Fail-open MUST NOT swallow auth, permission, or script execution errors
    mgr_open.redis.eval = AsyncMock(side_effect=redis.exceptions.AuthenticationError("Bad pass"))
    with pytest.raises(redis.exceptions.AuthenticationError):
        await mgr_open.acquire("user_auth_err")

    mgr_open.redis.eval = AsyncMock(side_effect=redis.exceptions.ResponseError("WRONGTYPE"))
    with pytest.raises(redis.exceptions.ResponseError):
        await mgr_open.acquire("user_type_err")


@pytest.mark.asyncio
async def test_wrap_stream_auto_renew_grace_period_exhausted(lease_manager):
    from unittest.mock import AsyncMock

    async def slow_generator():
        yield "start"
        await asyncio.sleep(0.25)
        yield "end"

    lease = await lease_manager.acquire("user_gp")
    # Simulate deadline expiring very shortly so grace period retries exhaust remaining window
    lease.expires_at = time.monotonic() + 0.05
    lease_manager.redis.eval = AsyncMock(side_effect=ConnectionError("Redis down"))

    chunks = []
    with pytest.raises(StreamLeaseLost):
        async for chunk in lease.wrap(slow_generator(), auto_renew=True, renew_interval=0.04):
            chunks.append(chunk)

    assert chunks == ["start"]


@pytest.mark.asyncio
async def test_manager_network_errors_in_renew_and_count(lease_manager):
    from unittest.mock import AsyncMock

    from fastapi_stream_lease import StreamLeaseUnavailable

    lease = await lease_manager.acquire("err_user")
    lease_manager.redis.eval = AsyncMock(side_effect=ConnectionError("Redis connection lost"))

    with pytest.raises(StreamLeaseUnavailable):
        await lease.renew()

    with pytest.raises(StreamLeaseUnavailable):
        await lease_manager.get_active_count("err_user")


@pytest.mark.asyncio
async def test_monotonic_deadline_tracking(lease_manager):
    lease = await lease_manager.acquire("mono_user")
    assert lease.expires_at > time.monotonic()
    initial_expires = lease.expires_at

    await asyncio.sleep(0.05)
    renewed = await lease.renew()
    assert renewed is True
    assert lease.expires_at > initial_expires
    await lease.release()


@pytest.mark.asyncio
async def test_wrap_without_auto_renew(lease_manager):
    async def simple_stream():
        yield "a"
        yield "b"

    lease = await lease_manager.acquire("no_renew")
    chunks = []
    async for chunk in lease.wrap(simple_stream(), auto_renew=False):
        chunks.append(chunk)
    assert chunks == ["a", "b"]
    assert await lease_manager.get_active_count("no_renew") == 0


@pytest.mark.asyncio
async def test_wrap_upstream_cancelled_error(lease_manager):
    async def cancelling_stream():
        yield "first"
        raise asyncio.CancelledError()

    lease = await lease_manager.acquire("cancel_user")
    with pytest.raises(asyncio.CancelledError):
        async for _ in lease.wrap(cancelling_stream(), auto_renew=True):
            pass

    assert await lease_manager.get_active_count("cancel_user") == 0


def test_start_auto_renew_no_running_task(lease_manager):
    lease = StreamLease(
        lease_id="test",
        user_id="user",
        user_key="k",
        global_key="g",
        manager=lease_manager,
    )
    with pytest.raises(RuntimeError, match="running asyncio task"):
        lease._start_auto_renew()


@pytest.mark.asyncio
async def test_lease_aexit_when_no_context_task(lease_manager):
    lease = await lease_manager.acquire("manual")
    # Call __aexit__ directly without having entered context
    await lease.__aexit__(None, None, None)
    assert await lease_manager.get_active_count("manual") == 0


@pytest.mark.asyncio
async def test_worker_exits_when_lease_marked_released(lease_manager):
    lease = await lease_manager.acquire("worker_exit")
    task, _ = lease._start_auto_renew(interval=0.05)
    await asyncio.sleep(0.01)
    lease._is_released = True
    await asyncio.sleep(0.08)
    assert task.done()
    await lease.release()


@pytest.mark.asyncio
async def test_context_manager_external_cancellation(lease_manager):
    entered = asyncio.Event()

    async def runner():
        async with lease_manager.lease("ext_cancel"):
            entered.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(runner())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert await lease_manager.get_active_count("ext_cancel") == 0


@pytest.mark.asyncio
async def test_lease_context_when_start_renew_fails(lease_manager):
    from unittest.mock import patch

    with patch.object(StreamLease, "_start_auto_renew", side_effect=RuntimeError("fail start")):
        with pytest.raises(RuntimeError, match="fail start"):
            async with lease_manager.lease("fail_user"):
                pass

    assert await lease_manager.get_active_count("fail_user") == 0


@pytest.mark.asyncio
async def test_worker_not_started_if_already_released(lease_manager):
    lease = await lease_manager.acquire("worker_released")
    lease._is_released = True
    task, _ = lease._start_auto_renew(interval=0.05)
    await asyncio.sleep(0.01)
    assert task.done()
    await lease.release()


@pytest.mark.asyncio
async def test_manager_non_network_errors_in_renew_release_and_count(lease_manager):
    from unittest.mock import AsyncMock

    import redis.exceptions

    lease = await lease_manager.acquire("err_user_non_net")

    # Non-network error in renew raises ResponseError
    lease_manager.redis.eval = AsyncMock(side_effect=redis.exceptions.ResponseError("WRONGTYPE"))
    with pytest.raises(redis.exceptions.ResponseError):
        await lease.renew()

    # Non-network error in release logs error but does not re-raise
    await lease.release()

    # Non-network error in get_active_count raises ResponseError
    with pytest.raises(redis.exceptions.ResponseError):
        await lease_manager.get_active_count("err_user_non_net")


def test_is_network_error_helper():
    import redis.exceptions

    from fastapi_stream_lease.manager import is_network_error

    assert is_network_error(redis.exceptions.ConnectionError("down")) is True
    assert is_network_error(redis.exceptions.TimeoutError("timed out")) is True
    assert is_network_error(ConnectionResetError("reset")) is True
    assert is_network_error(asyncio.TimeoutError()) is True
    assert is_network_error(OSError("os err")) is True
    assert is_network_error(redis.exceptions.ReadOnlyError("READONLY replica")) is True

    # Excluded errors
    assert is_network_error(redis.exceptions.AuthenticationError("bad auth")) is False
    assert is_network_error(redis.exceptions.AuthorizationError("no perm")) is False
    assert is_network_error(redis.exceptions.ResponseError("syntax")) is False
    assert is_network_error(ValueError("bad value")) is False


def test_safe_uncancel_edge_cases(monkeypatch):
    from fastapi_stream_lease.lease import _safe_uncancel

    # Test when task is None
    monkeypatch.setattr(asyncio, "current_task", lambda: None)
    _safe_uncancel()

    # Test when task has no uncancel method (Python 3.10)
    class DummyTask:
        pass

    monkeypatch.setattr(asyncio, "current_task", lambda: DummyTask())
    _safe_uncancel()


@pytest.mark.asyncio
async def test_auto_renew_non_network_error_cancels_stream(lease_manager):
    import redis.exceptions

    async def infinite_stream():
        yield "chunk_1"
        await asyncio.sleep(0.2)
        yield "chunk_2"

    from fastapi_stream_lease.lua import RENEW_SCRIPT

    lease = await lease_manager.acquire("unhandled_err_user")
    original_eval = lease_manager.redis.eval

    async def flaky_eval(script, *args, **kwargs):
        if script == RENEW_SCRIPT:
            raise redis.exceptions.ResponseError("NOPERM")
        return await original_eval(script, *args, **kwargs)

    lease_manager.redis.eval = flaky_eval

    chunks = []
    with pytest.raises(StreamLeaseLost):
        async for chunk in lease.wrap(infinite_stream(), auto_renew=True, renew_interval=0.04):
            chunks.append(chunk)

    assert chunks == ["chunk_1"]
    assert await lease_manager.get_active_count("unhandled_err_user") == 0


@pytest.mark.asyncio
async def test_manager_lease_context_auto_renew_non_network_error_cancels_context(lease_manager):
    import redis.exceptions

    from fastapi_stream_lease.lua import RENEW_SCRIPT

    original_eval = lease_manager.redis.eval

    async def flaky_eval(script, *args, **kwargs):
        if script == RENEW_SCRIPT:
            raise redis.exceptions.ResponseError("NOPERM")
        return await original_eval(script, *args, **kwargs)

    lease_manager.redis.eval = flaky_eval

    with pytest.raises(StreamLeaseLost):
        async with lease_manager.lease("ctx_unhandled_err", renew_interval=0.04):
            await asyncio.sleep(0.2)

    assert await lease_manager.get_active_count("ctx_unhandled_err") == 0


def test_lease_config_validation_hooks_and_retry_after():
    with pytest.raises(ValueError, match="retry_after_seconds cannot be negative"):
        LeaseConfig(retry_after_seconds=-1)

    with pytest.raises(TypeError, match="on_acquired must be callable"):
        LeaseConfig(on_acquired="not_a_callable")

    with pytest.raises(TypeError, match="on_released must be callable"):
        LeaseConfig(on_released="not_a_callable")

    with pytest.raises(ValueError, match="hook_queue_size must be greater than 0"):
        LeaseConfig(hook_queue_size=0)


@pytest.mark.asyncio
async def test_lifecycle_hooks_invocation(fake_redis):
    from unittest.mock import AsyncMock

    events = []

    def sync_acquired(lease):
        events.append(("acquired", lease.user_id))

    async def async_rejected(user_id, reason):
        events.append(("rejected", user_id, reason))

    def buggy_lost(lease, reason):
        events.append(("lost", lease.user_id, reason))
        raise RuntimeError("Buggy hook error")

    async def async_backend_err(exc):
        events.append(("backend_error", type(exc).__name__))

    config = LeaseConfig(
        lease_seconds=2.0,
        max_per_user=1,
        on_acquired=sync_acquired,
        on_rejected=async_rejected,
        on_lost=buggy_lost,
        on_backend_error=async_backend_err,
    )
    mgr = StreamLeaseManager(redis=fake_redis, config=config)

    # 1. Acquire triggers on_acquired
    lease = await mgr.acquire("user_hook")
    assert lease.user_id == "user_hook"
    await mgr.drain()
    assert ("acquired", "user_hook") in events

    # 2. Limit rejection triggers on_rejected
    with pytest.raises(StreamLeaseRejected):
        await mgr.acquire("user_hook")
    await mgr.drain()
    assert ("rejected", "user_hook", "user_limit") in events

    # 3. Backend error triggers on_backend_error
    original_eval = fake_redis.eval
    mgr.redis.eval = AsyncMock(side_effect=ConnectionError("Backend dropped"))
    with pytest.raises(StreamLeaseUnavailable):
        await mgr.acquire("user_down")
    await mgr.drain()
    assert ("backend_error", "ConnectionError") in events

    # 4. Fallback lease triggers both on_backend_error and on_acquired
    mgr_open = StreamLeaseManager(
        redis=fake_redis,
        config=LeaseConfig(
            fail_open=True,
            on_acquired=sync_acquired,
            on_backend_error=async_backend_err,
        ),
    )
    mgr_open.redis.eval = AsyncMock(side_effect=ConnectionError("Backend dropped"))
    fallback_lease = await mgr_open.acquire("user_fb")
    assert fallback_lease._is_fallback is True
    await mgr_open.drain()
    assert ("acquired", "user_fb") in events
    assert ("backend_error", "ConnectionError") in events

    # 5. worker lost hook triggers on_lost (and buggy hook does not raise)
    fake_redis.eval = original_eval
    mgr_real = StreamLeaseManager(redis=fake_redis, config=config)
    real_lease = await mgr_real.acquire("user_lost_hook")
    # Expire immediately in redis
    await fake_redis.flushall()
    # Direct renew returns False
    assert await real_lease.renew() is False

    # 6. Test on_lost triggered in worker during wrap
    lost_events = []

    def track_lost(lost_lease, reason):
        lost_events.append((lost_lease.user_id, reason))

    config_lost = LeaseConfig(lease_seconds=2.0, on_lost=track_lost)
    mgr_lost = StreamLeaseManager(redis=fake_redis, config=config_lost)
    l_active = await mgr_lost.acquire("user_bg_lost")
    await fake_redis.flushall()

    async def sample_stream():
        yield 1
        await asyncio.sleep(0.1)
        yield 2

    with pytest.raises(StreamLeaseLost):
        async for _ in l_active.wrap(sample_stream(), auto_renew=True, renew_interval=0.04):
            pass

    await mgr_lost.drain()
    assert ("user_bg_lost", "redis_revoked") in lost_events


@pytest.mark.asyncio
async def test_slow_on_lost_does_not_delay_stream_cancellation(fake_redis):
    """
    Ensure slow async telemetry hooks (e.g. Datadog/CloudWatch taking hundreds of ms)
    do NOT delay cancelling the stream when a lease is lost.

    Cancellation must be issued immediately before or concurrently with the hook.
    """
    hook_started = asyncio.Event()
    hook_finished = asyncio.Event()

    async def slow_on_lost(lease, reason):
        hook_started.set()
        await asyncio.sleep(0.3)
        hook_finished.set()

    config = LeaseConfig(lease_seconds=2.0, on_lost=slow_on_lost)
    mgr = StreamLeaseManager(redis=fake_redis, config=config)
    lease = await mgr.acquire("user_slow_hook")

    chunks_after_loss = 0

    async def infinite_generator():
        nonlocal chunks_after_loss
        yield "chunk_1"
        while True:
            await asyncio.sleep(0.04)
            if hook_started.is_set():
                chunks_after_loss += 1
            yield f"chunk_{chunks_after_loss}"

    # Invalidate lease in redis
    await fake_redis.flushall()

    start_time = time.monotonic()
    with pytest.raises(StreamLeaseLost):
        async for _ in lease.wrap(infinite_generator(), auto_renew=True, renew_interval=0.04):
            pass
    elapsed = time.monotonic() - start_time

    # Cancellation must occur immediately when loss is detected, NOT after the slow hook finishes
    assert elapsed < 0.25, f"Stream cancellation was delayed by slow hook: elapsed={elapsed:.3f}s"
    assert chunks_after_loss <= 1, f"Stream kept running during hook: {chunks_after_loss} chunks"


@pytest.mark.asyncio
async def test_slow_async_hooks_do_not_delay_acquire_or_renew(fake_redis):
    """Ensure slow async telemetry callbacks do not delay acquire() or renew()."""

    async def slow_acquired(lease):
        await asyncio.sleep(0.3)

    async def slow_backend_err(exc):
        await asyncio.sleep(0.3)

    config = LeaseConfig(
        lease_seconds=2.0,
        on_acquired=slow_acquired,
        on_backend_error=slow_backend_err,
    )
    mgr = StreamLeaseManager(redis=fake_redis, config=config)

    start = time.monotonic()
    lease = await mgr.acquire("user_slow_acquire")
    elapsed = time.monotonic() - start
    assert elapsed < 0.20, f"Acquisition was delayed by async hook: elapsed={elapsed:.3f}s"

    # Simulate slow backend error on renew()
    mgr.redis.eval = AsyncMock(side_effect=ConnectionError("Redis timed out"))
    start_renew = time.monotonic()
    with pytest.raises(StreamLeaseUnavailable):
        await mgr.renew(lease)
    elapsed_renew = time.monotonic() - start_renew
    assert elapsed_renew < 0.20, (
        f"Renewal was delayed by slow async hook: elapsed={elapsed_renew:.3f}s"
    )

    await lease.release()


@pytest.mark.asyncio
async def test_sync_hooks_execute_out_of_band(fake_redis):
    """Verify that synchronous hooks run out-of-band via dispatcher without blocking."""
    events = []

    def sync_acquired(lease):
        events.append("acquired")

    def sync_released(lease, reason):
        events.append(f"released_{reason}")

    config = LeaseConfig(
        lease_seconds=2.0,
        on_acquired=sync_acquired,
        on_released=sync_released,
    )
    mgr = StreamLeaseManager(redis=fake_redis, config=config)
    lease = await mgr.acquire("user_sync")
    # Dispatched out-of-band to dispatcher queue
    await mgr.drain()
    assert events == ["acquired"]

    await lease.release()
    await mgr.drain()
    assert events == ["acquired", "released_manual"]


@pytest.mark.asyncio
async def test_release_triggers_on_backend_error_hook(fake_redis):
    """Ensure manager.release() notifies on_backend_error when a network error occurs."""
    backend_errors = []

    def on_backend_err(exc):
        backend_errors.append(exc)

    config = LeaseConfig(on_backend_error=on_backend_err)
    mgr = StreamLeaseManager(redis=fake_redis, config=config)
    lease = await mgr.acquire("user_release_err")

    mgr.redis.eval = AsyncMock(side_effect=ConnectionError("Redis unreachable during release"))
    await mgr.release(lease)
    await mgr.drain()

    assert len(backend_errors) == 1
    assert isinstance(backend_errors[0], ConnectionError)


@pytest.mark.parametrize(
    "exit_mode,expected_reason",
    [
        ("normal", "completed"),
        ("error", "error"),
        ("cancelled", "cancelled"),
        ("lost", "lost"),
    ],
)
@pytest.mark.asyncio
async def test_on_released_hook_context_manager(fake_redis, exit_mode, expected_reason):
    """Verify on_released hook receives correct reason on context manager exits."""
    released_events = []

    def on_rel(lease, reason):
        released_events.append((lease.user_id, reason))

    config = LeaseConfig(lease_seconds=2.0, on_released=on_rel)
    mgr = StreamLeaseManager(redis=fake_redis, config=config)

    if exit_mode == "normal":
        async with mgr.lease("u_rel_norm"):
            pass
    elif exit_mode == "error":
        with pytest.raises(RuntimeError, match="boom"):
            async with mgr.lease("u_rel_err"):
                raise RuntimeError("boom")
    elif exit_mode == "cancelled":

        async def cancel_block():
            async with mgr.lease("u_rel_canc"):
                raise asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await cancel_block()
    elif exit_mode == "lost":
        with pytest.raises(StreamLeaseLost):
            async with mgr.lease("u_rel_lost", renew_interval=0.04):
                await fake_redis.flushall()
                await asyncio.sleep(0.1)

    await mgr.drain()
    assert len(released_events) == 1
    assert released_events[0][1] == expected_reason


@pytest.mark.parametrize(
    "stream_mode,expected_reason",
    [
        ("normal", "completed"),
        ("error", "error"),
        ("cancelled", "cancelled"),
        ("manual", "manual"),
    ],
)
@pytest.mark.asyncio
async def test_on_released_hook_stream_wrap(fake_redis, stream_mode, expected_reason):
    """Verify on_released hook receives correct reason on stream wrap and manual release."""
    released_events = []

    def on_rel(lease, reason):
        released_events.append((lease.user_id, reason))

    config = LeaseConfig(lease_seconds=2.0, on_released=on_rel)
    mgr = StreamLeaseManager(redis=fake_redis, config=config)
    lease = await mgr.acquire("u_wrap_rel")

    if stream_mode == "manual":
        await lease.release()
    elif stream_mode == "normal":

        async def s_normal():
            yield 1
            yield 2

        async for _ in lease.wrap(s_normal(), auto_renew=False):
            pass
    elif stream_mode == "error":

        async def s_err():
            yield 1
            raise RuntimeError("stream fail")

        with pytest.raises(RuntimeError, match="stream fail"):
            async for _ in lease.wrap(s_err(), auto_renew=False):
                pass
    elif stream_mode == "cancelled":

        async def s_canc():
            yield 1
            raise asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            async for _ in lease.wrap(s_canc(), auto_renew=False):
                pass

    await mgr.drain()
    assert len(released_events) == 1
    assert released_events[0][1] == expected_reason

    # Calling release() again must not re-trigger on_released (exactly-once release)
    await lease.release()
    await mgr.drain()
    assert len(released_events) == 1


@pytest.mark.asyncio
async def test_verify_cluster_config_registration_and_mismatch(fake_redis):
    """Verify cluster configuration fingerprint registration, persistence, and drift detection."""
    from fastapi_stream_lease.exceptions import ConfigurationMismatchError, StreamLeaseUnavailable

    # 1. First worker registers its configuration successfully
    cfg1 = LeaseConfig(key_prefix="worker_test", max_global=100, max_per_user=2, lease_seconds=10.0)
    mgr1 = StreamLeaseManager(redis=fake_redis, config=cfg1)
    assert await mgr1.verify_cluster_config() is True

    # Check key is populated in redis, has algorithm_version, and has NO TTL (persistent)
    val = await fake_redis.get(cfg1.config_key)
    assert val is not None
    import json

    data = json.loads(val.decode("utf-8") if isinstance(val, bytes) else val)
    assert data["algorithm_version"] == 1
    ttl = await fake_redis.ttl(cfg1.config_key)
    assert ttl == -1, f"Config key should be persistent (no TTL), got ttl={ttl}"

    # 2. Second worker with identical configuration passes verification
    mgr1_clone = StreamLeaseManager(redis=fake_redis, config=cfg1)
    assert await mgr1_clone.verify_cluster_config() is True

    # 3. Third worker with conflicting configuration fails non-strict verification (returns False)
    cfg_conflicting = LeaseConfig(
        key_prefix="worker_test", max_global=500, max_per_user=2, lease_seconds=10.0
    )
    mgr_conflict = StreamLeaseManager(redis=fake_redis, config=cfg_conflicting)
    assert await mgr_conflict.verify_cluster_config(strict=False) is False

    # 4. Strict mode raises ConfigurationMismatchError
    with pytest.raises(ConfigurationMismatchError) as exc_info:
        await mgr_conflict.verify_cluster_config(strict=True)
    assert "max_global" in str(exc_info.value)
    assert exc_info.value.existing_config["max_global"] == 100
    assert exc_info.value.current_config["max_global"] == 500

    # 5. Network error during verify: strict=False returns False, strict=True raises
    from unittest.mock import AsyncMock

    errors = []
    cfg_err = LeaseConfig(
        key_prefix="worker_err",
        on_backend_error=lambda e: errors.append(type(e).__name__),
    )
    orig_set = fake_redis.set
    mgr_err = StreamLeaseManager(redis=fake_redis, config=cfg_err)
    fake_redis.set = AsyncMock(side_effect=ConnectionError("Redis down"))

    # non-strict: returns False
    assert await mgr_err.verify_cluster_config(strict=False) is False
    await mgr_err.drain()
    assert errors == ["ConnectionError"]

    # strict: raises StreamLeaseUnavailable (fail-fast on k8s startup)
    with pytest.raises(StreamLeaseUnavailable):
        await mgr_err.verify_cluster_config(strict=True)

    fake_redis.set = orig_set

    # 6. Config mismatch on fail_open
    cfg_fo_mismatch = LeaseConfig(
        key_prefix="worker_test",
        max_global=100,
        max_per_user=2,
        lease_seconds=10.0,
        fail_open=True,
    )
    mgr_fo = StreamLeaseManager(redis=fake_redis, config=cfg_fo_mismatch)
    assert await mgr_fo.verify_cluster_config(strict=False) is False
    with pytest.raises(ConfigurationMismatchError) as exc_info_fo:
        await mgr_fo.verify_cluster_config(strict=True)
    assert "fail_open" in str(exc_info_fo.value)

    # 7. Disappearing/unresolvable config key must NEVER return True
    mgr_unresolvable = StreamLeaseManager(redis=fake_redis, config=cfg1)
    mgr_unresolvable.redis.set = AsyncMock(return_value=None)
    mgr_unresolvable.redis.get = AsyncMock(return_value=None)
    assert await mgr_unresolvable.verify_cluster_config(strict=False) is False
    with pytest.raises(StreamLeaseUnavailable):
        await mgr_unresolvable.verify_cluster_config(strict=True)


@pytest.mark.asyncio
async def test_verify_cluster_config_concurrent_race_condition(fake_redis):
    """Verify atomic SET NX ensures exactly one configuration wins among concurrent workers."""
    cfg_a = LeaseConfig(
        key_prefix="race_cluster", max_global=50, max_per_user=2, lease_seconds=10.0
    )
    cfg_b = LeaseConfig(
        key_prefix="race_cluster", max_global=100, max_per_user=2, lease_seconds=10.0
    )

    workers_a = [StreamLeaseManager(redis=fake_redis, config=cfg_a) for _ in range(25)]
    workers_b = [StreamLeaseManager(redis=fake_redis, config=cfg_b) for _ in range(25)]

    all_workers = workers_a + workers_b
    import random

    random.shuffle(all_workers)

    results = await asyncio.gather(*(w.verify_cluster_config(strict=False) for w in all_workers))

    # Exactly 25 workers must succeed (the ones matching the winner)
    # and exactly 25 workers must detect the mismatch (returning False)
    true_count = sum(1 for r in results if r is True)
    false_count = sum(1 for r in results if r is False)

    assert true_count == 25
    assert false_count == 25


@pytest.mark.asyncio
async def test_lease_context_exception_releases_with_error_reason(fake_redis):
    """Verify that an unhandled exception inside async with lease sets release reason='error'."""
    released_reasons = []
    config = LeaseConfig(on_released=lambda lease, reason: released_reasons.append(reason))
    manager = StreamLeaseManager(redis=fake_redis, config=config)

    lease = await manager.acquire("user_err")
    with pytest.raises(ZeroDivisionError):
        async with lease:
            _ = 1 / 0

    await manager.drain(timeout=2.0)
    assert released_reasons == ["error"]
    assert await manager.get_active_count("user_err") == 0
    await manager.close()


@pytest.mark.asyncio
async def test_verify_cluster_config_with_bytes_and_string_payloads(fake_redis):
    """Verify verify_cluster_config handles both raw bytes and decoded string Redis responses."""
    config = LeaseConfig(key_prefix="payload_test", max_global=10, max_per_user=1)
    manager = StreamLeaseManager(redis=fake_redis, config=config)
    payload = config.fingerprint_dict()

    # Case 1: redis.get returns raw bytes (simulating default redis-py without decode_responses)
    manager.redis.set = AsyncMock(return_value=False)
    manager.redis.get = AsyncMock(return_value=json.dumps(payload).encode("utf-8"))
    assert await manager.verify_cluster_config(strict=True) is True

    # Case 2: redis.get returns decoded str (simulating decode_responses=True)
    manager.redis.get = AsyncMock(return_value=json.dumps(payload))
    assert await manager.verify_cluster_config(strict=True) is True
    await manager.close()
