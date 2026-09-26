from __future__ import annotations

import asyncio
import time
from contextlib import aclosing

import pytest

from fastapi_stream_lease import StreamLease, StreamLeaseLost


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
    # Simulate deadline expiring soon so retries exhaust grace period
    lease.expires_at = time.time() + 0.05

    # Wipe redis so renewal fails during stream
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
        await asyncio.sleep(2.4)
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

    from fastapi_stream_lease import LeaseConfig, StreamLeaseManager, StreamLeaseUnavailable

    # Fail-closed (default)
    mgr_closed = StreamLeaseManager(fake_redis, LeaseConfig(fail_open=False))
    mgr_closed.redis.eval = AsyncMock(side_effect=ConnectionError("Redis down"))
    with pytest.raises(StreamLeaseUnavailable):
        await mgr_closed.acquire("user_err")

    # Fail-open
    mgr_open = StreamLeaseManager(fake_redis, LeaseConfig(fail_open=True))
    mgr_open.redis.eval = AsyncMock(side_effect=ConnectionError("Redis down"))
    fallback_lease = await mgr_open.acquire("user_fallback")
    assert fallback_lease._is_fallback is True
    assert await fallback_lease.renew() is True
    await fallback_lease.release()


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
