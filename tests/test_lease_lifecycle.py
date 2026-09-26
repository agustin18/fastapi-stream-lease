from __future__ import annotations

import asyncio
from contextlib import aclosing

import pytest

from fastapi_stream_lease import StreamLeaseLost


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
        await asyncio.sleep(0.15)
        yield "end"

    lease = await lease_manager.acquire("user_1")

    # Wipe redis so renewal fails during stream
    await fake_redis.flushall()

    chunks = []
    with pytest.raises(StreamLeaseLost):
        async for chunk in lease.wrap(lingering_generator(), auto_renew=True, renew_interval=0.05):
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
