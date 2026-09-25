from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from fastapi_stream_lease import StreamLeaseRejected


@pytest.mark.asyncio
async def test_acquire_and_release(lease_manager):
    # Initial state should be 0 active
    assert await lease_manager.get_active_count("user_1") == 0
    assert await lease_manager.get_active_count() == 0

    # Acquire one lease
    lease1 = await lease_manager.acquire("user_1")
    assert lease1.lease_id is not None
    assert await lease_manager.get_active_count("user_1") == 1
    assert await lease_manager.get_active_count() == 1

    # Release it
    await lease1.release()
    assert await lease_manager.get_active_count("user_1") == 0
    assert await lease_manager.get_active_count() == 0


@pytest.mark.asyncio
async def test_user_concurrency_limit(lease_manager):
    # Config has max_per_user=2
    lease1 = await lease_manager.acquire("user_1")
    lease2 = await lease_manager.acquire("user_1")

    assert await lease_manager.get_active_count("user_1") == 2

    # 3rd acquire for user_1 should raise user_limit
    with pytest.raises(StreamLeaseRejected) as exc_info:
        await lease_manager.acquire("user_1")
    assert exc_info.value.reason == "user_limit"
    assert exc_info.value.retry_after == 5

    # Another user can still acquire fine
    lease_other = await lease_manager.acquire("user_2")
    assert await lease_manager.get_active_count("user_2") == 1

    # Cleanup
    await lease1.release()
    await lease2.release()
    await lease_other.release()


@pytest.mark.asyncio
async def test_global_concurrency_limit(lease_manager):
    # Config has max_per_user=2, max_global=4
    l1 = await lease_manager.acquire("user_1")
    l2 = await lease_manager.acquire("user_1")
    l3 = await lease_manager.acquire("user_2")
    l4 = await lease_manager.acquire("user_2")

    assert await lease_manager.get_active_count() == 4

    # 5th acquire across any user should hit global_limit
    with pytest.raises(StreamLeaseRejected) as exc_info:
        await lease_manager.acquire("user_3")
    assert exc_info.value.reason == "global_limit"

    # Releasing one allows user_3 to acquire
    await l1.release()
    l5 = await lease_manager.acquire("user_3")
    assert l5 is not None

    # Cleanup
    await l2.release()
    await l3.release()
    await l4.release()
    await l5.release()


@pytest.mark.asyncio
async def test_lease_expiration_and_self_cleanup(lease_manager):
    # lease_seconds is 2.0s
    _ = await lease_manager.acquire("user_1")
    _ = await lease_manager.acquire("user_1")
    assert await lease_manager.get_active_count("user_1") == 2

    # Wait for lease to expire (2.1s)
    await asyncio.sleep(2.1)

    # Next acquire should auto-purge expired leases and succeed
    lease3 = await lease_manager.acquire("user_1")
    assert lease3 is not None
    # Now active count should be 1 (only lease3 is active)
    assert await lease_manager.get_active_count("user_1") == 1

    await lease3.release()


@pytest.mark.asyncio
async def test_lease_renewal(lease_manager):
    lease = await lease_manager.acquire("user_1")

    # Renew should succeed on active lease
    assert await lease.renew() is True
    assert await lease_manager.renew(lease) is True

    await lease.release()

    # Renew after release should fail
    assert await lease.renew() is False


@pytest.mark.asyncio
async def test_acquire_unexpected_code(lease_manager):
    lease_manager.redis.eval = AsyncMock(return_value=99)
    with pytest.raises(RuntimeError, match="Unexpected stream lease acquisition return code: 99"):
        await lease_manager.acquire("user_1")


@pytest.mark.asyncio
async def test_release_exception_handling(lease_manager):
    lease = await lease_manager.acquire("user_1")
    lease_manager.redis.eval = AsyncMock(side_effect=ConnectionError("Redis connection lost"))
    # Should catch exception and log warning without re-raising
    await lease.release()
