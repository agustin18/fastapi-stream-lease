"""Integration checks against a real Redis server when REDIS_URL is set."""

from __future__ import annotations

import asyncio
import os
import sys
from uuid import uuid4

import pytest
import redis.asyncio as redis

from fastapi_stream_lease import LeaseConfig, StreamLeaseManager, StreamLeaseRejected


@pytest.fixture
async def real_manager():
    url = os.environ.get("REDIS_URL")
    if not url:
        pytest.skip("Set REDIS_URL to run real Redis integration tests")
    # redis-py 8 defaults to RESP3; Redis 5 only supports RESP2.
    client = redis.from_url(url, protocol=2)
    manager = StreamLeaseManager(
        client,
        LeaseConfig(lease_seconds=0.3, max_per_user=2, max_global=3, key_prefix=uuid4().hex),
    )
    try:
        await client.ping()
        yield manager
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_real_redis_atomic_limits_across_managers(real_manager):
    peer = StreamLeaseManager(real_manager.redis, real_manager.config)

    async def attempt(index):
        manager = real_manager if index % 2 else peer
        try:
            return await manager.acquire(f"user_{index % 2}")
        except StreamLeaseRejected:
            return None

    leases = [lease for lease in await asyncio.gather(*(attempt(i) for i in range(20))) if lease]
    try:
        assert len(leases) == 3
        assert await real_manager.get_active_count() == 3
        assert await real_manager.get_active_count("user_0") <= 2
        assert await real_manager.get_active_count("user_1") <= 2
    finally:
        await asyncio.gather(*(lease.release() for lease in leases))


@pytest.mark.asyncio
async def test_real_redis_expired_lease_stays_expired(real_manager):
    expired = await real_manager.acquire("user_1")
    await asyncio.sleep(0.35)
    replacement = await real_manager.acquire("user_1")
    try:
        assert await expired.renew() is False
        assert await real_manager.get_active_count("user_1") == 1
        assert await real_manager.get_active_count() == 1
    finally:
        await replacement.release()


@pytest.mark.asyncio
async def test_real_redis_cancelled_context_releases_lease(real_manager):
    started = asyncio.Event()

    async def socket_like_task():
        async with real_manager.lease("socket"):
            started.set()
            await asyncio.sleep(1)

    task = asyncio.create_task(socket_like_task())
    await started.wait()
    await asyncio.sleep(0.45)
    assert await real_manager.get_active_count("socket") == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await real_manager.get_active_count("socket") == 0


@pytest.mark.asyncio
async def test_real_redis_limit_applies_across_processes(real_manager):
    leases = [
        await real_manager.acquire("parent_1"),
        await real_manager.acquire("parent_1"),
        await real_manager.acquire("parent_2"),
    ]
    child_code = """
import asyncio
import sys
import redis.asyncio as redis
from fastapi_stream_lease import LeaseConfig, StreamLeaseManager, StreamLeaseRejected

async def main():
    client = redis.from_url(sys.argv[1], protocol=2)
    manager = StreamLeaseManager(
        client, LeaseConfig(lease_seconds=0.3, max_per_user=2, max_global=3,
                            key_prefix=sys.argv[2])
    )
    try:
        await manager.acquire("child")
    except StreamLeaseRejected as exc:
        print(exc.reason)
    finally:
        await client.aclose()

asyncio.run(main())
"""
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            child_code,
            os.environ["REDIS_URL"],
            real_manager.config.key_prefix,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        assert process.returncode == 0, stderr.decode()
        assert stdout.strip() == b"global_limit"
    finally:
        await asyncio.gather(*(lease.release() for lease in leases))
