"""Integration checks against a real Redis server when REDIS_URL is set."""

from __future__ import annotations

import asyncio
import os
import sys
from uuid import uuid4

import pytest
import redis.asyncio as redis

from fastapi_stream_lease import (
    LeaseConfig,
    StreamLeaseLost,
    StreamLeaseManager,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)


async def _safe_close(client: redis.Redis) -> None:
    if hasattr(client, "aclose"):
        await client.aclose()
    elif hasattr(client, "close"):
        res = client.close()
        if asyncio.iscoroutine(res):
            await res


@pytest.fixture
async def real_manager():
    url = os.environ.get("REDIS_URL")
    if not url:
        pytest.skip("Set REDIS_URL to run real Redis integration tests")
    # redis-py 8 defaults to RESP3; Redis 5 only supports RESP2.
    client = redis.from_url(url, protocol=2)
    manager = StreamLeaseManager(
        client,
        LeaseConfig(lease_seconds=0.5, max_per_user=2, max_global=3, key_prefix=uuid4().hex),
    )
    try:
        await client.ping()
        yield manager
    finally:
        await _safe_close(client)


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
    await asyncio.sleep(0.7)
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
        async with real_manager.lease("socket", renew_interval=0.1):
            started.set()
            await asyncio.sleep(1)

    task = asyncio.create_task(socket_like_task())
    await started.wait()
    await asyncio.sleep(0.35)
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
        client, LeaseConfig(lease_seconds=0.5, max_per_user=2, max_global=3,
                            key_prefix=sys.argv[2])
    )
    try:
        await manager.acquire("child")
    except StreamLeaseRejected as exc:
        print(exc.reason)
    finally:
        if hasattr(client, "aclose"):
            await client.aclose()
        else:
            await client.close()

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


@pytest.mark.asyncio
async def test_real_redis_nonexistent_lease_renewal_returns_false(real_manager):
    lease = await real_manager.acquire("phantom")
    # Release early so the lease is permanently removed from Redis
    await lease.release()
    assert await lease.renew() is False
    assert await real_manager.get_active_count("phantom") == 0


@pytest.mark.asyncio
async def test_real_redis_stream_auto_renew_lost_lease_terminates(real_manager):
    async def lingering_stream():
        yield "chunk_start"
        await asyncio.sleep(0.4)
        yield "chunk_never"

    lease = await real_manager.acquire("victim")
    # Wipe the lease directly in Redis so renewal returns 0
    await real_manager.release(lease)

    chunks = []
    with pytest.raises(StreamLeaseLost):
        async for chunk in lease.wrap(lingering_stream(), auto_renew=True, renew_interval=0.05):
            chunks.append(chunk)

    assert chunks == ["chunk_start"]


@pytest.mark.asyncio
async def test_real_redis_network_failure_fail_open_and_closed():
    # Use an unroutable port with short timeout to simulate network outage
    broken_client = redis.from_url(
        "redis://127.0.0.1:65530/0",
        socket_timeout=0.1,
        socket_connect_timeout=0.1,
    )
    try:
        # Fail-closed
        mgr_closed = StreamLeaseManager(broken_client, LeaseConfig(fail_open=False))
        with pytest.raises(StreamLeaseUnavailable):
            await mgr_closed.acquire("unreachable_user")

        # Fail-open
        mgr_open = StreamLeaseManager(broken_client, LeaseConfig(fail_open=True))
        fallback = await mgr_open.acquire("unreachable_user")
        assert fallback._is_fallback is True
        assert await fallback.renew() is True
        await fallback.release()
    finally:
        await _safe_close(broken_client)


@pytest.mark.asyncio
async def test_real_redis_verify_cluster_config_concurrent_race():
    """Verify atomic SET NX across concurrent workers against real Redis server."""
    url = os.environ.get("REDIS_URL")
    if not url:
        pytest.skip("Set REDIS_URL to run real Redis integration tests")
    client = redis.from_url(url, protocol=2)
    prefix = f"race_real_{uuid4().hex[:8]}"

    cfg_a = LeaseConfig(key_prefix=prefix, max_global=10, max_per_user=1, fail_open=False)
    cfg_b = LeaseConfig(key_prefix=prefix, max_global=20, max_per_user=1, fail_open=True)

    workers_a = [StreamLeaseManager(client, config=cfg_a) for _ in range(15)]
    workers_b = [StreamLeaseManager(client, config=cfg_b) for _ in range(15)]
    all_workers = workers_a + workers_b
    import random

    random.shuffle(all_workers)

    try:
        results = await asyncio.gather(
            *(w.verify_cluster_config(strict=False) for w in all_workers)
        )
        assert sum(1 for r in results if r is True) == 15
        assert sum(1 for r in results if r is False) == 15
    finally:
        await client.delete(cfg_a.config_key)
        await _safe_close(client)
