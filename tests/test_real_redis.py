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


@pytest.mark.asyncio
async def test_real_redis_resource_plateau_under_stream_churn(real_manager):
    """Verify asyncio tasks, memory, and Redis keys return to baseline after heavy stream churn."""
    initial_tasks = len(asyncio.all_tasks())

    async def sample_stream():
        for i in range(5):
            yield f"chunk_{i}"
            await asyncio.sleep(0.02)

    # Run multiple batches of streaming churn
    for _batch_idx in range(3):

        async def run_client(uid: int):
            lease = await real_manager.acquire(f"soak_user_{uid}")
            chunks = []
            async for chunk in lease.wrap(sample_stream(), auto_renew=True, renew_interval=0.03):
                chunks.append(chunk)
            assert len(chunks) == 5

        await asyncio.gather(*(run_client(i) for i in range(2)))

    # Teardown & verification
    active_count = await real_manager.get_active_count()
    assert active_count == 0

    # Ensure all background auto-renew tasks have completed cleanly
    await asyncio.sleep(0.05)
    final_tasks = len(asyncio.all_tasks())
    assert final_tasks <= initial_tasks + 1


@pytest.mark.asyncio
async def test_real_redis_wrapper_overhead_budget(real_manager):
    """Sanity check: verify StreamLeaseManager Python wrapper overhead is within 1ms budget."""
    import statistics
    import time

    from fastapi_stream_lease.lua import ACQUIRE_SCRIPT, RELEASE_SCRIPT

    client = real_manager.redis
    raw_prefix = f"bench_raw_{uuid4().hex[:8]}"
    wrap_prefix = f"bench_wrap_{uuid4().hex[:8]}"
    raw_config = LeaseConfig(
        lease_seconds=60.0, max_per_user=100, max_global=1000, key_prefix=raw_prefix
    )
    wrap_config = LeaseConfig(
        lease_seconds=60.0, max_per_user=100, max_global=1000, key_prefix=wrap_prefix
    )
    bench_manager = StreamLeaseManager(client, wrap_config)

    # Warm up connection
    await client.ping()

    raw_latencies: list[float] = []
    wrapped_latencies: list[float] = []

    iterations = 25
    created_raw_keys: set[str] = set()
    try:
        for i in range(iterations):
            user_id = f"user_{i}"
            lease_id = f"raw_lease_{i}"
            raw_user_key = raw_config.user_key(user_id)
            raw_global_key = raw_config.global_key
            created_raw_keys.add(raw_user_key)
            created_raw_keys.add(raw_global_key)

            if i % 2 == 0:
                t0 = time.perf_counter()
                await client.eval(
                    ACQUIRE_SCRIPT,
                    2,
                    raw_user_key,
                    raw_global_key,
                    60.0,
                    lease_id,
                    100,
                    1000,
                    120.0,
                )
                await client.eval(RELEASE_SCRIPT, 2, raw_user_key, raw_global_key, lease_id)
                raw_latencies.append(time.perf_counter() - t0)

                t0 = time.perf_counter()
                lease = await bench_manager.acquire(user_id)
                await lease.release()
                wrapped_latencies.append(time.perf_counter() - t0)
            else:
                t0 = time.perf_counter()
                lease = await bench_manager.acquire(user_id)
                await lease.release()
                wrapped_latencies.append(time.perf_counter() - t0)

                t0 = time.perf_counter()
                await client.eval(
                    ACQUIRE_SCRIPT,
                    2,
                    raw_user_key,
                    raw_global_key,
                    60.0,
                    lease_id,
                    100,
                    1000,
                    120.0,
                )
                await client.eval(RELEASE_SCRIPT, 2, raw_user_key, raw_global_key, lease_id)
                raw_latencies.append(time.perf_counter() - t0)
    finally:
        if created_raw_keys:
            await client.delete(*created_raw_keys)
        await bench_manager.close(drain=True)

    # Paired delta calculation (eliminates external variance)
    deltas = [(w - r) * 1000.0 for w, r in zip(wrapped_latencies, raw_latencies, strict=True)]
    median_overhead_ms = statistics.median(deltas)

    # Overhead budget threshold: <= 1.0 ms
    assert median_overhead_ms <= 1.0, (
        f"Wrapper overhead exceeded budget: {median_overhead_ms:.3f}ms"
    )


@pytest.mark.asyncio
async def test_real_redis_circuit_breaker_half_open_probe_recovery(real_manager):
    """
    Verify complete circuit breaker lifecycle against live Redis:
    1. Breaker is enabled with short recovery_timeout.
    2. Simulated transient connection failure trips breaker to OPEN.
    3. During OPEN cooldown, acquire fails fast without touching Redis.
    4. After cooldown, state transitions to HALF_OPEN.
    5. Acquire probe executes against live Redis, successfully acquiring lease
       and healing breaker to CLOSED.
    """
    import redis.exceptions

    from fastapi_stream_lease.circuit_breaker import (
        BackendFailurePolicy,
        CircuitBreakerConfig,
        CircuitState,
    )

    client = real_manager.redis
    prefix = f"cb_live_{uuid4().hex[:8]}"
    cb_cfg = CircuitBreakerConfig(
        failure_threshold=1,
        recovery_timeout=0.05,
        jitter=0.0,
        half_open_max_probes=1,
    )
    policy = BackendFailurePolicy(circuit_breaker=cb_cfg)
    config = LeaseConfig(lease_seconds=30.0, key_prefix=prefix, failure_policy=policy)
    manager = StreamLeaseManager(redis=client, config=config)

    try:
        assert manager.circuit_state == CircuitState.CLOSED

        # 1. Trip breaker to OPEN via transient error
        assert manager._circuit_breaker is not None
        manager._circuit_breaker.record_failure(redis.exceptions.ConnectionError("simulated"))
        assert manager.circuit_state == CircuitState.OPEN

        # 2. Acquire fast-fails during OPEN without hitting Redis
        with pytest.raises(StreamLeaseUnavailable, match="Circuit breaker is OPEN"):
            await manager.acquire("user_during_open")

        # 3. Wait for recovery timeout to transition to HALF_OPEN
        await asyncio.sleep(0.06)
        assert manager.circuit_state == CircuitState.HALF_OPEN

        # 4. Probe request executes against REAL Redis
        lease = await manager.acquire("user_probe_recovery")
        assert lease.lease_id is not None
        assert not lease._is_fallback

        # Real Redis answered and executed script -> breaker healed back to CLOSED!
        assert manager.circuit_state == CircuitState.CLOSED
        assert manager._circuit_breaker.consecutive_failures == 0

        # 5. Subsequent acquire proceeds normally
        lease2 = await manager.acquire("user_post_recovery")
        assert lease2.lease_id is not None
        assert not lease2._is_fallback

        await lease.release()
        await lease2.release()
    finally:
        await manager.close(drain=True)
