from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel
from redis.exceptions import ConnectionError

from fastapi_stream_lease import (
    ConfigurationMismatchError,
    LeaseConfig,
    StreamLeaseLost,
    StreamLeaseManager,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("REDIS_SENTINEL_HOSTS"),
    reason="Set REDIS_SENTINEL_HOSTS to run real Redis Sentinel failover tests",
)


def parse_sentinel_hosts(hosts_env: str) -> list[tuple[str, int]]:
    """Parse comma-separated 'host:port' strings into a list of tuples."""
    sentinels = []
    for item in hosts_env.split(","):
        host, port_str = item.strip().split(":")
        sentinels.append((host, int(port_str)))
    return sentinels


@pytest.fixture
def sentinel_cluster():
    hosts_env = os.environ.get(
        "REDIS_SENTINEL_HOSTS", "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"
    )
    service_name = os.environ.get("REDIS_SENTINEL_SERVICE", "mymaster")
    sentinel_hosts = parse_sentinel_hosts(hosts_env)
    sentinel = Sentinel(sentinel_hosts, socket_timeout=2.0)
    return {
        "sentinel": sentinel,
        "service_name": service_name,
        "sentinel_hosts": sentinel_hosts,
    }


@pytest.mark.asyncio
async def test_sentinel_basic_lease_lifecycle(sentinel_cluster):
    """Verify basic acquire, renew, count, and release through Redis Sentinel master connection."""
    client = sentinel_cluster["sentinel"].master_for(
        sentinel_cluster["service_name"], socket_timeout=2.0
    )
    await client.ping()
    prefix = f"sentinel_basic_{uuid4().hex[:8]}"
    config = LeaseConfig(
        lease_seconds=5.0,
        max_per_user=2,
        max_global=10,
        key_prefix=prefix,
    )
    manager = StreamLeaseManager(redis=client, config=config)

    # 1. Acquire
    lease = await manager.acquire("user_1")
    assert lease.lease_id is not None
    assert await manager.get_active_count("user_1") == 1
    assert await manager.get_active_count() == 1

    # 2. Manual renew
    assert await lease.renew() is True

    # 3. Release
    await lease.release()
    assert await manager.get_active_count("user_1") == 0
    await manager.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_sentinel_failover_during_active_stream_renewal(sentinel_cluster):
    """
    Test zero-dropped-lease resilience during real Sentinel master failover.

    A long-running lease is held in a context manager while SENTINEL failover
    is triggered. ReadOnlyError and ConnectionError during master promotion
    must trigger adaptive grace-period retries and reconnect to the new master
    without cancelling the active stream.
    """
    client = sentinel_cluster["sentinel"].master_for(
        sentinel_cluster["service_name"], socket_timeout=2.0
    )
    sentinel = sentinel_cluster["sentinel"]
    service_name = sentinel_cluster["service_name"]
    first_sentinel_host, first_sentinel_port = sentinel_cluster["sentinel_hosts"][0]

    prefix = f"sentinel_failover_{uuid4().hex[:8]}"
    config = LeaseConfig(
        lease_seconds=8.0,
        max_per_user=2,
        max_global=10,
        key_prefix=prefix,
    )
    manager = StreamLeaseManager(redis=client, config=config)

    # Record initial master address
    initial_master = await sentinel.discover_master(service_name)

    lease_started = asyncio.Event()
    failover_done = asyncio.Event()
    test_failed: list[Exception] = []

    async def streaming_task():
        try:
            async with manager.lease("user_failover", renew_interval=1.0) as lease:
                lease_started.set()
                # Wait until failover has fully completed and verified
                await failover_done.wait()
                assert lease._is_released is False
        except Exception as exc:
            test_failed.append(exc)
            raise

    task = asyncio.create_task(streaming_task())
    await lease_started.wait()

    # Trigger real failover via Sentinel administration connection
    admin_conn = aioredis.from_url(f"redis://{first_sentinel_host}:{first_sentinel_port}")
    try:
        await admin_conn.execute_command("SENTINEL", "failover", service_name)
    finally:
        await admin_conn.aclose()

    # Poll Sentinel until master address changes to the promoted replica
    new_master = initial_master
    for _ in range(30):
        await asyncio.sleep(0.5)
        current = await sentinel.discover_master(service_name)
        if current != initial_master:
            new_master = current
            break

    assert new_master != initial_master, f"Failover did not change master from {initial_master}"

    # Allow at least 2 renewal cycles on the new master
    await asyncio.sleep(2.5)

    # Let the streaming task conclude successfully
    failover_done.set()
    await task
    assert len(test_failed) == 0, f"Streaming task failed during failover: {test_failed}"

    # Verify final state on new master
    assert await manager.get_active_count("user_failover") == 0
    await manager.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_sentinel_verify_cluster_config_across_failover(sentinel_cluster):
    """
    Verify cluster configuration consistency survives Sentinel master failover.

    A configuration key registered on the original master replicates to the replica.
    After failover, verify_cluster_config on the promoted master continues to
    successfully detect matches and reject incompatible workers.
    """
    client = sentinel_cluster["sentinel"].master_for(
        sentinel_cluster["service_name"], socket_timeout=2.0
    )
    await client.ping()
    prefix = f"sentinel_cfg_{uuid4().hex[:8]}"
    config_a = LeaseConfig(
        key_prefix=prefix,
        max_global=50,
        max_per_user=3,
        lease_seconds=10.0,
        fail_open=False,
    )
    config_incompatible = LeaseConfig(
        key_prefix=prefix,
        max_global=100,  # Drift
        max_per_user=3,
        lease_seconds=10.0,
        fail_open=False,
    )

    manager_a = StreamLeaseManager(redis=client, config=config_a)
    manager_inc = StreamLeaseManager(redis=client, config=config_incompatible)

    # 1. Register canonical config on current master
    assert await manager_a.verify_cluster_config(strict=True) is True

    # 2. Wait for asynchronous replication to sync key to replica
    await asyncio.sleep(1.0)

    # 3. Verify incompatible config is rejected
    with pytest.raises(ConfigurationMismatchError) as exc_info:
        await manager_inc.verify_cluster_config(strict=True)
    assert "max_global" in str(exc_info.value)

    # 4. Same compatible config verifies as True
    manager_a2 = StreamLeaseManager(redis=client, config=config_a)
    assert await manager_a2.verify_cluster_config(strict=True) is True

    await manager_a.close()
    await manager_a2.close()
    await manager_inc.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_sentinel_outage_exceeding_lease_ttl_terminates_stream(sentinel_cluster):
    """
    Verify failure model: if master failover outage exceeds remaining lease TTL,
    the adaptive grace period deadline is reached and the stream is cancelled with StreamLeaseLost.
    """
    client = sentinel_cluster["sentinel"].master_for(
        sentinel_cluster["service_name"], socket_timeout=2.0
    )
    await client.ping()
    prefix = f"sentinel_timeout_{uuid4().hex[:8]}"

    # Very short lease (0.8s) with renew_interval 0.2s
    config = LeaseConfig(
        lease_seconds=0.8,
        max_per_user=1,
        max_global=5,
        key_prefix=prefix,
    )
    manager = StreamLeaseManager(redis=client, config=config)

    # Intentionally point redis.eval to a broken connection simulating an unrecoverable outage
    lease = await manager.acquire("user_timeout")

    with pytest.raises(StreamLeaseLost):
        async with lease:
            # Simulate total backend outage during renewal
            manager.redis.eval = AsyncMock(side_effect=ConnectionError("Master unreachable"))
            # Sleep longer than lease_seconds
            await asyncio.sleep(1.5)

    await manager.close()
    await client.aclose()
