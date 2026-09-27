from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from uuid import uuid4

import pytest
import redis.asyncio as aioredis
from redis.asyncio.cluster import RedisCluster

from fastapi_stream_lease import (
    ConfigurationMismatchError,
    LeaseConfig,
    StreamLease,
    StreamLeaseLost,
    StreamLeaseManager,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("REDIS_CLUSTER_NODES"),
    reason="Set REDIS_CLUSTER_NODES to run real Redis Cluster failover tests",
)


def parse_cluster_nodes(nodes_env: str) -> list[tuple[str, int]]:
    """Parse comma-separated 'host:port' strings into a list of tuples."""
    nodes = []
    for item in nodes_env.split(","):
        host, port_str = item.strip().split(":")
        nodes.append((host, int(port_str)))
    return nodes


async def wait_for_cluster_ready(cluster: RedisCluster, timeout: float = 30.0) -> None:
    """Poll until Redis Cluster reports state:ok, all 16384 slots assigned, and replicas healthy."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        try:
            info = await cluster.cluster_info()
            state = info.get("cluster_state") or info.get(b"cluster_state")
            if isinstance(state, bytes):
                state = state.decode("utf-8")
            slots = info.get("cluster_slots_assigned") or info.get(b"cluster_slots_assigned")
            if isinstance(slots, bytes):
                slots = slots.decode("utf-8")
            known_nodes = info.get("cluster_known_nodes") or info.get(b"cluster_known_nodes")
            if isinstance(known_nodes, bytes):
                known_nodes = known_nodes.decode("utf-8")

            if state == "ok" and int(slots) == 16384 and int(known_nodes) >= 6:
                slots_data = await cluster.cluster_slots()
                has_replicas = True
                if isinstance(slots_data, dict):
                    for info_slot in slots_data.values():
                        if not info_slot.get("replicas"):
                            has_replicas = False
                            break
                elif isinstance(slots_data, list):
                    for entry in slots_data:
                        if len(entry) < 4 or not entry[3]:
                            has_replicas = False
                            break
                if has_replicas:
                    return
        except Exception:
            pass
        await asyncio.sleep(0.5)
    raise TimeoutError(f"Redis Cluster did not become fully ready with replicas within {timeout}s")


@pytest.fixture
async def cluster_client():
    """Provide a real RedisCluster client connected to the test cluster."""
    nodes_env = os.environ.get(
        "REDIS_CLUSTER_NODES", "redis-cluster-1:7000,redis-cluster-2:7001,redis-cluster-3:7002"
    )
    first_node = nodes_env.split(",")[0].strip()
    client = RedisCluster.from_url(f"redis://{first_node}")
    await wait_for_cluster_ready(client)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_cluster_basic_lease_lifecycle(cluster_client):
    """
    Verify basic lease lifecycle on real multi-node Redis Cluster:
    acquire, active counts, manual renew, and release.
    """
    prefix = f"cluster_basic_{uuid4().hex[:8]}"
    config = LeaseConfig(
        lease_seconds=5.0,
        max_per_user=2,
        max_global=5,
        key_prefix=prefix,
    )
    manager = StreamLeaseManager(redis=cluster_client, config=config)

    # Acquire
    lease1 = await manager.acquire("user_alpha")
    assert lease1.lease_id is not None
    assert await manager.get_active_count("user_alpha") == 1
    assert await manager.get_active_count() == 1

    # Second acquire for same user
    lease2 = await manager.acquire("user_alpha")
    assert await manager.get_active_count("user_alpha") == 2
    assert await manager.get_active_count() == 2

    # Renew
    assert await lease1.renew() is True
    assert await manager.renew(lease2) is True

    # Release
    await lease1.release()
    assert await manager.get_active_count("user_alpha") == 1
    await lease2.release()
    assert await manager.get_active_count("user_alpha") == 0

    await manager.close()


@pytest.mark.asyncio
async def test_cluster_hash_tag_cross_slot_immunity(cluster_client):
    """
    Verify atomic multi-key Lua execution across different slots and masters.
    Distinct prefixes map to different hash slots on different cluster masters.
    All must execute without CROSSSLOT errors due to the common hash tag {prefix}.
    """
    managers = []
    leases = []
    try:
        for i in range(5):
            prefix = f"cluster_slot_tag_{i}_{uuid4().hex[:8]}"
            config = LeaseConfig(
                lease_seconds=5.0,
                max_per_user=3,
                max_global=10,
                key_prefix=prefix,
            )
            mgr = StreamLeaseManager(redis=cluster_client, config=config)
            managers.append(mgr)

            # Acquire concurrent leases
            l1 = await mgr.acquire(f"user_{i}_a")
            l2 = await mgr.acquire(f"user_{i}_b")
            leases.extend([l1, l2])

            assert await mgr.get_active_count() == 2
            assert await l1.renew() is True
            assert await l2.renew() is True
    finally:
        for lease in leases:
            with suppress(Exception):
                await lease.release()
        for mgr in managers:
            await mgr.close()


@pytest.mark.asyncio
async def test_cluster_verify_config_drift_detection(cluster_client):
    """
    Verify verify_cluster_config() registers the canonical config on Redis Cluster
    and correctly detects configuration drift across cluster pods.
    """
    prefix = f"cluster_config_{uuid4().hex[:8]}"
    config_a = LeaseConfig(
        key_prefix=prefix,
        max_global=15,
        max_per_user=3,
        lease_seconds=10.0,
    )
    config_incompatible = LeaseConfig(
        key_prefix=prefix,
        max_global=999,  # Drifted limit
        max_per_user=3,
        lease_seconds=10.0,
    )

    manager_a = StreamLeaseManager(redis=cluster_client, config=config_a)
    manager_drift = StreamLeaseManager(redis=cluster_client, config=config_incompatible)

    # Initial verification registers canonical state
    assert await manager_a.verify_cluster_config(strict=True) is True

    # Same config validates successfully
    manager_same = StreamLeaseManager(redis=cluster_client, config=config_a)
    assert await manager_same.verify_cluster_config(strict=True) is True

    # Drifted config is rejected with ConfigurationMismatchError
    with pytest.raises(ConfigurationMismatchError) as exc_info:
        await manager_drift.verify_cluster_config(strict=True)
    assert "max_global" in str(exc_info.value)

    await manager_a.close()
    await manager_drift.close()
    await manager_same.close()


def find_slot_nodes(
    slots_data: dict | list, target_slot: int
) -> tuple[tuple[str, int], tuple[str, int]]:
    """Return ((master_host, master_port), (replica_host, replica_port)) for target_slot."""
    if isinstance(slots_data, dict):
        for (start_slot, end_slot), info in slots_data.items():
            if start_slot <= target_slot <= end_slot:
                master = info["primary"]
                replicas = info.get("replicas", [])
                if not replicas:
                    msg = f"Slot {target_slot} has no replicas assigned yet: {slots_data}"
                    raise ValueError(msg)
                return master, replicas[0]
    elif isinstance(slots_data, list):
        for entry in slots_data:
            start_slot, end_slot = entry[0], entry[1]
            if start_slot <= target_slot <= end_slot:
                m_info = entry[2]
                if len(entry) < 4 or not entry[3]:
                    msg = f"Slot {target_slot} has no replicas assigned yet: {slots_data}"
                    raise ValueError(msg)
                r_info = entry[3]
                m_host = m_info[0].decode() if isinstance(m_info[0], bytes) else m_info[0]
                r_host = r_info[0].decode() if isinstance(r_info[0], bytes) else r_info[0]
                return (m_host, m_info[1]), (r_host, r_info[1])
    raise ValueError(f"Slot {target_slot} not found in cluster slots data")


async def discover_slot_nodes(
    cluster: RedisCluster, slot: int, timeout: float = 15.0
) -> tuple[tuple[str, int], tuple[str, int]]:
    """Poll cluster_slots until slot has settled primary and replica nodes."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        try:
            slots_map = await cluster.cluster_slots()
            return find_slot_nodes(slots_map, slot)
        except ValueError:
            await asyncio.sleep(0.5)
    raise TimeoutError(f"Slot {slot} nodes (primary + replica) did not settle within {timeout}s")


@pytest.mark.asyncio
async def test_cluster_failover_during_active_stream_renewal(cluster_client):
    """
    Test zero-dropped-lease resilience during Redis Cluster node failover:
    1. Identify the master node and replica serving the slot for {prefix}.
    2. Hold an active stream lease renewing every 1s.
    3. Trigger manual CLUSTER FAILOVER on the replica.
    4. Verify the client handles MOVED redirections transparently and stream completes.
    """
    prefix = f"cluster_failover_{uuid4().hex[:8]}"
    config = LeaseConfig(
        lease_seconds=15.0,
        max_per_user=2,
        max_global=10,
        key_prefix=prefix,
    )
    manager = StreamLeaseManager(redis=cluster_client, config=config)

    # Find the slot for prefix and discover master & replica nodes
    slot = await cluster_client.cluster_keyslot(config.global_key)
    (master_host, master_port), (replica_host, replica_port) = await discover_slot_nodes(
        cluster_client, slot
    )

    lease_started = asyncio.Event()
    failover_done = asyncio.Event()
    test_failed: list[Exception] = []
    active_leases: list[StreamLease] = []

    async def streaming_task():
        try:
            async with manager.lease("user_cluster_failover", renew_interval=1.0) as lease:
                active_leases.append(lease)
                lease_started.set()
                await failover_done.wait()
                assert lease._is_released is False
        except Exception as exc:
            test_failed.append(exc)
            raise

    task = asyncio.create_task(streaming_task())
    await lease_started.wait()

    # Flush replication to replica
    assert active_leases[0].lease_id is not None
    raw_master = aioredis.from_url(f"redis://{master_host}:{master_port}")
    try:
        with suppress(Exception):
            await raw_master.execute_command("WAIT", 1, 1000)
    finally:
        await raw_master.aclose()

    # Trigger CLUSTER FAILOVER on the replica node
    raw_replica = aioredis.from_url(f"redis://{replica_host}:{replica_port}")
    try:
        await raw_replica.execute_command("CLUSTER", "FAILOVER")
    finally:
        await raw_replica.aclose()

    # Wait for failover to complete across the cluster
    for _ in range(30):
        await asyncio.sleep(0.5)
        try:
            new_slots = await cluster_client.cluster_slots()
            (new_host, _), _ = find_slot_nodes(new_slots, slot)
            if new_host == replica_host:
                break
        except Exception:
            continue

    # Let renewals run on the promoted master
    await asyncio.sleep(2.5)
    failover_done.set()
    await task

    assert len(test_failed) == 0, f"Streaming task failed during cluster failover: {test_failed}"
    await manager.close()


@pytest.mark.asyncio
async def test_cluster_outage_exceeding_lease_ttl_terminates_stream(cluster_client):
    """
    Verify failure model on Redis Cluster: when all nodes serving the slot
    are unresponsive exceeding the remaining lease TTL, the adaptive grace period
    expires and the stream owner task is cancelled with StreamLeaseLost.
    """
    prefix = f"cluster_outage_{uuid4().hex[:8]}"
    # Short lease (1.2s)
    config = LeaseConfig(
        lease_seconds=1.2,
        max_per_user=1,
        max_global=5,
        key_prefix=prefix,
    )
    manager = StreamLeaseManager(redis=cluster_client, config=config)
    lease = await manager.acquire("user_outage")

    slot = await cluster_client.cluster_keyslot(config.global_key)
    (m_host, m_port), (r_host, r_port) = await discover_slot_nodes(cluster_client, slot)

    conn_master = aioredis.from_url(f"redis://{m_host}:{m_port}", socket_timeout=0.5)
    conn_replica = aioredis.from_url(f"redis://{r_host}:{r_port}", socket_timeout=0.5)

    async def pause_node(conn: aioredis.Redis):
        try:
            await conn.execute_command("DEBUG", "SLEEP", 3.0)
        except Exception:
            pass
        finally:
            await conn.aclose()

    try:
        with pytest.raises(StreamLeaseLost):
            async with lease:
                asyncio.create_task(pause_node(conn_master))
                asyncio.create_task(pause_node(conn_replica))
                await asyncio.sleep(3.5)
    finally:
        await asyncio.sleep(3.5)
        await manager.close()
