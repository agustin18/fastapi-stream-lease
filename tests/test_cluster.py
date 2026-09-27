from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from typing import Any
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


async def wait_for_slot_primary(
    cluster: RedisCluster, slot: int, expected_host: str, timeout: float = 30.0
) -> str:
    """
    Poll cluster_slots until slot has expected_host as primary.
    Raises TimeoutError if deadline exceeded.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    last_primary: str | None = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            slots_map = await cluster.cluster_slots()
            (curr_host, _), _ = find_slot_nodes(slots_map, slot)
            last_primary = curr_host
            if curr_host == expected_host:
                return curr_host
        except Exception:
            pass
        await asyncio.sleep(0.5)
    raise TimeoutError(
        f"Slot {slot} primary did not transition to expected replica "
        f"'{expected_host}' within {timeout}s (last primary: '{last_primary}')"
    )


async def safe_close_client(client: Any) -> None:
    """Safely close client across redis-py 5.0.0 (close()) and 5.0.1+ (aclose())."""
    if hasattr(client, "aclose"):
        await client.aclose()
    elif hasattr(client, "close"):
        res = client.close()
        if asyncio.iscoroutine(res):
            await res


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
        await safe_close_client(client)


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
    Deterministically find prefixes mapping to each of the 3 primary nodes.
    For each shard, assert KEYSLOT(user_key) == KEYSLOT(global_key) == KEYSLOT(config_key).
    Execute acquire, renew, and release across all 3 masters without CROSSSLOT errors.
    """
    slots_data = await cluster_client.cluster_slots()
    primary_nodes: set[tuple[str, int]] = set()
    if isinstance(slots_data, dict):
        for info in slots_data.values():
            primary_nodes.add(info["primary"])
    elif isinstance(slots_data, list):
        for entry in slots_data:
            m_info = entry[2]
            m_host = m_info[0].decode() if isinstance(m_info[0], bytes) else m_info[0]
            primary_nodes.add((m_host, m_info[1]))

    assert len(primary_nodes) >= 3, f"Expected at least 3 primary nodes, got {primary_nodes}"

    # Search candidate prefixes until we have mapped at least one prefix to each distinct primary
    prefixes_by_primary: dict[tuple[str, int], str] = {}
    candidate_idx = 0
    while len(prefixes_by_primary) < len(primary_nodes) and candidate_idx < 1000:
        cand_prefix = f"shard_{candidate_idx}_{uuid4().hex[:6]}"
        global_key = f"{{{cand_prefix}}}:global"
        slot = await cluster_client.cluster_keyslot(global_key)
        (primary_host, primary_port), _ = find_slot_nodes(slots_data, slot)
        node_key = (primary_host, primary_port)
        if node_key in primary_nodes and node_key not in prefixes_by_primary:
            prefixes_by_primary[node_key] = cand_prefix
        candidate_idx += 1

    assert len(prefixes_by_primary) == len(primary_nodes), (
        f"Could not map prefixes to all {len(primary_nodes)} primaries: {prefixes_by_primary}"
    )

    managers: list[StreamLeaseManager] = []
    leases: list[StreamLease] = []
    try:
        for prefix in prefixes_by_primary.values():
            user_key = f"{{{prefix}}}:user:test_user"
            global_key = f"{{{prefix}}}:global"
            config_key = f"{{{prefix}}}:config"

            # Assert all 3 keys hash to the exact same cluster slot
            slot_user = await cluster_client.cluster_keyslot(user_key)
            slot_global = await cluster_client.cluster_keyslot(global_key)
            slot_config = await cluster_client.cluster_keyslot(config_key)
            assert slot_user == slot_global == slot_config, (
                f"Hash slot mismatch for prefix {prefix}: "
                f"user={slot_user}, global={slot_global}, config={slot_config}"
            )

            config = LeaseConfig(
                lease_seconds=5.0,
                max_per_user=2,
                max_global=10,
                key_prefix=prefix,
            )
            mgr = StreamLeaseManager(redis=cluster_client, config=config)
            managers.append(mgr)

            l1 = await mgr.acquire("test_user")
            leases.append(l1)
            assert await mgr.get_active_count("test_user") == 1
            assert await mgr.get_active_count() == 1
            assert await l1.renew() is True
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


@pytest.mark.asyncio
async def test_cluster_failover_during_active_stream_renewal(cluster_client):
    """
    Test stream lease survival during coordinated Redis Cluster node failover:
    1. Identify the master node and replica serving the slot for {prefix}.
    2. Hold an active stream lease renewing every 1s.
    3. Trigger manual CLUSTER FAILOVER on the replica.
    4. Assert slot primary updates to the replica and renewals continue without interruption.
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
        await safe_close_client(raw_master)

    # Trigger CLUSTER FAILOVER on the replica node
    raw_replica = aioredis.from_url(f"redis://{replica_host}:{replica_port}")
    try:
        await raw_replica.execute_command("CLUSTER", "FAILOVER")
    finally:
        await safe_close_client(raw_replica)

    # Wait for failover to complete across the cluster with strict primary assertion
    new_primary = await wait_for_slot_primary(cluster_client, slot, replica_host, timeout=30.0)
    assert new_primary == replica_host, (
        f"Expected {replica_host} to become primary, got {new_primary}"
    )

    # Let renewals run on the promoted master
    await asyncio.sleep(2.5)
    failover_done.set()
    await task

    assert len(test_failed) == 0, f"Streaming task failed during cluster failover: {test_failed}"
    await manager.close()


@pytest.mark.asyncio
async def test_cluster_hard_master_failure_and_election_failover(cluster_client):
    """
    Test automatic failover resilience without manual CLUSTER FAILOVER:
    1. Identify master and replica for slot {prefix}.
    2. Hold active stream lease renewing every 1s (TTL 15s).
    3. Issue WAIT 1 1000 and verify lease state exists directly on the replica ZSET.
    4. Issue DEBUG SLEEP 5.0 on master (> cluster-node-timeout 2000ms) to trigger
       PFAIL -> FAIL -> replica election -> promotion automatically.
    5. Poll wait_for_slot_primary asserting the old replica becomes new primary.
    6. Verify stream lease continues renewing on promoted primary without errors.
    """
    prefix = f"cluster_hard_failover_{uuid4().hex[:8]}"
    config = LeaseConfig(
        lease_seconds=15.0,
        max_per_user=2,
        max_global=10,
        key_prefix=prefix,
    )
    manager = StreamLeaseManager(redis=cluster_client, config=config)

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
            async with manager.lease("user_cluster_hard_failover", renew_interval=1.0) as lease:
                active_leases.append(lease)
                lease_started.set()
                await failover_done.wait()
                assert lease._is_released is False
        except Exception as exc:
            test_failed.append(exc)
            raise

    task = asyncio.create_task(streaming_task())
    await lease_started.wait()

    lease_id = active_leases[0].lease_id
    assert lease_id is not None

    # Sync write to replica via WAIT
    raw_master = aioredis.from_url(f"redis://{master_host}:{master_port}")
    try:
        with suppress(Exception):
            await raw_master.execute_command("WAIT", 1, 1000)
    finally:
        await safe_close_client(raw_master)

    # Verify directly on replica that lease member exists in ZSET
    raw_replica = aioredis.from_url(f"redis://{replica_host}:{replica_port}")
    try:
        await raw_replica.execute_command("READONLY")
        user_key = config.user_key("user_cluster_hard_failover")
        score = await raw_replica.zscore(user_key, lease_id)
        assert score is not None, (
            f"Lease {lease_id} was not replicated to replica {replica_host}:{replica_port}"
        )
    finally:
        await safe_close_client(raw_replica)

    # Freeze master with DEBUG SLEEP 5.0 (> cluster-node-timeout 2000ms)
    # Master will stop responding, causing other masters to mark it PFAIL -> FAIL
    # and the replica will trigger automatic election and promotion.
    conn_freeze = aioredis.from_url(f"redis://{master_host}:{master_port}", socket_timeout=0.5)

    async def sleep_master():
        try:
            await conn_freeze.execute_command("DEBUG", "SLEEP", 5.0)
        except Exception:
            pass
        finally:
            await safe_close_client(conn_freeze)

    _sleep_task = asyncio.create_task(sleep_master())

    # Wait for automatic election and promotion
    new_primary = await wait_for_slot_primary(cluster_client, slot, replica_host, timeout=30.0)
    assert new_primary == replica_host, f"Expected {replica_host} to be promoted, got {new_primary}"

    # Let renewals run on newly promoted primary
    await asyncio.sleep(2.5)
    failover_done.set()
    await task

    assert len(test_failed) == 0, (
        f"Streaming task failed during automatic cluster failover: {test_failed}"
    )
    # Wait for former primary to wake up from DEBUG SLEEP and re-converge
    await asyncio.sleep(3.0)
    await wait_for_cluster_ready(cluster_client)
    await manager.close()


@pytest.mark.asyncio
async def test_cluster_outage_exceeding_lease_ttl_terminates_stream():
    """
    Verify failure model on Redis Cluster: when all nodes serving the slot
    are unresponsive exceeding the remaining lease TTL, the adaptive grace period
    expires and the stream owner task is cancelled with StreamLeaseLost.
    Asserts on_lost callback receives reason == 'backend_timeout' (not unexpected_error).
    """
    nodes_env = os.environ.get(
        "REDIS_CLUSTER_NODES", "redis-cluster-1:7000,redis-cluster-2:7001,redis-cluster-3:7002"
    )
    first_node = nodes_env.split(",")[0].strip()
    client = RedisCluster.from_url(
        f"redis://{first_node}", socket_timeout=0.2, cluster_error_retry_attempts=1
    )
    await wait_for_cluster_ready(client)

    prefix = f"cluster_outage_{uuid4().hex[:8]}"
    lost_reasons: list[str] = []
    # Short lease (1.2s)
    config = LeaseConfig(
        lease_seconds=1.2,
        max_per_user=1,
        max_global=5,
        key_prefix=prefix,
        on_lost=lambda lease, reason: lost_reasons.append(reason),
    )
    manager = StreamLeaseManager(redis=client, config=config)
    try:
        lease = await manager.acquire("user_outage")

        slot = await client.cluster_keyslot(config.global_key)
        (m_host, m_port), (r_host, r_port) = await discover_slot_nodes(client, slot)

        conn_master = aioredis.from_url(f"redis://{m_host}:{m_port}", socket_timeout=0.5)
        conn_replica = aioredis.from_url(f"redis://{r_host}:{r_port}", socket_timeout=0.5)

        async def pause_node(conn: aioredis.Redis):
            try:
                await conn.execute_command("DEBUG", "SLEEP", 3.5)
            except Exception:
                pass
            finally:
                await safe_close_client(conn)

        with pytest.raises(StreamLeaseLost):
            async with lease:
                _task_m = asyncio.create_task(pause_node(conn_master))
                _task_r = asyncio.create_task(pause_node(conn_replica))
                await asyncio.sleep(4.0)
    finally:
        # Wait for paused nodes to wake up and cluster to fully recover
        await asyncio.sleep(4.0)
        await wait_for_cluster_ready(client)
        await manager.close()
        await safe_close_client(client)

    assert lost_reasons == ["backend_timeout"], (
        f"Expected lost reason ['backend_timeout'], got: {lost_reasons}"
    )


@pytest.mark.asyncio
async def test_cluster_verify_cluster_config_across_failover(cluster_client):
    """
    Verify canonical configuration key persistence across Redis Cluster failover:
    1. Write canonical config to old primary A via verify_cluster_config(strict=True).
    2. WAIT 1 1000 to synchronize write to replica B.
    3. Raw GET on replica B to ensure replication succeeded.
    4. Trigger failover to promote replica B.
    5. Raw GET on promoted B to confirm key persisted across election.
    6. Run verify_cluster_config on promoted primary: succeeds with same config,
       and rejects conflicting configuration with ConfigurationMismatchError.
    """
    await wait_for_cluster_ready(cluster_client)
    prefix = f"cluster_cfg_failover_{uuid4().hex[:8]}"
    config_a = LeaseConfig(
        key_prefix=prefix,
        max_global=20,
        max_per_user=4,
        lease_seconds=15.0,
    )
    config_mismatch = LeaseConfig(
        key_prefix=prefix,
        max_global=999,
        max_per_user=4,
        lease_seconds=15.0,
    )

    slot = await cluster_client.cluster_keyslot(config_a.config_key)
    (master_host, master_port), (replica_host, replica_port) = await discover_slot_nodes(
        cluster_client, slot
    )

    mgr_a = StreamLeaseManager(redis=cluster_client, config=config_a)
    try:
        # Step 1: Register canonical config on current primary
        assert await mgr_a.verify_cluster_config(strict=True) is True

        # Step 2: Flush replication to replica via WAIT
        raw_master = aioredis.from_url(f"redis://{master_host}:{master_port}")
        try:
            with suppress(Exception):
                await raw_master.execute_command("WAIT", 1, 1000)
        finally:
            await safe_close_client(raw_master)

        # Step 3: Raw GET on replica B
        raw_replica = aioredis.from_url(f"redis://{replica_host}:{replica_port}")
        try:
            await raw_replica.execute_command("READONLY")
            replica_data = await raw_replica.get(config_a.config_key)
            assert replica_data is not None, (
                f"Config key {config_a.config_key} not found on replica {replica_host}"
            )

            # Step 4: Coordinated failover to promote replica B
            await raw_replica.execute_command("CLUSTER", "FAILOVER")

            # Step 5: Wait for replica B to become primary
            new_primary = await wait_for_slot_primary(cluster_client, slot, replica_host)
            assert new_primary == replica_host

            # Raw GET on newly promoted primary
            promoted_data = await raw_replica.get(config_a.config_key)
            assert promoted_data == replica_data, "Config key content changed after failover"
        finally:
            await safe_close_client(raw_replica)

        # Step 6: verify_cluster_config against newly promoted primary
        mgr_same = StreamLeaseManager(redis=cluster_client, config=config_a)
        assert await mgr_same.verify_cluster_config(strict=True) is True
        await mgr_same.close()

        mgr_drift = StreamLeaseManager(redis=cluster_client, config=config_mismatch)
        with pytest.raises(ConfigurationMismatchError):
            await mgr_drift.verify_cluster_config(strict=True)
        await mgr_drift.close()
    finally:
        await mgr_a.close()
