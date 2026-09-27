from __future__ import annotations

import asyncio
import json
import os
from contextlib import suppress
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel
from redis.exceptions import ConnectionError

from fastapi_stream_lease import (
    ConfigurationMismatchError,
    LeaseConfig,
    StreamLease,
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


async def wait_for_writable_master(
    sentinel: Sentinel, service_name: str, timeout: float = 25.0
) -> tuple[str, int]:
    """Poll until Sentinel reports a master that successfully executes write commands."""
    for _ in range(int(timeout / 0.5)):
        try:
            client = sentinel.master_for(service_name, socket_timeout=1.0)
            test_key = f"sentinel_writable_{uuid4().hex[:8]}"
            await client.set(test_key, "1", ex=5)
            await client.delete(test_key)
            await client.aclose()
            return await sentinel.discover_master(service_name)
        except Exception:
            await asyncio.sleep(0.5)
    raise TimeoutError(f"Master for '{service_name}' did not become writable within {timeout}s")


async def wait_for_sentinel_settled(
    sentinel: Sentinel,
    sentinel_hosts: list[tuple[str, int]],
    service_name: str,
    timeout: float = 30.0,
) -> tuple[str, int]:
    """
    Poll Sentinel cluster until:
    1. Sentinel confirms quorum and majority via `SENTINEL ckquorum`.
    2. Master has flags 'master' (not failover_in_progress, not s_down, not o_down).
    3. Sentinels agree on topology (num-other-sentinels >= 2, num-slaves >= 1).
    4. At least one replica is in healthy 'slave' state ready for failover promotion.
    5. Master executes write commands successfully.
    """
    first_host, first_port = sentinel_hosts[0]
    deadline = asyncio.get_running_loop().time() + timeout
    settled = False

    while asyncio.get_running_loop().time() < deadline:
        try:
            admin_conn = aioredis.from_url(f"redis://{first_host}:{first_port}")
            try:
                # 1. Quorum and majority reachable check
                ck = await admin_conn.execute_command("SENTINEL", "ckquorum", service_name)
                if isinstance(ck, bytes):
                    ck = ck.decode("utf-8")
                if not ck.startswith("OK"):
                    await asyncio.sleep(0.5)
                    continue

                # 2. Master status and topology discovery
                master_info = await admin_conn.execute_command("SENTINEL", "master", service_name)
                raw_flags = master_info.get(b"flags") or master_info.get("flags", b"")
                if isinstance(raw_flags, bytes):
                    raw_flags = raw_flags.decode("utf-8")

                num_other_val = master_info.get(b"num-other-sentinels") or master_info.get(
                    "num-other-sentinels", 0
                )
                num_slaves_val = master_info.get(b"num-slaves") or master_info.get("num-slaves", 0)
                num_other = int(num_other_val)
                num_slaves = int(num_slaves_val)

                if (
                    "failover_in_progress" not in raw_flags
                    and "s_down" not in raw_flags
                    and "o_down" not in raw_flags
                    and num_other >= 2
                    and num_slaves >= 1
                ):
                    # 3. Healthy replica check
                    cur_master_ip = master_info.get(b"ip") or master_info.get("ip")
                    if isinstance(cur_master_ip, bytes):
                        cur_master_ip = cur_master_ip.decode("utf-8")

                    replicas = await admin_conn.execute_command(
                        "SENTINEL", "replicas", service_name
                    )
                    for rep in replicas:
                        rep_flags = rep.get(b"flags") or rep.get("flags", b"")
                        if isinstance(rep_flags, bytes):
                            rep_flags = rep_flags.decode("utf-8")
                        link_status = rep.get(b"master-link-status") or rep.get(
                            "master-link-status", b""
                        )
                        if isinstance(link_status, bytes):
                            link_status = link_status.decode("utf-8")
                        rep_ip = rep.get(b"ip") or rep.get("ip")
                        if isinstance(rep_ip, bytes):
                            rep_ip = rep_ip.decode("utf-8")

                        if (
                            rep_ip != cur_master_ip
                            and "slave" in rep_flags
                            and "s_down" not in rep_flags
                            and "disconnected" not in rep_flags
                            and link_status == "ok"
                        ):
                            settled = True
                            break
                    if settled:
                        break
            finally:
                await admin_conn.aclose()
        except Exception:
            pass
        await asyncio.sleep(0.5)

    if not settled:
        raise TimeoutError(
            f"Sentinel cluster for '{service_name}' failed to settle within {timeout}s"
        )

    return await wait_for_writable_master(sentinel, service_name, timeout=timeout)


async def get_replica_address(
    sentinel_hosts: list[tuple[str, int]], service_name: str
) -> tuple[str, int]:
    """Find the host and port of an active, healthy replica from Sentinel."""
    first_host, first_port = sentinel_hosts[0]
    admin_conn = aioredis.from_url(f"redis://{first_host}:{first_port}")
    try:
        master_info = await admin_conn.execute_command("SENTINEL", "master", service_name)
        cur_master_ip = master_info.get(b"ip") or master_info.get("ip")
        if isinstance(cur_master_ip, bytes):
            cur_master_ip = cur_master_ip.decode("utf-8")

        replicas = await admin_conn.execute_command("SENTINEL", "replicas", service_name)
        for rep in replicas:
            rep_flags = rep.get(b"flags") or rep.get("flags", b"")
            if isinstance(rep_flags, bytes):
                rep_flags = rep_flags.decode("utf-8")
            link_status = rep.get(b"master-link-status") or rep.get("master-link-status", b"")
            if isinstance(link_status, bytes):
                link_status = link_status.decode("utf-8")

            ip = rep.get(b"ip") or rep.get("ip")
            port = rep.get(b"port") or rep.get("port")
            if isinstance(ip, bytes):
                ip = ip.decode("utf-8")

            if (
                ip != cur_master_ip
                and "slave" in rep_flags
                and "s_down" not in rep_flags
                and "disconnected" not in rep_flags
                and link_status == "ok"
            ):
                return ip, int(port)
    finally:
        await admin_conn.aclose()
    raise RuntimeError(f"No healthy replica found for '{service_name}'")


async def wait_for_key_replication(
    sentinel_hosts: list[tuple[str, int]],
    service_name: str,
    key: str,
    expected_value: str | None = None,
    timeout: float = 5.0,
) -> None:
    """Poll replica node directly until key matches expected_value."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        try:
            replica_host, replica_port = await get_replica_address(sentinel_hosts, service_name)
            replica_client = aioredis.from_url(
                f"redis://{replica_host}:{replica_port}", socket_timeout=1.0
            )
            try:
                val = await replica_client.get(key)
                if val is not None:
                    if isinstance(val, bytes):
                        val = val.decode("utf-8")
                    if expected_value is None or val == expected_value:
                        return
            finally:
                await replica_client.aclose()
        except Exception:
            pass
        await asyncio.sleep(0.05)
    raise TimeoutError(f"Key '{key}' was not replicated to replica within {timeout}s")


async def wait_for_lease_replication(
    sentinel_hosts: list[tuple[str, int]],
    service_name: str,
    user_key: str,
    lease_id: str,
    timeout: float = 5.0,
) -> None:
    """Poll replica node directly until lease_id is confirmed in the replica's user sorted set."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        try:
            replica_host, replica_port = await get_replica_address(sentinel_hosts, service_name)
            replica_client = aioredis.from_url(
                f"redis://{replica_host}:{replica_port}", socket_timeout=1.0
            )
            try:
                score = await replica_client.zscore(user_key, lease_id)
                if score is not None and float(score) > 0:
                    return
            finally:
                await replica_client.aclose()
        except Exception:
            pass
        await asyncio.sleep(0.05)
    raise TimeoutError(f"Lease '{lease_id}' was not replicated to replica within {timeout}s")


@pytest.fixture
async def sentinel_cluster():
    """
    Connect to Sentinel topology and ensure quorum readiness before running tests.

    Waits until Sentinels have discovered each other (quorum agreement:
    num-other-sentinels >= 2, num-slaves >= 1) and master is writable.
    """
    hosts_env = os.environ.get(
        "REDIS_SENTINEL_HOSTS", "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"
    )
    service_name = os.environ.get("REDIS_SENTINEL_SERVICE", "mymaster")
    sentinel_hosts = parse_sentinel_hosts(hosts_env)
    sentinel = Sentinel(sentinel_hosts, socket_timeout=2.0)

    # Topology readiness verification
    await wait_for_sentinel_settled(sentinel, sentinel_hosts, service_name)

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
async def test_sentinel_forced_failover_client_reconnection(sentinel_cluster):
    """
    Test zero-dropped-lease resilience during controlled SENTINEL FAILOVER (forced failover).

    A long-running lease is held in a context manager while SENTINEL failover
    is triggered. ReadOnlyError and ConnectionError during master promotion
    must trigger adaptive grace-period retries and reconnect to the new master
    without cancelling the active stream.
    """
    sentinel = sentinel_cluster["sentinel"]
    service_name = sentinel_cluster["service_name"]
    first_sentinel_host, first_sentinel_port = sentinel_cluster["sentinel_hosts"][0]

    initial_master = await wait_for_sentinel_settled(
        sentinel, sentinel_cluster["sentinel_hosts"], service_name
    )
    client = sentinel.master_for(service_name, socket_timeout=2.0)
    await client.ping()

    prefix = f"sentinel_forced_{uuid4().hex[:8]}"
    config = LeaseConfig(
        lease_seconds=15.0,
        max_per_user=2,
        max_global=10,
        key_prefix=prefix,
    )
    manager = StreamLeaseManager(redis=client, config=config)

    lease_started = asyncio.Event()
    failover_done = asyncio.Event()
    test_failed: list[Exception] = []
    active_leases: list[StreamLease] = []

    async def streaming_task():
        try:
            async with manager.lease("user_failover", renew_interval=1.0) as lease:
                active_leases.append(lease)
                lease_started.set()
                await failover_done.wait()
                assert lease._is_released is False
        except Exception as exc:
            test_failed.append(exc)
            raise

    task = asyncio.create_task(streaming_task())
    await lease_started.wait()

    # Flush replication to replica so lease is present before failover
    with suppress(Exception):
        await client.execute_command("WAIT", 1, 1000)

    # Confirm lease was replicated to replica before triggering failover
    held_lease = active_leases[0]
    await wait_for_lease_replication(
        sentinel_cluster["sentinel_hosts"],
        service_name,
        held_lease.user_key,
        held_lease.lease_id,
    )

    # Trigger forced failover via Sentinel administration connection
    admin_conn = aioredis.from_url(f"redis://{first_sentinel_host}:{first_sentinel_port}")
    try:
        await admin_conn.execute_command("SENTINEL", "failover", service_name)
    finally:
        await admin_conn.aclose()

    # Poll Sentinel until master address changes and is writable
    for _ in range(60):
        await asyncio.sleep(0.5)
        try:
            current = await sentinel.discover_master(service_name)
            if current != initial_master:
                break
        except Exception:
            continue

    new_master = await wait_for_writable_master(sentinel, service_name, timeout=25.0)
    assert new_master != initial_master, f"Failover did not change master from {initial_master}"

    # Allow at least 2 renewal cycles on the new master
    await asyncio.sleep(2.5)

    failover_done.set()
    await task
    assert len(test_failed) == 0, f"Streaming task failed during failover: {test_failed}"

    # Verify final state on new master
    assert await manager.get_active_count("user_failover") == 0
    await manager.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_sentinel_hard_master_failure_and_election_failover(sentinel_cluster):
    """
    Test zero-dropped-lease resilience during REAL master failure:
    SDOWN -> ODOWN (quorum 2) -> leader election -> replica promotion.

    Simulates a master server hang/crash via DEBUG SLEEP. The Sentinel cluster
    detects down-after-milliseconds timeout, forms quorum, elects a leader, and
    promotes the replica to master. The active streaming task's renewal loop
    survives the outage window via adaptive grace-period retries and reconnects
    to the newly elected master without dropping the stream.
    """
    sentinel = sentinel_cluster["sentinel"]
    service_name = sentinel_cluster["service_name"]

    initial_master = await wait_for_sentinel_settled(
        sentinel, sentinel_cluster["sentinel_hosts"], service_name
    )
    client = sentinel.master_for(service_name, socket_timeout=2.0)
    await client.ping()

    prefix = f"sentinel_hard_{uuid4().hex[:8]}"
    config = LeaseConfig(
        lease_seconds=15.0,
        max_per_user=2,
        max_global=10,
        key_prefix=prefix,
    )
    manager = StreamLeaseManager(redis=client, config=config)

    lease_started = asyncio.Event()
    failover_done = asyncio.Event()
    test_failed: list[Exception] = []
    active_leases_hard: list[StreamLease] = []

    async def streaming_task():
        try:
            async with manager.lease("user_hard_failover", renew_interval=1.0) as lease:
                active_leases_hard.append(lease)
                lease_started.set()
                await failover_done.wait()
                assert lease._is_released is False
        except Exception as exc:
            test_failed.append(exc)
            raise

    task = asyncio.create_task(streaming_task())
    await lease_started.wait()

    # Flush replication to replica so lease is present before inducing crash
    with suppress(Exception):
        await client.execute_command("WAIT", 1, 1000)

    # Confirm lease was replicated to replica before inducing master crash
    held_lease_hard = active_leases_hard[0]
    await wait_for_lease_replication(
        sentinel_cluster["sentinel_hosts"],
        service_name,
        held_lease_hard.user_key,
        held_lease_hard.lease_id,
    )

    # Induce hard unresponsiveness on current master via DEBUG SLEEP (2.5s)
    # Master event loop stops responding; Sentinels observe down-after-milliseconds (1000ms),
    # reach ODOWN (quorum 2/2), elect leader, and promote the replica.
    master_raw = aioredis.from_url(
        f"redis://{initial_master[0]}:{initial_master[1]}",
        socket_timeout=1.0,
    )

    async def pause_master():
        try:
            await master_raw.execute_command("DEBUG", "SLEEP", 2.5)
        except Exception:
            pass
        finally:
            await master_raw.aclose()

    asyncio.create_task(pause_master())

    # Wait for Sentinel to detect failure, elect leader, and promote replica
    for _ in range(60):
        await asyncio.sleep(0.5)
        try:
            current = await sentinel.discover_master(service_name)
            if current != initial_master:
                break
        except Exception:
            continue

    new_master = await wait_for_writable_master(sentinel, service_name, timeout=25.0)
    assert new_master != initial_master, (
        f"Real failure did not trigger master promotion from {initial_master}"
    )

    # Allow at least 2 renewal cycles on the promoted master
    await asyncio.sleep(2.5)

    failover_done.set()
    await task
    assert len(test_failed) == 0, (
        f"Streaming task failed during hard failure failover: {test_failed}"
    )

    # Verify lease was cleanly released on the new master
    assert await manager.get_active_count("user_hard_failover") == 0
    await manager.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_sentinel_verify_cluster_config_across_failover(sentinel_cluster):
    """
    Verify cluster configuration consistency survives a real Sentinel master failover.

    Self-contained test:
    1. Identify initial master A.
    2. Register canonical configuration fingerprint on master A.
    3. Verify and wait for canonical config key to be replicated to replica B.
    4. Trigger failover A -> B and verify Sentinel promotes B to master.
    5. Perform raw GET on promoted master B to prove key survived failover.
    6. On promoted master B: verify compatible worker config validates as True.
    7. On promoted master B: verify drifted config is rejected with ConfigurationMismatchError.
    """
    sentinel = sentinel_cluster["sentinel"]
    service_name = sentinel_cluster["service_name"]
    first_sentinel_host, first_sentinel_port = sentinel_cluster["sentinel_hosts"][0]

    initial_master = await wait_for_sentinel_settled(
        sentinel, sentinel_cluster["sentinel_hosts"], service_name
    )
    client_a = sentinel.master_for(service_name, socket_timeout=2.0)
    await client_a.ping()

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
        max_global=100,  # Drifted config
        max_per_user=3,
        lease_seconds=10.0,
        fail_open=False,
    )

    manager_a = StreamLeaseManager(redis=client_a, config=config_a)
    fingerprint_json = json.dumps(config_a.fingerprint_dict(), sort_keys=True)

    # 1. Register canonical config on current master A
    assert await manager_a.verify_cluster_config(strict=True) is True

    # Flush replication to replica so config key is present before failover
    with suppress(Exception):
        await client_a.execute_command("WAIT", 1, 1000)

    # 2. Wait explicitly until canonical config key is confirmed on replica B
    await wait_for_key_replication(
        sentinel_cluster["sentinel_hosts"],
        service_name,
        manager_a.config.config_key,
        expected_value=fingerprint_json,
    )

    # 3. Trigger failover from master A to replica B
    admin_conn = aioredis.from_url(f"redis://{first_sentinel_host}:{first_sentinel_port}")
    try:
        await admin_conn.execute_command("SENTINEL", "failover", service_name)
    finally:
        await admin_conn.aclose()

    # Poll Sentinel until master changes to B and is writable
    for _ in range(60):
        await asyncio.sleep(0.5)
        try:
            current = await sentinel.discover_master(service_name)
            if current != initial_master:
                break
        except Exception:
            continue

    new_master = await wait_for_writable_master(sentinel, service_name, timeout=25.0)
    assert new_master != initial_master, f"Failover failed to switch from {initial_master}"

    # 4. Connect to promoted master B
    client_b = sentinel.master_for(service_name, socket_timeout=2.0)
    await client_b.ping()

    # 5. Raw GET on promoted master B proves key actually survived replication + failover
    raw_config = await client_b.get(manager_a.config.config_key)
    if isinstance(raw_config, bytes):
        raw_config = raw_config.decode("utf-8")
    assert raw_config == fingerprint_json, "Canonical config was lost during failover"

    manager_b = StreamLeaseManager(redis=client_b, config=config_a)
    manager_inc = StreamLeaseManager(redis=client_b, config=config_incompatible)

    # 6. Compatible worker on new master B validates successfully
    assert await manager_b.verify_cluster_config(strict=True) is True

    # 7. Incompatible worker on new master B is rejected
    with pytest.raises(ConfigurationMismatchError) as exc_info:
        await manager_inc.verify_cluster_config(strict=True)
    assert "max_global" in str(exc_info.value)

    await manager_a.close()
    await manager_b.close()
    await manager_inc.close()
    await client_a.aclose()
    await client_b.aclose()


@pytest.mark.asyncio
async def test_sentinel_real_outage_exceeding_lease_ttl_terminates_stream(sentinel_cluster):
    """
    Verify failure model with real backend outage: if an unrecoverable outage
    exceeds the remaining lease TTL, the adaptive grace-period deadline expires
    and the stream owner task is cancelled with StreamLeaseLost.
    """
    sentinel = sentinel_cluster["sentinel"]
    service_name = sentinel_cluster["service_name"]

    await wait_for_sentinel_settled(sentinel, sentinel_cluster["sentinel_hosts"], service_name)
    # Master client with responsive socket timeout (0.5s) to detect outages promptly
    client = sentinel.master_for(service_name, socket_timeout=0.5)
    await client.ping()
    prefix = f"sentinel_real_outage_{uuid4().hex[:8]}"

    # Short lease (1.2s) with renew_interval 0.2s
    config = LeaseConfig(
        lease_seconds=1.2,
        max_per_user=1,
        max_global=5,
        key_prefix=prefix,
    )
    manager = StreamLeaseManager(redis=client, config=config)
    lease = await manager.acquire("user_outage")

    cur_master = await sentinel.discover_master(service_name)
    replica_addr = await get_replica_address(sentinel_cluster["sentinel_hosts"], service_name)

    conn_master = aioredis.from_url(f"redis://{cur_master[0]}:{cur_master[1]}", socket_timeout=0.5)
    conn_replica = aioredis.from_url(
        f"redis://{replica_addr[0]}:{replica_addr[1]}", socket_timeout=0.5
    )

    async def pause_node(conn: aioredis.Redis):
        try:
            await conn.execute_command("DEBUG", "SLEEP", 3.0)
        except Exception:
            pass
        finally:
            await conn.aclose()

    # Induce total cluster outage by pausing both nodes for 3.0s;
    # verify stream owner task is cancelled with StreamLeaseLost once lease TTL (1.2s) expires
    try:
        with pytest.raises(StreamLeaseLost):
            async with lease:
                asyncio.create_task(pause_node(conn_master))
                asyncio.create_task(pause_node(conn_replica))
                await asyncio.sleep(3.5)
    finally:
        # Settle topology: wait for paused nodes to wake up and rejoin
        await asyncio.sleep(3.5)
        await manager.close()
        await client.aclose()


@pytest.mark.asyncio
async def test_sentinel_mock_outage_exceeding_lease_ttl_terminates_stream(sentinel_cluster):
    """
    Unit-level simulation of backend outage: verifies StreamLeaseLost cancellation
    algorithm when network errors persist past lease TTL.
    """
    sentinel = sentinel_cluster["sentinel"]
    service_name = sentinel_cluster["service_name"]

    await wait_for_writable_master(sentinel, service_name)
    client = sentinel.master_for(service_name, socket_timeout=2.0)
    await client.ping()
    prefix = f"sentinel_mock_outage_{uuid4().hex[:8]}"

    config = LeaseConfig(
        lease_seconds=0.8,
        max_per_user=1,
        max_global=5,
        key_prefix=prefix,
    )
    manager = StreamLeaseManager(redis=client, config=config)
    lease = await manager.acquire("user_timeout")

    with pytest.raises(StreamLeaseLost):
        async with lease:
            manager.redis.eval = AsyncMock(side_effect=ConnectionError("Master unreachable"))
            await asyncio.sleep(1.5)

    await manager.close()
    await client.aclose()
