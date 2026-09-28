# fastapi-stream-lease

[![CI](https://github.com/agustin18/fastapi-stream-lease/actions/workflows/ci.yml/badge.svg)](https://github.com/agustin18/fastapi-stream-lease/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/fastapi-stream-lease.svg)](https://pypi.org/project/fastapi-stream-lease/)
[![Python](https://img.shields.io/pypi/pyversions/fastapi-stream-lease.svg)](https://pypi.org/project/fastapi-stream-lease/)

Limit **simultaneously active** SSE, LLM token streams, and WebSocket sessions across FastAPI/Starlette workers with Redis. Set a per-user limit, a global limit, or both. An extra connection receives HTTP 429 before streaming begins.

Request rate limiters answer “how many requests arrived this minute?” This package answers “how many streams are open right now?” It is useful when a connection can remain open for seconds or minutes. For a general-purpose distributed semaphore, consider [py-redis-limiters](https://pypi.org/project/redis-limiters/) or [py-redis-semaphore](https://pypi.org/project/py-redis-semaphore/).

## Install

Requires Python 3.10+ and Redis 5.0+.

With Redis 5 and redis-py 8+, construct your client with `redis.from_url(url, protocol=2)`: redis-py 8 defaults to RESP3, which Redis 5 does not support. See the [redis-py protocol documentation](https://github.com/redis/redis-py#resp3-support).

```bash
pip install 'fastapi-stream-lease[fastapi]'
```

The `fastapi` extra supplies FastAPI for the HTTP helpers; the core package only requires `redis`.

## Try it locally

The [runnable SSE example](examples/sse_demo.py) accepts one API key from `STREAM_DEMO_TOKEN` and uses it as a demo identity. Start Redis, clone this repository, then run:

```bash
git clone https://github.com/agustin18/fastapi-stream-lease.git
cd fastapi-stream-lease
pip install -e '.[fastapi]' uvicorn
export STREAM_DEMO_TOKEN=local-secret
export REDIS_URL=redis://localhost:6379/0
uvicorn examples.sse_demo:app
```

In another terminal, open two streams, then try a third with the same key:

```bash
curl -N -H 'X-API-Key: local-secret' http://localhost:8000/stream
```

The first two requests stream events; the third receives `429` with a `Retry-After` header until a slot is freed. Replace the demo API key with your application's authenticated user or account ID before production use. Never use an unverified request parameter as the user ID.

## FastAPI integration

```python
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.responses import StreamingResponse
import redis.asyncio as redis

from fastapi_stream_lease import (
    LeaseConfig,
    StreamLeaseManager,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)

# Set explicit timeouts so slow Redis calls do not block worker threads
redis_client = redis.from_url(
    "redis://localhost:6379",
    socket_timeout=1.0,
    socket_connect_timeout=1.0,
)
manager = StreamLeaseManager(
    redis_client,
    LeaseConfig(max_per_user=2, max_global=500, lease_seconds=30),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Verify cluster configuration consistency on worker startup (fail-fast on mismatch or outage)
    await manager.verify_cluster_config(strict=True)
    yield
    # Clean teardown: drain telemetry callbacks and close resources
    await manager.close(drain=True, timeout=5.0)
    await redis_client.aclose()


app = FastAPI(lifespan=lifespan)


async def authenticated_user_id() -> str:
    # Illustrative placeholder: replace with your auth dependency (e.g. API key or JWT sub).
    # For fully runnable code, see examples/sse_demo.py and examples/websocket_demo.py.
    return "user-123"


@app.exception_handler(StreamLeaseRejected)
async def rejected(request: Request, exc: StreamLeaseRejected):
    return exc.as_response()


@app.exception_handler(StreamLeaseUnavailable)
async def unavailable(request: Request, exc: StreamLeaseUnavailable):
    return exc.as_response()


@app.get("/stream")
async def stream(user_id: str = Depends(authenticated_user_id)):
    async def events():
        yield "data: token or event\n\n"

    # 1-Line Protected StreamingResponse (auto-acquires, wraps, and cleans up on errors)
    return await manager.stream(user_id, events())

    # Or manually acquire and convert:
    # lease = await manager.acquire(user_id)
    # return lease.as_streaming_response(events())
```

Close the Redis client in your application's lifespan shutdown handler. Reuse the same `StreamLeaseManager` and `key_prefix` across workers that share limits. `max_per_user=0` or `max_global=0` disables that limit; the global count is unavailable when global tracking is disabled.

For WebSockets, keep the context open for the whole session:

```python
async with manager.lease(user_id) as lease:
    while True:
        message = await websocket.receive_text()
        await websocket.send_text(message)
```

The manager context and `async with lease` both renew while open. Handle normal WebSocket disconnects in your route as usual. If renewal fails or the lease expires, `StreamLeaseLost` interrupts the stream or context. Catch it at the application boundary if you want to record a metric or send an application-specific WebSocket close code. `wrap(auto_renew=False)` disables automatic renewal; use it only if you renew the lease yourself.

## Behavior and limits

- Acquisition, renewal, expiration cleanup, and release use atomic Redis Lua scripts. The keys share a Redis Cluster hash tag, so a user and global limit can be checked in one script.
- Every lease expires after `lease_seconds` without a successful renewal. `wrap()` and `manager.lease()` renew every half interval by default. Transient Redis connection errors trigger fast retries across the remaining lease TTL (Adaptive Grace Period), preventing temporary hiccups from dropping active streams.
- If Redis is unavailable on initial acquisition, `StreamLeaseUnavailable` (HTTP 503) is raised by default (`fail_open=False`). Set `fail_open=True` in `LeaseConfig` if your application prefers allowing streams during Redis outages (graceful degradation).
- Normal completion or cancellation attempts immediate release. If Redis is unavailable during release, the lease is removed after expiration; cleanup of the key itself uses a longer TTL. An async iterator abandoned without being closed may also hold its slot until expiration. Use `contextlib.aclosing()` if your own consumer stops iteration early.
- `get_active_count(user_id)` counts active leases for one identity; `get_active_count()` counts globally when `max_global` is enabled. Neither is a historical usage metric.
- All workers sharing limits must use the same key prefix and compatible limit settings. Lease expiration is measured by Redis, avoiding clock differences among application workers.

## Distributed Guarantees & Failure Model

| Failure Mode | System Behavior | Guarantee Level | Operational Trade-off |
|---|---|---|---|
| **Worker Process Crash (`SIGKILL`)** | Lease expires in Redis after `lease_seconds` via Redis `TIME` score. Subsequent acquisitions automatically sweep expired members. | **Strong (Self-healing within $TTL$)** | Slot remains held until `lease_seconds` elapses; no zombie leases persist permanently. |
| **Transient Redis Disconnect (Renewal)** | Renewal worker enters Adaptive Grace Period, retrying across the remaining TTL. If connection recovers before deadline, stream proceeds normally. | **High Availability** | If outage exceeds remaining TTL, lease is revoked, cancelling stream immediately. |
| **Event Loop Stalled / Process Paused > TTL** | Redis lease expires while local worker is stalled (e.g. extreme GC pause, VM suspension, CPU starvation). When execution resumes, the next renewal detects expiration and terminates the stream immediately with `StreamLeaseLost`. | **Eventual Safety** | Slots self-clean in Redis, but a local worker stalled past TTL cannot observe cancellation until its event loop resumes execution. |
| **Redis Outage on Acquire (`fail_open=False`)** | Immediate fail-closed rejection raising `StreamLeaseUnavailable` (`HTTP 503 Service Unavailable`). | **Strict Safety** | Limits strictly enforced; incoming streams rejected until Redis is reachable. |
| **Redis Outage on Acquire (`fail_open=True`)** | Grants an uncoordinated in-memory fallback lease using worker monotonic clock (`time.monotonic()`). | **Graceful Degradation** | Fallback acquisitions are intentionally unthrottled across and *within* worker processes during outage; fallback leases do not retroactively register upon Redis recovery. |
| **Worker Clock Drift** | All lease evaluations and expiration purges use `redis.call('TIME')`. | **Absolute** | Worker system clock or NTP skew cannot cause premature expiration or lingering leases. |
| **Slow Observability / Metric Hooks** | All lifecycle hooks (`on_acquired`, `on_released`, `on_lost`, `on_rejected`, `on_backend_error`) run out-of-band via an internal bounded FIFO queue and threadpool (`asyncio.to_thread` for sync callables). | **Strong Guarantee** | Slow APM/Datadog/StatsD calls cannot delay stream cancellation, block acquire returns, or consume renewal retry windows. |
| **Redis Sentinel Master Failover** | `READONLY` transitions during replica write are retried. Asynchronous Redis replication can lose recently acknowledged writes if a master fails before syncing; un-replicated leases are detected as lost on next renewal and cancelled cleanly. In split-brain network partitions, divergent masters can temporarily allow concurrent leases across partitions until the partition heals or `min-replicas-to-write` blocks writes on the isolated master. | **High Availability** | Seamlessly rides out master elections shorter than remaining `lease_seconds` when the lease state is present on the promoted replica. Divergent writes are terminated rather than resurrected. `min-replicas-to-write` can bound or reduce the stale-master write window during network partitions, at the cost of write availability. Redis Sentinel remains eventually consistent and cannot guarantee a strict cluster-wide concurrency bound across all network partitions. |
| **Redis Cluster Multi-Key Coordination** | Keys share hash tag `{prefix}` (`{prefix}:user:...` and `{prefix}:global`), guaranteeing placement on the same hash slot for atomic Lua execution. | **Atomic Lua Execution** | Atomically validates both per-user and global capacity in a single Redis round-trip without `CROSSSLOT` errors. Coordinated keys share one cluster slot, which can become a hot slot at extreme throughput. |
| **Redis Cluster Slot Failover & Replication** | Replicated leases and configuration keys survive slot promotion when a replica is elected. Acknowledged writes (leases or `{prefix}:config`) that have not reached the promoted replica before master failure may be lost. Un-replicated leases are detected as lost on next renewal and cleanly cancelled. Redis Cluster uses asynchronous replication and may lose recently acknowledged writes during failovers. Applications requiring stronger acknowledgment semantics can evaluate Redis replication controls separately; these do not provide CP guarantees. | **High Availability** | Seamlessly transitions across cluster node failover when lease state reached the promoted replica. Streams with un-replicated leases are terminated on next renewal rather than resurrected. Different concurrency domains (different `{prefix}`) distribute across cluster shards, but all keys for a single prefix reside on one hash slot. |

### Fail-Open Fallback Lease Lifecycle

When `fail_open=True` is enabled in `LeaseConfig`, the manager grants fallback leases during Redis outages to maintain service availability:
- **Completely Unthrottled Fallback:** While Redis is unavailable, fallback acquisitions are intentionally unthrottled; neither per-user nor global limits are enforced, even within a single worker process (no local in-memory semaphore is maintained).
- **Local Time Basis:** Fallback leases use `time.monotonic()` locally and are isolated to the executing worker process.
- **No Retroactive Registration:** Active fallback leases do not attempt retroactive registration into Redis when connectivity returns. They complete locally and release normally.

## Backend Failure Policy & Worker-Local Circuit Breaker

During sustained Redis outages, network partitions, or slow master failovers, repeatedly sending requests to an unreachable backend wastes event-loop time, fills connection pools, and increases application latency. `fastapi-stream-lease` provides an opt-in, worker-local circuit breaker and failure degradation policy to protect your event loops.

```python
from fastapi_stream_lease import (
    BackendFailurePolicy,
    CircuitBreakerConfig,
    FallbackMode,
    LeaseConfig,
    StreamLeaseManager,
)

policy = BackendFailurePolicy(
    fallback_mode=FallbackMode.FAIL_CLOSED,  # Or FallbackMode.FAIL_OPEN
    circuit_breaker=CircuitBreakerConfig(
        failure_threshold=5,  # Consecutive transient errors before tripping OPEN
        recovery_timeout=10.0,  # Base cooldown seconds before HALF_OPEN testing
        jitter=1.0,  # Random uniform jitter added to recovery window
        half_open_max_probes=1,  # Max concurrent probe requests allowed in HALF_OPEN
    ),
)

config = LeaseConfig(
    max_per_user=2,
    max_global=100,
    lease_seconds=30.0,
    failure_policy=policy,
)
manager = StreamLeaseManager(redis=client, config=config)
```

### Circuit Breaker States & Transitions

1. **`CLOSED` (Normal Operation):** All requests proceed to Redis. Transient network/timeout errors increment consecutive failure counters. Non-transient errors (such as authentication or configuration errors) and local connection pool exhaustion (`MaxConnectionsError`) are ignored.
2. **`OPEN` (Fast-Failing):** When consecutive transient errors reach `failure_threshold`, the circuit breaker trips to `OPEN`. For the duration of `recovery_timeout + uniform(0, jitter)`:
   - If `fallback_mode=FAIL_CLOSED`: `acquire()` immediately raises `StreamLeaseUnavailable` (`HTTP 503 Service Unavailable`) without attempting any network I/O.
   - If `fallback_mode=FAIL_OPEN`: `acquire()` immediately grants an in-memory fallback lease without touching Redis.
3. **`HALF_OPEN` (Controlled Probing):** When the recovery cooldown elapses, the breaker admits up to `half_open_max_probes` concurrent trial requests to test backend health:
   - If a probe successfully communicates with Redis, the backend is proven reachable: the circuit immediately heals back to `CLOSED`, resetting all failure counters. (Note: rate-limiting outcomes such as HTTP 429 `StreamLeaseRejected` still prove the backend is healthy and heal the circuit).
   - If a probe encounters a transient error, the circuit immediately trips back to `OPEN` with a fresh recovery timeout and jitter.
   - If a probe is cancelled (`asyncio.CancelledError`) or encounters an unhandled non-transient error, RAII probe tracking automatically releases the probe slot so subsequent requests can test recovery without getting stuck.

### The Golden Asymmetry: Renewals Never Block

Acquisition (`acquire()`) and renewal (`renew()`) have asymmetric failure costs:
- **`acquire()`** creates new concurrency. Fast-failing an acquire protects the backend from additional load during an outage.
- **`renew()`** protects existing, active streams. If an active stream misses renewals for `lease_seconds`, it is terminated.

Therefore, **an `OPEN` circuit breaker never blocks renewal attempts**. Active streams continue attempting renewals during their remaining TTL (Adaptive Grace Period). If Redis recovers before lease TTL expires, the first successful renewal immediately heals the circuit breaker back to `CLOSED`, allowing subsequent acquisitions to resume seamlessly for that manager instance.

### Architecture & Anti-Herd Mitigations

- **Worker-Local Semantics:** State is maintained in-memory per `StreamLeaseManager` instance. There is zero distributed coordination in Redis to manage circuit breaker state, eliminating circular dependencies (we never ask Redis whether Redis is alive).
- **Probabilistic Herd Mitigation:** `half_open_max_probes` limits probe concurrency per `StreamLeaseManager` instance. Random recovery jitter (`recovery_timeout + uniform(0, jitter)`) statistically desynchronizes probe attempts across multi-worker clusters, preventing thundering herd spikes when Redis recovers.
- **Cluster Fingerprint Compatibility:** Circuit breaker settings are worker-local operational tuning parameters. They are not part of the shared Redis canonical configuration fingerprint (`{prefix}:config`), allowing rolling tuning changes across workers without configuration mismatch errors.
- **Operational Health Inspection:** Read `manager.circuit_state` (`CircuitState.CLOSED`, `CircuitState.OPEN`, `CircuitState.HALF_OPEN`, or `None` if disabled) to inspect breaker state in health-check endpoints or custom monitors. `CircuitState` is exported from the package root:
  ```python
  from fastapi_stream_lease import CircuitState

  if manager.circuit_state == CircuitState.OPEN:
      logger.warning("Redis coordination breaker is currently OPEN")
  ```

## Production and Operational Guide

- **Redis Client Timeouts:** Always configure explicit timeouts on your Redis client (e.g. `socket_timeout=1.0, socket_connect_timeout=1.0`). Without timeouts, an unreachable Redis instance can block asyncio event loop execution indefinitely.
- **Fail-Open vs. Fail-Closed Strategy:**
  - `fail_open=False` (Default): Raises `StreamLeaseUnavailable` (HTTP 503) when Redis is unreachable. Enforces limits during transient network partitions at the cost of rejecting requests when the backend is down. (Note: asynchronous Redis replication or master failover can still lose recently acknowledged writes if a master fails before syncing to its replica).
  - `fail_open=True`: Automatically grants in-memory fallback leases when Redis encounters network or timeout errors. Keeps streaming endpoints open during outages, with the operational trade-off that limits are not coordinated across workers until Redis recovers. Authentication, authorization, and script syntax errors never fail open.
- **Definitive Revocation vs. Network Errors:** If Redis explicitly reports that a lease is missing or expired (`renew()` returning 0) or encounters an unhandled execution error, `wrap()` and `lease()` cancel the stream immediately to prevent exceeding limits. Transient network disconnects trigger rapid retries until the monotonic lease deadline is reached.
- **Pluggable Observability Adapters (Prometheus & OpenTelemetry):**
  Install the optional dependencies:
  ```bash
  pip install fastapi-stream-lease[prometheus]  # Prometheus adapter
  pip install fastapi-stream-lease[otel]        # OpenTelemetry adapter
  pip install fastapi-stream-lease[all]         # All optional extras
  ```
  Wire the adapter into `StreamLeaseManager`:
  ```python
  from fastapi_stream_lease.observability.prometheus import PrometheusMetrics

  metrics = PrometheusMetrics()
  manager = StreamLeaseManager(redis=client, config=config, telemetry=metrics)
  ```
  Or for OpenTelemetry:
  ```python
  from fastapi_stream_lease.observability.otel import OpenTelemetryMetrics

  metrics = OpenTelemetryMetrics()
  manager = StreamLeaseManager(redis=client, config=config, telemetry=metrics)
  ```
  Exposes standardized metrics (`operations_total`, `operation_duration_seconds`, `lost_total`, `backend_errors_total`, `fallback_total`, `hook_dropped_total`, `hook_queue_depth`, `circuit_state`, `short_circuited_total`) with bounded label cardinality.
  - **Multi-Manager Isolation (`telemetry_scope`):** When multiple managers share a single Prometheus registry or OpenTelemetry meter, configure distinct static `telemetry_scope` values on `LeaseConfig` (e.g. `LeaseConfig(telemetry_scope="llm_heavy")`). Circuit breaker metrics (`circuit_state`, `short_circuited_total`) are labeled with `scope` (defaulting to `key_prefix`). `telemetry_scope` must be static and low-cardinality; do not use dynamic user IDs or request identifiers.
  - **Upstream Stream Teardown & Ownership (`close_source=True`):** When wrapping streams via `manager.stream()`, `lease.as_streaming_response()`, or `lease.wrap()`, `fastapi-stream-lease` assumes ownership of closing the underlying upstream stream upon client disconnect, completion, or error, releasing active Redis leases deterministically without ghost leases. To retain caller ownership and prevent closing the source, pass `close_source=False`. Synchronous close methods are offloaded to worker threads; `upstream_cleanup_timeout` (default 2.0s) bounds how long teardown waits without blocking the event loop.

- **Zero-Dependency Lifecycle Hooks (Custom Telemetry):**
  Alternatively, `LeaseConfig` provides zero-dependency callback hooks (supporting both sync and async callables) to plug directly into Datadog, StatsD, or Sentry:
  ```python
  config = LeaseConfig(
      on_acquired=lambda lease: PROMETHEUS_ACTIVE.inc(),
      on_released=lambda lease, reason: PROMETHEUS_ACTIVE.dec(),
      on_rejected=lambda uid, reason: PROMETHEUS_REJECTED.labels(reason=reason).inc(),
      on_lost=lambda lease, reason: PROMETHEUS_LOST.labels(reason=reason).inc(),
      on_backend_error=lambda exc: PROMETHEUS_BACKEND_ERRORS.inc(),
      hook_queue_size=1024,
  )
  ```
  - **Best-Effort Delivery:** Lifecycle callbacks are designed strictly for out-of-band telemetry and monitoring. If callbacks execute slower than event arrival and fill `hook_queue_size`, new events are dropped with a rate-limited log warning to preserve event-loop responsiveness. **Never rely on lifecycle hooks for financial billing, credit deduction, or security-critical audits.**
  - `on_released` receives a deterministic termination reason when delivered (`completed`, `cancelled`, `error`, `lost`, or `manual`). Lifecycle hooks remain best-effort telemetry signals and should not be treated as an authoritative source of active lease state.
  - **Async vs Sync Hook Execution:** Synchronous hooks are offloaded to an internal thread pool via `asyncio.to_thread` to protect the event loop. Asynchronous hooks execute directly on the event loop and must remain cooperative (do not perform blocking synchronous calls like `time.sleep()` or blocking I/O inside an async callback). If a synchronous callback is already executing in the thread pool, `manager.close(timeout=...)` will wait up to the timeout, but Python cannot forcibly terminate an active OS thread.
  - Dispatcher telemetry properties for operational monitoring: `manager.dispatcher.queued_count`, `manager.dispatcher.dropped_count`, `manager.dispatcher.error_count`, and `manager.dispatcher.queue_depth`.
  - On application shutdown, flush all pending telemetry events gracefully:
  ```python
  await manager.close(drain=True, timeout=5.0)
  ```
- **Cluster Configuration Drift Detection:**
  In distributed environments with multiple worker processes or Kubernetes pods, ensure all instances share identical limit configurations. Canonical configuration is registered atomically via `SET ... NX` (persistent key `{prefix}:config` with no TTL expiration to prevent split-brain during rolling deploys):
  ```python
  # Logs a warning on drift or raises ConfigurationMismatchError if strict=True.
  # Retries transient errors during Sentinel failovers; raises StreamLeaseUnavailable
  # on prolonged outages to trigger fail-fast Kubernetes CrashLoopBackOff.
  await manager.verify_cluster_config(strict=True)
  ```
  During transient Redis reconnects or Sentinel master elections (which typically resolve in 1–3 seconds), `verify_cluster_config()` retries across a short bounded window (`retry_attempts=3`, `retry_delay=0.1s` by default). If the coordination backend remains unavailable past all retries, `strict=True` raises `StreamLeaseUnavailable` to trigger fail-fast container exit so Kubernetes restarts the container or does not route traffic to unverified pods.
- **Redis Connection Pool Sizing (High Concurrency & Fan-Out):**
  In modern `redis-py` (v8.1+), the default asynchronous `ConnectionPool` caps capacity to `max_connections=100` if not explicitly specified. In high-concurrency streaming services (handling hundreds of concurrent active SSE or WebSocket streams with periodic background auto-renewals and simultaneous client disconnections), ensure your Redis client connection pool is sized adequately to prevent client-side `MaxConnectionsError`:
  ```python
  import redis.asyncio as redis

  # Size max_connections to accommodate peak concurrent streams and renewals
  pool = redis.ConnectionPool.from_url("redis://localhost:6379/0", max_connections=500)
  client = redis.Redis.from_pool(pool)
  ```

### Changing Cluster Configuration Safely

Because `{prefix}:config` is persistent (stored with `SET ... NX` without TTL expiration) to prevent transient split-brain during normal rolling deployments, changing limit settings (e.g. updating `max_global`, `max_per_user`, `lease_seconds`, or `fail_open`) across an active cluster requires an intentional operational migration:

1. **Option A: Prefix Versioning (Recommended for Zero-Downtime Blue/Green):**
   Update your configuration's `key_prefix` (e.g. from `myapp:streams:v1` to `myapp:streams:v2`). New worker pods establish and register their new canonical configuration immediately under the new prefix, while old pods gracefully drain active leases under the old prefix.
   > [!WARNING]
   > During Blue/Green overlap, old and new prefixes represent independent concurrency domains in Redis. Per-user and global limits are not shared across distinct prefixes, so aggregate concurrency may temporarily exceed either deployment's configured limit until old pods finish draining. Use Option B if a single strict cluster-wide limit must be maintained throughout migration.
2. **Option B: Configuration Reset for In-Place Rolling Updates:**
   If you need to keep the exact same prefix:
   - Drain or stop existing worker instances.
   - Wait for remaining active leases to naturally expire (or revoke them).
   - Delete the canonical configuration key in Redis:
     ```bash
     redis-cli DEL "{my_prefix}:config"
     ```
   - Start the updated worker instances. The first new pod will atomically register the updated configuration fingerprint with `SET ... NX`, and subsequent pods will verify compatibility against it.
- **Redis Failover & Sentinel Support:** Automatically classifies `ReadOnlyError` (thrown when hitting a replica during master election) and `ConnectionError` as transient conditions, enabling adaptive renewal retries to ride out failovers without dropping active streams. Note that the Sentinel configuration in `docker-compose.sentinel.yml` uses aggressive test timings (`down-after-milliseconds 1000`, `failover-timeout 5000`); production environments should use standard recommended operational timeouts (`down-after-milliseconds` 5000–30000ms, `failover-timeout` 60000–180000ms).
- **Redis Cluster Support:** All keys use Redis hash tags (`{prefix}:user:...` and `{prefix}:global`), guaranteeing user and global sorted sets reside on the same hash slot for atomic multi-key Lua operations without `CROSSSLOT` errors. Validated under real 6-node sharded topologies (`docker-compose.cluster.yml`) with automated slot failover, `MOVED` redirection recovery, and drift verification (`tests/test_cluster.py`).
- **Reproducible Concurrency Benchmarks:**
  Run throughput and latency benchmarks against your local Redis instance with pre-warmed connection pool and multi-run statistics:
  ```bash
  docker compose run --rm backend uv run python benchmarks/bench_lease_concurrency.py --count 1000 --concurrency 50 --runs 3
  ```
- **Docker Compose Testing Stack:** Run the test suite and distributed coordination topologies cleanly:
  ```bash
  # Standard unit and integration test suite:
  docker compose run --rm backend uv run pytest

  # Redis Sentinel failover & chaos test suite:
  docker compose -f docker-compose.sentinel.yml up -d --wait
  docker compose -f docker-compose.sentinel.yml run --rm backend uv run pytest -o addopts='' tests/test_sentinel.py -vv -s

  # Redis Cluster 6-node multi-shard test suite:
  docker compose -f docker-compose.cluster.yml up -d --wait
  docker compose -f docker-compose.cluster.yml run --rm backend uv run --all-extras pytest -o addopts='' tests/test_cluster.py -vv -s
  ```

## Examples directory

- [`examples/sse_demo.py`](examples/sse_demo.py): Server-Sent Events with API key authentication.
- [`examples/websocket_demo.py`](examples/websocket_demo.py): WebSocket streams with standard close codes (`1008 Policy Violation`, `1013 Try Again Later`).
- [`examples/openai_streaming_demo.py`](examples/openai_streaming_demo.py): LLM token streaming with official `openai` SDK and simulated fallback.
- [`examples/prometheus_metrics_demo.py`](examples/prometheus_metrics_demo.py): Prometheus metrics integration with zero-dependency lifecycle hooks.
- [`examples/sse_client_resilient.py`](examples/sse_client_resilient.py): Resilient Python SSE client with exponential backoff & jitter.

## Security Best Practices

1. **Authenticated Opaque Principal IDs:**
   - Always derive `user_id` from a verified authenticated principal (e.g. session user ID, UUID, database primary key).
   - **Never pass raw, unverified client input** (such as path parameters or query strings) directly as `user_id`. Doing so allows malicious clients to create arbitrary keys in Redis and bypass concurrency bounds.
   - Avoid using sensitive PII (emails, full names) or raw bearer tokens as `user_id`, as these appear in Redis keys (`{prefix}:user:{user_id}`) and operational debug logs.
2. **Dependency Floors vs. Production Security:**
   - The test matrix validates compatibility down to minimum floors (`fastapi>=0.100.0`, `starlette>=0.27.0`, `redis>=5.0.0`).
   - These are **compatibility floors**, not security recommendations. Production deployments should always maintain up-to-date versions of FastAPI, Starlette, and Redis to benefit from upstream security patches and CVE remediations.
3. **Automated Vulnerability Scanning:**
   - All commits and pull requests are audited in CI via `pip-audit` to detect known vulnerabilities across dependencies.

## Stability & Versioning Policy

- **Semantic Versioning (SemVer):** `fastapi-stream-lease` strictly follows SemVer.
- **Public API Contract:**
  - Symbols exported from the top-level package (`fastapi_stream_lease`) constitute the public API: `StreamLeaseManager`, `StreamLease`, `LeaseConfig`, circuit breaker configurations (`CircuitBreakerConfig`, `CircuitState`, `BackendFailurePolicy`, `FallbackMode`), and exceptions (`StreamLeaseError`, etc.).
  - `ProtectedStreamingResponse` is an integration implementation detail of `manager.stream(...)` and `lease.as_streaming_response(...)`; while accessible for custom subclassing, standard applications should rely on the high-level manager and lease methods.
  - Internal modules, private helper methods prefixed with `_`, and internal Lua script layouts are not covered by stability guarantees and may change between minor releases.
- **Observability Contract:**
  - Exported metric names (`fastapi_stream_lease_*`) and dimensional label keys (`scope`, `operation`, `state`) are treated as breaking-change contracts and will remain consistent across major versions.
- **Path to v1.0.0:**
  - Version `0.4.0` represents feature completeness. The subsequent `v1.0.0` release is a formal declaration of long-term API stability and freeze following external community bake time.

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) for the local workflow and [SECURITY.md](SECURITY.md) for private vulnerability reports. Changes are proposed through pull requests and merged by the maintainer after CI passes. The package is licensed under [MIT](LICENSE).

CI checks formatting, lint, strict static typing, dependency vulnerability scanning (pip-audit), Redis 5, 7, and 8 behavior, Redis Sentinel and 6-node Redis Cluster failover chaos validation, package build, and 100% combined statement and branch coverage. Reports and feedback from production deployments are welcome for continuously documenting operational limits.
