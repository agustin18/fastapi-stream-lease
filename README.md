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
from fastapi import Depends, FastAPI, Request
from fastapi.responses import StreamingResponse
import redis.asyncio as redis

from fastapi_stream_lease import (
    LeaseConfig,
    StreamLeaseManager,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)

app = FastAPI()
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
| **Event Loop Stalled / Process Paused > TTL** | Redis lease expires while local worker is stalled (e.g. extreme GC pause, VM suspension, CPU starvation). When execution resumes, the next renewal detects expiration and terminates the stream immediately. | **Eventual Safety** | Slots are preserved in Redis, but a local worker stalled past TTL cannot observe cancellation until its event loop resumes execution. |
| **Redis Outage on Acquire (`fail_open=False`)** | Immediate fail-closed rejection raising `StreamLeaseUnavailable` (`HTTP 503 Service Unavailable`). | **Strict Safety** | Limits strictly enforced; incoming streams rejected until Redis is reachable. |
| **Redis Outage on Acquire (`fail_open=True`)** | Grants an uncoordinated in-memory fallback lease using worker monotonic clock (`time.monotonic()`). | **Graceful Degradation** | Fallback acquisitions are intentionally unthrottled across and *within* worker processes during outage; fallback leases do not retroactively register upon Redis recovery. |
| **Worker Clock Drift** | All lease evaluations and expiration purges use `redis.call('TIME')`. | **Absolute** | Worker system clock or NTP skew cannot cause premature expiration or lingering leases. |
| **Slow Observability / Metric Hooks** | All lifecycle hooks (`on_acquired`, `on_released`, `on_lost`, `on_rejected`, `on_backend_error`) run out-of-band via background tasks and threadpool offloading (`asyncio.to_thread` for sync callables). | **Strong Guarantee** | Slow APM/Datadog/StatsD calls cannot delay stream cancellation, block acquire returns, or consume renewal retry windows. |
| **Redis Sentinel Master Failover** | `READONLY` transitions during replica write are retried. Asynchronous Redis replication can lose recently acknowledged writes if a master fails before syncing; un-replicated leases are detected as lost on next renewal and cancelled cleanly. | **High Availability** | Seamlessly rides out master elections shorter than remaining `lease_seconds`. Divergent writes are terminated rather than resurrected. |
| **Redis Cluster Multi-Key Coordination** | Keys share hash tag `{prefix}` (`{prefix}:user:...` and `{prefix}:global`), guaranteeing placement on the same hash slot for atomic Lua execution. | **Atomic Lua Execution** | Atomically validates both per-user and global capacity in a single Redis round-trip without `CROSSSLOT` errors. Coordinated keys share one cluster slot, which can become a hot slot at extreme throughput. |

### Fail-Open Fallback Lease Lifecycle

When `fail_open=True` is enabled in `LeaseConfig`, the manager grants fallback leases during Redis outages to maintain service availability:
- **Completely Unthrottled Fallback:** While Redis is unavailable, fallback acquisitions are intentionally unthrottled; neither per-user nor global limits are enforced, even within a single worker process (no local in-memory semaphore is maintained).
- **Local Time Basis:** Fallback leases use `time.monotonic()` locally and are isolated to the executing worker process.
- **No Retroactive Registration:** Active fallback leases do not attempt retroactive registration into Redis when connectivity returns. They complete locally and release normally.

## Production and Operational Guide

- **Redis Client Timeouts:** Always configure explicit timeouts on your Redis client (e.g. `socket_timeout=1.0, socket_connect_timeout=1.0`). Without timeouts, an unreachable Redis instance can block asyncio event loop execution indefinitely.
- **Fail-Open vs. Fail-Closed Strategy:**
  - `fail_open=False` (Default): Raises `StreamLeaseUnavailable` (HTTP 503) when Redis is unreachable. Enforces limits during transient network partitions at the cost of rejecting requests when the backend is down. (Note: asynchronous Redis replication or master failover can still lose recently acknowledged writes if a master fails before syncing to its replica).
  - `fail_open=True`: Automatically grants in-memory fallback leases when Redis encounters network or timeout errors. Keeps streaming endpoints open during outages, with the operational trade-off that limits are not coordinated across workers until Redis recovers. Authentication, authorization, and script syntax errors never fail open.
- **Definitive Revocation vs. Network Errors:** If Redis explicitly reports that a lease is missing or expired (`renew()` returning 0) or encounters an unhandled execution error, `wrap()` and `lease()` cancel the stream immediately to prevent exceeding limits. Transient network disconnects trigger rapid retries until the monotonic lease deadline is reached.
- **Observability and Lifecycle Hooks:**
  `LeaseConfig` provides zero-dependency callback hooks (supporting both sync and async callables) to plug directly into Prometheus, Datadog, StatsD, or Sentry:
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
  `on_released` receives the release reason (`completed`, `cancelled`, `error`, `lost`, or `manual`), guaranteeing that active connection gauges decrement accurately across all stream terminations.
  All lifecycle hooks are managed by an internal bounded FIFO `HookDispatcher`. On application shutdown, flush all pending telemetry events gracefully:
  ```python
  await manager.drain(timeout=5.0)
  ```
- **Cluster Configuration Drift Detection:**
  In distributed environments with multiple worker processes or Kubernetes pods, ensure all instances share identical limit configurations:
  ```python
  # Logs a warning on drift or raises ConfigurationMismatchError if strict=True
  await manager.verify_cluster_config(strict=True)
  ```
- **Redis Failover & Sentinel Support:** Automatically classifies `ReadOnlyError` (thrown when hitting a replica during master election) as a transient condition, enabling adaptive renewal retries to ride out failovers without dropping active streams.
- **Redis Cluster:** All keys use Redis hash tags (`{prefix}:user:...` and `{prefix}:global`), guaranteeing user and global sorted sets reside on the same hash slot for multi-key atomic Lua operations. As with any multi-key Lua coordination, evaluate slot contention and failover behavior under your specific topology.
- **Reproducible Concurrency Benchmarks:**
  Run throughput and latency benchmarks against your local Redis instance:
  ```bash
  docker compose run --rm backend uv run python benchmarks/bench_lease_concurrency.py --count 1000 --concurrency 50
  ```
- **Docker Compose Testing Stack:** Run the test suite and Redis dependency cleanly across Linux, macOS, and Windows:
  ```bash
  docker compose run --rm backend uv run pytest
  ```

## Examples directory

- [`examples/sse_demo.py`](examples/sse_demo.py): Server-Sent Events with API key authentication.
- [`examples/websocket_demo.py`](examples/websocket_demo.py): WebSocket streams with standard close codes (`1008 Policy Violation`, `1013 Try Again Later`).
- [`examples/openai_streaming_demo.py`](examples/openai_streaming_demo.py): LLM token streaming with official `openai` SDK and simulated fallback.
- [`examples/prometheus_metrics_demo.py`](examples/prometheus_metrics_demo.py): Prometheus metrics integration with zero-dependency lifecycle hooks.
- [`examples/sse_client_resilient.py`](examples/sse_client_resilient.py): Resilient Python SSE client with exponential backoff & jitter.

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) for the local workflow and [SECURITY.md](SECURITY.md) for private vulnerability reports. Changes are proposed through pull requests and merged by the maintainer after CI passes. The package is licensed under [MIT](LICENSE).

CI checks formatting, lint, types, Redis 5, 7, and 8 behavior, package build, and a minimum of 95% combined line and branch coverage. This is a small beta project; reports from real deployments are especially helpful for documenting operational limits.
