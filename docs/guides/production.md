# Production deployment

The lease engine is deliberately small: Redis is the source of truth, and each worker
keeps only local runtime state. The deployment details below are the ones that usually
matter when streams stay open behind a load balancer.

## One manager per worker process

Create the Redis client and `StreamLeaseManager` inside the application lifespan, or in a
server `post_fork` hook. Do not share a client created before a `fork()`:

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
import redis.asyncio as redis

from fastapi_stream_lease import LeaseConfig, StreamLeaseManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = redis.from_url(
        "redis://redis:6379/0",
        socket_timeout=1.0,
        socket_connect_timeout=1.0,
        max_connections=500,
    )
    manager = StreamLeaseManager(
        client,
        LeaseConfig(max_per_user=2, max_global=1000, lease_seconds=30.0),
    )
    await manager.verify_cluster_config(strict=True)
    app.state.manager = manager
    try:
        yield
    finally:
        await manager.close(drain=True, timeout=30.0)
        if hasattr(client, "aclose"):
            await client.aclose()
        else:
            await client.close()
```

If you use Gunicorn with `--preload`, the module-level app is imported before workers are
forked. Prefer creating the Redis client in the lifespan as shown above, or use a
`post_fork` hook. All workers that share a limit must use the same `key_prefix` and
compatible `LeaseConfig` values.

## Graceful shutdown

`manager.close(drain=True, timeout=...)` does three things:

1. Stops new acquisitions in the current worker.
2. Waits for active local streams to finish naturally.
3. Force-releases leases that are still active after the timeout.

Wire it into the lifespan shutdown path. In Kubernetes, allow enough time for endpoint
propagation before the process starts draining:

```yaml
spec:
  terminationGracePeriodSeconds: 60
  containers:
    - name: api
      lifecycle:
        preStop:
          exec:
            command: ["sh", "-c", "sleep 5"]
```

The `preStop` delay gives the Service time to stop routing new connections to the pod.
`manager.close()` then drains existing streams. If your streams can run longer than the
grace period, either increase `terminationGracePeriodSeconds` or cap stream duration in
your application. The forced-release path prevents ghost leases in Redis, but the process
may be killed before every upstream connection has closed.

## Nginx and ingress

SSE needs an unbuffered response and a read timeout longer than your longest stream:

```nginx
location /stream {
    proxy_pass http://api;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_cache off;
    proxy_read_timeout 3600s;
}
```

If you cannot disable buffering globally, return `X-Accel-Buffering: no` from the
streaming endpoint and let Nginx honor it. Disable response compression for
`text/event-stream`; compressing a long stream can delay chunks.

## Prometheus with multiple workers

`active_leases` and `hook_queue_depth` are worker-local in-memory gauges. They are not a
cluster-wide count. There are two common ways to expose them:

- **Scrape each worker separately.** Give each worker its own metrics port or use a
  sidecar per process. This preserves the per-worker meaning.
- **Use `prometheus_client` multiprocess mode.** Set `PROMETHEUS_MULTIPROC_DIR` before the
  process starts. The client exposes gauge series per process by default; aggregate them
  in PromQL:

```promql
# Active streams across the selected workers.
sum by (scope) (fastapi_stream_lease_active_leases)

# 1 if any worker reports HALF_OPEN, 2 if any reports OPEN.
max by (scope) (fastapi_stream_lease_circuit_state)
```

Do not confuse the gauge with the global Redis count. If you need the exact cluster-wide
active count, call `await manager.get_active_count()` (it reads Redis) from a dedicated
health or admin endpoint, not from a high-frequency liveness probe.

`manager.circuit_state` is useful for readiness and dashboards. It is worker-local and
reflects whether that worker is currently fast-failing acquisitions.

## Redis sizing and policy

- Set `socket_timeout` and `socket_connect_timeout` on the client. Without timeouts, a
  network stall can block an event loop.
- Size `max_connections` for peak concurrent streams plus renewals. In recent redis-py
  versions the default pool cap is 100 connections.
- Use `maxmemory-policy noeviction` for the lease database. Evicting active lease keys
  looks like a lost lease to the worker and can temporarily bypass limits.
- Persistence is optional: leases are ephemeral and self-heal through TTL. Sentinel or
  Cluster replication improves availability, but Redis replication is asynchronous.
- Use a dedicated Redis database or key prefix. Do not share a prefix with unrelated
  applications.

## Failure policy

| Policy | Redis outage behavior | Trade-off |
|---|---|---|
| `fail_open=False` (default) | Reject new acquisitions with HTTP 503. | Limits stay strict; streaming stops during the outage. |
| `fail_open=True` | Grant worker-local fallback leases without coordination. | Streaming continues; limits are not enforced across workers until Redis recovers. |

Authentication, authorization, and script errors never fail open. Existing streams keep
trying renewals during the adaptive grace period; if Redis recovers before the lease TTL,
they continue normally. See the [failure model](failure-model.md) for the full matrix.

## Changing limits without splitting the cluster

`{prefix}:config` is registered atomically with `SET NX` and has no TTL. This prevents two
rolling-deploy generations from silently using different limits. To change limits:

- **Preferred:** use a new `key_prefix` (for example `myapp:streams:v2`) and let old pods
  drain under the old prefix. During the overlap, the two prefixes are independent
  concurrency domains.
- **Same prefix:** stop old workers, wait for leases to expire, delete the config key, then
  start the new generation:

```bash
redis-cli DEL "{myapp:streams}:config"
```

The first new worker registers the new fingerprint; subsequent workers verify against it.
`verify_cluster_config(strict=True)` turns a mismatch into a startup failure instead of
running with inconsistent limits.

## Related

- [Observability](observability.md): metric names and adapter setup.
- [Failure model](failure-model.md): crash, failover, and outage behavior.
- [README: production guide](https://github.com/agustin18/fastapi-stream-lease#production-and-operational-guide)
  for the long-form operational notes.
