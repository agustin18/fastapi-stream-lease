# Failure model

The package is designed around one invariant: a slot is held by a lease, and every lease
has a Redis-side expiration time. Redis is the source of truth; workers can disappear
without leaving permanent locks.

## Crash and disconnect behavior

| Situation | What happens | Practical result |
|---|---|---|
| Client disconnects | `ProtectedStreamingResponse` closes the iterator; `StreamLease.wrap()` releases the lease. | Slot is freed immediately, usually in under a millisecond. |
| Generator finishes | The lease is released in the iterator's `finally` block. | Slot is freed immediately. |
| Worker is killed | Renewals stop; Redis expires the lease after `lease_seconds`. | Slot self-heals within one TTL. |
| Worker pauses longer than the TTL | The next renewal sees the lease is gone and raises `StreamLeaseLost`. | The stale stream is cancelled instead of running unbounded. |
| Client disconnects during release | Shielded cleanup still attempts the Redis release; if Redis is unreachable, TTL is the fallback. | No permanent ghost lease. |
| Upstream cleanup hangs | `upstream_cleanup_timeout` bounds teardown, then the lease release still runs. | A misbehaving upstream cannot block the slot forever. |

## Redis outage behavior

`fail_open=False` is the default. New acquisitions fail closed with
`StreamLeaseUnavailable` (HTTP 503). Existing streams continue renewal attempts during the
adaptive grace period; if Redis returns before their TTL, they continue normally.

`fail_open=True` grants a worker-local fallback lease when Redis is unreachable. The
fallback is intentionally uncoordinated: limits are not enforced across workers during the
outage, and fallback leases are not registered retroactively when Redis recovers.

Authentication, authorization, and Lua script errors never fail open. A local connection
pool exhaustion is treated as an availability error, not as a Redis outage, and does not
trip the circuit breaker.

## Circuit breaker

The circuit breaker is worker-local and opt-in through
`LeaseConfig(failure_policy=BackendFailurePolicy(...))`. It protects acquisition from a
sustained Redis outage:

- `CLOSED`: acquisitions go to Redis.
- `OPEN`: acquisitions fast-fail without network I/O; existing renewals are never blocked.
- `HALF_OPEN`: a bounded number of probes test whether Redis recovered.

A successful probe heals the breaker. A failed probe re-opens it with fresh jitter. The
state is exposed as `manager.circuit_state` and through the Prometheus/OpenTelemetry
adapters.

## Redis Cluster and Sentinel

All keys for one prefix share a hash tag (`{prefix}:user:...` and `{prefix}:global`), so
the user and global limits can be checked in one Lua script without `CROSSSLOT` errors.
Cluster failover and Sentinel master election are handled as transient conditions; a lease
that did not replicate before a failover is detected on the next renewal and cancelled
cleanly.

Redis replication is asynchronous. The package does not claim linearizable or CP
semantics across a network partition. If you need a strict cluster-wide bound under
partitions, review the Redis deployment's replication and `min-replicas-to-write` settings
for your threat model.

## The full matrix

The README contains the detailed failure-mode matrix, including the exact guarantees and
trade-offs for worker crashes, Redis outages, clock drift, slow hooks, Sentinel failover,
and Cluster slot promotion:

[Distributed guarantees and failure model](https://github.com/agustin18/fastapi-stream-lease#distributed-guarantees--failure-model)

## Related

- [Production deployment](production.md): sizing, shutdown, and fail-open policy.
- [Observability](observability.md): lost leases, backend errors, and circuit state.
