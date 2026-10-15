# Changelog

## 0.6.0 — 2026-10-15

- **Native Active Leases Gauge & Observability Architecture (M-2)**:
  - Added native `fastapi_stream_lease_active_leases` gauge to both `PrometheusMetrics` and `OpenTelemetryMetrics` adapters, labeled with static `scope` to ensure zero collisions across multiple managers sharing a telemetry registry.
  - Gauge values are driven directly in-memory from `StreamLeaseManager._register_active_lease` and `StreamLeaseManager._unregister_active_lease`, guaranteeing 100% drift-free tracking across the worker process lifecycle without polling delays or Redis network calls.
  - Primed to `0.0` upon manager initialization and reset to `0.0` on graceful shutdown drain or forced lease clearance.
  - Defined `ActiveLeasesTelemetry` runtime protocol and integrated into `StreamLeaseTelemetry` while preserving 100% backward compatibility for custom legacy v0.3.0 `TelemetryAdapter` implementations via defensive dispatch.
- **FastAPI Exception Handlers Ergonomics (DX-1)**:
  - Introduced 1-line exception handler registration helper `register_exception_handlers(app)` and convenience method `manager.register_exception_handlers(app)`.
  - Automatically maps `StreamLeaseRejected` to HTTP 429 (`Retry-After` header + JSON error detail) and `StreamLeaseUnavailable` to HTTP 503 (`Retry-After` header + backend unavailable JSON), eliminating boilerplate in application setup.
  - Strict input validation rejecting non-FastAPI/Starlette application instances.
- **Opt-in Tracing & Limit Response Headers (DX-2)**:
  - Added `include_lease_headers: bool = False` to `LeaseConfig` with per-request overrides on `StreamLease.as_streaming_response()` and `StreamLeaseManager.stream()`.
  - When enabled, automatically injects `X-Stream-Lease-Id`, `X-Stream-Lease-User-Limit`, and `X-Stream-Lease-Global-Limit` into response headers for end-to-end tracing and client visibility.
  - Retained strict opt-in default (`False`) to protect private infrastructure identifiers and limit topologies from unintentional external leakage.
- **Documentation & Examples Refresh (DX-3)**:
  - Updated `examples/openai_streaming_demo.py` showcasing `acquire_timeout=0.5` non-blocking burst absorption and 1-line `manager.register_exception_handlers(app)`.
  - Updated `examples/prometheus_metrics_demo.py` showcasing native `register_exception_handlers` and drift-free active gauge telemetry.
  - Added comprehensive "Unit Testing FastAPI Endpoints with FakeRedis" recipe to `README.md`, documenting zero-infrastructure unit testing patterns with `fakeredis.aioredis.FakeRedis`.

## 0.5.1 — 2026-10-15

- **Shutdown Timeout Budget & Shared Deadline Enforcement (SE-1)**:
  - Enforced a shared monotonic deadline (`deadline = time.monotonic() + timeout`) across `StreamLeaseManager.close()` and `HookDispatcher.close()`, eliminating multiplicative timeout compounding (up to $3\times$) during container shutdown and preventing SIGKILL termination in orchestrators like Kubernetes.
  - Mitigated the hook guillotine edge case by granting a minimal 50ms grace floor (`max(remaining, min(timeout, 0.05))`) when `_force_release_active_leases` runs, allowing in-memory `on_released(reason="shutdown_drain")` lifecycle callbacks to drain completely without being prematurely dropped as backpressure drops.
- **Reactive Fast Wakeup on Manager Shutdown (M-3)**:
  - Introduced `_closing_event: asyncio.Event` within `StreamLeaseManager` triggered immediately upon `close()`, awakening pending acquisition loops in near-0ms and raising `StreamLeaseUnavailable` without awaiting idle backoff sleep ticks.
- **Dynamic Retry-After Adaptation & Anti-Thundering Herd Jitter (M-1)**:
  - Calibrated the acquisition backoff sleep delay to adapt to Lua-computed `retry_after_ms` when earlier expiration slots are imminent (`min(0.05, retry_after_ms / 1000.0)`).
  - Applied micro-jitter ($\pm 15\%$, `random.uniform(0.85, 1.15)`) bounded by remaining timeout, preventing synchronized polling waves against Redis under high concurrency saturation while strictly preserving the 50ms polling ceiling for clean socket disconnects.
- **Fail-Fast Validation on Global Stream Counts (SE-2)**:
  - Added explicit $O(1)$ fail-fast validation in `manager.get_active_count()`: querying global count (`user_id=None`) when `max_global == 0` raises `ValueError` immediately, preventing silent zero counts when global Redis tracking is disabled.
- **Transport Release Safety & Renewal Cancellation (SE-4)**:
  - Ensured `lease.cancel_renewal()` executes deterministically in the `finally:` block of `manager.release(lease)`, stopping active renewal tasks without corrupting `lease._is_released` or emitting spurious lifecycle callbacks during rollback races.

## 0.5.0 — 2026-10-15

- **Acquisition Timeout Grace Window (`acquire_timeout`) & Dynamic $O(1)$ `Retry-After`**:
  - Added `acquire_timeout: float = 0.0` configuration to `LeaseConfig` with per-operation overrides on `manager.acquire(..., timeout=...)`, `manager.lease(..., timeout=...)`, and `manager.stream(..., timeout=...)`.
  - Non-blocking cooperative retry loop absorbs transient concurrency bursts, client reconnection storms, and connection races without returning premature HTTP 429 rejections.
  - Preserves exact lease lifetime across retry waits: local lease TTL (`expires_at`) is calibrated to the monotonic timestamp when acquisition actually succeeds, while span duration records total end-to-end latency for observability.
  - Dynamic $O(1)$ `Retry-After` estimation directly inside Redis Lua: `ACQUIRE_SCRIPT` queries the earliest expiring lease in the Sorted Set via `ZRANGE ... 0 0 WITHSCORES` and computes the exact milliseconds until capacity becomes available. Propagated through `StreamLeaseRejected.retry_after` and standard HTTP `Retry-After` response headers.
  - Strict parameter validation: non-finite values (`NaN`, `inf`), booleans, and negative timeouts are explicitly rejected with `ValueError`.
  - Cluster fingerprint compatibility: `acquire_timeout` is maintained as a worker-local operational parameter, ensuring zero `ConfigurationMismatchError` during rolling upgrades across pod versions.
- **Active Lease Tracking & Graceful Reactive Drain on Shutdown**:
  - Added worker-local active lease tracking (`manager.active_leases_count`, `manager._active_leases`) within `StreamLeaseManager`.
  - Reactive drain on shutdown via `await manager.close(drain=True, timeout=5.0)` utilizing `asyncio.Event` (`_drained_event`), eliminating polling loops and waking up immediately once all active streams conclude naturally.
  - Shielded concurrent batch force-release: lingering active leases past the drain timeout are released in parallel in a single network round-trip via `asyncio.gather` with cancellation shielding (`anyio.CancelScope(shield=True)`), preventing ghost leases in Redis during SIGTERM / container shutdowns.
  - Blinded TOCTOU race mitigation during worker shutdown: acquisitions in flight when `manager.close()` is initiated execute an immediate cancellation-shielded rollback release in Redis before raising `StreamLeaseUnavailable`.
  - Synchronous heartbeat cancellation: `StreamLease.cancel_renewal()` executes synchronously upon release and manager teardown to eliminate background renewal task leakage.
- **Client Compatibility & Legacy Connection Pool Exhaustion**:
  - Support `redis-py` 4.6.0+ without changing the public API, Redis keys, Lua scripts, or default policies. The server requirement remains Redis 5.0+.
  - Treat legacy local connection-pool exhaustion (`ConnectionError` with pool messages) as an availability error without tripping the circuit breaker or granting fail-open fallback leases, matching modern `MaxConnectionsError` handling. Genuine backend outages retain their configured behavior.
  - Add real-pool and lifecycle regressions, extend client compatibility CI, and update RESP2 fixtures, cleanup examples and contributor documentation.
- **Security & CI Supply Chain Hardening**:
  - Added automated dependency vulnerability auditing via `pip-audit` in CI.
  - Updated GitHub Actions pinning to latest audited SHAs across all automated workflows.

## 0.4.0 — 2026-09-28

- **Deterministic Upstream Iterator Cancellation & ASGI Disconnect Determinism (`close_source`)**:
  - Added `close_source: bool = True` to `lease.wrap()`, `lease.as_streaming_response()`, and `manager.stream()`. **Ownership semantic note**: When `close_source=True`, `fastapi-stream-lease` takes ownership of closing the underlying upstream stream upon completion, early cancellation, or error; pass `close_source=False` to retain exact v0.3.0 non-closing semantics.
  - Introduced `ProtectedStreamingResponse(StreamingResponse)` guaranteeing that real ASGI client disconnect events in Starlette/FastAPI deterministically invoke `await body_iterator.aclose()` in outer teardown blocks, releasing active Redis leases immediately without manual `contextlib.aclosing()` wrappers.
  - Universal level cancellation resilience: shielded cleanup and Redis release using AnyIO cancel scopes (`anyio.CancelScope(shield=True)`) and persistent release task references on `StreamLease`, ensuring that nested task cancellation does not abort the Redis Lua release script or produce ghost leases in both HTTP responses and standalone lease contexts.
  - Dual-target traversal: ensures both outer async iterable/generator sources and inner iterators returned by `aiter(stream)` are closed cleanly without redundant double-close invocations.
  - Asynchronous & synchronous close safety: awaits both native `aclose()` and coroutines returned by `async def close()`. Synchronous or non-coroutine `close()` and `aclose()` calls are safely offloaded to worker threads via `asyncio.to_thread`, preventing event-loop starvation.
  - Bounded global cleanup timeout budget: added `upstream_cleanup_timeout: float = 2.0` (configurable and strictly validated in `LeaseConfig`) enforced globally across all targets and composite teardown phases via `asyncio.wait_for`, preventing misbehaving or stalled upstreams from delaying lease releases. Note: synchronous worker threads cannot be forcibly killed, but cooperative teardown never blocks beyond the timeout budget.
  - Safe error suppression: upstream teardown exceptions are logged defensively at debug level and suppressed, guaranteeing that the Redis release attempt proceeds unconditionally; if Redis itself is temporarily unreachable, lease TTL provides the final self-healing guarantee.
- **Circuit Breaker Observability Telemetry & Scope Labeling**:
  - Added `circuit_state` Gauge to Prometheus and OpenTelemetry adapters, exporting current worker-local circuit breaker state (`CLOSED = 0`, `HALF_OPEN = 1`, `OPEN = 2`).
  - Added `short_circuited_total` Counter measuring total backend requests rejected because the circuit breaker denied a permit.
  - Labeled with `scope`, `operation` (`acquire` or `count`), and `state` (`open` or `half_open`).
  - Added `telemetry_scope: str | None = None` to `LeaseConfig` (defaulting to `key_prefix`), allowing multiple managers sharing the same `key_prefix` to report isolated circuit breaker metrics without gauge/counter label collisions on shared registries.
  - Maintained 100% backward compatibility with v0.3.0 `TelemetryAdapter` protocol via modular `CircuitBreakerTelemetry` and composite `StreamLeaseTelemetry` protocols.
- **FastAPI / Starlette Minimum Floor & Soak Benchmark Hardening**:
  - Added automated CI matrix job `test-fastapi-min` validating `fastapi>=0.100.0` and `starlette>=0.27.0` minimum floor compatibility.
  - Hardened `benchmarks/bench_soak.py` with steady-state vs transient warmup plateau evaluation and monitor sample starvation detection.

## 0.3.0 — 2026-09-28

- **Adaptive Circuit Breaker & Thundering Herd Protection**: Added production-grade circuit breaker (`CircuitBreakerConfig`, `CircuitState`, `BackendFailurePolicy`) safeguarding Redis backends during outages, Sentinel failovers, and network partitions. Features state transitions (`CLOSED`, `OPEN`, `HALF_OPEN`), bounded probe concurrency (`half_open_max_probes`), jittered recovery cooldown, and fast-failing with `StreamLeaseUnavailable` or graceful fallback under `FallbackMode.FAIL_OPEN`.
- **Golden Asymmetry & Epoch-Based RAII Permits**: Active stream renewals bypass circuit breaker open states to prioritize existing connections over new requests. Successful renewals or reachable backend probes immediately heal the breaker to `CLOSED`. Generation-tracked RAII permits (`CircuitPermit`) ensure cancellation and async timeout safety without leaking half-open probe slots or corrupting state across epochs.
- **Strict Exception & Availability Classification**: Decoupled circuit breaker failure eligibility from connection availability semantics: `MaxConnectionsError` is recognized as an availability condition (normalizing to HTTP 503 and eligible for renewal grace period retries) while strictly prevented from tripping the circuit breaker or enabling fail-open fallbacks. Excluded deterministic configuration, syntax, and auth failures (`AuthenticationError`, `AuthorizationError`, `ExternalAuthProviderError`, `ClusterCrossSlotError`), narrowed builtin error handling from generic `OSError` to `ConnectionError`/`TimeoutError`, and preserved transient handling for `ReadOnlyError`, `ClusterDownError`, and nested `RedisClusterException` network causes across `redis-py` 5.0.0 through 5.2+.
- **Encapsulated Circuit State API**: Publicly exported `CircuitState` enum (`CLOSED`, `OPEN`, `HALF_OPEN`) from package root and exposed read-only `manager.circuit_state` property on `StreamLeaseManager`, keeping internal breaker mechanics and mutability cleanly encapsulated.
- **Manager-Local Circuit Breaker Tuning & Rolling Upgrades**: Circuit breaker settings remain worker-local operational parameters intentionally excluded from the shared Redis configuration fingerprint (`verify_cluster_config`), enabling canary deployments and runtime tuning adjustments without triggering `ConfigurationMismatchError`. Effective fail-open and fail-closed behaviors remain enforced and synchronized via the backward-compatible `fail_open` fingerprint field.
- **Observability Adapters (Prometheus & OpenTelemetry)**: Added production-ready pluggable telemetry adapters for Prometheus and OpenTelemetry conforming to a standardized observability contract, recording operation throughput and duration (`operations_total`, `operation_duration_seconds`), lease loss reasons (`lost_total`), backend errors (`backend_errors_total`), circuit breaker fallback events (`fallback_total`), and internal hook dispatcher queue telemetry (`hook_dropped_total`, `hook_queue_depth`).
- **Production Benchmark & Soak Harnesses**: Added reproducible wrapper overhead benchmarks (`benchmarks/bench_wrapper_overhead.py`), soak endurance harnesses (`benchmarks/bench_soak.py`), and CI regression thresholds (`benchmarks/baseline-overhead.json`) verifying sub-millisecond wrapper execution overhead ($O(\log N + M)$ amortized Redis Sorted Set operations) and zero memory or task leaks under sustained concurrency.
- **Nightly Chaos & Matrix Verification**: Introduced automated continuous chaos testing across Redis Sentinel and 6-node Redis Cluster topologies validating zero stream drops and self-healing under primary crashes.

## 0.2.0 — 2026-09-27

- **6-Node Redis Cluster Validation & Chaos Resilience Suite**: Added end-to-end integration and chaos failover test suite running against a production-grade 6-node Redis Cluster (3 masters, 3 replicas, 16,384 hash slots) verifying zero stream drops across live primary promotions and transparent recovery from `MOVED` redirections.
- **Deterministic Multi-Key Hash Tag Slot Guarantee**: Deterministically maps key prefixes across all three distinct primary shards, validating slot equivalence `KEYSLOT(user) == KEYSLOT(global) == KEYSLOT(config)` and asserting atomic multi-key Lua scripts execute without `CROSSSLOT` errors on any shard.
- **Uncoordinated Hard Master Failure & Replica Election**: Validated automatic replica election and uninterrupted lease renewals under uncoordinated primary hard crashes (`DEBUG SLEEP`) without manual failover commands.
- **Transient Redis Cluster Exception Classification**: Expanded `is_network_error()` with precomputed exception lookup sets (`ReadOnlyError`, `ClusterDownError`, `MasterDownError`, `SlotNotCoveredError`, `TryAgainError`, `ClusterError`) while strictly classifying `ClusterCrossSlotError` as non-transient.
- **Dual-Node Outage Stream Cancellation**: Verified fail-closed lease termination and `StreamLeaseLost` cancellation with `lost_reasons == ["backend_timeout"]` when all nodes serving a slot become unreachable.
- **Minimum Dependency Floor (`redis>=5.0.0`) Across All Topologies**: Verified full compatibility with minimum floor `redis==5.0.0` across the entire unit suite, Redis Sentinel chaos tests, and 6-node Redis Cluster failover tests. Added response normalization for legacy Redis protocol array returns and safe teardown bridging `close()` and `aclose()`.
- **Production/Stable GA Status**: Promoted package classifier to `Development Status :: 5 - Production/Stable`, graduating from beta after extensive failover and chaos verification across standalone, Sentinel, and Cluster topologies.
- **Distributed Guarantees & Replication Failure Model Documentation**: Documented Redis Cluster asynchronous replication limits, un-replicated write loss windows during failover, and `WAIT` safety boundaries in the official distributed guarantees documentation.

## 0.2.0b1 — 2026-09-27

- **Real Redis Sentinel Failover & Chaos Test Suite**: Added comprehensive multi-node Redis Sentinel automated testing (1 Master, 1 Replica, 3 Sentinels with Quorum 2) covering quorum health checks (`SENTINEL ckquorum`), lease and configuration replication before failovers, unresponsive master crashes (`DEBUG SLEEP`), and total cluster outage cancellation boundaries.
- **Failover-Safe Startup Cluster Verification**: Hardened `verify_cluster_config` with bounded retry loops (strictly validating `retry_attempts >= 1` and `retry_delay >= 0`) to seamlessly survive transient failovers and master promotions during pod startup.
- **Asynchronous Bounded Hook Dispatcher (`HookDispatcher`)**: Decoupled all lifecycle telemetry hooks (`on_acquired`, `on_released`, `on_lost`, `on_rejected`, `on_backend_error`) into an isolated, bounded, out-of-band FIFO worker. Sync callbacks execute in threadpools via `asyncio.to_thread` without blocking the asyncio event loop or delaying stream cancellations. Telemetry queue overflow drops excess events gracefully with rate-limited logging.
- **Immediate Task Cancellation on Lease Revocation**: Requesting immediate owner-task cancellation as soon as lease loss or disconnect is detected, before telemetry callbacks are enqueued.
- **Atomic Cluster Configuration Consistency (`verify_cluster_config`)**: Added startup fingerprint verification using persistent atomic Redis `SET NX` (`{prefix}:config`), detecting limit drift across pods (including `max_global`, `max_per_user`, `lease_seconds`, and `fail_open`) without TTL expiration races. A 3-attempt retry loop safely verifies canonical state even under concurrent node initialization.
- **Graceful Telemetry Drain (`manager.close`)**: Added `await manager.close(drain=True, timeout=5.0)` to allow applications to flush pending observability events before terminating worker processes or closing Redis connections.
- **Standardized Lifespan Patterns in Examples**: Aligned all runnable examples (`sse_demo.py`, `websocket_demo.py`, `openai_streaming_demo.py`, `prometheus_metrics_demo.py`) with startup cluster verification and shutdown draining.
- **Documented Safe Cluster Migration & Telemetry Semantics**: Added step-by-step guides for zero-downtime cluster configuration changes and documented the best-effort nature of out-of-band telemetry hooks.
- **Hardened CI/CD Supply Chain**: Configured matrix testing across Python 3.10 through 3.14 on Redis 5, 7, and 8, with single-build artifact verification and PyPI publishing.

## 0.1.5 — 2026-09-26

- **1-Line Protected Streaming Helper (`manager.stream`)**: Added `await manager.stream(user_id, generator)` and `lease.as_streaming_response(generator)` returning protected Starlette/FastAPI `StreamingResponse` objects in a single call with automatic error cleanup to prevent lingering ghost leases.
- **Redis Sentinel & Master Failover Resilience (`ReadOnlyError`)**: Classified `ReadOnlyError` as a transient condition in `is_network_error()`, allowing adaptive renewal retries to ride out Sentinel master failover without dropping active streams.
- **Production LLM Token Streaming Example**: Added `examples/openai_streaming_demo.py` showcasing how to protect OpenAI, Anthropic, and Ollama streaming endpoints with strict per-user concurrency limits.
- **Prometheus Observability Example**: Added `examples/prometheus_metrics_demo.py` showing how to wire zero-dependency lifecycle hooks into Prometheus counters and gauges.

## 0.1.4 — 2026-09-26

- **Worker Crash Resilience (P1 Fix)**: Background auto-renewal worker catches unhandled execution and Redis errors (e.g. `ResponseError`, `NOPERM`, `WRONGTYPE`), logs the incident with traceback, marks the lease as lost, and cleanly terminates the stream/context instead of crashing silently and leaving unmanaged streams running.
- **Zero-Dependency Lifecycle & Telemetry Hooks**: Added customizable callback hooks to `LeaseConfig` (`on_acquired`, `on_rejected`, `on_lost`, `on_backend_error`) supporting both sync and async callables to effortlessly integrate Prometheus, Datadog, StatsD, or Sentry without extra runtime dependencies.
- **Consistent Context Manager Renewal Intervals**: Added `renew_interval: float | None = None` parameter to `manager.lease()` matching `wrap()`, allowing customized renewal pacing for WebSocket and background task contexts.
- **Redis Client Timeouts in Examples**: Updated all examples (`examples/sse_demo.py`, `examples/websocket_demo.py`) with explicit client timeouts (`socket_timeout=2.0, socket_connect_timeout=2.0`).
- **Resilient SSE Client Example**: Added `examples/sse_client_resilient.py` demonstrating automatic reconnection with exponential backoff, jitter, and HTTP 429 / 503 `Retry-After` header handling to prevent client thundering herds.
- **Documentation Hardening**: Clarified fail-closed concurrency guarantees with respect to asynchronous master-replica failovers in Redis.

## 0.1.3 — 2026-09-26

- **Immediate Termination on Lost Leases**: Auto-renew background worker differentiates explicit lease revocation/loss (Redis returns 0) from transient network outages. A lost lease cuts the stream immediately without retries to strictly prevent concurrency breaches.
- **Scoped `fail_open` to Transient Network Errors**: Isolated `fail_open` strictly to transient network/timeout errors (`ConnectionError`, `TimeoutError`, `asyncio.TimeoutError`, `OSError`). Authentication, authorization, syntax, and Lua script errors never fail-open, preventing unbounded concurrency on misconfigured infrastructure.
- **Conservative Monotonic TTL Tracking**: Replaced wall-clock time with monotonic time (`time.monotonic()`) stamped *before* issuing network commands to Redis, ensuring latency strictly contracts local grace periods rather than exceeding distributed Redis TTL.
- **Sanitized HTTP 503 Responses**: Updated examples, README, and tests to register `StreamLeaseUnavailable` exception handlers and sanitize error responses, preventing internal socket/backend errors from leaking to public API clients.
- **WebSocket Demo & Operational Guide**: Added fully runnable `examples/websocket_demo.py` showcasing standard status codes (1008 Policy Violation on 429, 1013 Try Again Later on 503), along with an extensive operational guide covering Redis timeouts, cluster hashing (`{user_id}`), and monitoring metrics.

## 0.1.2 — 2026-09-26

- **Adaptive Grace Period Retries**: Prevent transient Redis network hiccups from abruptly killing active streams during auto-renewal by calculating remaining TTL and retrying until TTL is actually exhausted.
- **Backend Availability Handling**: Added `StreamLeaseUnavailable` (HTTP 503 Service Unavailable) with optional `Retry-After` header support when Redis backend errors occur.
- **Fail-Open Strategy**: Added `fail_open: bool = False` configuration option in `LeaseConfig` allowing graceful degradation (allowing streams) during Redis outages.
- **100% Test Coverage**: Attained full 100% line and branch test coverage across all core modules.

## 0.1.1 — 2026-09-26

- Renew leases for long-running `manager.lease()` contexts, including WebSockets.
- Renew leases in the direct `async with lease` context as well.
- Stop streams and contexts when their lease can no longer be renewed.
- Prevent expired leases from being renewed and use Redis time across workers.
- Gate PyPI publication on CI for the release commit; add real Redis integration checks.
- Enforce at least 95% combined line and branch coverage, build validation, and Redis 5 compatibility in CI.
- Clarify documentation, add a runnable SSE example and contribution/security guidance.

## 0.1.0 — 2026-09-25

- Initial distributed SSE and stream concurrency lease manager.
