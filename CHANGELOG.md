# Changelog

## 0.3.0 — 2026-09-28

- **Adaptive Circuit Breaker & Thundering Herd Protection**: Added production-grade circuit breaker (`CircuitBreakerConfig`, `CircuitState`, `BackendFailurePolicy`) safeguarding Redis backends during outages, Sentinel failovers, and network partitions. Features state transitions (`CLOSED`, `OPEN`, `HALF_OPEN`), bounded probe concurrency (`half_open_max_probes`), jittered recovery cooldown, and fast-failing with `StreamLeaseUnavailable` or graceful fallback under `FallbackMode.FAIL_OPEN`.
- **Golden Asymmetry & Epoch-Based RAII Permits**: Active stream renewals bypass circuit breaker open states to prioritize existing connections over new requests. Successful renewals or reachable backend probes immediately heal the breaker to `CLOSED`. Generation-tracked RAII permits (`CircuitPermit`) ensure cancellation and async timeout safety without leaking half-open probe slots or corrupting state across epochs.
- **Strict Exception Classification**: Refined transient classification to exclude deterministic configuration, pool, and auth failures (`MaxConnectionsError`, `AuthenticationError`, `AuthorizationError`, `ExternalAuthProviderError`, `ClusterCrossSlotError`), while preserving transient handling for `ReadOnlyError`, `ClusterDownError`, and nested `RedisClusterException` network causes across `redis-py` 5.0.0 through 5.2+.
- **Encapsulated Circuit State API**: Publicly exported `CircuitState` enum (`CLOSED`, `OPEN`, `HALF_OPEN`) from package root and exposed read-only `manager.circuit_state` property on `StreamLeaseManager`, keeping internal breaker mechanics and mutability cleanly encapsulated.
- **Manager-Local Circuit Breaker Tuning & Rolling Upgrades**: Circuit breaker settings remain worker-local operational parameters intentionally excluded from the shared Redis configuration fingerprint (`verify_cluster_config`), enabling canary deployments and runtime tuning adjustments without triggering `ConfigurationMismatchError`. Effective fail-open and fail-closed behaviors remain enforced and synchronized via the backward-compatible `fail_open` fingerprint field.
- **Observability Adapters (Prometheus & OpenTelemetry)**: Added production-ready pluggable telemetry adapters for Prometheus and OpenTelemetry conforming to a standardized observability contract, recording operation throughput and duration (`operations_total`, `operation_duration_seconds`), lease loss reasons (`lost_total`), backend errors (`backend_errors_total`), circuit breaker fallback events (`fallback_total`), and internal hook dispatcher queue telemetry (`hook_dropped_total`, `hook_queue_depth`).
- **Production Benchmark & Soak Harnesses**: Added reproducible wrapper overhead benchmarks (`benchmarks/bench_wrapper_overhead.py`), soak endurance harnesses (`benchmarks/bench_soak.py`), and CI regression thresholds (`benchmarks/baseline-overhead.json`) verifying sub-millisecond execution overhead ($O(1)$ Lua operations) and zero memory or task leaks under sustained concurrency.
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
