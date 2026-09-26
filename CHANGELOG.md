# Changelog

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
