# Changelog

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
