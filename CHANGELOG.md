# Changelog

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
