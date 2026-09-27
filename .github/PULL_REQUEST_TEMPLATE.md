## Summary

Provide a concise description of the purpose of this PR and the problem it solves. Link related issues if applicable.

Fixes #(issue)

## Type of Change

- [ ] 🐛 Bug fix (non-breaking change which fixes an issue)
- [ ] ✨ New feature (non-breaking change which adds functionality)
- [ ] 💥 Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] ⚡ Performance improvement
- [ ] 📝 Documentation update
- [ ] 🔧 Refactoring / Code quality / CI

## Quality & Architectural Checklist

- [ ] **1:1 Sacred Test Mapping:** Tests are added or extended in existing corresponding `test_*.py` files (no redundant `_new.py` files).
- [ ] **100.00% Coverage Invariant:** Statement and branch coverage maintained at 100% (`docker compose run --rm backend uv run pytest`).
- [ ] **Failure Isolation:** Telemetry, callbacks, or logging failures cannot break distributed Redis lease coordination.
- [ ] **Cardinality Safety:** No dynamic identifiers (`user_id`, `lease_id`, arbitrary prefixes) are exposed as metric labels or trace attributes.
- [ ] **Strong Typing & Linting:** Clean pass on `mypy src --strict` and `ruff check .` (including `ASYNC`, `PERF`, `SIM`, and `T20`).
- [ ] **Multi-Topology Verified:** Distributed changes verified against Cluster (3M+3R) and Sentinel failover suites.

## How Was This Tested?

Describe the unit, integration, or chaos tests executed to verify these changes. Include Redis-backed checks for concurrency or lease changes.
