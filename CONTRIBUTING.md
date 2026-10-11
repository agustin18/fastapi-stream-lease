# Contributing

Issues and pull requests are welcome. Please open an issue before a substantial API change so the behavior can be agreed on first. The maintainer reviews and merges all pull requests.

## Local setup

Install [uv](https://docs.astral.sh/uv/), then run:

```bash
uv sync --extra dev
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```

Tests in `tests/test_real_redis.py` run when `REDIS_URL` points to an isolated Redis database. CI supplies Redis automatically.

For local integration testing, start Redis and run `REDIS_URL=redis://localhost:6379/15 uv run pytest -o addopts='' tests/test_real_redis.py`.

Alternatively, run the entire test suite including real Redis integration without installing local dependencies via Docker Compose:

```bash
docker compose up -d redis
docker compose run --rm backend uv run pytest
docker compose down
```

The default test command requires at least 95% combined line and branch coverage. CI also checks Redis servers 5, 7, and 8, the redis-py 4.6.0 client floor and modern clients, including Cluster and Sentinel. It builds both distributions and validates package metadata. Server and Python client versions are independent.

For the client floor, run `uv run --with "redis==4.6.0" pytest`; the overlay resolves dependencies normally. Real integration tests need an isolated `REDIS_URL`. The fixtures select RESP2 for newer clients and omit the unsupported `protocol` argument on 4.6. Use `aclose()` when available, otherwise await `close()`; disconnect caller-owned pools separately.

Pool-exhaustion regression tests must cover actual `ConnectionPool` and `BlockingConnectionPool` errors, both fallback policies, and circuit permit ownership. A capacity error must not enable fail-open or count as a remote outage.

GitHub Actions are pinned to full commit hashes so a changed version tag cannot silently change the release pipeline. The comment beside each hash shows the readable release version, and Dependabot proposes grouped updates.

Add a focused test for changed behavior. Update the README for public API or failure-mode changes. Keep PRs small enough to review and describe the user-visible effect. CI must pass before merge.

## Releases

Only the maintainer publishes releases. Version numbers in `pyproject.toml` and `src/fastapi_stream_lease/__init__.py` must match the `vX.Y.Z` release tag. The publishing workflow reruns CI for that tag and then uploads through PyPI Trusted Publishing.
