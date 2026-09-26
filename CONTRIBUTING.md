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

Tests in `tests/test_real_redis.py` run when `REDIS_URL` points to an isolated Redis database. CI supplies Redis automatically. For local integration testing, start Redis and run `REDIS_URL=redis://localhost:6379/15 uv run pytest tests/test_real_redis.py`.

Add a focused test for changed behavior. Update the README for public API or failure-mode changes. Keep PRs small enough to review and describe the user-visible effect. CI must pass before merge.

## Releases

Only the maintainer publishes releases. Version numbers in `pyproject.toml` and `src/fastapi_stream_lease/__init__.py` must match the `vX.Y.Z` release tag. The publishing workflow reruns CI for that tag and then uploads through PyPI Trusted Publishing.
