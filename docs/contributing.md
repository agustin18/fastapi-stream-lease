# Contributing

Issues and pull requests are welcome. Open an issue before a substantial public API change
so the behavior can be agreed on first. The maintainer reviews and merges all pull
requests.

The canonical contributor guide is
[CONTRIBUTING.md](https://github.com/agustin18/fastapi-stream-lease/blob/main/CONTRIBUTING.md).
Security reports should follow
[SECURITY.md](https://github.com/agustin18/fastapi-stream-lease/blob/main/SECURITY.md), not
a public issue.

## Local checks

Install [uv](https://docs.astral.sh/uv/), then run:

```bash
uv sync --extra dev
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```

Tests in `tests/test_real_redis.py` run when `REDIS_URL` points to an isolated Redis
database. The default pytest command requires 100% combined statement and branch coverage.

For the Docker Compose path:

```bash
docker compose up -d redis
docker compose run --rm backend uv run pytest
docker compose down
```

## Documentation preview

Build the docs locally with the docs extra:

```bash
uv sync --extra dev --extra docs
uv run mkdocs serve
```

Open the local URL printed by MkDocs. Before opening a pull request, verify the strict
build:

```bash
uv run mkdocs build --strict
```

## Pull requests

- Keep the change focused.
- Add a regression test for behavior changes.
- Update the README or docs for public API or failure-mode changes.
- Use Conventional Commits (`feat:`, `fix:`, `docs:`, `perf:`, `chore:`).
- CI must pass before merge.

## Releases

Release versions are calculated from Conventional Commits since the previous tag.
`feat` requires a minor bump; `feat(demo)` changes limited to demo, docs, tests, and
release metadata are the documented patch exception. Run
`python scripts/release_version.py --next` to preview the next version; the publish
workflow verifies the tag before uploading.
