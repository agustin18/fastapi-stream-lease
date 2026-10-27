---
description: Install fastapi-stream-lease and run a minimal FastAPI and Redis streaming example with allowed and rejected paths.
---

# Quickstart

This page runs a minimal FastAPI app against a local Redis instance and verifies both the
allowed and rejected paths.

## Requirements

- Python 3.10 or newer
- Redis 5.0 or newer
- `redis-py` 4.6.0 or newer (installed by the package)

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install 'fastapi-stream-lease[fastapi]' uvicorn
```

If you are working from a clone, install the project instead:

```bash
pip install -e '.[fastapi]' uvicorn
```

## Start Redis

```bash
docker run --rm -p 6379:6379 redis:7-alpine
```

For a local Redis already listening on the default port, skip this step.

## Minimal application

Create `app.py`:

```python
import asyncio
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
import redis.asyncio as redis

from fastapi_stream_lease import LeaseConfig, StreamLeaseManager


client = redis.from_url(
    "redis://localhost:6379/0",
    socket_timeout=1.0,
    socket_connect_timeout=1.0,
)
manager = StreamLeaseManager(
    client,
    LeaseConfig(max_per_user=1, max_global=100, lease_seconds=30.0),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await manager.verify_cluster_config(strict=True)
    yield
    await manager.close(drain=True, timeout=5.0)
    if hasattr(client, "aclose"):
        await client.aclose()
    else:
        await client.close()


app = FastAPI(lifespan=lifespan)
manager.register_exception_handlers(app)


async def current_user_id() -> str:
    # Replace this placeholder with a verified principal from your auth layer.
    return "user-123"


@app.get("/stream")
async def stream(user_id: str = Depends(current_user_id)):
    async def events():
        for index in range(10):
            yield f"data: chunk {index}\n\n"
            await asyncio.sleep(1)

    return await manager.stream(user_id, events())
```

Run it:

```bash
uvicorn app:app --reload
```

## Verify the limit

Open the first stream:

```bash
curl -N http://127.0.0.1:8000/stream
```

In a second terminal, open another stream with the same identity:

```bash
curl -i http://127.0.0.1:8000/stream
```

With `max_per_user=1`, the second request returns `429 Too Many Requests` and a
`Retry-After` header. Close the first request and retry: the slot is released immediately.

The response also includes lease headers only when `include_lease_headers=True` is set in
`LeaseConfig`.

## Test without Redis

`fakeredis` is useful for application test suites:

```python
import pytest
from fakeredis.aioredis import FakeRedis
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from fastapi_stream_lease import LeaseConfig, StreamLeaseManager


@pytest.fixture
def app():
    app = FastAPI()
    client = FakeRedis()
    manager = StreamLeaseManager(
        client,
        LeaseConfig(max_per_user=1, include_lease_headers=True),
    )
    manager.register_exception_handlers(app)

    @app.get("/stream")
    async def stream_route():
        async def events():
            yield "data: chunk\n\n"

        return await manager.stream("test_user", events())

    return app


@pytest.mark.asyncio
async def test_stream_headers_and_limit(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/stream")
        assert response.status_code == 200
        assert "x-stream-lease-id" in response.headers
        assert response.headers["x-stream-lease-user-limit"] == "1"
```

## Next steps

- [SSE and streaming](guides/sse.md): add heartbeats and use `sse-starlette`.
- [Production deployment](guides/production.md): multi-worker metrics, Nginx, and
  Kubernetes shutdown.
- [API reference](api.md): every public class and exception.
