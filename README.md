# fastapi-stream-lease

[![CI](https://github.com/agustin18/fastapi-stream-lease/actions/workflows/ci.yml/badge.svg)](https://github.com/agustin18/fastapi-stream-lease/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/fastapi-stream-lease.svg)](https://pypi.org/project/fastapi-stream-lease/)
[![Python](https://img.shields.io/pypi/pyversions/fastapi-stream-lease.svg)](https://pypi.org/project/fastapi-stream-lease/)

Limit **simultaneously active** SSE, LLM token streams, and WebSocket sessions across FastAPI/Starlette workers with Redis. Set a per-user limit, a global limit, or both. An extra connection receives HTTP 429 before streaming begins.

Request rate limiters answer “how many requests arrived this minute?” This package answers “how many streams are open right now?” It is useful when a connection can remain open for seconds or minutes. For a general-purpose distributed semaphore, consider [py-redis-limiters](https://pypi.org/project/redis-limiters/) or [py-redis-semaphore](https://pypi.org/project/py-redis-semaphore/).

## Install

Requires Python 3.10+ and Redis 5.0+.

With Redis 5 and redis-py 8+, construct your client with `redis.from_url(url, protocol=2)`: redis-py 8 defaults to RESP3, which Redis 5 does not support. See the [redis-py protocol documentation](https://github.com/redis/redis-py#resp3-support).

```bash
pip install 'fastapi-stream-lease[fastapi]'
```

The `fastapi` extra supplies FastAPI for the HTTP helpers; the core package only requires `redis`.

## Try it locally

The [runnable SSE example](examples/sse_demo.py) accepts one API key from `STREAM_DEMO_TOKEN` and uses it as a demo identity. Start Redis, clone this repository, then run:

```bash
git clone https://github.com/agustin18/fastapi-stream-lease.git
cd fastapi-stream-lease
pip install -e '.[fastapi]' uvicorn
export STREAM_DEMO_TOKEN=local-secret
export REDIS_URL=redis://localhost:6379/0
uvicorn examples.sse_demo:app
```

In another terminal, open two streams, then try a third with the same key:

```bash
curl -N -H 'X-API-Key: local-secret' http://localhost:8000/stream
```

The first two requests stream events; the third receives `429` with a `Retry-After` header until a slot is freed. Replace the demo API key with your application's authenticated user or account ID before production use. Never use an unverified request parameter as the user ID.

## FastAPI integration

```python
from fastapi import Depends, FastAPI, Request
from fastapi.responses import StreamingResponse
import redis.asyncio as redis

from fastapi_stream_lease import LeaseConfig, StreamLeaseManager, StreamLeaseRejected

app = FastAPI()
redis_client = redis.from_url("redis://localhost:6379")
manager = StreamLeaseManager(
    redis_client,
    LeaseConfig(max_per_user=2, max_global=500, lease_seconds=30),
)


async def authenticated_user_id() -> str:
    # Replace this function with your existing authentication dependency.
    raise NotImplementedError


@app.exception_handler(StreamLeaseRejected)
async def rejected(request: Request, exc: StreamLeaseRejected):
    return exc.as_response()


@app.get("/stream")
async def stream(user_id: str = Depends(authenticated_user_id)):
    lease = await manager.acquire(user_id)

    async def events():
        yield "data: first event\n\n"
        # Yield more events here.

    return StreamingResponse(lease.wrap(events()), media_type="text/event-stream")
```

Close the Redis client in your application's lifespan shutdown handler. Reuse the same `StreamLeaseManager` and `key_prefix` across workers that share limits. `max_per_user=0` or `max_global=0` disables that limit; the global count is unavailable when global tracking is disabled.

For WebSockets, keep the context open for the whole session:

```python
async with manager.lease(user_id) as lease:
    while True:
        message = await websocket.receive_text()
        await websocket.send_text(message)
```

The manager context and `async with lease` both renew while open. Handle normal WebSocket disconnects in your route as usual. If renewal fails or the lease expires, `StreamLeaseLost` interrupts the stream or context. Catch it at the application boundary if you want to record a metric or send an application-specific WebSocket close code. `wrap(auto_renew=False)` disables automatic renewal; use it only if you renew the lease yourself.

## Behavior and limits

- Acquisition, renewal, expiration cleanup, and release use atomic Redis Lua scripts. The keys share a Redis Cluster hash tag, so a user and global limit can be checked in one script.
- Every lease expires after `lease_seconds` without a successful renewal. `wrap()` and `manager.lease()` renew every half interval by default. A short Redis outage can therefore end an active stream. New acquisitions propagate Redis errors to the application.
- Normal completion or cancellation attempts immediate release. If Redis is unavailable during release, the lease is removed after expiration; cleanup of the key itself uses a longer TTL. An async iterator abandoned without being closed may also hold its slot until expiration. Use `contextlib.aclosing()` if your own consumer stops iteration early.
- `get_active_count(user_id)` counts active leases for one identity; `get_active_count()` counts globally when `max_global` is enabled. Neither is a historical usage metric.
- All workers sharing limits must use the same key prefix and compatible limit settings. Lease expiration is measured by Redis, avoiding clock differences among application workers.

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) for the local workflow and [SECURITY.md](SECURITY.md) for private vulnerability reports. Changes are proposed through pull requests and merged by the maintainer after CI passes. The package is licensed under [MIT](LICENSE).

CI checks formatting, lint, types, Redis 5 and 7 behavior, package build, and a minimum of 95% combined line and branch coverage. This is a small beta project; reports from real deployments are especially helpful for documenting operational limits.
