---
description: Protect FastAPI SSE and LLM token streams with per-user and global Redis concurrency leases.
---

# SSE and streaming

Server-Sent Events are long-lived HTTP responses. A normal request limiter releases its
slot when the handler returns, which is too early for a stream that may stay open for
minutes. `fastapi-stream-lease` keeps the slot for the lifetime of the iterator and
releases it when the iterator closes.

## The built-in response

`manager.stream()` is the shortest path. It acquires a lease, returns a
`ProtectedStreamingResponse`, and arranges for the generator to be closed if the ASGI
client disconnects:

```python
from fastapi import Depends, FastAPI
from fastapi_stream_lease import LeaseConfig, StreamLeaseManager
import redis.asyncio as redis

app = FastAPI()
client = redis.from_url("redis://localhost:6379", socket_timeout=1.0)
manager = StreamLeaseManager(client, LeaseConfig(max_per_user=2, max_global=500))
manager.register_exception_handlers(app)


async def current_user_id() -> str:
    return "user-123"


@app.get("/events")
async def events(user_id: str = Depends(current_user_id)):
    async def token_stream():
        async for token in call_llm(user_id):
            yield f"data: {token}\n\n"

    return await manager.stream(user_id, token_stream())
```

The default `media_type` is `text/event-stream`. You can pass `status_code`, `headers`,
`auto_renew`, `renew_interval`, and `close_source` through to the response helper.

## Heartbeats with sse-starlette

SSE proxies and load balancers often close an idle connection. `sse-starlette` adds
periodic comment pings and a small ASGI wrapper. Use `lease.wrap()` so the lease still
renews while the response is open:

```bash
pip install sse-starlette
```

```python
from sse_starlette.sse import EventSourceResponse

from fastapi_stream_lease import StreamLeaseManager


@app.get("/events")
async def events(user_id: str = Depends(current_user_id)):
    lease = await manager.acquire(user_id)
    try:
        return EventSourceResponse(
            lease.wrap(call_llm(user_id)),
            ping=15,
            media_type="text/event-stream",
        )
    except Exception:
        await lease.release(reason="error")
        raise
```

`ping=15` sends an SSE comment every 15 seconds. The wrapped iterator renews the Redis
lease every half TTL by default, so a quiet LLM stream does not lose its slot.

`EventSourceResponse` cancels its content task when the client disconnects. The `finally`
block inside `lease.wrap()` still runs, closing the upstream iterator and releasing the
Redis lease.

## Do I need sse-starlette?

No. The built-in response is enough if your application or reverse proxy already sends
heartbeats, or if streams are short enough that idle timeouts are not a concern. Use
`sse-starlette` when you need configurable pings, retry hints, or its ASGI disconnect
handling.

## Upstream ownership

By default, `close_source=True` means the package takes ownership of the upstream stream:

- The outer async iterable is closed.
- The inner iterator returned by `aiter()` is closed.
- Both `aclose()` and synchronous `close()` are supported.
- Synchronous cleanup runs in a worker thread and is bounded by
  `LeaseConfig.upstream_cleanup_timeout` (2 seconds by default).

Pass `close_source=False` when the caller owns the generator and will close it elsewhere.
Do not leave an iterator unclosed: an abandoned generator can hold its slot until the
lease TTL expires.

## WebSockets

For a WebSocket session, keep the lease open for the entire connection:

```python
from fastapi import WebSocket
from fastapi_stream_lease import StreamLeaseLost


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, user_id: str = Depends(current_user_id)):
    await websocket.accept()
    try:
        async with manager.lease(user_id) as lease:
            while True:
                message = await websocket.receive_text()
                await websocket.send_text(message)
    except StreamLeaseLost:
        await websocket.close(code=1013, reason="Stream lease lost")
```

The context manager renews the lease while it is open. Handle normal disconnects in your
route as usual. A renewal failure raises `StreamLeaseLost` and stops the session.

## Identity and security

Always derive `user_id` from a verified authentication dependency. Never accept a raw
query parameter or path segment as the lease identity. The value becomes part of a Redis
key and controls the per-user limit. See the
[security section](https://github.com/agustin18/fastapi-stream-lease#security-best-practices)
for the full checklist.

## Related

- [Production deployment](production.md): reverse proxy buffering and graceful shutdown.
- [Observability](observability.md): see active leases and rejections.
- [API reference](../api.md): `StreamLeaseManager.stream`, `StreamLease.wrap`, and
  `ProtectedStreamingResponse`.
