# fastapi-stream-lease

[![CI](https://github.com/agustin18/fastapi-stream-lease/actions/workflows/ci.yml/badge.svg)](https://github.com/agustin18/fastapi-stream-lease/actions)
[![PyPI version](https://img.shields.io/pypi/v/fastapi-stream-lease.svg)](https://pypi.org/project/fastapi-stream-lease/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

**Distributed stream and SSE concurrency lease manager for FastAPI and Starlette, backed by atomic Redis Lua scripts.**

---

## ⚡ The Problem: Why Traditional Rate Limiters Fail for Streams & LLMs

Standard rate limiters (such as `fastapi-limiter` or `slowapi`) count **requests per unit of time** (e.g. *5 requests per minute*).

While this works for standard REST APIs, it completely breaks down for **long-lived streaming connections** (Server-Sent Events, WebSockets, or streaming LLM tokens from OpenAI / Claude / Ollama):

1. **Duration Blindness:** A user can make a single request that stays open for 15 minutes, consuming a server socket the entire time. A rate limiter considers this "1 request" and allows the user to open 50 more tabs.
2. **Zombie Connection Leaks:** When mobile users switch networks or close tabs abruptly without clean TCP closure, worker connections remain blocked until timeout, causing connection pool exhaustion and denial of service.
3. **Multi-Worker Desynchronization:** In-memory concurrency limiters (like `asyncio.Semaphore`) fail across multi-process deployments (Gunicorn / Docker containers) because workers cannot share state.

```
 Traditional Rate Limiter:               fastapi-stream-lease:
 ┌───────────────────────┐               ┌──────────────────────────────────────────────┐
 │ Request 1 -> ALLOWED  │               │ Stream 1 (Active)  -> LEASE ACQUIRED (1/2)   │
 │ Request 2 -> ALLOWED  │               │ Stream 2 (Active)  -> LEASE ACQUIRED (2/2)   │
 │ (Both streams active  │               │ Stream 3 (Attempt) -> REJECTED: HTTP 429     │
 │  for 10 minutes,      │               │                       Retry-After: 5         │
 │  server sockets exhausted!)           │ Stream 1 disconnects -> LEASE RELEASED       │
 └───────────────────────┘               │ Stream 3 re-attempt -> LEASE ACQUIRED (2/2)  │
                                         └──────────────────────────────────────────────┘
```

`fastapi-stream-lease` solves this with **Sliding Distributed Leases** inside **atomic Redis Lua scripts**:
- Enforces strict concurrency limits **per user** (`max_per_user`) and **globally** (`max_global`).
- Leases automatically self-expire if the client or worker dies without clean closure (zero zombies).
- A background renewal task keeps long-running streams alive even during slow Time-To-First-Token (TTFT) pauses.
- Released immediately when the stream finishes or client disconnects.

---

## 🚀 Installation

```bash
pip install fastapi-stream-lease
```

Or using `uv`:

```bash
uv add fastapi-stream-lease
```

*(Requires Redis 5.0+ and Python 3.10+)*

---

## 💡 Quickstart

Protect an SSE or LLM streaming endpoint in just a few lines:

```python
from fastapi import FastAPI, Depends, Request
from fastapi.responses import StreamingResponse
import redis.asyncio as redis

from fastapi_stream_lease import (
    StreamLeaseManager,
    LeaseConfig,
    StreamLeaseRejected,
)

app = FastAPI()
redis_client = redis.from_url("redis://localhost:6379")

# Configure lease boundaries:
# Each user can hold at most 2 concurrent streams; cluster max is 500.
lease_manager = StreamLeaseManager(
    redis=redis_client,
    config=LeaseConfig(
        max_per_user=2,
        max_global=500,
        lease_seconds=30.0,
    ),
)


# Convert lease rejections into clean HTTP 429 Too Many Requests responses:
@app.exception_handler(StreamLeaseRejected)
async def lease_rejected_handler(request: Request, exc: StreamLeaseRejected):
    return exc.as_response()


@app.get("/api/chat/stream")
async def chat_stream(user_id: str = "user_123"):
    # 1. Acquire lease (raises StreamLeaseRejected if limit reached)
    lease = await lease_manager.acquire(user_id)

    async def token_generator():
        # Example: streaming tokens from an LLM
        for word in ["Hello", "world", "this", "is", "streamed!"]:
            yield f"data: {word}\n\n"

    # 2. Wrap generator: guarantees background auto-renewal and release on disconnect
    return StreamingResponse(
        lease.wrap(token_generator()),
        media_type="text/event-stream",
    )
```

---

## 🛠️ How It Works (Algorithmic Math)

All concurrency validations, expirations, and insertions run inside **atomic Lua scripts** on Redis:

1. **Sorted Sets (`ZSET`):** Active streams are stored in Redis `ZSET`s where the value is a unique `lease_id` and the score is the epoch expiration timestamp (`now + lease_seconds`).
2. **Atomic Eviction:** Before checking capacity, `ZREMRANGEBYSCORE` purges all expired entries in $O(\log N + M)$.
3. **Capacity Check:** `ZCARD` verifies current stream count in $O(1)$ against `max_per_user` and `max_global`.
4. **Acquisition:** If capacity permits, `ZADD` registers the lease in $O(\log N)$ and updates the key TTL.
5. **Auto-Renewal:** While the stream is active, `lease.wrap()` spawns a lightweight background worker that calls `ZADD` to advance the expiration score every `lease_seconds / 2`.
6. **Guaranteed Release:** When the stream completes or the client disconnects, `ZREM` removes the lease immediately in the `finally:` block.

---

## ⚙️ Configuration Options

Customize `LeaseConfig`:

```python
from fastapi_stream_lease import LeaseConfig

config = LeaseConfig(
    lease_seconds=30.0,  # Lease expiration window (seconds)
    max_per_user=3,  # Maximum active streams per user/key
    max_global=1000,  # Maximum active streams across the entire cluster
    key_prefix="my_app:sse",  # Custom Redis key prefix
)
```

---

## 🧪 Testing & Observability

You can inspect the live count of active streams at any time:

```python
# Active streams for a specific user:
active_user_streams = await lease_manager.get_active_count("user_123")

# Active streams across the entire cluster:
active_global_streams = await lease_manager.get_active_count()
```

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).
