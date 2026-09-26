"""Run with STREAM_DEMO_TOKEN=local-secret uvicorn examples.sse_demo:app."""

from __future__ import annotations

import asyncio
import hmac
import os
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader

from fastapi_stream_lease import (
    LeaseConfig,
    StreamLeaseManager,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)

api_key_header = APIKeyHeader(name="X-API-Key")
redis_client = redis.from_url(
    os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
    socket_timeout=2.0,
    socket_connect_timeout=2.0,
)
manager = StreamLeaseManager(redis_client, LeaseConfig(max_per_user=2, max_global=10))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Verify cluster configuration consistency at startup
    await manager.verify_cluster_config(strict=True)
    yield
    # Gracefully drain background tasks and close Redis client
    await manager.close(drain=True, timeout=5.0)
    await redis_client.aclose()


app = FastAPI(lifespan=lifespan)


async def authenticated_user(api_key: str = Depends(api_key_header)) -> str:
    expected = os.environ.get("STREAM_DEMO_TOKEN")
    if not expected or not hmac.compare_digest(api_key, expected):
        raise HTTPException(status_code=401, detail="Invalid API key")
    return "demo-user"


@app.exception_handler(StreamLeaseRejected)
async def rejected(request: Request, exc: StreamLeaseRejected):
    return exc.as_response()


@app.exception_handler(StreamLeaseUnavailable)
async def unavailable(request: Request, exc: StreamLeaseUnavailable):
    return exc.as_response()


@app.get("/stream")
async def stream(user_id: str = Depends(authenticated_user)):
    lease = await manager.acquire(user_id)

    async def events():
        for index in range(5):
            yield f"data: event {index}\n\n"
            await asyncio.sleep(2)

    return StreamingResponse(lease.wrap(events()), media_type="text/event-stream")
