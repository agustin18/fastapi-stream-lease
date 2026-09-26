"""
OpenAI & LLM Token Streaming Demo with fastapi-stream-lease

Demonstrates protecting an AI chat streaming endpoint with 1-line lease management:
- Strictly limits concurrent AI generation streams per user.
- Automatically releases leases on completion, abort, or network disconnect.
- Protects LLM token generation budget from multi-tab abuse or runaway scrapers.

Run with:
    STREAM_DEMO_TOKEN=secret python -m uvicorn examples.openai_streaming_demo:app --port 8000
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as redis
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import APIKeyHeader

try:
    from openai import AsyncOpenAI

    _HAS_OPENAI = True
except ImportError:
    AsyncOpenAI = None  # type: ignore[assignment,misc]
    _HAS_OPENAI = False

from fastapi_stream_lease import (
    LeaseConfig,
    StreamLeaseManager,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)

logger = logging.getLogger("openai_streaming_demo")
api_key_header = APIKeyHeader(name="X-API-Key")
redis_client = redis.from_url(
    os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
    socket_timeout=2.0,
    socket_connect_timeout=2.0,
)
manager = StreamLeaseManager(
    redis_client,
    LeaseConfig(
        max_per_user=1,  # Strictly allow only 1 active LLM generation stream per user
        max_global=100,  # Cluster-wide safeguard against total LLM capacity exhaustion
        lease_seconds=30.0,
    ),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    api_key = os.environ.get("OPENAI_API_KEY")
    client: Any = None
    if _HAS_OPENAI and api_key and AsyncOpenAI is not None:
        client = AsyncOpenAI(api_key=api_key)
        logger.info("Initialized shared AsyncOpenAI client connection pool")
    app.state.openai_client = client
    yield
    if client is not None:
        await client.close()
    await redis_client.aclose()


app = FastAPI(title="LLM Streaming Protection Demo", lifespan=lifespan)


async def authenticated_user(api_key: str = Depends(api_key_header)) -> str:
    expected = os.environ.get("STREAM_DEMO_TOKEN", "local-secret")
    if not hmac.compare_digest(api_key, expected):
        raise HTTPException(status_code=401, detail="Invalid API key")
    return "user-enterprise-1"


@app.exception_handler(StreamLeaseRejected)
async def rejected_handler(request: Request, exc: StreamLeaseRejected):
    return exc.as_response()


@app.exception_handler(StreamLeaseUnavailable)
async def unavailable_handler(request: Request, exc: StreamLeaseUnavailable):
    return exc.as_response()


async def llm_token_stream(prompt: str, client: Any = None) -> AsyncIterator[str]:
    """Streams LLM tokens using the shared AsyncOpenAI client when available,

    or falls back to a simulated token stream if unconfigured.
    """
    if client is not None:
        model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
        response = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        )
        async for chunk in response:
            content = chunk.choices[0].delta.content or ""
            if content:
                yield f"data: {content}\n\n"
        yield "data: [DONE]\n\n"
        return

    # Fallback simulation when openai package or OPENAI_API_KEY is not configured
    logger.info("OPENAI_API_KEY not detected; running simulated token stream")
    tokens = [
        "Hello",
        "!",
        " I",
        " am",
        " your",
        " AI",
        " assistant",
        ".",
        " This",
        " stream",
        " is",
        " concurrency",
        "-managed",
        " by",
        " Redis",
        " leases",
        ".\n\n",
    ]
    for token in tokens:
        yield f"data: {token}\n\n"
        await asyncio.sleep(0.08)


@app.get("/v1/chat/stream")
async def chat_stream(
    request: Request,
    prompt: str = "Hello",
    user_id: str = Depends(authenticated_user),
):
    """
    Protected LLM chat completion endpoint.

    Uses `await manager.stream(...)` to acquire the lease, wrap the generator,
    and return an SSE StreamingResponse in a single, safe call.
    """
    client = getattr(request.app.state, "openai_client", None)
    return await manager.stream(
        user_id=user_id,
        stream=llm_token_stream(prompt, client=client),
        media_type="text/event-stream",
    )
