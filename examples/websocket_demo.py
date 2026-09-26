"""Run with STREAM_DEMO_TOKEN=local-secret uvicorn examples.websocket_demo:app."""

from __future__ import annotations

import hmac
import logging
import os
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, status

from fastapi_stream_lease import (
    LeaseConfig,
    StreamLeaseLost,
    StreamLeaseManager,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)

logger = logging.getLogger(__name__)

redis_client = redis.from_url(
    os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
    socket_timeout=2.0,
    socket_connect_timeout=2.0,
)
manager = StreamLeaseManager(
    redis_client,
    LeaseConfig(max_per_user=2, max_global=10, lease_seconds=15),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await redis_client.aclose()


app = FastAPI(lifespan=lifespan)


def authenticate_token(token: str | None) -> str:
    expected = os.environ.get("STREAM_DEMO_TOKEN", "local-secret")
    if not token or not hmac.compare_digest(token, expected):
        raise ValueError("Invalid authentication token")
    return "demo-user"


@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(default=""),
):
    try:
        user_id = authenticate_token(token)
    except ValueError:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Unauthorized")
        return

    try:
        async with manager.lease(user_id) as _lease:
            await websocket.accept()
            while True:
                data = await websocket.receive_text()
                await websocket.send_text(f"Echo: {data}")
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected normally by client")
    except StreamLeaseRejected:
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="Stream limit reached (429)",
        )
    except StreamLeaseUnavailable:
        await websocket.close(
            code=status.WS_1013_TRY_AGAIN_LATER,
            reason="Coordination service temporarily unavailable (503)",
        )
    except StreamLeaseLost:
        await websocket.close(
            code=status.WS_1001_GOING_AWAY,
            reason="Lease lost or revoked during session",
        )
