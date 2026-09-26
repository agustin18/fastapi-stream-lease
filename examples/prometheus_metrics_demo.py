"""
Prometheus Observability Demo with fastapi-stream-lease

Demonstrates wiring zero-dependency lifecycle hooks (`on_acquired`, `on_rejected`,
`on_lost`, `on_backend_error`) into Prometheus counters and gauges.

Requirements:
    pip install prometheus-client fastapi uvicorn redis
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

from fastapi_stream_lease import (
    LeaseConfig,
    StreamLease,
    StreamLeaseManager,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)

# Define Prometheus metrics
ACTIVE_STREAMS = Gauge(
    "fastapi_stream_leases_active", "Number of currently active streaming leases"
)
REJECTED_STREAMS = Counter(
    "fastapi_stream_lease_rejected_total",
    "Total number of rejected stream attempts",
    ["reason"],
)
LOST_STREAMS = Counter(
    "fastapi_stream_lease_lost_total",
    "Total number of streams terminated due to lease loss",
    ["reason"],
)
BACKEND_ERRORS = Counter(
    "fastapi_stream_lease_backend_errors_total",
    "Total number of Redis coordination backend errors",
    ["error_type"],
)


def on_acquired(lease: StreamLease) -> None:
    ACTIVE_STREAMS.inc()


def on_rejected(user_id: str | int, reason: str) -> None:
    REJECTED_STREAMS.labels(reason=reason).inc()


def on_released(lease: StreamLease, reason: str) -> None:
    ACTIVE_STREAMS.dec()


def on_lost(lease: StreamLease, reason: str) -> None:
    LOST_STREAMS.labels(reason=reason).inc()


def on_backend_error(exc: Exception) -> None:
    BACKEND_ERRORS.labels(error_type=type(exc).__name__).inc()


redis_client = redis.from_url(
    os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
    socket_timeout=2.0,
    socket_connect_timeout=2.0,
)
manager = StreamLeaseManager(
    redis_client,
    LeaseConfig(
        max_per_user=2,
        max_global=50,
        lease_seconds=15.0,
        on_acquired=on_acquired,
        on_rejected=on_rejected,
        on_released=on_released,
        on_lost=on_lost,
        on_backend_error=on_backend_error,
    ),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await redis_client.aclose()


app = FastAPI(title="Prometheus Metrics Demo", lifespan=lifespan)


@app.exception_handler(StreamLeaseRejected)
async def rejected_handler(request, exc: StreamLeaseRejected):
    return exc.as_response()


@app.exception_handler(StreamLeaseUnavailable)
async def unavailable_handler(request, exc: StreamLeaseUnavailable):
    return exc.as_response()


@app.get("/metrics")
def metrics():
    """Prometheus scrape endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/stream/{user_id}")
async def stream(user_id: str):
    async def sample_gen():
        for i in range(5):
            yield f"data: metric_event_{i}\n\n"
            await asyncio.sleep(0.1)

    return await manager.stream(user_id, sample_gen())
