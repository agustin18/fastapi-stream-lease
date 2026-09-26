from __future__ import annotations

import asyncio

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from httpx import ASGITransport, AsyncClient

from fastapi_stream_lease import StreamLeaseRejected


@pytest.fixture
def fastapi_app(lease_manager):
    app = FastAPI()

    @app.exception_handler(StreamLeaseRejected)
    async def stream_lease_rejected_handler(request: Request, exc: StreamLeaseRejected):
        return exc.as_response()

    @app.get("/stream/{user_id}")
    async def stream_endpoint(user_id: str, count: int = 3, delay: float = 0.05):
        lease = await lease_manager.acquire(user_id)

        async def token_generator():
            for i in range(count):
                yield f"data: token_{i}\n\n"
                await asyncio.sleep(delay)

        return StreamingResponse(
            lease.wrap(token_generator()),
            media_type="text/event-stream",
        )

    return app


@pytest.mark.asyncio
async def test_fastapi_streaming_success(fastapi_app):
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/stream/alice?count=3&delay=0.01")
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]
        assert "data: token_0" in response.text
        assert "data: token_2" in response.text


@pytest.mark.asyncio
async def test_fastapi_concurrency_limit_429(fastapi_app, lease_manager):
    transport = ASGITransport(app=fastapi_app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Start 2 slow concurrent streams for user bob (max_per_user=2)
        async def fetch_stream():
            res = await client.get("/stream/bob?count=5&delay=0.1")
            return res.status_code

        # Launch 2 background requests
        task1 = asyncio.create_task(fetch_stream())
        task2 = asyncio.create_task(fetch_stream())

        # Give them a moment to acquire leases
        await asyncio.sleep(0.05)
        assert await lease_manager.get_active_count("bob") == 2

        # 3rd request should immediately be rejected with 429
        rejected_resp = await client.get("/stream/bob?count=1")
        assert rejected_resp.status_code == 429
        assert rejected_resp.headers.get("retry-after") == "5"
        payload = rejected_resp.json()
        assert payload["code"] == "stream_connection_limit"
        assert payload["reason"] == "user_limit"
        assert payload["detail"] == "User stream limit reached"

        # Wait for the first two to finish
        code1 = await task1
        code2 = await task2
        assert code1 == 200
        assert code2 == 200

        # Now that they finished, bob's active count is 0 and new request succeeds
        assert await lease_manager.get_active_count("bob") == 0
        new_resp = await client.get("/stream/bob?count=1&delay=0.01")
        assert new_resp.status_code == 200


def test_stream_lease_rejected_as_http_exception():
    exc = StreamLeaseRejected(reason="global_limit", retry_after=10)
    assert exc.detail == "Global stream limit reached"

    http_exc = exc.as_http_exception()
    assert http_exc.status_code == 429
    assert http_exc.headers["Retry-After"] == "10"
    assert http_exc.detail == "Global stream limit reached"


def test_stream_lease_missing_starlette_raises_runtime_error(monkeypatch):
    import sys

    exc = StreamLeaseRejected(reason="user_limit")

    monkeypatch.setitem(sys.modules, "starlette.responses", None)
    with pytest.raises(RuntimeError, match="Starlette or FastAPI must be installed"):
        exc.as_response()

    monkeypatch.setitem(sys.modules, "starlette.exceptions", None)
    with pytest.raises(RuntimeError, match="FastAPI or Starlette must be installed"):
        exc.as_http_exception()
