from __future__ import annotations

import asyncio

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from httpx import ASGITransport, AsyncClient

from fastapi_stream_lease import StreamLeaseRejected, StreamLeaseUnavailable


@pytest.fixture
def fastapi_app(lease_manager):
    app = FastAPI()

    @app.exception_handler(StreamLeaseRejected)
    async def stream_lease_rejected_handler(request: Request, exc: StreamLeaseRejected):
        return exc.as_response()

    @app.exception_handler(StreamLeaseUnavailable)
    async def stream_lease_unavailable_handler(request: Request, exc: StreamLeaseUnavailable):
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


def test_stream_lease_unavailable_methods(monkeypatch):
    import sys

    from fastapi_stream_lease import StreamLeaseUnavailable

    exc = StreamLeaseUnavailable(retry_after=7, detail="Redis down")
    assert str(exc) == "Redis down"

    resp = exc.as_response()
    assert resp.status_code == 503
    assert resp.headers["Retry-After"] == "7"

    http_exc = exc.as_http_exception()
    assert http_exc.status_code == 503
    assert http_exc.headers["Retry-After"] == "7"
    assert http_exc.detail == "Redis down"

    monkeypatch.setitem(sys.modules, "starlette.responses", None)
    with pytest.raises(RuntimeError, match="Starlette or FastAPI must be installed"):
        exc.as_response()

    monkeypatch.setitem(sys.modules, "starlette.exceptions", None)
    with pytest.raises(RuntimeError, match="FastAPI or Starlette must be installed"):
        exc.as_http_exception()


@pytest.mark.asyncio
async def test_fastapi_unavailable_503_response(fastapi_app, lease_manager):
    from unittest.mock import AsyncMock

    lease_manager.redis.eval = AsyncMock(side_effect=ConnectionError("Host unreachable"))
    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/stream/alice")
        assert resp.status_code == 503
        assert resp.headers.get("retry-after") == "5"
        payload = resp.json()
        assert payload["code"] == "stream_lease_backend_unavailable"
        assert "temporarily unavailable" in payload["detail"]
        assert "Host unreachable" not in payload["detail"]


def test_stream_lease_base_error_not_implemented():
    from fastapi_stream_lease.exceptions import StreamLeaseError

    base_exc = StreamLeaseError()
    with pytest.raises(NotImplementedError):
        base_exc.as_response()
    with pytest.raises(NotImplementedError):
        base_exc.as_http_exception()


@pytest.mark.asyncio
async def test_as_streaming_response_and_manager_stream(lease_manager, monkeypatch):
    import sys
    from unittest.mock import patch

    async def token_gen():
        yield "token_1\n"
        yield "token_2\n"

    # 1. lease.as_streaming_response()
    lease = await lease_manager.acquire("direct_resp_user")
    resp = lease.as_streaming_response(token_gen(), media_type="text/plain")
    assert resp.status_code == 200
    assert resp.media_type == "text/plain"

    chunks = []
    async for chunk in resp.body_iterator:
        chunks.append(chunk)
    assert chunks == ["token_1\n", "token_2\n"]
    assert await lease_manager.get_active_count("direct_resp_user") == 0

    # 2. manager.stream() success
    resp2 = await lease_manager.stream("manager_stream_user", token_gen())
    assert resp2.status_code == 200
    assert resp2.media_type == "text/event-stream"
    chunks2 = []
    async for chunk in resp2.body_iterator:
        chunks2.append(chunk)
    assert chunks2 == ["token_1\n", "token_2\n"]
    assert await lease_manager.get_active_count("manager_stream_user") == 0

    # 3. manager.stream() setup error cleans up lease
    import dataclasses

    released_reasons = []
    config_with_hook = dataclasses.replace(
        lease_manager.config,
        on_released=lambda lease, reason: released_reasons.append(reason),
    )
    custom_mgr = lease_manager.__class__(lease_manager.redis, config_with_hook)
    lease_err = await custom_mgr.acquire("cleanup_user")
    with patch.object(lease_err, "as_streaming_response", side_effect=ValueError("stream error")):
        with patch.object(custom_mgr, "acquire", return_value=lease_err):
            with pytest.raises(ValueError, match="stream error"):
                await custom_mgr.stream("cleanup_user", token_gen())
    assert await custom_mgr.get_active_count("cleanup_user") == 0
    await asyncio.sleep(0.02)
    assert released_reasons == ["error"]

    # 4. as_streaming_response without starlette
    lease_no_starlette = await lease_manager.acquire("no_starlette_user")
    monkeypatch.setitem(sys.modules, "starlette.responses", None)
    with pytest.raises(RuntimeError, match="Starlette or FastAPI must be installed"):
        lease_no_starlette.as_streaming_response(token_gen())
    await lease_no_starlette.release()
