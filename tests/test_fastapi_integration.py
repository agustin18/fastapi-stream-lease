from __future__ import annotations

import asyncio

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from httpx import ASGITransport, AsyncClient

from fastapi_stream_lease import (
    StreamLeaseManager,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)
from fastapi_stream_lease.lease import ProtectedStreamingResponse


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
        expected_retry_after = str(int(lease_manager.config.lease_seconds))
        assert rejected_resp.headers.get("retry-after") == expected_retry_after
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


@pytest.mark.asyncio
async def test_manager_stream_close_source_forwarding(lease_manager) -> None:
    """Verifies manager.stream forwards close_source to StreamingResponse body iterator."""
    import contextlib

    closed = False

    async def token_gen():
        nonlocal closed
        try:
            yield "token1"
            yield "token2"
        finally:
            closed = True

    resp = await lease_manager.stream("forward_user", token_gen(), close_source=True)
    async with contextlib.aclosing(resp.body_iterator):
        async for chunk in resp.body_iterator:
            if chunk == "token1":
                break

    assert closed is True
    assert await lease_manager.get_active_count("forward_user") == 0


@pytest.mark.asyncio
async def test_fastapi_real_asgi_client_disconnect_determinism(fastapi_app, lease_manager) -> None:
    """CRITICAL P1 AUDIT TEST (UP-03):
    Verifies that a real ASGI client disconnect triggers deterministic upstream cleanup
    and releases the Redis lease immediately without requiring manual aclosing()
    or waiting for garbage collection.
    """
    first_token_sent = asyncio.Event()
    never_set_event = asyncio.Event()

    close_started = asyncio.Event()
    close_gate = asyncio.Event()
    close_completed = asyncio.Event()

    release_started = asyncio.Event()
    release_gate = asyncio.Event()
    release_completed = asyncio.Event()

    orig_mgr_release = lease_manager.release

    async def gated_mgr_release(lease):
        release_started.set()
        await release_gate.wait()
        await orig_mgr_release(lease)
        release_completed.set()

    lease_manager.release = gated_mgr_release

    class UpstreamLLMStream:
        def __aiter__(self):
            return self

        async def __anext__(self):
            first_token_sent.set()
            await never_set_event.wait()
            raise StopAsyncIteration

        async def aclose(self):
            close_started.set()
            await close_gate.wait()
            close_completed.set()

    @fastapi_app.get("/stream_real_disconnect")
    async def stream_disconnect_endpoint():
        return await lease_manager.stream("user_asgi_disc", UpstreamLLMStream(), close_source=True)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "path": "/stream_real_disconnect",
        "raw_path": b"/stream_real_disconnect",
        "query_string": b"",
        "headers": [],
    }

    client_disconnected = asyncio.Event()
    req_sent = False

    async def receive():
        nonlocal req_sent
        if not req_sent:
            req_sent = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await client_disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        pass

    app_task = asyncio.create_task(fastapi_app(scope, receive, send))
    await first_token_sent.wait()

    # Trigger client disconnect while upstream is still waiting indefinitely
    client_disconnected.set()

    # Upstream cleanup must start and app_task must not complete until close_gate is opened
    await close_started.wait()
    assert not app_task.done(), "app_task finished before upstream cleanup completed!"

    close_gate.set()
    await close_completed.wait()

    # Redis release must start and app_task must not complete until release_gate is opened
    await release_started.wait()
    assert not app_task.done(), "app_task finished before Redis release completed!"

    release_gate.set()
    await release_completed.wait()

    from contextlib import suppress

    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(app_task, timeout=5.0)

    assert await lease_manager.get_active_count("user_asgi_disc") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cleanup_mode",
    [
        "no_aclose",
        "non_awaitable",
        "raises_exception",
        "raises_cancelled",
    ],
)
async def test_protected_streaming_response_cleanup_variants(cleanup_mode: str) -> None:
    """Verifies ProtectedStreamingResponse safely handles missing,
    non-awaitable, erroring, and cancelled aclose calls.
    """
    from fastapi_stream_lease import ProtectedStreamingResponse

    async def dummy_gen():
        yield b"chunk1"

    resp = ProtectedStreamingResponse(dummy_gen())

    class MockIter:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    mock_iter = MockIter()
    if cleanup_mode == "non_awaitable":
        mock_iter.aclose = lambda: None  # type: ignore[attr-defined]
    elif cleanup_mode == "raises_exception":

        async def failing():
            raise RuntimeError("cleanup failed")

        mock_iter.aclose = failing  # type: ignore[attr-defined]
    elif cleanup_mode == "raises_cancelled":

        async def cancelling():
            raise asyncio.CancelledError()

        mock_iter.aclose = cancelling  # type: ignore[attr-defined]

    resp.body_iterator = mock_iter

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "method": "GET",
        "headers": [],
    }

    req_sent = False

    async def receive():
        nonlocal req_sent
        if not req_sent:
            req_sent = True
            return {"type": "http.request", "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message):
        pass

    await resp(scope, receive, send)


@pytest.mark.asyncio
async def test_protected_streaming_response_cleans_unreleased_lease(fake_redis) -> None:
    """Verifies that ProtectedStreamingResponse explicitly releases an unreleased lease."""
    manager = StreamLeaseManager(fake_redis)
    lease = await manager.acquire("unreleased_user")

    async def empty_gen():
        if False:
            yield b""

    resp = ProtectedStreamingResponse(empty_gen())
    resp.lease = lease

    scope = {"type": "http", "method": "GET", "headers": []}

    async def receive():
        return {"type": "http.disconnect"}

    async def send(msg):
        pass

    assert not lease._is_released
    await resp(scope, receive, send)
    assert lease._is_released is True
    await manager.close()


@pytest.mark.asyncio
async def test_protected_streaming_response_awaits_pending_release_task(fake_redis) -> None:
    """Verifies that ProtectedStreamingResponse awaits a pending background release task."""
    manager = StreamLeaseManager(fake_redis)
    lease = await manager.acquire("pending_release_user")

    async def empty_gen():
        if False:
            yield b""

    resp = ProtectedStreamingResponse(empty_gen())
    resp.lease = lease

    release_finished = False

    async def slow_release():
        nonlocal release_finished
        await asyncio.sleep(0.05)
        release_finished = True

    lease._is_released = True
    release_task = asyncio.create_task(slow_release())
    lease._release_task = release_task

    scope = {"type": "http", "method": "GET", "headers": []}

    async def receive():
        return {"type": "http.disconnect"}

    async def send(msg):
        pass

    await resp(scope, receive, send)
    assert release_finished is True
    await manager.close()


@pytest.mark.asyncio
async def test_register_exception_handlers_fastapi(lease_manager) -> None:
    """Verifies register_exception_handlers correctly binds HTTP 429 and 503 responses."""
    from fastapi_stream_lease import register_exception_handlers

    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/rejected")
    async def rejected_endpoint():
        raise StreamLeaseRejected("user_limit", retry_after=7)

    @app.get("/unavailable")
    async def unavailable_endpoint():
        raise StreamLeaseUnavailable(retry_after=12)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp_rej = await client.get("/rejected")
        assert resp_rej.status_code == 429
        assert resp_rej.headers["retry-after"] == "7"
        assert resp_rej.json()["code"] == "stream_connection_limit"
        assert resp_rej.json()["reason"] == "user_limit"

        resp_unav = await client.get("/unavailable")
        assert resp_unav.status_code == 503
        assert resp_unav.headers["retry-after"] == "12"
        assert resp_unav.json()["code"] == "stream_lease_backend_unavailable"


@pytest.mark.asyncio
async def test_manager_register_exception_handlers_ergonomics(lease_manager) -> None:
    """Verifies manager.register_exception_handlers ergonomics method."""
    app = FastAPI()
    lease_manager.register_exception_handlers(app)

    @app.get("/rejected_via_mgr")
    async def rejected_endpoint():
        raise StreamLeaseRejected("global_limit", retry_after=3)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/rejected_via_mgr")
        assert resp.status_code == 429
        assert resp.headers["retry-after"] == "3"
        assert resp.json()["reason"] == "global_limit"


def test_register_exception_handlers_invalid_app() -> None:
    """Verifies register_exception_handlers validates app type."""
    from fastapi_stream_lease import register_exception_handlers

    with pytest.raises(TypeError, match="must be a FastAPI or Starlette application"):
        register_exception_handlers(object())


@pytest.mark.asyncio
async def test_include_lease_headers_opt_in_behavior(fake_redis) -> None:
    """Verifies include_lease_headers opt-in, override, and limit headers."""
    from fastapi_stream_lease import LeaseConfig, StreamLeaseManager

    # Default: include_lease_headers is False
    mgr_default = StreamLeaseManager(fake_redis, LeaseConfig(max_per_user=2, max_global=50))
    lease = await mgr_default.acquire("user_default_hdr")

    async def gen():
        yield b"chunk"

    resp_no_hdr = lease.as_streaming_response(gen())
    assert "x-stream-lease-id" not in resp_no_hdr.headers
    assert "x-stream-lease-user-limit" not in resp_no_hdr.headers
    assert "x-stream-lease-global-limit" not in resp_no_hdr.headers
    await lease.release()
    await mgr_default.close()

    # Opted-in via LeaseConfig: include_lease_headers is True
    mgr_opted_in = StreamLeaseManager(
        fake_redis,
        LeaseConfig(max_per_user=3, max_global=100, include_lease_headers=True),
    )
    lease2 = await mgr_opted_in.acquire("user_opted_hdr")
    resp_with_hdr = lease2.as_streaming_response(
        gen(),
        headers={"custom-header": "test-val"},
    )
    assert resp_with_hdr.headers["custom-header"] == "test-val"
    assert resp_with_hdr.headers["x-stream-lease-id"] == str(lease2.lease_id)
    assert resp_with_hdr.headers["x-stream-lease-user-limit"] == "3"
    assert resp_with_hdr.headers["x-stream-lease-global-limit"] == "100"

    # Explicit override: include_lease_headers=False when config has True
    resp_overridden = lease2.as_streaming_response(gen(), include_lease_headers=False)
    assert "x-stream-lease-id" not in resp_overridden.headers
    await lease2.release()

    # Explicit override: include_lease_headers=True in manager.stream()
    resp_stream = await mgr_opted_in.stream("user_opted_hdr", gen(), include_lease_headers=True)
    assert "x-stream-lease-id" in resp_stream.headers
    assert resp_stream.headers["x-stream-lease-user-limit"] == "3"

    await mgr_opted_in.close()
