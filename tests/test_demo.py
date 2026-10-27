from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient
from starlette.requests import Request

from demo.app import (
    SESSION_COOKIE,
    DemoConfig,
    IPConcurrencyLimiter,
    RateLimiter,
    _client_ip,
    create_app,
)

pytestmark = pytest.mark.asyncio


def session_id(client: AsyncClient) -> str:
    cookie = client.cookies.get(SESSION_COOKIE)
    assert cookie
    return cookie.split(".", 1)[0]


def make_config(**overrides: object) -> DemoConfig:
    values: dict[str, object] = {
        "lease_seconds": 2.0,
        "max_per_user": 1,
        "max_global": 2,
        "max_per_ip": 2,
        "stream_seconds": 0.2,
        "token_interval": 0.05,
        "rate_limit_per_minute": 100,
        "session_secret": "test-session-secret",
        "cookie_secure": False,
        "key_prefix": "demo_test",
    }
    values.update(overrides)
    return DemoConfig(**values)  # type: ignore[arg-type]


async def test_demo_config_hides_session_secret_from_repr():
    assert "test-session-secret" not in repr(make_config())


@pytest.fixture
async def demo_app(fake_redis):
    app = create_app(fake_redis, make_config(), close_redis=False)
    try:
        yield app
    finally:
        await app.state.manager.close(drain=False)


@pytest.fixture
async def demo_client(demo_app):
    transport = ASGITransport(app=demo_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def wait_for_count(manager, user_id, expected: int, timeout: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if await manager.get_active_count(user_id) == expected:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"Timed out waiting for active count {expected} for {user_id!r}")


async def test_index_serves_demo_and_reuses_session_cookie(demo_client):
    response = await demo_client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "FastAPI Stream Lease" in response.text
    set_cookie = response.headers.get("set-cookie", "")
    assert SESSION_COOKIE in set_cookie
    assert "HttpOnly" in set_cookie

    session = demo_client.cookies.get(SESSION_COOKIE)
    assert session
    assert "." in session

    second = await demo_client.get("/")
    assert SESSION_COOKIE not in second.headers.get("set-cookie", "")
    assert demo_client.cookies.get(SESSION_COOKIE) == session


async def test_forged_session_cookie_is_replaced(demo_client):
    response = await demo_client.get(
        "/",
        headers={"cookie": f"{SESSION_COOKIE}=forged"},
    )

    assert response.status_code == 200
    assert SESSION_COOKIE in response.headers.get("set-cookie", "")
    session = demo_client.cookies.get(SESSION_COOKIE)
    assert session and session != "forged" and "." in session


async def test_tampered_session_cookie_is_replaced(demo_app):
    transport = ASGITransport(app=demo_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.get("/")
        session = client.cookies.get(SESSION_COOKIE)
        assert session
        replacement = "0" if session[-1] != "0" else "1"
        tampered = f"{session[:-1]}{replacement}"
        client.cookies.clear()

        response = await client.get(
            "/",
            headers={"cookie": f"{SESSION_COOKIE}={tampered}"},
        )

    assert response.status_code == 200
    assert SESSION_COOKIE in response.headers.get("set-cookie", "")
    assert tampered not in response.headers.get("set-cookie", "")


async def test_security_headers_are_set(demo_client):
    response = await demo_client.get("/")

    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert "camera=()" in response.headers["permissions-policy"]
    assert "default-src 'none'" in response.headers["content-security-policy"]
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
    assert response.headers["cache-control"] == "no-store"
    assert "strict-transport-security" not in response.headers

    stats = await demo_client.get("/demo/stats")
    assert stats.headers["cache-control"] == "no-store"


async def test_hsts_header_is_set_for_secure_cookies(fake_redis):
    app = create_app(fake_redis, make_config(cookie_secure=True), close_redis=False)
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/")

    assert response.headers["strict-transport-security"] == ("max-age=31536000; includeSubDomains")
    await app.state.manager.close(drain=False)


async def test_stream_without_prior_visit_sets_cookie(demo_client):
    response = await demo_client.get("/demo/stream")

    assert response.status_code == 200
    assert demo_client.cookies.get(SESSION_COOKIE)


async def test_health_and_stats(demo_client):
    health = await demo_client.get("/healthz")
    assert health.status_code == 200
    assert health.json() == {"status": "ok"}

    stats = await demo_client.get("/demo/stats")
    assert stats.status_code == 200
    payload = stats.json()
    assert payload == {
        "active_global": 0,
        "max_global": 2,
        "max_per_user": 1,
        "lease_seconds": 2.0,
        "stream_seconds": 0.2,
    }


async def test_health_returns_503_when_redis_ping_fails(demo_client, demo_app):
    demo_app.state.redis.ping = AsyncMock(side_effect=ConnectionError("down"))

    response = await demo_client.get("/healthz")

    assert response.status_code == 503
    assert response.json() == {"status": "degraded"}


async def test_stats_cache_reuses_recent_result(demo_client, demo_app):
    calls = 0
    original = demo_app.state.manager.get_active_count

    async def counted(user_id=None):
        nonlocal calls
        calls += 1
        return await original(user_id)

    demo_app.state.manager.get_active_count = counted

    first = await demo_client.get("/demo/stats")
    second = await demo_client.get("/demo/stats")

    assert first.json() == second.json()
    assert calls == 1


async def test_stats_cache_expires(demo_client, demo_app, monkeypatch):
    calls = 0
    original = demo_app.state.manager.get_active_count

    async def counted(user_id=None):
        nonlocal calls
        calls += 1
        return await original(user_id)

    demo_app.state.manager.get_active_count = counted
    monkeypatch.setattr("demo.app.STATS_CACHE_SECONDS", 0.0)

    await demo_client.get("/demo/stats")
    await demo_client.get("/demo/stats")

    assert calls == 2


@pytest.mark.parametrize("fetch_site", ["same-origin", "none"])
async def test_stream_allows_direct_and_same_origin_requests(demo_client, fetch_site):
    response = await demo_client.get(
        "/demo/stream",
        headers={"sec-fetch-site": fetch_site},
    )

    assert response.status_code == 200


async def test_stream_blocks_cross_site_requests(demo_client):
    response = await demo_client.get(
        "/demo/stream",
        headers={"sec-fetch-site": "cross-site"},
    )

    assert response.status_code == 403
    assert response.json()["code"] == "cross_site_stream_blocked"
    assert SESSION_COOKIE not in response.headers.get("set-cookie", "")


async def test_stream_returns_synthetic_sse_and_releases_lease(demo_client, demo_app):
    await demo_client.get("/")
    session = session_id(demo_client)

    response = await demo_client.get("/demo/stream")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-stream-lease-id"]

    events = [
        json.loads(line.removeprefix("data: "))
        for line in response.text.splitlines()
        if line.startswith("data: ")
    ]
    assert len(events) == 4
    assert [event["index"] for event in events] == [0, 1, 2, 3]
    assert all(event["total"] == 4 for event in events)
    assert await demo_app.state.manager.get_active_count(session) == 0


async def test_second_stream_for_same_session_is_rejected(demo_client, demo_app):
    await demo_client.get("/")
    session = session_id(demo_client)
    manager = demo_app.state.manager

    first_task = asyncio.create_task(demo_client.get("/demo/stream"))
    try:
        await wait_for_count(manager, session, 1)
        rejected = await demo_client.get("/demo/stream")

        assert rejected.status_code == 429
        assert rejected.headers["retry-after"] == "2"
        assert rejected.json()["reason"] == "user_limit"

        first = await first_task
        assert first.status_code == 200
    finally:
        if not first_task.done():
            first_task.cancel()
            with suppress(asyncio.CancelledError):
                await first_task

    assert await manager.get_active_count(session) == 0
    allowed = await demo_client.get("/demo/stream")
    assert allowed.status_code == 200


async def test_ip_limit_blocks_another_session_from_same_ip(fake_redis):
    app = create_app(
        fake_redis,
        make_config(max_per_ip=1, max_global=2),
        close_redis=False,
    )
    transport = ASGITransport(app=app)
    manager = app.state.manager

    async with (
        AsyncClient(transport=transport, base_url="http://test") as first_client,
        AsyncClient(transport=transport, base_url="http://test") as second_client,
    ):
        await first_client.get("/")
        await second_client.get("/")
        first_session = session_id(first_client)

        first_task = asyncio.create_task(first_client.get("/demo/stream"))
        try:
            await wait_for_count(manager, first_session, 1)
            rejected = await second_client.get("/demo/stream")

            assert rejected.status_code == 429
            assert rejected.json()["code"] == "demo_ip_limit"

            first = await first_task
            assert first.status_code == 200
        finally:
            if not first_task.done():
                first_task.cancel()
                with suppress(asyncio.CancelledError):
                    await first_task

        allowed = await second_client.get("/demo/stream")
        assert allowed.status_code == 200

    await manager.close(drain=False)


async def test_global_limit_rejects_another_session(fake_redis):
    app = create_app(fake_redis, make_config(max_global=1), close_redis=False)
    transport = ASGITransport(app=app)
    manager = app.state.manager

    async with (
        AsyncClient(transport=transport, base_url="http://test") as first_client,
        AsyncClient(transport=transport, base_url="http://test") as second_client,
    ):
        await first_client.get("/")
        await second_client.get("/")
        first_session = session_id(first_client)
        second_session = session_id(second_client)
        assert first_session != second_session

        first_task = asyncio.create_task(first_client.get("/demo/stream"))
        try:
            await wait_for_count(manager, None, 1)
            rejected = await second_client.get("/demo/stream")

            assert rejected.status_code == 429
            assert rejected.json()["reason"] == "global_limit"

            first = await first_task
            assert first.status_code == 200
        finally:
            if not first_task.done():
                first_task.cancel()
                with suppress(asyncio.CancelledError):
                    await first_task

    await manager.close(drain=False)


async def test_rate_limit_rejects_burst_and_resets(fake_redis):
    now = [100.0]
    app = create_app(
        fake_redis,
        make_config(rate_limit_per_minute=1),
        clock=lambda: now[0],
        close_redis=False,
    )
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.get("/")

        first = await client.get("/demo/stream")
        assert first.status_code == 200

        rejected = await client.get("/demo/stream")
        assert rejected.status_code == 429
        assert rejected.headers["retry-after"] == "60"
        assert rejected.json()["code"] == "demo_rate_limit"

        now[0] += 61
        allowed = await client.get("/demo/stream")
        assert allowed.status_code == 200

    await app.state.manager.close(drain=False)


async def test_stats_returns_503_when_redis_is_unavailable(demo_client, demo_app):
    demo_app.state.manager.redis.eval = AsyncMock(side_effect=ConnectionError("down"))

    response = await demo_client.get("/demo/stats")

    assert response.status_code == 503
    assert response.json()["code"] == "stream_lease_backend_unavailable"


async def test_stream_returns_503_when_ip_limiter_backend_is_unavailable(
    demo_client,
    demo_app,
):
    demo_app.state.redis.eval = AsyncMock(side_effect=ConnectionError("down"))

    response = await demo_client.get("/demo/stream")

    assert response.status_code == 503
    assert response.json()["code"] == "stream_lease_backend_unavailable"


async def test_lifespan_verifies_cluster_config(fake_redis):
    app = create_app(fake_redis, make_config(), close_redis=False)

    async with app.router.lifespan_context(app):
        assert await app.state.manager.get_active_count() == 0


async def test_demo_config_from_env(monkeypatch):
    monkeypatch.setenv("DEMO_MAX_GLOBAL", "7")
    monkeypatch.setenv("DEMO_MAX_PER_USER", "2")
    monkeypatch.setenv("DEMO_MAX_PER_IP", "3")
    monkeypatch.setenv("DEMO_LEASE_SECONDS", "12.5")
    monkeypatch.setenv("DEMO_STREAM_SECONDS", "9.0")
    monkeypatch.setenv("DEMO_TOKEN_INTERVAL", "0.25")
    monkeypatch.setenv("DEMO_RATE_LIMIT_PER_MINUTE", "8")
    monkeypatch.setenv("DEMO_RATE_LIMIT_WINDOW_SECONDS", "30")
    monkeypatch.setenv("DEMO_COOKIE_SECURE", "1")
    monkeypatch.setenv("DEMO_KEY_PREFIX", "from_env_demo")
    monkeypatch.setenv("DEMO_SESSION_SECRET", "from-env-session-secret")

    config = DemoConfig.from_env()

    assert config.max_global == 7
    assert config.max_per_user == 2
    assert config.max_per_ip == 3
    assert config.lease_seconds == 12.5
    assert config.stream_seconds == 9.0
    assert config.token_interval == 0.25
    assert config.rate_limit_per_minute == 8
    assert config.rate_limit_window_seconds == 30.0
    assert config.cookie_secure is True
    assert config.key_prefix == "from_env_demo"
    assert config.session_secret == "from-env-session-secret"


async def test_demo_config_from_env_generates_session_secret(monkeypatch):
    monkeypatch.delenv("DEMO_SESSION_SECRET", raising=False)

    config = DemoConfig.from_env()

    assert config.session_secret


@pytest.mark.parametrize(
    "overrides",
    [
        {"lease_seconds": 0},
        {"stream_seconds": 0},
        {"token_interval": 0},
        {"max_per_user": 0},
        {"max_global": 0},
        {"max_per_ip": 0},
        {"rate_limit_per_minute": 0},
        {"rate_limit_window_seconds": 0},
        {"key_prefix": ""},
        {"session_secret": ""},
    ],
)
async def test_demo_config_rejects_invalid_values(overrides):
    with pytest.raises(ValueError):
        DemoConfig(**overrides)


def _request(*, client=("127.0.0.1", 1234)):
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [],
    }
    if client is not None:
        scope["client"] = client
    return Request(scope)


async def test_client_ip_uses_peer_and_unknown():
    assert _client_ip(_request()) == "127.0.0.1"
    assert _client_ip(_request(client=None)) == "unknown"


async def test_ip_concurrency_limiter_rejects_and_releases(fake_redis):
    limiter = IPConcurrencyLimiter(
        fake_redis,
        limit=2,
        key_prefix="ip_test",
        ttl_seconds=1.0,
    )
    ip = "203.0.113.20"

    assert await limiter.acquire(ip) is True
    assert await limiter.acquire(ip) is True
    assert await limiter.acquire(ip) is False

    await limiter.release(ip)
    await limiter.release(ip)
    await limiter.release(ip)

    assert await limiter.acquire(ip) is True


async def test_ip_concurrency_limiter_expires_slots(fake_redis):
    limiter = IPConcurrencyLimiter(
        fake_redis,
        limit=1,
        key_prefix="ip_ttl_test",
        ttl_seconds=0.05,
    )
    ip = "203.0.113.21"

    assert await limiter.acquire(ip) is True
    assert await limiter.acquire(ip) is False
    await asyncio.sleep(0.1)

    assert await limiter.acquire(ip) is True


async def test_ip_concurrency_limiter_ignores_release_errors(fake_redis):
    limiter = IPConcurrencyLimiter(
        fake_redis,
        limit=1,
        key_prefix="ip_error_test",
        ttl_seconds=1.0,
    )
    fake_redis.eval = AsyncMock(side_effect=ConnectionError("down"))

    await limiter.release("203.0.113.22")


async def test_rate_limiter_prunes_stale_keys(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(RateLimiter, "_PRUNE_THRESHOLD", 1)
    limiter = RateLimiter(limit=10, window_seconds=10, clock=lambda: now[0])

    assert await limiter.allow("first") is True
    now[0] = 100.0
    assert await limiter.allow("second") is True

    assert "first" not in limiter._events
    assert "second" in limiter._events


class _ClosableRedis:
    def __init__(self, inner):
        self.inner = inner
        self.closed = False

    def __getattr__(self, name):
        return getattr(self.inner, name)

    async def aclose(self):
        self.closed = True


class _CloseOnlyRedis(_ClosableRedis):
    aclose = None

    async def close(self):
        self.closed = True


async def test_lifespan_closes_redis_with_aclose(fake_redis):
    proxy = _ClosableRedis(fake_redis)
    app = create_app(proxy, make_config(), close_redis=True)

    async with app.router.lifespan_context(app):
        pass

    assert proxy.closed is True


async def test_lifespan_closes_redis_with_legacy_close(fake_redis):
    proxy = _CloseOnlyRedis(fake_redis)
    app = create_app(proxy, make_config(), close_redis=True)

    async with app.router.lifespan_context(app):
        pass

    assert proxy.closed is True


async def test_lifespan_closes_redis_when_verification_fails(fake_redis):
    proxy = _ClosableRedis(fake_redis)
    app = create_app(proxy, make_config(), close_redis=True)
    app.state.manager.verify_cluster_config = AsyncMock(side_effect=RuntimeError("verify failed"))

    with pytest.raises(RuntimeError, match="verify failed"):
        async with app.router.lifespan_context(app):
            pass

    assert proxy.closed is True


async def test_lifespan_closes_redis_when_manager_close_fails(fake_redis):
    proxy = _ClosableRedis(fake_redis)
    app = create_app(proxy, make_config(), close_redis=True)
    app.state.manager.close = AsyncMock(side_effect=RuntimeError("close failed"))

    with pytest.raises(RuntimeError, match="close failed"):
        async with app.router.lifespan_context(app):
            pass

    assert proxy.closed is True
