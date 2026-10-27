"""Small public demo for fastapi-stream-lease.

The demo intentionally uses a synthetic SSE stream so it runs without LLM
credentials. Each browser session gets one stream; the second one is rejected
until the first releases its Redis lease.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from fastapi_stream_lease import LeaseConfig, StreamLeaseManager, StreamLeaseUnavailable

SESSION_COOKIE = "stream_lease_demo_session"
SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]{16,64}$")
SESSION_MAX_AGE_SECONDS = 3600
STATS_CACHE_SECONDS = 1.0
INDEX_HTML = (Path(__file__).parent / "static" / "index.html").read_text(encoding="utf-8")
DEMO_TOKENS = (
    "stream ",
    "tokens ",
    "while ",
    "the ",
    "lease ",
    "is ",
    "held. ",
)


@dataclass(frozen=True)
class DemoConfig:
    """Runtime settings for the local demo."""

    lease_seconds: float = 25.0
    max_per_user: int = 1
    max_global: int = 20
    max_per_ip: int = 1
    stream_seconds: float = 20.0
    token_interval: float = 0.5
    rate_limit_per_minute: int = 30
    rate_limit_window_seconds: float = 60.0
    key_prefix: str = "stream_lease_demo"
    session_secret: str = field(default_factory=lambda: secrets.token_urlsafe(32), repr=False)
    cookie_secure: bool = False

    def __post_init__(self) -> None:
        if not math.isfinite(self.lease_seconds) or self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be finite and greater than 0")
        if not math.isfinite(self.stream_seconds) or self.stream_seconds <= 0:
            raise ValueError("stream_seconds must be finite and greater than 0")
        if not math.isfinite(self.token_interval) or self.token_interval <= 0:
            raise ValueError("token_interval must be finite and greater than 0")
        if self.max_per_user < 1:
            raise ValueError("max_per_user must be at least 1")
        if self.max_global < 1:
            raise ValueError("max_global must be at least 1")
        if self.max_per_ip < 1:
            raise ValueError("max_per_ip must be at least 1")
        if self.rate_limit_per_minute < 1:
            raise ValueError("rate_limit_per_minute must be at least 1")
        if not math.isfinite(self.rate_limit_window_seconds) or self.rate_limit_window_seconds <= 0:
            raise ValueError("rate_limit_window_seconds must be finite and greater than 0")
        if not self.key_prefix:
            raise ValueError("key_prefix cannot be empty")
        if not self.session_secret:
            raise ValueError("session_secret cannot be empty")

    @classmethod
    def from_env(cls) -> DemoConfig:
        return cls(
            lease_seconds=float(os.environ.get("DEMO_LEASE_SECONDS", "25")),
            max_per_user=int(os.environ.get("DEMO_MAX_PER_USER", "1")),
            max_global=int(os.environ.get("DEMO_MAX_GLOBAL", "20")),
            max_per_ip=int(os.environ.get("DEMO_MAX_PER_IP", "1")),
            stream_seconds=float(os.environ.get("DEMO_STREAM_SECONDS", "20")),
            token_interval=float(os.environ.get("DEMO_TOKEN_INTERVAL", "0.5")),
            rate_limit_per_minute=int(os.environ.get("DEMO_RATE_LIMIT_PER_MINUTE", "30")),
            rate_limit_window_seconds=float(os.environ.get("DEMO_RATE_LIMIT_WINDOW_SECONDS", "60")),
            key_prefix=os.environ.get("DEMO_KEY_PREFIX", "stream_lease_demo"),
            session_secret=os.environ.get("DEMO_SESSION_SECRET") or secrets.token_urlsafe(32),
            cookie_secure=os.environ.get("DEMO_COOKIE_SECURE", "0").lower()
            in {"1", "true", "yes", "on"},
        )


class RateLimiter:
    """In-memory sliding-window limiter for the public demo endpoint."""

    _PRUNE_THRESHOLD = 10_000

    def __init__(self, limit: int, window_seconds: float, clock: Callable[[], float]) -> None:
        self._limit = limit
        self._window_seconds = window_seconds
        self._clock = clock
        self._events: dict[str, deque[float]] = {}
        self._lock = asyncio.Lock()

    async def allow(self, key: str) -> bool:
        async with self._lock:
            now = self._clock()
            cutoff = now - self._window_seconds
            events = self._events.setdefault(key, deque())
            while events and events[0] <= cutoff:
                events.popleft()
            if len(events) >= self._limit:
                return False
            events.append(now)
            if len(self._events) > self._PRUNE_THRESHOLD:
                stale_keys = [
                    event_key
                    for event_key, event_times in self._events.items()
                    if not event_times or event_times[-1] <= cutoff
                ]
                for stale_key in stale_keys:
                    self._events.pop(stale_key, None)
            return True


class IPConcurrencyLimiter:
    """Redis-backed cap on concurrent streams from one client IP."""

    _ACQUIRE_SCRIPT = """
local current = redis.call("INCR", KEYS[1])
if current == 1 then
    redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
if current > tonumber(ARGV[1]) then
    redis.call("DECR", KEYS[1])
    return 0
end
return 1
"""

    _RELEASE_SCRIPT = """
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
if current <= 1 then
    redis.call("DEL", KEYS[1])
else
    redis.call("DECR", KEYS[1])
end
return 1
"""

    def __init__(
        self,
        redis_client: Any,
        *,
        limit: int,
        key_prefix: str,
        ttl_seconds: float,
    ) -> None:
        self._redis = redis_client
        self._limit = limit
        self._key_prefix = key_prefix
        self._ttl_ms = max(1, int(ttl_seconds * 1000))

    def _key(self, ip: str) -> str:
        return f"{self._key_prefix}:demo:ip:{ip}"

    async def acquire(self, ip: str) -> bool:
        result = await self._redis.eval(
            self._ACQUIRE_SCRIPT,
            1,
            self._key(ip),
            self._limit,
            self._ttl_ms,
        )
        return int(result) == 1

    async def release(self, ip: str) -> None:
        with suppress(Exception):
            await self._redis.eval(self._RELEASE_SCRIPT, 1, self._key(ip))


def _client_ip(request: Request) -> str:
    if request.client is not None:
        return request.client.host
    return "unknown"


def _session_signature(session_id: str, secret: str) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        session_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _signed_session(session_id: str, secret: str) -> str:
    return f"{session_id}.{_session_signature(session_id, secret)}"


def _session_for(request: Request, config: DemoConfig) -> tuple[str, bool]:
    current = request.cookies.get(SESSION_COOKIE)
    if current:
        session_id, separator, signature = current.rpartition(".")
        if (
            separator
            and SESSION_ID_RE.fullmatch(session_id)
            and hmac.compare_digest(
                signature,
                _session_signature(session_id, config.session_secret),
            )
        ):
            return session_id, False
    return secrets.token_urlsafe(24), True


def _set_session_cookie(response: Response, session_id: str, config: DemoConfig) -> None:
    response.set_cookie(
        SESSION_COOKIE,
        _signed_session(session_id, config.session_secret),
        max_age=SESSION_MAX_AGE_SECONDS,
        httponly=True,
        samesite="lax",
        secure=config.cookie_secure,
        path="/",
    )


async def _release_ip_slot(
    limiter: IPConcurrencyLimiter,
    ip: str,
    stream: AsyncIterator[str],
) -> AsyncIterator[str]:
    try:
        async for chunk in stream:
            yield chunk
    finally:
        await limiter.release(ip)


async def _synthetic_stream(config: DemoConfig) -> AsyncIterator[str]:
    total = max(1, round(config.stream_seconds / config.token_interval))
    for index in range(total):
        payload = {
            "index": index,
            "total": total,
            "token": DEMO_TOKENS[index % len(DEMO_TOKENS)],
        }
        yield f"data: {json.dumps(payload)}\n\n"
        await asyncio.sleep(config.token_interval)


def create_app(
    redis_client: Any | None = None,
    config: DemoConfig | None = None,
    *,
    clock: Callable[[], float] = time.monotonic,
    close_redis: bool = True,
) -> FastAPI:
    """Build the demo app with injectable Redis and clock for tests."""

    settings = config or DemoConfig.from_env()
    if redis_client is None:
        redis_client = redis.from_url(
            os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
            socket_timeout=2.0,
            socket_connect_timeout=2.0,
        )

    manager = StreamLeaseManager(
        redis_client,
        LeaseConfig(
            lease_seconds=settings.lease_seconds,
            max_per_user=settings.max_per_user,
            max_global=settings.max_global,
            key_prefix=settings.key_prefix,
            include_lease_headers=True,
        ),
    )
    rate_limiter = RateLimiter(
        settings.rate_limit_per_minute,
        settings.rate_limit_window_seconds,
        clock,
    )
    ip_limiter = IPConcurrencyLimiter(
        redis_client,
        limit=settings.max_per_ip,
        key_prefix=settings.key_prefix,
        ttl_seconds=max(
            60.0,
            settings.lease_seconds * 2.0,
            settings.stream_seconds + 5.0,
        ),
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            await manager.verify_cluster_config(strict=True)
            yield
        finally:
            try:
                await manager.close(drain=True, timeout=5.0)
            finally:
                if close_redis:
                    aclose = getattr(redis_client, "aclose", None)
                    if aclose is not None:
                        await aclose()
                    else:
                        await redis_client.close()

    app = FastAPI(title="FastAPI Stream Lease Demo", lifespan=lifespan)
    app.state.config = settings
    app.state.manager = manager
    app.state.rate_limiter = rate_limiter
    app.state.ip_limiter = ip_limiter
    app.state.redis = redis_client
    manager.register_exception_handlers(app)

    @app.middleware("http")
    async def security_headers(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault(
            "Permissions-Policy",
            "camera=(), microphone=(), geolocation=()",
        )
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'none'; connect-src 'self'; img-src 'self' data:; "
            "style-src 'unsafe-inline'; script-src 'unsafe-inline'; "
            "base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
        )
        if settings.cookie_secure:
            response.headers.setdefault(
                "Strict-Transport-Security",
                "max-age=31536000; includeSubDomains",
            )
        if request.url.path in {"/", "/demo/stats"}:
            response.headers["Cache-Control"] = "no-store"
        return response

    stats_cache: tuple[float, dict[str, float | int]] | None = None
    stats_lock = asyncio.Lock()

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        session_id, created = _session_for(request, settings)
        response = HTMLResponse(INDEX_HTML)
        if created:
            _set_session_cookie(response, session_id, settings)
        return response

    @app.get("/healthz")
    async def healthz(request: Request) -> JSONResponse:
        try:
            healthy = bool(
                await asyncio.wait_for(
                    request.app.state.redis.ping(),
                    timeout=1.0,
                )
            )
        except Exception:
            healthy = False
        if not healthy:
            return JSONResponse(status_code=503, content={"status": "degraded"})
        return JSONResponse({"status": "ok"})

    @app.get("/demo/stats")
    async def stats() -> dict[str, float | int]:
        nonlocal stats_cache
        async with stats_lock:
            now = time.monotonic()
            cached = stats_cache
            if cached is not None and now - cached[0] < STATS_CACHE_SECONDS:
                return cached[1]
            payload: dict[str, float | int] = {
                "active_global": await manager.get_active_count(),
                "max_global": settings.max_global,
                "max_per_user": settings.max_per_user,
                "lease_seconds": settings.lease_seconds,
                "stream_seconds": settings.stream_seconds,
            }
            stats_cache = (now, payload)
            return payload

    @app.get("/demo/stream")
    async def demo_stream(request: Request) -> Response:
        fetch_site = request.headers.get("sec-fetch-site")
        if fetch_site is not None and fetch_site not in {"same-origin", "none"}:
            return JSONResponse(
                status_code=403,
                content={
                    "code": "cross_site_stream_blocked",
                    "detail": "Cross-site stream requests are not allowed.",
                },
            )
        ip = _client_ip(request)
        if not await rate_limiter.allow(ip):
            return JSONResponse(
                status_code=429,
                content={
                    "code": "demo_rate_limit",
                    "detail": "Too many demo stream attempts; try again in a minute.",
                },
                headers={"Retry-After": str(math.ceil(settings.rate_limit_window_seconds))},
            )

        try:
            ip_allowed = await ip_limiter.acquire(ip)
        except Exception as exc:
            raise StreamLeaseUnavailable(
                detail="Demo concurrency backend is temporarily unavailable"
            ) from exc
        if not ip_allowed:
            return JSONResponse(
                status_code=429,
                content={
                    "code": "demo_ip_limit",
                    "detail": "Too many active demo streams from this network.",
                },
                headers={"Retry-After": str(math.ceil(settings.lease_seconds))},
            )

        session_id, created = _session_for(request, settings)
        try:
            response: Response = await manager.stream(
                session_id,
                _release_ip_slot(ip_limiter, ip, _synthetic_stream(settings)),
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )
            if created:
                _set_session_cookie(response, session_id, settings)
        except Exception:
            await ip_limiter.release(ip)
            raise
        return response

    return app


app = create_app()
