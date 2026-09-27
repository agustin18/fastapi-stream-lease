from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import redis.exceptions

from fastapi_stream_lease.config import LeaseConfig
from fastapi_stream_lease.dispatcher import HookDispatcher
from fastapi_stream_lease.exceptions import (
    ConfigurationMismatchError,
    StreamLeaseLost,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)
from fastapi_stream_lease.lease import StreamLease, _safe_uncancel
from fastapi_stream_lease.lua import (
    ACQUIRE_SCRIPT,
    COUNT_SCRIPT,
    RELEASE_SCRIPT,
    RENEW_SCRIPT,
)

logger = logging.getLogger(__name__)

_NON_TRANSIENT_REDIS_ERRORS: tuple[type[BaseException], ...] = tuple(
    cls
    for name in ("AuthenticationError", "AuthorizationError", "ClusterCrossSlotError")
    if (cls := getattr(redis.exceptions, name, None)) is not None
)

_TRANSIENT_REDIS_ERRORS: tuple[type[BaseException], ...] = tuple(
    cls
    for name in (
        "ConnectionError",
        "TimeoutError",
        "ReadOnlyError",
        "ClusterDownError",
        "MasterDownError",
        "SlotNotCoveredError",
        "TryAgainError",
        "ClusterError",
    )
    if (cls := getattr(redis.exceptions, name, None)) is not None
)

_TRANSIENT_BUILTIN_ERRORS: tuple[type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
    OSError,
)


def is_network_error(exc: BaseException) -> bool:
    """Return True for transient network, timeout, failover, or cluster state conditions."""
    if isinstance(exc, _NON_TRANSIENT_REDIS_ERRORS):
        return False
    if isinstance(exc, _TRANSIENT_REDIS_ERRORS):
        return True
    if isinstance(exc, _TRANSIENT_BUILTIN_ERRORS):
        return True
    return False


class StreamLeaseManager:
    """
    Coordinates distributed stream concurrency leases backed by atomic Redis Lua scripts.
    """

    def __init__(
        self,
        redis: Any,
        config: LeaseConfig | None = None,
        metrics: Any = None,
    ) -> None:
        self.redis = redis
        self.config: LeaseConfig = config or LeaseConfig()
        self.metrics = metrics
        self.dispatcher = HookDispatcher(
            max_queue_size=self.config.hook_queue_size,
            sync_inline=False,
        )

    async def __aenter__(self) -> StreamLeaseManager:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def drain(self, timeout: float = 5.0) -> None:
        """Wait for all queued lifecycle callbacks to complete execution."""
        await self.dispatcher.drain(timeout=timeout)

    async def close(self, drain: bool = True, timeout: float = 5.0) -> None:
        """Shut down the background lifecycle hook dispatcher."""
        await self.dispatcher.close(drain=drain, timeout=timeout)

    async def acquire(self, user_id: str | int) -> StreamLease:
        """
        Acquire a new stream lease for the given user.

        Raises:
            StreamLeaseRejected: If user or global concurrency limits are exceeded.
            RuntimeError: If Redis evaluation fails unexpectedly.
        """
        lease_id = uuid4().hex
        user_key = self.config.user_key(user_id)
        global_key = self.config.global_key
        start_monotonic = time.monotonic()
        try:
            result = await self.redis.eval(
                ACQUIRE_SCRIPT,
                2,
                user_key,
                global_key,
                self.config.lease_seconds,
                lease_id,
                self.config.max_per_user,
                self.config.max_global,
                self.config.redis_ttl,
            )
        except Exception as exc:
            duration = time.monotonic() - start_monotonic
            if is_network_error(exc):
                self.dispatcher.dispatch(self.config.on_backend_error, exc)
                if self.config.fail_open:
                    if self.metrics is not None:
                        self.metrics.record_fallback()
                        self.metrics.record_operation("acquire", "fallback", duration)
                    logger.warning(
                        "Redis backend unavailable during acquire; "
                        "fail_open=True allows fallback lease %s: %s",
                        lease_id,
                        exc,
                    )
                    lease = StreamLease(
                        lease_id=lease_id,
                        user_id=user_id,
                        user_key=user_key,
                        global_key=global_key,
                        manager=self,
                        created_at=time.time(),
                        created_monotonic=start_monotonic,
                    )
                    lease._is_fallback = True
                    self.dispatcher.dispatch(self.config.on_acquired, lease)
                    return lease
                if self.metrics is not None:
                    self.metrics.record_operation("acquire", "backend_error", duration)
                logger.warning(
                    "Redis backend unavailable during acquire for user %s: %s",
                    user_id,
                    exc,
                )
                raise StreamLeaseUnavailable(
                    detail="Stream lease coordination backend is temporarily unavailable",
                    retry_after=self.config.retry_after_seconds,
                ) from exc
            logger.error(
                "Execution error during stream lease acquire for user %s: %s",
                user_id,
                exc,
                exc_info=True,
            )
            raise

        code = int(result)
        duration = time.monotonic() - start_monotonic
        if code == 2:
            if self.metrics is not None:
                self.metrics.record_operation("acquire", "rejected", duration)
            self.dispatcher.dispatch(self.config.on_rejected, user_id, "user_limit")
            raise StreamLeaseRejected(reason="user_limit")
        if code == 3:
            if self.metrics is not None:
                self.metrics.record_operation("acquire", "rejected", duration)
            self.dispatcher.dispatch(self.config.on_rejected, user_id, "global_limit")
            raise StreamLeaseRejected(reason="global_limit")
        if code != 1:
            raise RuntimeError(f"Unexpected stream lease acquisition return code: {code}")

        if self.metrics is not None:
            self.metrics.record_operation("acquire", "success", duration)

        lease = StreamLease(
            lease_id=lease_id,
            user_id=user_id,
            user_key=user_key,
            global_key=global_key,
            manager=self,
            created_at=time.time(),
            created_monotonic=start_monotonic,
        )
        self.dispatcher.dispatch(self.config.on_acquired, lease)
        return lease

    async def renew(self, lease: StreamLease) -> bool:
        """
        Renew an active lease, extending its TTL in Redis.

        Returns:
            bool: True if successfully extended, False if the lease expired or was evicted in Redis.
        Raises:
            StreamLeaseUnavailable: If the Redis backend is unreachable.
        """
        check_global = 1 if self.config.max_global > 0 else 0
        start_monotonic = time.monotonic()
        try:
            result = await self.redis.eval(
                RENEW_SCRIPT,
                2,
                lease.user_key,
                lease.global_key,
                lease.lease_id,
                self.config.lease_seconds,
                self.config.redis_ttl,
                check_global,
            )
            duration = time.monotonic() - start_monotonic
            success = int(result) == 1
            if self.metrics is not None:
                outcome = "success" if success else "revoked"
                self.metrics.record_operation("renew", outcome, duration)
            return success
        except Exception as exc:
            duration = time.monotonic() - start_monotonic
            if is_network_error(exc):
                if self.metrics is not None:
                    self.metrics.record_operation("renew", "backend_error", duration)
                self.dispatcher.dispatch(self.config.on_backend_error, exc)
                logger.warning(
                    "Network error renewing stream lease %s: %s",
                    lease.lease_id,
                    exc,
                )
                raise StreamLeaseUnavailable(
                    detail="Stream lease coordination backend is temporarily unavailable",
                    retry_after=self.config.retry_after_seconds,
                ) from exc
            logger.error(
                "Execution error renewing stream lease %s: %s",
                lease.lease_id,
                exc,
                exc_info=True,
            )
            raise

    async def release(self, lease: StreamLease) -> None:
        """
        Release an active lease immediately from Redis.
        """
        start_monotonic = time.monotonic()
        try:
            await self.redis.eval(
                RELEASE_SCRIPT,
                2,
                lease.user_key,
                lease.global_key,
                lease.lease_id,
            )
            duration = time.monotonic() - start_monotonic
            if self.metrics is not None:
                self.metrics.record_operation("release", "success", duration)
        except Exception as exc:
            duration = time.monotonic() - start_monotonic
            if is_network_error(exc):
                if self.metrics is not None:
                    self.metrics.record_operation("release", "backend_error", duration)
                self.dispatcher.dispatch(self.config.on_backend_error, exc)
                logger.warning("Network error releasing stream lease %s: %s", lease.lease_id, exc)
            else:
                logger.error(
                    "Execution error releasing stream lease %s: %s",
                    lease.lease_id,
                    exc,
                    exc_info=True,
                )

    async def get_active_count(self, user_id: str | int | None = None) -> int:
        """
        Return the current number of active (non-expired) streams for a user or globally.
        """
        target_key = (
            self.config.user_key(user_id) if user_id is not None else self.config.global_key
        )
        try:
            count = await self.redis.eval(COUNT_SCRIPT, 1, target_key)
            return int(count)
        except Exception as exc:
            if is_network_error(exc):
                self.dispatcher.dispatch(self.config.on_backend_error, exc)
                logger.warning(
                    "Network error querying active stream count for %s: %s",
                    target_key,
                    exc,
                )
                raise StreamLeaseUnavailable(
                    detail="Stream lease coordination backend is temporarily unavailable",
                    retry_after=self.config.retry_after_seconds,
                ) from exc
            logger.error(
                "Execution error querying active stream count for %s: %s",
                target_key,
                exc,
                exc_info=True,
            )
            raise

    @asynccontextmanager
    async def lease(
        self, user_id: str | int, renew_interval: float | None = None
    ) -> AsyncIterator[StreamLease]:
        """
        Context manager for acquiring and safely releasing a stream lease for scoped
        executions (such as WebSockets, background tasks, or pub/sub loops).

        For HTTP StreamingResponse (SSE / LLM tokens), use `lease = await acquire()`
        and `return StreamingResponse(lease.wrap(...))` instead.

        Example:
            async with lease_manager.lease(user_id=42) as lease:
                while True:
                    msg = await websocket.receive_text()
                    ...
        """
        stream_lease = await self.acquire(user_id)
        renew_task: asyncio.Task[None] | None = None
        lease_lost = asyncio.Event()
        reason = "completed"
        try:
            renew_task, lease_lost = stream_lease._start_auto_renew(renew_interval)
            yield stream_lease
        except asyncio.CancelledError:
            if lease_lost.is_set():
                reason = "lost"
                _safe_uncancel()
                raise StreamLeaseLost(stream_lease.lease_id) from None
            reason = "cancelled"
            raise
        except Exception:
            reason = "error"
            raise
        finally:
            if renew_task is not None:
                await stream_lease._stop_auto_renew(renew_task)
            await stream_lease.release(reason=reason)

    async def stream(
        self,
        user_id: str | int,
        stream: AsyncIterable[Any],
        media_type: str = "text/event-stream",
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        auto_renew: bool = True,
        renew_interval: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Acquire a lease and return a protected Starlette/FastAPI StreamingResponse.

        Provides 1-line streaming integration:
            @app.get("/stream")
            async def stream_view(user_id: str = Depends(auth)):
                return await manager.stream(user_id, token_generator())

        If lease acquisition fails (e.g. 429 Too Many Requests or 503 Unavailable),
        an exception is raised immediately. If stream setup fails before returning,
        the acquired lease is safely released to prevent lingering ghost leases.
        """
        lease = await self.acquire(user_id)
        try:
            return lease.as_streaming_response(
                stream,
                media_type=media_type,
                status_code=status_code,
                headers=headers,
                auto_renew=auto_renew,
                renew_interval=renew_interval,
                **kwargs,
            )
        except Exception:
            await lease.release(reason="error")
            raise

    async def verify_cluster_config(
        self,
        strict: bool = False,
        retry_attempts: int = 3,
        retry_delay: float = 0.1,
    ) -> bool:
        """
        Verify that this worker's configuration matches cluster configuration in Redis.

        If no configuration is registered yet, this worker's fingerprint is recorded atomically.
        If a mismatch is detected:
          - If strict=True: raises ConfigurationMismatchError.
          - If strict=False: logs a warning and returns False.

        Transient network errors during Sentinel election or network blips are retried up to
        `retry_attempts` times (spaced by `retry_delay`). If the coordination backend remains
        unavailable past all retry attempts:
          - If strict=True: raises StreamLeaseUnavailable (fail-fast on k8s CrashLoopBackOff).
          - If strict=False: logs a warning, triggers on_backend_error, and returns False.

        Returns:
            bool: True if configuration matches or was registered; False otherwise.
        """
        if retry_attempts < 1:
            raise ValueError("retry_attempts must be at least 1")
        if retry_delay < 0:
            raise ValueError("retry_delay must be non-negative")

        start_monotonic = time.monotonic()
        fingerprint = self.config.fingerprint_dict()
        fingerprint_json = json.dumps(fingerprint, sort_keys=True)
        config_key = self.config.config_key

        existing_data: dict[str, Any] | None = None
        for attempt in range(retry_attempts):
            try:
                # Atomic canonical registration: only sets if key does not exist (NX=True), no TTL
                registered = await self.redis.set(config_key, fingerprint_json, nx=True)
                if registered:
                    duration = time.monotonic() - start_monotonic
                    if self.metrics is not None:
                        self.metrics.record_operation("verify_config", "success", duration)
                    return True

                existing = await self.redis.get(config_key)
                if existing is not None:
                    if isinstance(existing, bytes):
                        existing = existing.decode("utf-8")
                    existing_data = json.loads(existing)
                    break
            except Exception as exc:
                if is_network_error(exc):
                    if attempt < retry_attempts - 1:
                        await asyncio.sleep(retry_delay)
                        continue
                    duration = time.monotonic() - start_monotonic
                    if self.metrics is not None:
                        self.metrics.record_operation("verify_config", "backend_error", duration)
                    self.dispatcher.dispatch(self.config.on_backend_error, exc)
                    if strict:
                        raise StreamLeaseUnavailable(
                            detail=(
                                "Stream lease coordination backend is unavailable during "
                                "cluster config verification"
                            ),
                            retry_after=self.config.retry_after_seconds,
                        ) from exc
                    logger.warning("Network error verifying cluster configuration: %s", exc)
                    return False
                logger.error(
                    "Execution error verifying cluster configuration on %s: %s",
                    config_key,
                    exc,
                    exc_info=True,
                )
                raise

        duration = time.monotonic() - start_monotonic
        if existing_data is None:
            if self.metrics is not None:
                self.metrics.record_operation("verify_config", "backend_error", duration)
            msg = f"Unable to establish or read canonical cluster configuration on '{config_key}'"
            if strict:
                raise StreamLeaseUnavailable(
                    detail=msg,
                    retry_after=self.config.retry_after_seconds,
                )
            logger.warning(msg)
            return False

        mismatches = {
            k: (v, existing_data.get(k))
            for k, v in fingerprint.items()
            if existing_data.get(k) != v
        }

        if mismatches:
            if self.metrics is not None:
                self.metrics.record_operation("verify_config", "rejected", duration)
            msg = (
                f"Cluster configuration mismatch on key '{config_key}': "
                f"worker has {fingerprint}, but cluster registered {existing_data}. "
                f"Mismatches: {mismatches}"
            )
            if strict:
                raise ConfigurationMismatchError(msg, existing_data, fingerprint)
            logger.warning(msg)
            return False

        if self.metrics is not None:
            self.metrics.record_operation("verify_config", "success", duration)
        return True
