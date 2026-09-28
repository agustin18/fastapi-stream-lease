from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterable, AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import Any
from uuid import uuid4

from fastapi_stream_lease.circuit_breaker import (
    CircuitBreaker,
    CircuitPermit,
    CircuitState,
    FallbackMode,
    is_network_error,
    is_transient_error,
)
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
from fastapi_stream_lease.observability.contract import (
    LostReason,
    Operation,
    Outcome,
    TelemetryAdapter,
    classify_backend_error,
)

logger = logging.getLogger(__name__)


class StreamLeaseManager:
    """
    Coordinates distributed stream concurrency leases backed by atomic Redis Lua scripts.
    """

    def __init__(
        self,
        redis: Any,
        config: LeaseConfig | None = None,
        telemetry: TelemetryAdapter | None = None,
        metrics: Any = None,
    ) -> None:
        self.redis = redis
        self.config: LeaseConfig = config or LeaseConfig()
        self.telemetry: TelemetryAdapter | None = telemetry if telemetry is not None else metrics
        self._circuit_breaker: CircuitBreaker | None = None
        if (
            self.config.failure_policy is not None
            and self.config.failure_policy.circuit_breaker is not None
        ):
            self._circuit_breaker = CircuitBreaker(self.config.failure_policy.circuit_breaker)
        self.dispatcher = HookDispatcher(
            max_queue_size=self.config.hook_queue_size,
            sync_inline=False,
            on_drop=self._on_hook_drop,
            on_error=self._on_hook_error,
            on_queue_change=self._on_hook_queue_change,
        )

    @property
    def circuit_state(self) -> CircuitState | None:
        """Current operational state of the circuit breaker, or None if disabled."""
        if self._circuit_breaker is not None:
            return self._circuit_breaker.state
        return None

    def _on_hook_drop(self) -> None:
        if self.telemetry is not None:
            try:
                self.telemetry.record_hook_drop()
            except Exception:
                logger.exception("Telemetry record_hook_drop failed")

    def _on_hook_error(self) -> None:
        if self.telemetry is not None:
            try:
                self.telemetry.record_hook_error()
            except Exception:
                logger.exception("Telemetry record_hook_error failed")

    def _on_hook_queue_change(self, delta: int) -> None:
        if self.telemetry is not None:
            try:
                self.telemetry.record_hook_queue_change(delta)
            except Exception:
                logger.exception("Telemetry record_hook_queue_change failed")

    def _safe_record_operation(
        self,
        operation: Operation | str,
        outcome: Outcome | str,
        duration: float,
    ) -> None:
        if self.telemetry is not None:
            try:
                self.telemetry.record_operation(operation, outcome, duration)
            except Exception:
                logger.exception(
                    "Telemetry record_operation failed; continuing without altering lease semantics"
                )

    def _safe_record_lost(self, reason: LostReason | str) -> None:
        if self.telemetry is not None:
            try:
                self.telemetry.record_lost(reason)
            except Exception:
                logger.exception(
                    "Telemetry record_lost failed; continuing without altering lease semantics"
                )

    def _safe_record_backend_error(self, exc: BaseException) -> None:
        if self.telemetry is not None:
            try:
                kind = classify_backend_error(exc)
                self.telemetry.record_backend_error(kind)
            except Exception:
                logger.exception(
                    "Telemetry record_backend_error failed; continuing without "
                    "altering lease semantics"
                )

    def _safe_record_fallback(self) -> None:
        if self.telemetry is not None:
            try:
                self.telemetry.record_fallback()
            except Exception:
                logger.exception(
                    "Telemetry record_fallback failed; continuing without altering lease semantics"
                )

    @contextmanager
    def _safe_trace_operation(self, operation: Operation | str) -> Iterator[Any]:
        span_cm: Any = None
        if self.telemetry is not None:
            try:
                span_cm = self.telemetry.trace_operation(operation)
            except Exception:
                logger.exception(
                    "Telemetry trace_operation failed; continuing without altering lease semantics"
                )
                span_cm = None

        if span_cm is None:
            yield None
            return

        try:
            span = span_cm.__enter__()
        except Exception:
            logger.exception("Telemetry span enter failed")
            yield None
            return

        try:
            yield span
        except BaseException as exc:
            try:
                span_cm.__exit__(type(exc), exc, exc.__traceback__)
            except Exception:
                logger.exception("Telemetry span exit failed")
            raise
        else:
            try:
                span_cm.__exit__(None, None, None)
            except Exception:
                logger.exception("Telemetry span exit failed")

    @property
    def metrics(self) -> Any:
        """Backwards compatibility property alias for self.telemetry."""
        return self.telemetry

    @metrics.setter
    def metrics(self, value: Any) -> None:
        self.telemetry = value

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

        # Circuit breaker fast-path check
        permit: CircuitPermit | None = None
        if self._circuit_breaker is not None:
            permit = self._circuit_breaker.acquire_permit()
            if not permit.allowed:
                duration = 0.0
                if self.config.effective_failure_policy.fallback_mode == FallbackMode.FAIL_OPEN:
                    self._safe_record_fallback()
                    self._safe_record_operation(Operation.ACQUIRE, Outcome.FALLBACK, duration)
                    logger.warning(
                        "Circuit breaker is %s; fallback_mode=FAIL_OPEN allows fallback lease %s",
                        self._circuit_breaker.state.value,
                        lease_id,
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

                self._safe_record_operation(Operation.ACQUIRE, Outcome.BACKEND_ERROR, duration)
                logger.warning(
                    "Circuit breaker is %s; fast-failing acquire for user %s",
                    self._circuit_breaker.state.value,
                    user_id,
                )
                raise StreamLeaseUnavailable(
                    detail=(
                        f"Circuit breaker is {self._circuit_breaker.state.value.upper()}: "
                        "Redis backend unavailable"
                    ),
                    retry_after=self.config.retry_after_seconds,
                )

        with self._safe_trace_operation(Operation.ACQUIRE):
            try:
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
                    if permit is not None:
                        permit.record_failure(exc)
                    duration = time.monotonic() - start_monotonic
                    if is_network_error(exc):
                        self._safe_record_backend_error(exc)
                        self.dispatcher.dispatch(self.config.on_backend_error, exc)
                        if is_transient_error(exc) and self.config.fail_open:
                            self._safe_record_fallback()
                            self._safe_record_operation(
                                Operation.ACQUIRE, Outcome.FALLBACK, duration
                            )
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
                        self._safe_record_operation(
                            Operation.ACQUIRE, Outcome.BACKEND_ERROR, duration
                        )
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

                # Backend call succeeded: Redis is reachable and executed the script.
                if permit is not None:
                    permit.record_backend_reachable()

                code = int(result)
                duration = time.monotonic() - start_monotonic
                if code == 2:
                    self._safe_record_operation(Operation.ACQUIRE, Outcome.REJECTED, duration)
                    self.dispatcher.dispatch(self.config.on_rejected, user_id, "user_limit")
                    raise StreamLeaseRejected(reason="user_limit")
                if code == 3:
                    self._safe_record_operation(Operation.ACQUIRE, Outcome.REJECTED, duration)
                    self.dispatcher.dispatch(self.config.on_rejected, user_id, "global_limit")
                    raise StreamLeaseRejected(reason="global_limit")
                if code != 1:
                    raise RuntimeError(f"Unexpected stream lease acquisition return code: {code}")

                self._safe_record_operation(Operation.ACQUIRE, Outcome.SUCCESS, duration)

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
            finally:
                if permit is not None:
                    permit.release()

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
            # Backend call succeeded: Redis is reachable and executed the renewal script.
            if self._circuit_breaker is not None:
                self._circuit_breaker.record_success()

            duration = time.monotonic() - start_monotonic
            success = int(result) == 1
            outcome = Outcome.SUCCESS if success else Outcome.REVOKED
            self._safe_record_operation(Operation.RENEW, outcome, duration)
            return success
        except Exception as exc:
            duration = time.monotonic() - start_monotonic
            if is_network_error(exc):
                if is_transient_error(exc) and self._circuit_breaker is not None:
                    self._circuit_breaker.record_failure(exc)
                self._safe_record_backend_error(exc)
                self._safe_record_operation(Operation.RENEW, Outcome.BACKEND_ERROR, duration)
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
            if self._circuit_breaker is not None:
                self._circuit_breaker.record_success()
            duration = time.monotonic() - start_monotonic
            self._safe_record_operation(Operation.RELEASE, Outcome.SUCCESS, duration)
        except Exception as exc:
            duration = time.monotonic() - start_monotonic
            if is_network_error(exc):
                if is_transient_error(exc) and self._circuit_breaker is not None:
                    self._circuit_breaker.record_failure(exc)
                self._safe_record_backend_error(exc)
                self._safe_record_operation(Operation.RELEASE, Outcome.BACKEND_ERROR, duration)
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
        permit: CircuitPermit | None = None
        if self._circuit_breaker is not None:
            permit = self._circuit_breaker.acquire_permit()
            if not permit.allowed:
                raise StreamLeaseUnavailable(
                    detail=(
                        f"Circuit breaker is {self._circuit_breaker.state.value.upper()}: "
                        "Redis backend unavailable"
                    ),
                    retry_after=self.config.retry_after_seconds,
                )
        target_key = (
            self.config.user_key(user_id) if user_id is not None else self.config.global_key
        )
        try:
            try:
                count = await self.redis.eval(COUNT_SCRIPT, 1, target_key)
            except Exception as exc:
                if permit is not None:
                    permit.record_failure(exc)
                if is_network_error(exc):
                    self._safe_record_backend_error(exc)
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
            if permit is not None:
                permit.record_backend_reachable()
            return int(count)
        finally:
            if permit is not None:
                permit.release()

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

        with self._safe_trace_operation(Operation.VERIFY_CONFIG):
            existing_data: dict[str, Any] | None = None
            for attempt in range(retry_attempts):
                try:
                    # Atomic canonical registration: only sets if key does not exist, no TTL
                    registered = await self.redis.set(config_key, fingerprint_json, nx=True)
                    if registered:
                        duration = time.monotonic() - start_monotonic
                        self._safe_record_operation(
                            Operation.VERIFY_CONFIG, Outcome.SUCCESS, duration
                        )
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
                        self._safe_record_backend_error(exc)
                        self._safe_record_operation(
                            Operation.VERIFY_CONFIG, Outcome.BACKEND_ERROR, duration
                        )
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
                self._safe_record_operation(
                    Operation.VERIFY_CONFIG, Outcome.BACKEND_ERROR, duration
                )
                msg = (
                    f"Unable to establish or read canonical cluster configuration on '{config_key}'"
                )
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
                self._safe_record_operation(Operation.VERIFY_CONFIG, Outcome.REJECTED, duration)
                msg = (
                    f"Cluster configuration mismatch on key '{config_key}': "
                    f"worker has {fingerprint}, but cluster registered {existing_data}. "
                    f"Mismatches: {mismatches}"
                )
                if strict:
                    raise ConfigurationMismatchError(msg, existing_data, fingerprint)
                logger.warning(msg)
                return False

            self._safe_record_operation(Operation.VERIFY_CONFIG, Outcome.SUCCESS, duration)
            return True
