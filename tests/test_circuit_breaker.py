from __future__ import annotations

import time
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
import redis.exceptions

from fastapi_stream_lease.circuit_breaker import (
    BackendFailurePolicy,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    FallbackMode,
)
from fastapi_stream_lease.config import LeaseConfig
from fastapi_stream_lease.exceptions import (
    StreamLeaseUnavailable,
)
from fastapi_stream_lease.manager import StreamLeaseManager
from fastapi_stream_lease.observability.contract import Operation, Outcome, TelemetryAdapter


class DummyTelemetry(TelemetryAdapter):
    def __init__(self) -> None:
        self.operations: list[tuple[Operation, Outcome, float]] = []
        self.fallbacks: int = 0
        self.backend_errors: list[str] = []

    def record_operation(
        self, operation: Operation | str, outcome: Outcome | str, duration: float
    ) -> None:
        self.operations.append((Operation(operation), Outcome(outcome), duration))

    def record_fallback(self) -> None:
        self.fallbacks += 1

    def record_backend_error(self, kind: str) -> None:
        self.backend_errors.append(kind)


# ============================================================================
# Unit Tests: Config Validation
# ============================================================================


@pytest.mark.parametrize(
    "failure_threshold,recovery_timeout,jitter,half_open_probes",
    [
        (0, 10.0, 1.0, 1),
        (-1, 10.0, 1.0, 1),
        (5, 0.0, 1.0, 1),
        (5, -1.0, 1.0, 1),
        (5, float("inf"), 1.0, 1),
        (5, float("nan"), 1.0, 1),
        (5, 10.0, -0.5, 1),
        (5, 10.0, float("inf"), 1),
        (5, 10.0, 1.0, 0),
        (5, 10.0, 1.0, -1),
    ],
)
def test_circuit_breaker_config_validation_invalid(
    failure_threshold: int,
    recovery_timeout: float,
    jitter: float,
    half_open_probes: int,
) -> None:
    with pytest.raises(ValueError):
        CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            jitter=jitter,
            half_open_max_probes=half_open_probes,
        )


def test_circuit_breaker_config_valid() -> None:
    cfg = CircuitBreakerConfig(
        failure_threshold=3,
        recovery_timeout=5.0,
        jitter=0.5,
        half_open_max_probes=2,
    )
    assert cfg.failure_threshold == 3
    assert cfg.recovery_timeout == 5.0
    assert cfg.jitter == 0.5
    assert cfg.half_open_max_probes == 2


@pytest.mark.parametrize(
    "fallback_mode",
    [FallbackMode.FAIL_CLOSED, FallbackMode.FAIL_OPEN, "fail_closed", "fail_open"],
)
def test_backend_failure_policy_valid(fallback_mode: Any) -> None:
    policy = BackendFailurePolicy(fallback_mode=fallback_mode)
    assert policy.fallback_mode in (FallbackMode.FAIL_CLOSED, FallbackMode.FAIL_OPEN)


def test_backend_failure_policy_invalid_mode() -> None:
    with pytest.raises(ValueError, match="Invalid fallback_mode"):
        BackendFailurePolicy(fallback_mode="invalid_mode")  # type: ignore[arg-type]


# ============================================================================
# Unit Tests: State Machine Transitions
# ============================================================================


def test_circuit_breaker_initial_state() -> None:
    breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=3, recovery_timeout=5.0, jitter=0.0)
    )
    assert breaker.state == CircuitState.CLOSED
    assert breaker.consecutive_failures == 0
    assert breaker.allow_request() is True


def test_circuit_breaker_ignores_non_transient_errors() -> None:
    breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=2, recovery_timeout=5.0, jitter=0.0)
    )
    # Non-transient errors: Auth, ValueError, etc.
    breaker.record_failure(redis.exceptions.AuthenticationError("Auth failed"))
    breaker.record_failure(ValueError("Bad value"))
    assert breaker.consecutive_failures == 0
    assert breaker.state == CircuitState.CLOSED
    assert breaker.allow_request() is True


@pytest.mark.parametrize(
    "failures,threshold,expected_state",
    [
        (1, 3, CircuitState.CLOSED),
        (2, 3, CircuitState.CLOSED),
        (3, 3, CircuitState.OPEN),
        (5, 3, CircuitState.OPEN),
    ],
)
def test_circuit_breaker_trips_to_open_on_threshold(
    failures: int, threshold: int, expected_state: CircuitState
) -> None:
    breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=threshold, recovery_timeout=5.0, jitter=0.0)
    )
    for _ in range(failures):
        breaker.record_failure(redis.exceptions.ConnectionError("Redis down"))

    assert breaker.state == expected_state
    if expected_state == CircuitState.OPEN:
        assert breaker.allow_request() is False


def test_circuit_breaker_half_open_transition_and_probe_success() -> None:
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=1.0,
            jitter=0.0,
            half_open_max_probes=1,
        )
    )
    breaker.record_failure(redis.exceptions.ConnectionError("err1"))
    breaker.record_failure(redis.exceptions.ConnectionError("err2"))
    assert breaker.state == CircuitState.OPEN
    assert breaker.allow_request() is False

    # Simulate passage of time past recovery_timeout
    with patch("time.monotonic", return_value=time.monotonic() + 1.5):
        assert breaker.state == CircuitState.HALF_OPEN
        # First probe should be allowed
        assert breaker.allow_request() is True
        # Second concurrent request while probe in flight should be rejected
        assert breaker.allow_request() is False

        # Probe succeeds
        breaker.record_success()
        assert breaker.state == CircuitState.CLOSED
        assert breaker.consecutive_failures == 0
        assert breaker.allow_request() is True


def test_circuit_breaker_half_open_probe_failure_reopens() -> None:
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=1.0,
            jitter=0.0,
            half_open_max_probes=1,
        )
    )
    breaker.record_failure(redis.exceptions.ConnectionError("err1"))
    breaker.record_failure(redis.exceptions.ConnectionError("err2"))
    assert breaker.state == CircuitState.OPEN

    base_time = time.monotonic()
    with patch("time.monotonic", return_value=base_time + 1.5):
        assert breaker.state == CircuitState.HALF_OPEN
        assert breaker.allow_request() is True

        # Probe fails
        breaker.record_failure(redis.exceptions.ConnectionError("still dead"))
        assert breaker.state == CircuitState.OPEN
        assert breaker.allow_request() is False


def test_circuit_breaker_reset() -> None:
    breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=2, recovery_timeout=5.0, jitter=0.0)
    )
    breaker.record_failure(redis.exceptions.ConnectionError("err"))
    breaker.record_failure(redis.exceptions.ConnectionError("err"))
    assert breaker.state == CircuitState.OPEN
    breaker.reset()
    assert breaker.state == CircuitState.CLOSED
    assert breaker.consecutive_failures == 0
    assert breaker.allow_request() is True


# ============================================================================
# Integration Tests: LeaseConfig & StreamLeaseManager
# ============================================================================


def test_lease_config_failure_policy_defaults() -> None:
    cfg_closed = LeaseConfig(fail_open=False)
    assert cfg_closed.failure_policy.fallback_mode == FallbackMode.FAIL_CLOSED
    assert cfg_closed.failure_policy.circuit_breaker is None

    cfg_open = LeaseConfig(fail_open=True)
    assert cfg_open.failure_policy.fallback_mode == FallbackMode.FAIL_OPEN
    assert cfg_open.failure_policy.circuit_breaker is None

    explicit_policy = BackendFailurePolicy(
        fallback_mode=FallbackMode.FAIL_OPEN,
        circuit_breaker=CircuitBreakerConfig(failure_threshold=3),
    )
    cfg_custom = LeaseConfig(failure_policy=explicit_policy)
    assert cfg_custom.failure_policy == explicit_policy
    assert cfg_custom.fail_open is True


@pytest.mark.asyncio
async def test_manager_acquire_circuit_breaker_open_fail_closed() -> None:
    mock_redis = AsyncMock()
    mock_redis.eval = AsyncMock(side_effect=redis.exceptions.ConnectionError("Redis down"))

    telemetry = DummyTelemetry()
    cb_cfg = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=10.0, jitter=0.0)
    policy = BackendFailurePolicy(fallback_mode=FallbackMode.FAIL_CLOSED, circuit_breaker=cb_cfg)
    config = LeaseConfig(failure_policy=policy)
    manager = StreamLeaseManager(redis=mock_redis, config=config, telemetry=telemetry)

    assert manager.circuit_breaker is not None

    # First attempt: network error trips failure count to 1
    with pytest.raises(StreamLeaseUnavailable):
        await manager.acquire("u1")
    assert manager.circuit_breaker.state == CircuitState.CLOSED
    assert mock_redis.eval.call_count == 1

    # Second attempt: network error trips failure count to 2 -> OPEN
    with pytest.raises(StreamLeaseUnavailable):
        await manager.acquire("u1")
    assert manager.circuit_breaker.state == CircuitState.OPEN
    assert mock_redis.eval.call_count == 2

    # Third attempt: Breaker is OPEN -> fails fast WITHOUT touching Redis
    with pytest.raises(StreamLeaseUnavailable, match="Circuit breaker is OPEN"):
        await manager.acquire("u1")
    assert mock_redis.eval.call_count == 2  # Call count DID NOT increase!


@pytest.mark.asyncio
async def test_manager_acquire_circuit_breaker_open_fail_open() -> None:
    mock_redis = AsyncMock()
    mock_redis.eval = AsyncMock(side_effect=redis.exceptions.ConnectionError("Redis down"))

    telemetry = DummyTelemetry()
    cb_cfg = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=10.0, jitter=0.0)
    policy = BackendFailurePolicy(fallback_mode=FallbackMode.FAIL_OPEN, circuit_breaker=cb_cfg)
    config = LeaseConfig(failure_policy=policy)
    manager = StreamLeaseManager(redis=mock_redis, config=config, telemetry=telemetry)

    # 2 failures trip breaker
    lease1 = await manager.acquire("u1")
    assert lease1._is_fallback is True
    lease2 = await manager.acquire("u1")
    assert lease2._is_fallback is True
    assert manager.circuit_breaker.state == CircuitState.OPEN
    assert mock_redis.eval.call_count == 2

    # 3rd attempt: Breaker is OPEN -> generates fallback WITHOUT touching Redis!
    lease3 = await manager.acquire("u1")
    assert lease3._is_fallback is True
    assert mock_redis.eval.call_count == 2  # Zero network roundtrip!
    assert telemetry.fallbacks == 3


@pytest.mark.asyncio
async def test_golden_asymmetry_renew_never_blocked_by_open_circuit_breaker() -> None:
    mock_redis = AsyncMock()
    # Mock acquire success first, then Redis goes down, then recovers for renew
    mock_redis.eval = AsyncMock(return_value=1)

    cb_cfg = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=10.0, jitter=0.0)
    policy = BackendFailurePolicy(fallback_mode=FallbackMode.FAIL_CLOSED, circuit_breaker=cb_cfg)
    config = LeaseConfig(failure_policy=policy)
    manager = StreamLeaseManager(redis=mock_redis, config=config)

    # Acquire an active lease while Redis is healthy
    lease = await manager.acquire("u1")
    assert lease.lease_id is not None

    # Trip the circuit breaker to OPEN via acquire failures
    mock_redis.eval.side_effect = redis.exceptions.ConnectionError("Redis died")
    with pytest.raises(StreamLeaseUnavailable):
        await manager.acquire("u2")
    with pytest.raises(StreamLeaseUnavailable):
        await manager.acquire("u3")

    assert manager.circuit_breaker.state == CircuitState.OPEN

    # Breaker is OPEN. New acquire fails fast without network
    with pytest.raises(StreamLeaseUnavailable, match="Circuit breaker is OPEN"):
        await manager.acquire("u4")

    # BUT RENEWAL IS NOT BLOCKED!
    # Redis comes back alive for the renewal
    mock_redis.eval.side_effect = None
    mock_redis.eval.return_value = 1

    renew_ok = await manager.renew(lease)
    assert renew_ok is True
    # The successful renew called Redis AND healed the breaker!
    assert manager.circuit_breaker.state == CircuitState.CLOSED
    assert manager.circuit_breaker.consecutive_failures == 0


@pytest.mark.asyncio
async def test_manager_renew_failure_records_circuit_breaker_failure() -> None:
    mock_redis = AsyncMock()
    mock_redis.eval = AsyncMock(return_value=1)

    cb_cfg = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=10.0, jitter=0.0)
    policy = BackendFailurePolicy(fallback_mode=FallbackMode.FAIL_CLOSED, circuit_breaker=cb_cfg)
    config = LeaseConfig(failure_policy=policy)
    manager = StreamLeaseManager(redis=mock_redis, config=config)

    lease = await manager.acquire("u1")
    assert manager.circuit_breaker.consecutive_failures == 0

    mock_redis.eval.side_effect = redis.exceptions.ConnectionError("Renew failed")
    with pytest.raises(StreamLeaseUnavailable):
        await manager.renew(lease)

    assert manager.circuit_breaker.consecutive_failures == 1


@pytest.mark.asyncio
async def test_manager_release_with_circuit_breaker() -> None:
    mock_redis = AsyncMock()
    mock_redis.eval = AsyncMock(return_value=1)

    cb_cfg = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=10.0, jitter=0.0)
    policy = BackendFailurePolicy(fallback_mode=FallbackMode.FAIL_CLOSED, circuit_breaker=cb_cfg)
    config = LeaseConfig(failure_policy=policy)
    manager = StreamLeaseManager(redis=mock_redis, config=config)

    lease = await manager.acquire("u1")

    # Release success
    await manager.release(lease)
    assert manager.circuit_breaker.consecutive_failures == 0

    # Release network failure
    mock_redis.eval.side_effect = redis.exceptions.ConnectionError("Release failed")
    await manager.release(lease)
    assert manager.circuit_breaker.consecutive_failures == 1


@pytest.mark.asyncio
async def test_manager_get_active_count_circuit_breaker() -> None:
    mock_redis = AsyncMock()
    mock_redis.eval = AsyncMock(return_value=5)

    cb_cfg = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=10.0, jitter=0.0)
    policy = BackendFailurePolicy(fallback_mode=FallbackMode.FAIL_CLOSED, circuit_breaker=cb_cfg)
    config = LeaseConfig(failure_policy=policy)
    manager = StreamLeaseManager(redis=mock_redis, config=config)

    # Success path
    count = await manager.get_active_count("u1")
    assert count == 5
    assert manager.circuit_breaker.consecutive_failures == 0

    # Network failure path
    mock_redis.eval.side_effect = redis.exceptions.ConnectionError("Count failed")
    with pytest.raises(StreamLeaseUnavailable):
        await manager.get_active_count("u1")
    assert manager.circuit_breaker.consecutive_failures == 1

    # Trip breaker to OPEN
    with pytest.raises(StreamLeaseUnavailable):
        await manager.get_active_count("u1")
    assert manager.circuit_breaker.state == CircuitState.OPEN

    # Breaker is OPEN -> fast-fails without network call
    eval_call_count_before = mock_redis.eval.call_count
    with pytest.raises(StreamLeaseUnavailable, match="Circuit breaker is OPEN"):
        await manager.get_active_count("u1")
    assert mock_redis.eval.call_count == eval_call_count_before
