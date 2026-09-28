from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
import redis.exceptions

from fastapi_stream_lease.circuit_breaker import (
    BackendFailurePolicy,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitPermit,
    CircuitState,
    FallbackMode,
    is_transient_error,
)
from fastapi_stream_lease.config import LeaseConfig
from fastapi_stream_lease.exceptions import (
    StreamLeaseRejected,
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
        (True, 10.0, 1.0, 1),
        (1.5, 10.0, 1.0, 1),
        (float("inf"), 10.0, 1.0, 1),
        (float("nan"), 10.0, 1.0, 1),
        (5, 0.0, 1.0, 1),
        (5, -1.0, 1.0, 1),
        (5, True, 1.0, 1),
        (5, float("inf"), 1.0, 1),
        (5, float("nan"), 1.0, 1),
        (5, 10.0, -0.5, 1),
        (5, 10.0, True, 1),
        (5, 10.0, float("inf"), 1),
        (5, 10.0, float("nan"), 1),
        (5, 10.0, 1.0, 0),
        (5, 10.0, 1.0, -1),
        (5, 10.0, 1.0, True),
        (5, 10.0, 1.0, 1.5),
        (5, 10.0, 1.0, float("inf")),
        (5, 10.0, 1.0, float("nan")),
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


@pytest.mark.parametrize("invalid_cb", ["foo", 123, True, [], {}])
def test_backend_failure_policy_invalid_circuit_breaker_type(invalid_cb: Any) -> None:
    with pytest.raises(
        TypeError, match="circuit_breaker must be an instance of CircuitBreakerConfig or None"
    ):
        BackendFailurePolicy(circuit_breaker=invalid_cb)


# ============================================================================
# Unit Tests: State Machine Transitions
# ============================================================================


def test_circuit_breaker_initial_state() -> None:
    breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=3, recovery_timeout=5.0, jitter=0.0)
    )
    assert breaker.state == CircuitState.CLOSED
    assert breaker.consecutive_failures == 0
    permit = breaker.acquire_permit()
    assert permit.allowed is True
    permit.release()


def test_circuit_breaker_ignores_non_transient_errors() -> None:
    breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=2, recovery_timeout=5.0, jitter=0.0)
    )
    # Non-transient errors: Auth, ValueError, etc.
    breaker.record_failure(redis.exceptions.AuthenticationError("Auth failed"))
    breaker.record_failure(ValueError("Bad value"))
    assert breaker.consecutive_failures == 0
    assert breaker.state == CircuitState.CLOSED
    permit = breaker.acquire_permit()
    assert permit.allowed is True
    permit.release()


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
        assert breaker.acquire_permit().allowed is False


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
    assert breaker.acquire_permit().allowed is False

    # Simulate passage of time past recovery_timeout
    with patch("time.monotonic", return_value=time.monotonic() + 1.5):
        assert breaker.state == CircuitState.HALF_OPEN
        # First probe should be allowed
        p1 = breaker.acquire_permit()
        assert p1.allowed is True
        # Second concurrent request while probe in flight should be rejected
        p2 = breaker.acquire_permit()
        assert p2.allowed is False

        # Probe succeeds
        p1.record_backend_reachable()
        p1.release()
        assert breaker.state == CircuitState.CLOSED
        assert breaker.consecutive_failures == 0
        p3 = breaker.acquire_permit()
        assert p3.allowed is True
        p3.release()


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
        p1 = breaker.acquire_permit()
        assert p1.allowed is True

        # Probe fails
        p1.record_failure(redis.exceptions.ConnectionError("still dead"))
        p1.release()
        assert breaker.state == CircuitState.OPEN
        assert breaker.acquire_permit().allowed is False


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
    permit = breaker.acquire_permit()
    assert permit.allowed is True
    permit.release()


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


def test_manager_circuit_state_property() -> None:
    mock_redis = AsyncMock()
    # Breaker disabled -> None
    mgr_no_cb = StreamLeaseManager(redis=mock_redis)
    assert mgr_no_cb._circuit_breaker is None
    assert mgr_no_cb.circuit_state is None

    # Breaker enabled -> CircuitState.CLOSED
    cb_cfg = CircuitBreakerConfig()
    policy = BackendFailurePolicy(circuit_breaker=cb_cfg)
    mgr_with_cb = StreamLeaseManager(redis=mock_redis, config=LeaseConfig(failure_policy=policy))
    assert mgr_with_cb._circuit_breaker is not None
    assert mgr_with_cb.circuit_state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_manager_acquire_circuit_breaker_open_fail_closed() -> None:
    mock_redis = AsyncMock()
    mock_redis.eval = AsyncMock(side_effect=redis.exceptions.ConnectionError("Redis down"))

    telemetry = DummyTelemetry()
    cb_cfg = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=10.0, jitter=0.0)
    policy = BackendFailurePolicy(fallback_mode=FallbackMode.FAIL_CLOSED, circuit_breaker=cb_cfg)
    config = LeaseConfig(failure_policy=policy)
    manager = StreamLeaseManager(redis=mock_redis, config=config, telemetry=telemetry)

    assert manager._circuit_breaker is not None

    # First attempt: network error trips failure count to 1
    with pytest.raises(StreamLeaseUnavailable):
        await manager.acquire("u1")
    assert manager.circuit_state == CircuitState.CLOSED
    assert mock_redis.eval.call_count == 1

    # Second attempt: network error trips failure count to 2 -> OPEN
    with pytest.raises(StreamLeaseUnavailable):
        await manager.acquire("u1")
    assert manager.circuit_state == CircuitState.OPEN
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
    assert manager.circuit_state == CircuitState.OPEN
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

    assert manager.circuit_state == CircuitState.OPEN

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
    assert manager.circuit_state == CircuitState.CLOSED
    assert manager._circuit_breaker.consecutive_failures == 0


@pytest.mark.asyncio
async def test_manager_renew_failure_records_circuit_breaker_failure() -> None:
    mock_redis = AsyncMock()
    mock_redis.eval = AsyncMock(return_value=1)

    cb_cfg = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=10.0, jitter=0.0)
    policy = BackendFailurePolicy(fallback_mode=FallbackMode.FAIL_CLOSED, circuit_breaker=cb_cfg)
    config = LeaseConfig(failure_policy=policy)
    manager = StreamLeaseManager(redis=mock_redis, config=config)

    lease = await manager.acquire("u1")
    assert manager._circuit_breaker.consecutive_failures == 0

    mock_redis.eval.side_effect = redis.exceptions.ConnectionError("Renew failed")
    with pytest.raises(StreamLeaseUnavailable):
        await manager.renew(lease)

    assert manager._circuit_breaker.consecutive_failures == 1


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
    assert manager._circuit_breaker.consecutive_failures == 0

    # Release network failure
    mock_redis.eval.side_effect = redis.exceptions.ConnectionError("Release failed")
    await manager.release(lease)
    assert manager._circuit_breaker.consecutive_failures == 1


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
    assert manager._circuit_breaker.consecutive_failures == 0

    # Network failure path
    mock_redis.eval.side_effect = redis.exceptions.ConnectionError("Count failed")
    with pytest.raises(StreamLeaseUnavailable):
        await manager.get_active_count("u1")
    assert manager._circuit_breaker.consecutive_failures == 1

    # Trip breaker to OPEN
    with pytest.raises(StreamLeaseUnavailable):
        await manager.get_active_count("u1")
    assert manager.circuit_state == CircuitState.OPEN

    # Breaker is OPEN -> fast-fails without network call
    eval_call_count_before = mock_redis.eval.call_count
    with pytest.raises(StreamLeaseUnavailable, match="Circuit breaker is OPEN"):
        await manager.get_active_count("u1")
    assert mock_redis.eval.call_count == eval_call_count_before


# ============================================================================
# Unit Tests: CB-01, CB-02, CB-03, CB-05 Regression Verifications
# ============================================================================


@pytest.mark.parametrize(
    "exc,expected_transient",
    [
        (redis.exceptions.MaxConnectionsError("Pool exhausted"), False),
        (
            getattr(
                redis.exceptions,
                "ExternalAuthProviderError",
                redis.exceptions.AuthenticationError,
            )("Auth failure"),
            False,
        ),
        (redis.exceptions.AuthenticationError("Auth error"), False),
        (redis.exceptions.AuthorizationError("Authz error"), False),
        (redis.exceptions.ClusterCrossSlotError("Cross slot"), False),
        (redis.exceptions.DataError("Data error"), False),
        (redis.exceptions.ResponseError("WRONGTYPE"), False),
        (FileNotFoundError("Missing file"), False),
        (PermissionError("Denied"), False),
        (ValueError("Bad value"), False),
        (redis.exceptions.ConnectionError("Connection lost"), True),
        (redis.exceptions.TimeoutError("Redis timeout"), True),
        (redis.exceptions.ReadOnlyError("Replica write"), True),
        (redis.exceptions.ClusterDownError("Cluster down"), True),
        (redis.exceptions.MasterDownError("Master down"), True),
        (redis.exceptions.SlotNotCoveredError("Uncovered slot"), True),
        (redis.exceptions.TryAgainError("Try again"), True),
        (ConnectionRefusedError("Refused"), True),
        (ConnectionResetError("Reset"), True),
        (TimeoutError("Builtin timeout"), True),
        (asyncio.TimeoutError(), True),
    ],
)
def test_error_classification_transient_vs_non_transient(
    exc: BaseException, expected_transient: bool
) -> None:
    assert is_transient_error(exc) is expected_transient


def test_redis_cluster_exception_transient_classification() -> None:
    cluster_exc_cls = getattr(redis.exceptions, "RedisClusterException", None)
    if cluster_exc_cls is None:
        pytest.skip("RedisClusterException not available in this redis-py version")

    # Bare cluster exception without cause -> non-transient
    bare_exc = cluster_exc_cls("EVAL - all keys must map to the same key slot")
    assert is_transient_error(bare_exc) is False

    # Cluster exception caused by underlying transient error -> transient
    conn_cause = ConnectionError("Connection refused")
    wrapped_conn = cluster_exc_cls("Cannot connect to cluster")
    wrapped_conn.__cause__ = conn_cause
    assert is_transient_error(wrapped_conn) is True

    # Cluster exception caused by non-transient error -> non-transient
    auth_cause = redis.exceptions.AuthenticationError("Auth failure")
    wrapped_auth = cluster_exc_cls("Auth failed on cluster node")
    wrapped_auth.__cause__ = auth_cause
    assert is_transient_error(wrapped_auth) is False


def test_circuit_permit_half_open_lifecycle() -> None:
    cb = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=1, recovery_timeout=0.01, jitter=0.0, half_open_max_probes=1
        )
    )
    cb.record_failure(redis.exceptions.ConnectionError("trip"))
    assert cb.state == CircuitState.OPEN

    time.sleep(0.015)
    assert cb.state == CircuitState.HALF_OPEN

    permit1 = cb.acquire_permit()
    assert isinstance(permit1, CircuitPermit)
    assert permit1.allowed is True
    assert permit1.is_probe is True

    # Concurrent request exceeds half_open_max_probes=1
    permit2 = cb.acquire_permit()
    assert permit2.allowed is False
    assert permit2.is_probe is False

    # Probing succeeds (backend is reachable)
    permit1.record_backend_reachable()
    permit1.release()
    assert cb.state == CircuitState.CLOSED
    assert cb.consecutive_failures == 0

    permit3 = cb.acquire_permit()
    assert permit3.allowed is True
    assert permit3.is_probe is False
    permit3.release()


def test_circuit_permit_half_open_cancellation_releases_probe() -> None:
    cb = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=1, recovery_timeout=0.01, jitter=0.0, half_open_max_probes=1
        )
    )
    cb.record_failure(redis.exceptions.ConnectionError("trip"))
    time.sleep(0.015)
    assert cb.state == CircuitState.HALF_OPEN

    permit = cb.acquire_permit()
    assert permit.allowed is True
    assert permit.is_probe is True

    # Permit is cancelled without settlement -> release() frees probe slot
    permit.release()

    # Next attempt should be allowed as probe instead of remaining blocked
    permit_next = cb.acquire_permit()
    assert permit_next.allowed is True
    assert permit_next.is_probe is True
    permit_next.release()


def test_circuit_permit_half_open_non_transient_error_releases_probe() -> None:
    cb = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=1, recovery_timeout=0.01, jitter=0.0, half_open_max_probes=1
        )
    )
    cb.record_failure(redis.exceptions.ConnectionError("trip"))
    time.sleep(0.015)
    assert cb.state == CircuitState.HALF_OPEN

    permit = cb.acquire_permit()
    assert permit.allowed is True

    # Non-transient failure (e.g. AuthenticationError)
    auth_err = redis.exceptions.AuthenticationError("Invalid credentials")
    permit.record_failure(auth_err)
    permit.release()

    # Breaker should not trip back to OPEN for non-transient, and probe slot is released
    assert cb.state == CircuitState.HALF_OPEN
    permit_next = cb.acquire_permit()
    assert permit_next.allowed is True
    permit_next.release()


def test_circuit_permit_standalone_and_noop_edges() -> None:
    # 1. Breaker is None
    permit = CircuitPermit(allowed=True, is_probe=True, breaker=None)
    permit.record_backend_reachable()
    # Double invocation when settled is a no-op
    permit.record_backend_reachable()
    permit.release()

    permit2 = CircuitPermit(allowed=True, is_probe=True, breaker=None)
    permit2.record_failure()
    # Double invocation when settled is a no-op
    permit2.record_failure()
    permit2.release()

    # 2. _release_probe when in flight is 0 is a no-op
    cb = CircuitBreaker()
    assert cb._half_open_probes_in_flight == 0
    cb._release_probe()
    assert cb._half_open_probes_in_flight == 0


def test_circuit_permit_generation_isolation_reopen_and_stale_release() -> None:
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=10.0,
            jitter=0.0,
            half_open_max_probes=1,
        )
    )
    assert breaker.generation == 0

    # Trip to OPEN (epoch 1)
    breaker.record_failure(redis.exceptions.ConnectionError("trip1"))
    assert breaker.state == CircuitState.OPEN
    assert breaker.generation == 1

    # Transition to HALF_OPEN (epoch 1)
    with patch("time.monotonic", return_value=time.monotonic() + 15.0):
        assert breaker.state == CircuitState.HALF_OPEN
        probe_a = breaker.acquire_permit()
        assert probe_a.allowed is True
        assert probe_a.is_probe is True
        assert probe_a.generation == 1
        assert breaker._half_open_probes_in_flight == 1

        # While probe_a is in flight, an error trips breaker back to OPEN (epoch 2)
        breaker.record_failure(redis.exceptions.ConnectionError("trip2"))
        assert breaker.state == CircuitState.OPEN
        assert breaker.generation == 2
        assert breaker._half_open_probes_in_flight == 0

    # Transition to HALF_OPEN (epoch 2)
    with patch("time.monotonic", return_value=time.monotonic() + 30.0):
        assert breaker.state == CircuitState.HALF_OPEN
        probe_b = breaker.acquire_permit()
        assert probe_b.allowed is True
        assert probe_b.is_probe is True
        assert probe_b.generation == 2
        assert breaker._half_open_probes_in_flight == 1

        # Probe A (from epoch 1) finishes late and calls release()
        probe_a.release()

        # Generational isolation: stale probe_a release MUST NOT decrement epoch 2 probe count!
        assert breaker._half_open_probes_in_flight == 1

        # Sibling request in epoch 2 remains rejected because probe_b is still in flight
        probe_c = breaker.acquire_permit()
        assert probe_c.allowed is False

        # Settle probe_b
        probe_b.record_backend_reachable()
        probe_b.release()
        assert breaker.state == CircuitState.CLOSED
        assert breaker.generation == 3


def test_circuit_permit_generation_isolation_sibling_probe_stale_success() -> None:
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=10.0,
            jitter=0.0,
            half_open_max_probes=2,
        )
    )
    # Trip to OPEN (epoch 1)
    breaker.record_failure(redis.exceptions.ConnectionError("trip1"))
    assert breaker.generation == 1

    # Transition to HALF_OPEN (epoch 1)
    with patch("time.monotonic", return_value=time.monotonic() + 15.0):
        assert breaker.state == CircuitState.HALF_OPEN
        probe_a = breaker.acquire_permit()
        probe_b = breaker.acquire_permit()
        assert probe_a.allowed and probe_a.is_probe
        assert probe_b.allowed and probe_b.is_probe
        assert probe_a.generation == 1
        assert probe_b.generation == 1
        assert breaker._half_open_probes_in_flight == 2

        # Probe A fails with transient error -> trips breaker back to OPEN (epoch 2)
        probe_a.record_failure(redis.exceptions.ConnectionError("probe_a failed"))
        probe_a.release()
        assert breaker.state == CircuitState.OPEN
        assert breaker.generation == 2

        # Probe B finishes late and reports backend reachable
        # Must be dropped due to stale generation!
        probe_b.record_backend_reachable()
        probe_b.release()

        # Breaker MUST remain OPEN in epoch 2, not falsely healed to CLOSED!
        assert breaker.state == CircuitState.OPEN
        assert breaker.generation == 2


def test_circuit_permit_generation_isolation_sibling_probe_stale_failure() -> None:
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=10.0,
            jitter=0.0,
            half_open_max_probes=2,
        )
    )
    # Trip to OPEN (epoch 1)
    breaker.record_failure(redis.exceptions.ConnectionError("trip1"))
    assert breaker.generation == 1

    # Transition to HALF_OPEN (epoch 1)
    with patch("time.monotonic", return_value=time.monotonic() + 15.0):
        assert breaker.state == CircuitState.HALF_OPEN
        probe_a = breaker.acquire_permit()
        probe_b = breaker.acquire_permit()
        assert probe_a.allowed and probe_a.is_probe
        assert probe_b.allowed and probe_b.is_probe
        assert probe_a.generation == 1
        assert probe_b.generation == 1

        # Probe A succeeds -> heals breaker to CLOSED (epoch 2)
        probe_a.record_backend_reachable()
        probe_a.release()
        assert breaker.state == CircuitState.CLOSED
        assert breaker.generation == 2
        assert breaker.consecutive_failures == 0

        # Probe B reports transient error late
        # Must be dropped due to stale generation!
        probe_b.record_failure(redis.exceptions.ConnectionError("probe_b late failure"))
        probe_b.release()

        # Breaker MUST remain CLOSED in epoch 2 with 0 failures!
        assert breaker.state == CircuitState.CLOSED
        assert breaker.generation == 2
        assert breaker.consecutive_failures == 0


@pytest.mark.asyncio
async def test_manager_half_open_acquire_429_heals_breaker() -> None:
    mock_redis = AsyncMock()
    # Return code 2: user limit reached
    mock_redis.eval = AsyncMock(return_value=2)

    cb_cfg = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.01, jitter=0.0)
    policy = BackendFailurePolicy(fallback_mode=FallbackMode.FAIL_CLOSED, circuit_breaker=cb_cfg)
    config = LeaseConfig(failure_policy=policy)
    manager = StreamLeaseManager(redis=mock_redis, config=config)

    # Trip breaker
    assert manager._circuit_breaker is not None
    manager._circuit_breaker.record_failure(redis.exceptions.ConnectionError("trip"))
    assert manager.circuit_state == CircuitState.OPEN

    await asyncio.sleep(0.015)
    assert manager.circuit_state == CircuitState.HALF_OPEN

    # Acquire returns 429
    with pytest.raises(StreamLeaseRejected) as exc_info:
        await manager.acquire("u1")
    assert exc_info.value.reason == "user_limit"

    # Backend was reachable! Breaker MUST be healed to CLOSED!
    assert manager.circuit_state == CircuitState.CLOSED
    assert manager._circuit_breaker.consecutive_failures == 0


@pytest.mark.asyncio
async def test_manager_half_open_acquire_cancellation_releases_probe() -> None:
    mock_redis = AsyncMock()
    mock_redis.eval = AsyncMock(side_effect=asyncio.CancelledError())

    cb_cfg = CircuitBreakerConfig(
        failure_threshold=1, recovery_timeout=0.01, jitter=0.0, half_open_max_probes=1
    )
    policy = BackendFailurePolicy(fallback_mode=FallbackMode.FAIL_CLOSED, circuit_breaker=cb_cfg)
    config = LeaseConfig(failure_policy=policy)
    manager = StreamLeaseManager(redis=mock_redis, config=config)

    # Trip breaker
    assert manager._circuit_breaker is not None
    manager._circuit_breaker.record_failure(redis.exceptions.ConnectionError("trip"))
    await asyncio.sleep(0.015)
    assert manager.circuit_state == CircuitState.HALF_OPEN

    with pytest.raises(asyncio.CancelledError):
        await manager.acquire("u1")

    # Probe slot was released! Next acquire should not be rejected as probe in flight
    mock_redis.eval = AsyncMock(return_value=1)
    lease = await manager.acquire("u2")
    assert lease.lease_id is not None
    assert manager.circuit_state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_manager_open_renew_zero_heals_breaker() -> None:
    mock_redis = AsyncMock()
    mock_redis.eval = AsyncMock(return_value=1)

    cb_cfg = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=10.0, jitter=0.0)
    policy = BackendFailurePolicy(fallback_mode=FallbackMode.FAIL_CLOSED, circuit_breaker=cb_cfg)
    config = LeaseConfig(failure_policy=policy)
    manager = StreamLeaseManager(redis=mock_redis, config=config)

    lease = await manager.acquire("u1")

    # Trip breaker to OPEN
    assert manager._circuit_breaker is not None
    manager._circuit_breaker.record_failure(redis.exceptions.ConnectionError("trip"))
    assert manager.circuit_state == CircuitState.OPEN

    # Renew returns 0 (lease expired/evicted in Redis)
    mock_redis.eval.side_effect = None
    mock_redis.eval.return_value = 0

    renew_ok = await manager.renew(lease)
    assert renew_ok is False

    # Redis answered and executed script -> breaker MUST be healed to CLOSED!
    assert manager.circuit_state == CircuitState.CLOSED
    assert manager._circuit_breaker.consecutive_failures == 0
