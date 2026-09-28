"""
Telemetry contract definitions, strict cardinality coercion, and TelemetryAdapter protocol.
"""

from __future__ import annotations

import asyncio
from contextlib import AbstractContextManager
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from ..circuit_breaker import CircuitState as CircuitState

DEFAULT_DURATION_BUCKETS: tuple[float, ...] = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
)


class Operation(str, Enum):
    """Operation types supported in stream lease telemetry."""

    ACQUIRE = "acquire"
    COUNT = "count"
    RENEW = "renew"
    RELEASE = "release"
    VERIFY_CONFIG = "verify_config"


class Outcome(str, Enum):
    """Result outcome of a stream lease operation."""

    SUCCESS = "success"
    REJECTED = "rejected"
    BACKEND_ERROR = "backend_error"
    REVOKED = "revoked"
    FALLBACK = "fallback"


class LostReason(str, Enum):
    """Reason classifications for lost stream leases."""

    REDIS_REVOKED = "redis_revoked"
    BACKEND_TIMEOUT = "backend_timeout"
    UNEXPECTED_ERROR = "unexpected_error"


class BackendErrorKind(str, Enum):
    """Category classification for Redis network and cluster errors."""

    CONNECTION = "connection"
    TIMEOUT = "timeout"
    READONLY = "readonly"
    CLUSTERDOWN = "clusterdown"
    UNKNOWN = "unknown"


def coerce_operation(operation: Operation | str) -> Operation:
    """Coerce input to Operation enum, raising ValueError on unknown values."""
    if isinstance(operation, Operation):
        return operation
    try:
        return Operation(str(operation))
    except ValueError as exc:
        allowed = [e.value for e in Operation]
        raise ValueError(
            f"Invalid operation '{operation}'. Must be strictly one of {allowed}"
        ) from exc


def coerce_outcome(outcome: Outcome | str) -> Outcome:
    """Coerce input to Outcome enum, raising ValueError on unknown values."""
    if isinstance(outcome, Outcome):
        return outcome
    try:
        return Outcome(str(outcome))
    except ValueError as exc:
        allowed = [e.value for e in Outcome]
        raise ValueError(f"Invalid outcome '{outcome}'. Must be strictly one of {allowed}") from exc


def coerce_lost_reason(reason: LostReason | str) -> LostReason:
    """Coerce input to LostReason enum, raising ValueError on unknown values."""
    if isinstance(reason, LostReason):
        return reason
    try:
        return LostReason(str(reason))
    except ValueError as exc:
        allowed = [e.value for e in LostReason]
        raise ValueError(
            f"Invalid lost reason '{reason}'. Must be strictly one of {allowed}"
        ) from exc


def coerce_backend_error_kind(kind: BackendErrorKind | str) -> BackendErrorKind:
    """Coerce input to BackendErrorKind enum, raising ValueError on unknown values."""
    if isinstance(kind, BackendErrorKind):
        return kind
    try:
        return BackendErrorKind(str(kind))
    except ValueError as exc:
        allowed = [e.value for e in BackendErrorKind]
        raise ValueError(
            f"Invalid backend error kind '{kind}'. Must be strictly one of {allowed}"
        ) from exc


def classify_backend_error(exc: BaseException) -> BackendErrorKind:
    """Classify an exception into a cardinality-safe BackendErrorKind."""
    name = type(exc).__name__
    if "Connection" in name or isinstance(exc, (ConnectionError, ConnectionResetError)):
        return BackendErrorKind.CONNECTION
    if "Timeout" in name or isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return BackendErrorKind.TIMEOUT
    if "ReadOnly" in name:
        return BackendErrorKind.READONLY
    if "ClusterDown" in name or "MasterDown" in name or "SlotNotCovered" in name:
        return BackendErrorKind.CLUSTERDOWN
    return BackendErrorKind.UNKNOWN


CIRCUIT_STATE_NUMERIC: dict[CircuitState, int] = {
    CircuitState.CLOSED: 0,
    CircuitState.HALF_OPEN: 1,
    CircuitState.OPEN: 2,
}


def coerce_circuit_state(state: CircuitState | str) -> CircuitState:
    """Coerce input to CircuitState enum, raising ValueError on unknown values."""
    if isinstance(state, CircuitState):
        return state
    try:
        return CircuitState(str(state))
    except ValueError as exc:
        allowed = [e.value for e in CircuitState]
        raise ValueError(
            f"Invalid circuit state '{state}'. Must be strictly one of {allowed}"
        ) from exc


@runtime_checkable
class TelemetryAdapter(Protocol):
    """
    Formal protocol defining the base contract for stream lease telemetry and metrics adapters.
    Adapters must be failure-isolated by the manager and should perform low-latency,
    non-blocking work.
    """

    def record_operation(
        self,
        operation: Operation | str,
        outcome: Outcome | str,
        duration: float,
    ) -> None:
        """Record an operation execution count and its latency in seconds."""
        ...

    def record_lost(self, reason: LostReason | str) -> None:
        """Record an unexpected lease loss event with classified cause."""
        ...

    def record_backend_error(self, kind: BackendErrorKind | str) -> None:
        """Record a transient backend Redis network or cluster error."""
        ...

    def record_fallback(self) -> None:
        """Record an emergency fallback lease granted when fail_open=True."""
        ...

    def record_hook_drop(self) -> None:
        """Record a dropped lifecycle hook due to dispatcher queue saturation."""
        ...

    def record_hook_error(self) -> None:
        """Record an exception raised within a lifecycle hook callback."""
        ...

    def record_hook_queue_change(self, delta: int) -> None:
        """Record an incremental adjustment to the pending lifecycle hook queue depth."""
        ...

    def trace_operation(self, operation: Operation | str) -> AbstractContextManager[Any]:
        """Optionally emit an open distributed tracing span for an operation."""
        ...


@runtime_checkable
class CircuitBreakerTelemetry(Protocol):
    """Protocol for telemetry adapters supporting circuit breaker metrics and scoping."""

    def record_circuit_state(
        self,
        state: CircuitState | str,
        scope: str = "default",
    ) -> None:
        """Record the current circuit breaker state (0=closed, 1=half_open, 2=open)."""
        ...

    def record_short_circuit(
        self,
        operation: Operation | str = Operation.ACQUIRE,
        state: CircuitState | str = CircuitState.OPEN,
        scope: str = "default",
    ) -> None:
        """Record an operation prevented from reaching Redis because the circuit
        breaker denied a permit.
        """
        ...


@runtime_checkable
class StreamLeaseTelemetry(TelemetryAdapter, CircuitBreakerTelemetry, Protocol):
    """Unified telemetry protocol supporting core lease metrics and circuit breaker telemetry."""

    ...
