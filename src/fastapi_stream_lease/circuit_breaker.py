"""
Worker-local circuit breaker and backend failure degradation policies for Redis outages.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from math import isfinite

import redis.exceptions

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

_ALL_TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    _TRANSIENT_REDIS_ERRORS + _TRANSIENT_BUILTIN_ERRORS
)


def is_transient_error(exc: BaseException) -> bool:
    """
    Return True if an exception represents a transient network, timeout, or cluster issue.
    """
    if isinstance(exc, _NON_TRANSIENT_REDIS_ERRORS):
        return False
    return isinstance(exc, _ALL_TRANSIENT_ERRORS)


# Backwards compatibility alias for manager module
is_network_error = is_transient_error


class CircuitState(str, Enum):
    """Operational states of the worker-local circuit breaker."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class FallbackMode(str, Enum):
    """Degradation policies when the coordination backend or circuit breaker is unavailable."""

    FAIL_CLOSED = "fail_closed"
    FAIL_OPEN = "fail_open"


def coerce_fallback_mode(mode: FallbackMode | str) -> FallbackMode:
    """Coerce string or FallbackMode to FallbackMode enum, raising ValueError on unknown values."""
    if isinstance(mode, FallbackMode):
        return mode
    try:
        return FallbackMode(str(mode))
    except ValueError as exc:
        allowed = [e.value for e in FallbackMode]
        raise ValueError(
            f"Invalid fallback_mode '{mode}'. Must be strictly one of {allowed}"
        ) from exc


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """Configuration tuning for the worker-local circuit breaker."""

    failure_threshold: int = 5
    """Number of consecutive transient failures before opening the circuit."""

    recovery_timeout: float = 10.0
    """Seconds to wait before allowing probe requests in HALF_OPEN state."""

    jitter: float = 1.0
    """Maximum random jitter (in seconds) added to recovery_timeout to prevent herd probes."""

    half_open_max_probes: int = 1
    """Maximum concurrent probe requests permitted during HALF_OPEN testing."""

    def __post_init__(self) -> None:
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if not isfinite(self.recovery_timeout) or self.recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be finite and > 0")
        if not isfinite(self.jitter) or self.jitter < 0:
            raise ValueError("jitter must be finite and >= 0")
        if self.half_open_max_probes < 1:
            raise ValueError("half_open_max_probes must be >= 1")


@dataclass(frozen=True)
class BackendFailurePolicy:
    """Encapsulated failure degradation policy and circuit breaker configuration."""

    circuit_breaker: CircuitBreakerConfig | None = None
    """Optional circuit breaker settings. If None, circuit breaker is disabled."""

    fallback_mode: FallbackMode = FallbackMode.FAIL_CLOSED
    """Degradation mode when Redis fails or circuit breaker is OPEN."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "fallback_mode", coerce_fallback_mode(self.fallback_mode))


@dataclass
class CircuitBreaker:
    """
    Worker-local, memory-only state machine protecting the event loop from down backends.
    """

    config: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _consecutive_failures: int = field(default=0, init=False)
    _open_until_monotonic: float = field(default=0.0, init=False)
    _half_open_probes_in_flight: int = field(default=0, init=False)

    @property
    def state(self) -> CircuitState:
        """Current lifecycle state of the circuit breaker."""
        if self._state == CircuitState.OPEN and time.monotonic() >= self._open_until_monotonic:
            return CircuitState.HALF_OPEN
        return self._state

    @property
    def consecutive_failures(self) -> int:
        """Count of contiguous transient errors recorded."""
        return self._consecutive_failures

    def allow_request(self) -> bool:
        """
        Check whether an outbound request should proceed.

        Returns:
            bool: True if request is allowed, False if rejected fast.
        """
        current_state = self.state
        if current_state == CircuitState.CLOSED:
            return True

        if current_state == CircuitState.OPEN:
            return False

        # HALF_OPEN state
        if self._state != CircuitState.HALF_OPEN:
            self._state = CircuitState.HALF_OPEN
            self._half_open_probes_in_flight = 0

        if self._half_open_probes_in_flight < self.config.half_open_max_probes:
            self._half_open_probes_in_flight += 1
            return True

        return False

    def record_success(self) -> None:
        """Record a successful backend operation, closing the circuit if in recovery."""
        self._consecutive_failures = 0
        self._half_open_probes_in_flight = 0
        self._state = CircuitState.CLOSED
        self._open_until_monotonic = 0.0

    def record_failure(self, exc: BaseException | None = None) -> None:
        """
        Record a failed backend operation.

        Non-transient exceptions are ignored. Transient errors increment failure count
        and trigger state transitions.
        """
        if exc is not None and not is_transient_error(exc):
            return

        current_state = self.state
        if current_state == CircuitState.HALF_OPEN:
            # Probe failed; immediately trip back to OPEN with fresh timeout
            self._trip_open()
            return

        self._consecutive_failures += 1
        if self._consecutive_failures >= self.config.failure_threshold:
            self._trip_open()

    def _trip_open(self) -> None:
        jitter_val = random.uniform(0, self.config.jitter) if self.config.jitter > 0 else 0.0
        duration = self.config.recovery_timeout + jitter_val
        self._state = CircuitState.OPEN
        self._open_until_monotonic = time.monotonic() + duration
        self._half_open_probes_in_flight = 0

    def reset(self) -> None:
        """Explicitly reset circuit breaker back to initial CLOSED state."""
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._open_until_monotonic = 0.0
        self._half_open_probes_in_flight = 0
