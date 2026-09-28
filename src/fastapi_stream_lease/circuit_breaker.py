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

_NON_TRANSIENT_CONNECTION_ERRORS: tuple[type[BaseException], ...] = tuple(
    cls
    for name in (
        "AuthenticationError",
        "AuthorizationError",
        "ExternalAuthProviderError",
        "MaxConnectionsError",
    )
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
)

_REDIS_CLUSTER_EXCEPTION_CLS = getattr(redis.exceptions, "RedisClusterException", None)
_MAX_CONNECTIONS_ERROR_CLS = getattr(redis.exceptions, "MaxConnectionsError", None)


def is_transient_error(exc: BaseException) -> bool:
    """
    Return True if an exception represents a transient network, timeout, or cluster issue.
    Returns False for non-transient errors such as connection pool exhaustion, authentication,
    invalid Redis commands, cross-slot violations, or local OS errors.
    """
    if isinstance(exc, _NON_TRANSIENT_CONNECTION_ERRORS):
        return False
    if isinstance(exc, _TRANSIENT_REDIS_ERRORS):
        return True
    if isinstance(exc, _TRANSIENT_BUILTIN_ERRORS):
        return True
    if _REDIS_CLUSTER_EXCEPTION_CLS is not None and isinstance(exc, _REDIS_CLUSTER_EXCEPTION_CLS):
        if exc.__cause__ is not None:
            return is_transient_error(exc.__cause__)
        return False
    return False


def is_availability_error(exc: BaseException) -> bool:
    """
    Return True if an exception represents a backend availability or connectivity condition.

    Covers both remote transient network/timeout conditions (is_transient_error) and
    client-side connection pool exhaustion (MaxConnectionsError). These conditions
    normalize to StreamLeaseUnavailable (HTTP 503) and are retryable during renewal grace periods,
    without tripping the circuit breaker or enabling fail-open fallbacks.
    """
    if _MAX_CONNECTIONS_ERROR_CLS is not None and isinstance(exc, _MAX_CONNECTIONS_ERROR_CLS):
        return True
    return is_transient_error(exc)


# Backwards compatibility alias for availability errors
is_network_error = is_availability_error


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
        if (
            isinstance(self.failure_threshold, bool)
            or not isinstance(self.failure_threshold, int)
            or self.failure_threshold < 1
        ):
            raise ValueError("failure_threshold must be an integer >= 1")
        if (
            isinstance(self.recovery_timeout, bool)
            or not isinstance(self.recovery_timeout, (int, float))
            or not isfinite(self.recovery_timeout)
            or self.recovery_timeout <= 0
        ):
            raise ValueError("recovery_timeout must be a finite number > 0")
        if (
            isinstance(self.jitter, bool)
            or not isinstance(self.jitter, (int, float))
            or not isfinite(self.jitter)
            or self.jitter < 0
        ):
            raise ValueError("jitter must be a finite number >= 0")
        if (
            isinstance(self.half_open_max_probes, bool)
            or not isinstance(self.half_open_max_probes, int)
            or self.half_open_max_probes < 1
        ):
            raise ValueError("half_open_max_probes must be an integer >= 1")


@dataclass(frozen=True)
class BackendFailurePolicy:
    """Encapsulated failure degradation policy and circuit breaker configuration."""

    circuit_breaker: CircuitBreakerConfig | None = None
    """Optional circuit breaker settings. If None, circuit breaker is disabled."""

    fallback_mode: FallbackMode = FallbackMode.FAIL_CLOSED
    """Degradation mode when Redis fails or circuit breaker is OPEN."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "fallback_mode", coerce_fallback_mode(self.fallback_mode))
        if self.circuit_breaker is not None and not isinstance(
            self.circuit_breaker, CircuitBreakerConfig
        ):
            raise TypeError(
                "circuit_breaker must be an instance of CircuitBreakerConfig or None, "
                f"got {type(self.circuit_breaker).__name__}"
            )


class CircuitPermit:
    """
    RAII permit representing authorization to execute an outbound backend call.

    Guarantees that probe counters are cleaned up in HALF_OPEN state regardless of
    whether the operation succeeds, fails, is rejected by business limits, or is
    interrupted by task cancellation. Permits carry a recovery epoch generation token
    to ensure stale in-flight probe completions do not corrupt newer recovery generations.
    """

    __slots__ = ("_breaker", "_settled", "allowed", "generation", "is_probe")

    def __init__(
        self,
        allowed: bool,
        is_probe: bool = False,
        generation: int = 0,
        breaker: CircuitBreaker | None = None,
    ) -> None:
        self.allowed = allowed
        self.is_probe = is_probe
        self.generation = generation
        self._breaker = breaker
        self._settled = False

    def record_backend_reachable(self) -> None:
        """Mark backend reachable, resetting failure counters and closing the circuit."""
        if self._settled:
            return
        self._settled = True
        if self._breaker is not None:
            self._breaker._record_permit_success(self)

    def record_failure(self, exc: BaseException | None = None) -> None:
        """Record a backend failure; trips the breaker if transient, or releases probe if not."""
        if self._settled:
            return
        self._settled = True
        if self._breaker is not None:
            self._breaker._record_permit_failure(self, exc)

    def release(self) -> None:
        """
        Cleanup hook invoked in finally block.

        If the permit was not settled (e.g. cancelled before completion, or unhandled
        non-transient exception), releases the probe slot back to the breaker.
        """
        if not self._settled:
            self._settled = True
            if self.is_probe and self._breaker is not None:
                self._breaker._release_probe(self)


@dataclass
class CircuitBreaker:
    """
    Worker-local, memory-only state machine protecting the event loop from down backends.

    Architecture Notes:
    - Manager-Instance-Local: State is held in-memory per StreamLeaseManager instance.
      If multiple managers exist, each maintains its own independent state machine.
    - Zero Distributed Coordination: The circuit breaker does not query Redis to determine
      health, eliminating circular dependencies during Redis outages.
    - Probabilistic Herd Mitigation: half_open_max_probes limits concurrent probes per
      StreamLeaseManager instance, while random recovery jitter desynchronizes probe attempts
      across the fleet to mitigate thundering herds without distributed locks.
    - Generational Isolation: Each trip to OPEN, recovery success, or reset advances an epoch
      generation token. In-flight probes from earlier generations cannot corrupt current
      recovery state.
    """

    config: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _consecutive_failures: int = field(default=0, init=False)
    _open_until_monotonic: float = field(default=0.0, init=False)
    _half_open_probes_in_flight: int = field(default=0, init=False)
    _generation: int = field(default=0, init=False)

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

    @property
    def generation(self) -> int:
        """Current recovery epoch generation."""
        return self._generation

    def acquire_permit(self) -> CircuitPermit:
        """
        Acquire a permit to execute an outbound backend request.

        Returns:
            CircuitPermit indicating whether the request is allowed and whether it is a probe.
        """
        current_state = self.state
        if current_state == CircuitState.CLOSED:
            return CircuitPermit(
                allowed=True, is_probe=False, generation=self._generation, breaker=self
            )

        if current_state == CircuitState.OPEN:
            return CircuitPermit(
                allowed=False, is_probe=False, generation=self._generation, breaker=self
            )

        # HALF_OPEN state
        if self._state != CircuitState.HALF_OPEN:
            self._state = CircuitState.HALF_OPEN
            self._half_open_probes_in_flight = 0

        if self._half_open_probes_in_flight < self.config.half_open_max_probes:
            self._half_open_probes_in_flight += 1
            return CircuitPermit(
                allowed=True, is_probe=True, generation=self._generation, breaker=self
            )

        return CircuitPermit(
            allowed=False, is_probe=False, generation=self._generation, breaker=self
        )

    def _release_probe(self, permit: CircuitPermit | None = None) -> None:
        """Release an in-flight probe slot back to the breaker in HALF_OPEN state."""
        if permit is not None and permit.generation != self._generation:
            # Stale permit from older epoch; ignore
            return
        if self._state == CircuitState.HALF_OPEN and self._half_open_probes_in_flight > 0:
            self._half_open_probes_in_flight -= 1

    def _record_permit_success(self, permit: CircuitPermit) -> None:
        """Record success from a permit, respecting generation isolation."""
        if permit.generation != self._generation:
            # Stale permit from a previous generation; drop
            return
        self.record_success()

    def _record_permit_failure(
        self, permit: CircuitPermit, exc: BaseException | None = None
    ) -> None:
        """Record failure from a permit, respecting generation isolation."""
        if permit.generation != self._generation:
            # Stale permit from a previous generation; drop
            return
        self.record_failure(exc)

    def record_success(self) -> None:
        """Record a successful backend operation, closing the circuit if in recovery."""
        was_open_or_recovering = self._state != CircuitState.CLOSED
        self._consecutive_failures = 0
        self._half_open_probes_in_flight = 0
        self._state = CircuitState.CLOSED
        self._open_until_monotonic = 0.0
        if was_open_or_recovering:
            self._generation += 1

    def record_failure(self, exc: BaseException | None = None) -> None:
        """
        Record a failed backend operation.

        Non-transient exceptions are ignored for failure counts, but if in HALF_OPEN,
        the probe slot is freed so subsequent requests can probe.
        Transient errors increment failure count and trigger state transitions.
        """
        current_state = self.state
        if exc is not None and not is_transient_error(exc):
            if current_state == CircuitState.HALF_OPEN:
                self._release_probe()
            return

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
        self._generation += 1

    def reset(self) -> None:
        """Explicitly reset circuit breaker back to initial CLOSED state."""
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._open_until_monotonic = 0.0
        self._half_open_probes_in_flight = 0
        self._generation += 1
