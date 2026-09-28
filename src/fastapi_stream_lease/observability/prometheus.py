"""
Prometheus metrics adapter for fastapi-stream-lease with strictly bounded cardinality.
"""

from __future__ import annotations

import logging
from contextlib import AbstractContextManager, nullcontext
from typing import Any

from fastapi_stream_lease.observability.contract import (
    CIRCUIT_STATE_NUMERIC,
    DEFAULT_DURATION_BUCKETS,
    BackendErrorKind,
    CircuitState,
    LostReason,
    Operation,
    Outcome,
    coerce_backend_error_kind,
    coerce_circuit_state,
    coerce_lost_reason,
    coerce_operation,
    coerce_outcome,
)

logger = logging.getLogger(__name__)


def require_prometheus_client() -> Any:
    """Verify that prometheus_client is installed or raise an actionable ImportError."""
    try:
        import prometheus_client

        return prometheus_client
    except ImportError as exc:
        raise ImportError(
            "The 'prometheus-client' package is required to use PrometheusMetrics. "
            "Install it via: pip install 'fastapi-stream-lease[prometheus]'"
        ) from exc


class PrometheusMetrics:
    """
    Adapter that registers and collects strictly bounded Prometheus metrics
    for stream lease operations, latencies, and failures.

    Note: PrometheusMetrics is application-scoped. Create a single instance per CollectorRegistry
    and share it across all StreamLeaseManager instances.
    """

    def __init__(self, registry: Any = None) -> None:
        prom = require_prometheus_client()
        self.registry = registry or prom.REGISTRY

        self.operations_total = prom.Counter(
            "fastapi_stream_lease_operations_total",
            "Total number of stream lease operations by type and outcome.",
            ["operation", "outcome"],
            registry=self.registry,
        )

        self.operation_duration_seconds = prom.Histogram(
            "fastapi_stream_lease_operation_duration_seconds",
            "Latency distribution of stream lease operations in seconds.",
            ["operation"],
            buckets=DEFAULT_DURATION_BUCKETS,
            registry=self.registry,
        )

        self.lost_total = prom.Counter(
            "fastapi_stream_lease_lost_total",
            "Total number of active stream leases lost unexpectedly.",
            ["reason"],
            registry=self.registry,
        )

        self.backend_errors_total = prom.Counter(
            "fastapi_stream_lease_backend_errors_total",
            "Total number of transient backend errors encountered with Redis.",
            ["kind"],
            registry=self.registry,
        )

        self.fallback_total = prom.Counter(
            "fastapi_stream_lease_fallback_total",
            "Total number of fallback emergency leases granted when fail_open=True.",
            registry=self.registry,
        )

        self.hook_dropped_total = prom.Counter(
            "fastapi_stream_lease_hook_dropped_total",
            "Total number of lifecycle telemetry hooks dropped due to queue backpressure.",
            registry=self.registry,
        )

        self.hook_errors_total = prom.Counter(
            "fastapi_stream_lease_hook_errors_total",
            "Total number of lifecycle telemetry hooks that raised exceptions.",
            registry=self.registry,
        )

        self.hook_queue_depth = prom.Gauge(
            "fastapi_stream_lease_hook_queue_depth",
            "Current number of queued lifecycle hooks in the background dispatcher.",
            registry=self.registry,
        )

        self.circuit_state = prom.Gauge(
            "fastapi_stream_lease_circuit_state",
            (
                "Last observed state of the worker-local circuit breaker during manager "
                "activity (0=closed, 1=half_open, 2=open)."
            ),
            ["scope"],
            registry=self.registry,
        )

        self.short_circuited_total = prom.Counter(
            "fastapi_stream_lease_short_circuited_total",
            (
                "Total backend calls prevented from reaching Redis because the circuit "
                "breaker denied a permit."
            ),
            ["operation", "state", "scope"],
            registry=self.registry,
        )
        self.circuit_short_circuited_total = self.short_circuited_total

    def record_operation(
        self,
        operation: Operation | str,
        outcome: Outcome | str,
        duration: float,
    ) -> None:
        """Record an operation count and duration histogram sample with strict enum validation."""
        op_enum = coerce_operation(operation)
        out_enum = coerce_outcome(outcome)

        self.operations_total.labels(operation=op_enum.value, outcome=out_enum.value).inc()
        if duration >= 0:
            self.operation_duration_seconds.labels(operation=op_enum.value).observe(duration)

    def record_lost(self, reason: LostReason | str) -> None:
        """Record an unexpected lease loss event with strict enum validation."""
        r_enum = coerce_lost_reason(reason)
        self.lost_total.labels(reason=r_enum.value).inc()

    def record_backend_error(self, kind: BackendErrorKind | str) -> None:
        """Record a classified backend Redis error with strict enum validation."""
        k_enum = coerce_backend_error_kind(kind)
        self.backend_errors_total.labels(kind=k_enum.value).inc()

    def record_fallback(self) -> None:
        """Record a fallback lease activation."""
        self.fallback_total.inc()

    def record_hook_drop(self) -> None:
        """Increment count of dropped telemetry hooks monotonically."""
        self.hook_dropped_total.inc()

    def record_hook_error(self) -> None:
        """Increment count of hook exceptions monotonically."""
        self.hook_errors_total.inc()

    def record_hook_queue_change(self, delta: int) -> None:
        """Adjust current pending queue depth of the dispatcher by delta."""
        if delta > 0:
            self.hook_queue_depth.inc(float(delta))
        elif delta < 0:
            self.hook_queue_depth.dec(float(-delta))

    def record_circuit_state(
        self,
        state: CircuitState | str,
        scope: str = "default",
    ) -> None:
        """Record the current circuit breaker state on the Prometheus gauge."""
        c_state = coerce_circuit_state(state)
        self.circuit_state.labels(scope=str(scope)).set(float(CIRCUIT_STATE_NUMERIC[c_state]))

    def record_short_circuit(
        self,
        operation: Operation | str = Operation.ACQUIRE,
        state: CircuitState | str = CircuitState.OPEN,
        scope: str = "default",
    ) -> None:
        """Increment the short-circuited backend operations counter."""
        op_enum = coerce_operation(operation)
        st_enum = coerce_circuit_state(state)
        self.short_circuited_total.labels(
            operation=op_enum.value,
            state=st_enum.value,
            scope=str(scope),
        ).inc()

    def trace_operation(self, operation: Operation | str) -> AbstractContextManager[Any]:
        """No-op context manager for metrics-only Prometheus adapter."""
        return nullcontext()
