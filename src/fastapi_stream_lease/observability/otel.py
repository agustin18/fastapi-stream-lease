"""
OpenTelemetry metrics and selective tracing adapter for fastapi-stream-lease.
"""

from __future__ import annotations

import logging
from contextlib import AbstractContextManager, nullcontext
from typing import Any, cast

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


def require_opentelemetry() -> Any:
    """Verify that opentelemetry-api is installed or raise an actionable ImportError."""
    try:
        import opentelemetry
        from opentelemetry import metrics, trace

        return opentelemetry, metrics, trace
    except ImportError as exc:
        raise ImportError(
            "The 'opentelemetry-api' package is required to use OpenTelemetryMetrics. "
            "Install it via: pip install 'fastapi-stream-lease[otel]'"
        ) from exc


class OpenTelemetryMetrics:
    """
    Adapter that records OpenTelemetry metrics and emits selective spans
    for high-value operations (acquire and verify_config) while avoiding
    trace span flooding during frequent lease renewals.
    """

    def __init__(
        self,
        meter_provider: Any = None,
        tracer_provider: Any = None,
        meter_name: str = "fastapi_stream_lease",
    ) -> None:
        _, metrics_module, trace_module = require_opentelemetry()

        self.meter = (
            meter_provider.get_meter(meter_name)
            if meter_provider is not None
            else metrics_module.get_meter(meter_name)
        )
        self.tracer = (
            tracer_provider.get_tracer(meter_name)
            if tracer_provider is not None
            else trace_module.get_tracer(meter_name)
        )

        self.operations_counter = self.meter.create_counter(
            "fastapi_stream_lease.operations",
            description="Total number of stream lease operations by type and outcome.",
            unit="1",
        )

        self.duration_histogram = self.meter.create_histogram(
            "fastapi_stream_lease.operation_duration",
            description="Latency distribution of stream lease operations in seconds.",
            unit="s",
            explicit_bucket_boundaries_advisory=DEFAULT_DURATION_BUCKETS,
        )

        self.lost_counter = self.meter.create_counter(
            "fastapi_stream_lease.lost",
            description="Total number of active stream leases lost unexpectedly.",
            unit="1",
        )

        self.backend_errors_counter = self.meter.create_counter(
            "fastapi_stream_lease.backend_errors",
            description="Total number of transient backend errors encountered with Redis.",
            unit="1",
        )

        self.fallback_counter = self.meter.create_counter(
            "fastapi_stream_lease.fallback",
            description="Total number of fallback emergency leases granted when fail_open=True.",
            unit="1",
        )

        self.hook_dropped_counter = self.meter.create_counter(
            "fastapi_stream_lease.hook_dropped",
            description="Total lifecycle telemetry hooks dropped due to queue backpressure.",
            unit="1",
        )

        self.hook_errors_counter = self.meter.create_counter(
            "fastapi_stream_lease.hook_errors",
            description="Total number of lifecycle telemetry hooks that raised exceptions.",
            unit="1",
        )

        self.hook_queue_depth_counter = self.meter.create_up_down_counter(
            "fastapi_stream_lease.hook_queue_depth",
            description="Current pending lifecycle hooks in background dispatcher across managers.",
            unit="1",
        )

        self.circuit_state_gauge = self.meter.create_gauge(
            "fastapi_stream_lease.circuit_state",
            description=(
                "Last observed state of the worker-local circuit breaker during manager "
                "activity (0=closed, 1=half_open, 2=open)."
            ),
            unit="1",
        )

        self.short_circuited_counter = self.meter.create_counter(
            "fastapi_stream_lease.short_circuited",
            description=(
                "Total backend calls prevented from reaching Redis because the circuit "
                "breaker denied a permit."
            ),
            unit="1",
        )

    def record_operation(
        self,
        operation: Operation | str,
        outcome: Outcome | str,
        duration: float,
    ) -> None:
        """Record an operation count and duration in OpenTelemetry with strict enum validation."""
        op_enum = coerce_operation(operation)
        out_enum = coerce_outcome(outcome)
        attrs = {"operation": op_enum.value, "outcome": out_enum.value}

        self.operations_counter.add(1, attrs)
        if duration >= 0:
            self.duration_histogram.record(duration, {"operation": op_enum.value})

    def record_lost(self, reason: LostReason | str) -> None:
        """Record an unexpected lease loss event with strict enum validation."""
        r_enum = coerce_lost_reason(reason)
        self.lost_counter.add(1, {"reason": r_enum.value})

    def record_backend_error(self, kind: BackendErrorKind | str) -> None:
        """Record a classified backend Redis error with strict enum validation."""
        k_enum = coerce_backend_error_kind(kind)
        self.backend_errors_counter.add(1, {"kind": k_enum.value})

    def record_fallback(self) -> None:
        """Record a fallback lease activation."""
        self.fallback_counter.add(1)

    def record_hook_drop(self) -> None:
        """Record a dropped lifecycle hook due to dispatcher queue saturation."""
        self.hook_dropped_counter.add(1)

    def record_hook_error(self) -> None:
        """Record a lifecycle hook callback exception."""
        self.hook_errors_counter.add(1)

    def record_hook_queue_change(self, delta: int) -> None:
        """Record an incremental adjustment to the pending lifecycle hook queue depth."""
        self.hook_queue_depth_counter.add(delta)

    def record_circuit_state(
        self,
        state: CircuitState | str,
        scope: str = "default",
    ) -> None:
        """Record current circuit breaker state in OpenTelemetry with strict enum validation."""
        c_state = coerce_circuit_state(state)
        self.circuit_state_gauge.set(
            CIRCUIT_STATE_NUMERIC[c_state],
            {"scope": str(scope)},
        )

    def record_short_circuit(
        self,
        operation: Operation | str = Operation.ACQUIRE,
        state: CircuitState | str = CircuitState.OPEN,
        scope: str = "default",
    ) -> None:
        """Record a short-circuited backend call in OpenTelemetry."""
        op_enum = coerce_operation(operation)
        st_enum = coerce_circuit_state(state)
        self.short_circuited_counter.add(
            1,
            {
                "operation": op_enum.value,
                "state": st_enum.value,
                "scope": str(scope),
            },
        )

    def trace_operation(self, operation: Operation | str) -> AbstractContextManager[Any]:
        """
        Selectively emit an OpenTelemetry span for acquire and verify_config.
        Omit spans for high-frequency renewals to avoid trace flooding in LLM streaming.
        """
        op_enum = coerce_operation(operation)
        if op_enum in (Operation.ACQUIRE, Operation.VERIFY_CONFIG):
            return cast(
                AbstractContextManager[Any],
                self.tracer.start_as_current_span(f"fastapi_stream_lease.{op_enum.value}"),
            )
        return nullcontext()
