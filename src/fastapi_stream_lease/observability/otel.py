"""
OpenTelemetry metrics and selective tracing adapter for fastapi-stream-lease.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from fastapi_stream_lease.observability.contract import (
    BackendErrorKind,
    LostReason,
    Operation,
    Outcome,
    validate_cardinality_safe,
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
            "The 'opentelemetry-api' and 'opentelemetry-sdk' packages are required. "
            "Install them via: pip install 'fastapi-stream-lease[otel]'"
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

    def record_operation(
        self,
        operation: Operation | str,
        outcome: Outcome | str,
        duration: float,
    ) -> None:
        """Record an operation count and duration in OpenTelemetry."""
        op_val = operation.value if isinstance(operation, Operation) else str(operation)
        out_val = outcome.value if isinstance(outcome, Outcome) else str(outcome)
        attrs = {"operation": op_val, "outcome": out_val}
        validate_cardinality_safe(attrs)

        self.operations_counter.add(1, attrs)
        if duration >= 0:
            self.duration_histogram.record(duration, {"operation": op_val})

    def record_lost(self, reason: LostReason | str) -> None:
        """Record an unexpected lease loss event."""
        r_val = reason.value if isinstance(reason, LostReason) else str(reason)
        attrs = {"reason": r_val}
        validate_cardinality_safe(attrs)
        self.lost_counter.add(1, attrs)

    def record_backend_error(self, kind: BackendErrorKind | str) -> None:
        """Record a classified backend Redis error."""
        k_val = kind.value if isinstance(kind, BackendErrorKind) else str(kind)
        attrs = {"kind": k_val}
        validate_cardinality_safe(attrs)
        self.backend_errors_counter.add(1, attrs)

    def record_fallback(self) -> None:
        """Record a fallback lease activation."""
        self.fallback_counter.add(1)

    @contextmanager
    def trace_operation(self, operation: Operation | str) -> Iterator[Any]:
        """
        Selectively emit an OpenTelemetry span for acquire and verify_config.
        Omit spans for high-frequency renewals to avoid trace flooding in LLM streaming.
        """
        op_val = operation.value if isinstance(operation, Operation) else str(operation)
        if op_val in (Operation.ACQUIRE.value, Operation.VERIFY_CONFIG.value):
            with self.tracer.start_as_current_span(f"fastapi_stream_lease.{op_val}") as span:
                yield span
        else:
            yield None
