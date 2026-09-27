"""
Prometheus metrics adapter for fastapi-stream-lease with cardinality-safe labels.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi_stream_lease.observability.contract import (
    DEFAULT_DURATION_BUCKETS,
    BackendErrorKind,
    LostReason,
    Operation,
    Outcome,
    validate_cardinality_safe,
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
    Adapter that registers and collects cardinality-safe Prometheus metrics
    for stream lease operations, latencies, and failures.
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

    def record_operation(
        self,
        operation: Operation | str,
        outcome: Outcome | str,
        duration: float,
    ) -> None:
        """Record an operation count and duration histogram sample."""
        op_val = operation.value if isinstance(operation, Operation) else str(operation)
        out_val = outcome.value if isinstance(outcome, Outcome) else str(outcome)
        validate_cardinality_safe({"operation": op_val, "outcome": out_val})

        self.operations_total.labels(operation=op_val, outcome=out_val).inc()
        if duration >= 0:
            self.operation_duration_seconds.labels(operation=op_val).observe(duration)

    def record_lost(self, reason: LostReason | str) -> None:
        """Record an unexpected lease loss event."""
        r_val = reason.value if isinstance(reason, LostReason) else str(reason)
        validate_cardinality_safe({"reason": r_val})
        self.lost_total.labels(reason=r_val).inc()

    def record_backend_error(self, kind: BackendErrorKind | str) -> None:
        """Record a classified backend Redis error."""
        k_val = kind.value if isinstance(kind, BackendErrorKind) else str(kind)
        validate_cardinality_safe({"kind": k_val})
        self.backend_errors_total.labels(kind=k_val).inc()

    def record_fallback(self) -> None:
        """Record a fallback lease activation."""
        self.fallback_total.inc()

    def record_hook_metrics(self, dropped_count: int, error_count: int, queue_depth: int) -> None:
        """Record telemetry dispatcher queue depth, dropped hooks, and errors."""
        # Update counters and gauge
        self.hook_dropped_total._value.set(float(dropped_count))
        self.hook_errors_total._value.set(float(error_count))
        self.hook_queue_depth.set(float(queue_depth))
