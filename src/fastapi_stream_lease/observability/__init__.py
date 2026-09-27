"""
Observability adapters for Prometheus and OpenTelemetry.
"""

from __future__ import annotations

from fastapi_stream_lease.observability.contract import (
    DEFAULT_DURATION_BUCKETS,
    BackendErrorKind,
    LostReason,
    Operation,
    Outcome,
    TelemetryAdapter,
)
from fastapi_stream_lease.observability.otel import OpenTelemetryMetrics
from fastapi_stream_lease.observability.prometheus import PrometheusMetrics

__all__ = [
    "DEFAULT_DURATION_BUCKETS",
    "BackendErrorKind",
    "LostReason",
    "OpenTelemetryMetrics",
    "Operation",
    "Outcome",
    "PrometheusMetrics",
    "TelemetryAdapter",
]
