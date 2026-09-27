"""
Telemetry contract definitions and cardinality safety constraints for stream lease metrics.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from enum import Enum

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

FORBIDDEN_METRIC_LABELS: frozenset[str] = frozenset(
    {"user_id", "lease_id", "user_key", "principal", "client_id"}
)


class Operation(str, Enum):
    """Operation types supported in stream lease telemetry."""

    ACQUIRE = "acquire"
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


def validate_cardinality_safe(labels: Mapping[str, str]) -> None:
    """
    Ensure that high-cardinality attributes (such as user_id or lease_id)
    are never included as metric labels.
    """
    for forbidden in FORBIDDEN_METRIC_LABELS:
        if forbidden in labels:
            raise ValueError(
                f"Forbidden metric label '{forbidden}' detected. "
                "High-cardinality identifiers must never be recorded as metric labels."
            )
