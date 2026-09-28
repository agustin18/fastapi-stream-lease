from __future__ import annotations

from fastapi_stream_lease.circuit_breaker import (
    BackendFailurePolicy,
    CircuitBreakerConfig,
    FallbackMode,
)
from fastapi_stream_lease.config import LeaseConfig
from fastapi_stream_lease.exceptions import (
    ConfigurationMismatchError,
    StreamLeaseError,
    StreamLeaseLost,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)
from fastapi_stream_lease.lease import StreamLease
from fastapi_stream_lease.manager import StreamLeaseManager

__version__ = "0.2.0"

__all__ = [
    "BackendFailurePolicy",
    "CircuitBreakerConfig",
    "ConfigurationMismatchError",
    "FallbackMode",
    "LeaseConfig",
    "StreamLease",
    "StreamLeaseError",
    "StreamLeaseLost",
    "StreamLeaseManager",
    "StreamLeaseRejected",
    "StreamLeaseUnavailable",
    "__version__",
]
