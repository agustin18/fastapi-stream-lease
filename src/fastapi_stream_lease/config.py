from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Any


@dataclass(frozen=True)
class LeaseConfig:
    """Configuration for stream lease management."""

    lease_seconds: float = 30.0
    """Duration (in seconds) of each lease before expiring in Redis if not renewed."""

    max_per_user: int = 3
    """Maximum concurrent streams allowed for a single user/principal key."""

    max_global: int = 500
    """Maximum concurrent streams allowed across the entire cluster."""

    key_prefix: str = "stream_lease"
    """Prefix for Redis keys (e.g. stream_lease:user:{id}, stream_lease:global)."""

    fail_open: bool = False
    """If True, allows streams to proceed unthrottled with an emergency stub if Redis is down."""

    retry_after_seconds: int = 5
    """Default Retry-After header value (in seconds) for HTTP 503 responses."""

    on_acquired: Any = None
    """Optional callback hook: on_acquired(lease: StreamLease) -> None | Awaitable[None]"""

    on_rejected: Any = None
    """Optional callback hook: on_rejected(user_id, reason) -> None | Awaitable[None]"""

    on_lost: Any = None
    """Optional callback hook: on_lost(lease: StreamLease, reason: str) -> None | Awaitable[None]"""

    on_released: Any = None
    """Optional callback hook: on_released(lease, reason) -> None | Awaitable[None]"""

    on_backend_error: Any = None
    """Optional callback hook: on_backend_error(exc: Exception) -> None | Awaitable[None]"""

    hook_queue_size: int = 1024
    """Maximum capacity of the background hook queue before dropping telemetry events."""

    def __post_init__(self) -> None:
        if not isfinite(self.lease_seconds) or self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be finite and greater than 0")
        if self.max_per_user < 0:
            raise ValueError("max_per_user cannot be negative")
        if self.max_global < 0:
            raise ValueError("max_global cannot be negative")
        if not self.key_prefix:
            raise ValueError("key_prefix cannot be empty")
        if self.retry_after_seconds < 0:
            raise ValueError("retry_after_seconds cannot be negative")
        if self.hook_queue_size <= 0:
            raise ValueError("hook_queue_size must be greater than 0")
        for hook_name in (
            "on_acquired",
            "on_rejected",
            "on_lost",
            "on_released",
            "on_backend_error",
        ):
            hook_val = getattr(self, hook_name)
            if hook_val is not None and not callable(hook_val):
                raise TypeError(f"{hook_name} must be callable if provided")
        if "{" in self.key_prefix or "}" in self.key_prefix:
            left = self.key_prefix.find("{")
            right = self.key_prefix.find("}")
            if (
                left < 0
                or right <= left + 1
                or self.key_prefix.count("{") != 1
                or self.key_prefix.count("}") != 1
            ):
                raise ValueError("key_prefix must contain one nonempty Redis hash tag")

    @property
    def _cluster_prefix(self) -> str:
        """Ensure prefix uses Redis hash tags {...} for slot affinity in Redis Cluster."""
        if "{" in self.key_prefix and "}" in self.key_prefix:
            return self.key_prefix
        return f"{{{self.key_prefix}}}"

    def user_key(self, user_id: str | int) -> str:
        return f"{self._cluster_prefix}:user:{user_id}"

    @property
    def global_key(self) -> str:
        return f"{self._cluster_prefix}:global"

    @property
    def redis_ttl(self) -> int:
        """TTL set on Redis keys to ensure dead keys self-clean (twice lease duration)."""
        return max(60, int(self.lease_seconds * 2))

    @property
    def config_key(self) -> str:
        """Redis key for storing and verifying cluster configuration fingerprint."""
        return f"{self._cluster_prefix}:config"

    def fingerprint_dict(self) -> dict[str, Any]:
        """Return critical cluster configuration parameters for consistency verification."""
        return {
            "algorithm_version": 1,
            "key_prefix": self.key_prefix,
            "max_per_user": self.max_per_user,
            "max_global": self.max_global,
            "lease_seconds": self.lease_seconds,
            "fail_open": self.fail_open,
        }
