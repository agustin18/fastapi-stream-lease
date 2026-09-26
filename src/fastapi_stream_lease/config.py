from __future__ import annotations

from dataclasses import dataclass
from math import isfinite


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

    def __post_init__(self) -> None:
        if not isfinite(self.lease_seconds) or self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be finite and greater than 0")
        if self.max_per_user < 0:
            raise ValueError("max_per_user cannot be negative")
        if self.max_global < 0:
            raise ValueError("max_global cannot be negative")
        if not self.key_prefix:
            raise ValueError("key_prefix cannot be empty")
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
