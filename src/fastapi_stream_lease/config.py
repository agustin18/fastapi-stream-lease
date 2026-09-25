from __future__ import annotations

from dataclasses import dataclass


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

    def user_key(self, user_id: str | int) -> str:
        return f"{self.key_prefix}:user:{user_id}"

    @property
    def global_key(self) -> str:
        return f"{self.key_prefix}:global"

    @property
    def redis_ttl(self) -> int:
        """TTL set on Redis keys to ensure dead keys self-clean (twice lease duration)."""
        return max(60, int(self.lease_seconds * 2))
