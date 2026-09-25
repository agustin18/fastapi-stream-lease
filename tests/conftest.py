from __future__ import annotations

import fakeredis.aioredis
import pytest

from fastapi_stream_lease import LeaseConfig, StreamLeaseManager


@pytest.fixture
async def fake_redis():
    """Provide an isolated, in-memory async Redis instance with Lua support."""
    client = fakeredis.aioredis.FakeRedis()
    try:
        yield client
    finally:
        await client.flushall()
        await client.aclose()


@pytest.fixture
def lease_config():
    return LeaseConfig(
        lease_seconds=2.0,
        max_per_user=2,
        max_global=4,
        key_prefix="test_lease",
    )


@pytest.fixture
def lease_manager(fake_redis, lease_config):
    return StreamLeaseManager(redis=fake_redis, config=lease_config)
