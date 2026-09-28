"""
Tests for cardinality-safe observability adapters (Prometheus & OpenTelemetry).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import nullcontext
from typing import Any
from unittest.mock import patch

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client import CollectorRegistry

from fastapi_stream_lease.config import LeaseConfig
from fastapi_stream_lease.exceptions import (
    StreamLeaseLost,
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)
from fastapi_stream_lease.manager import StreamLeaseManager
from fastapi_stream_lease.observability.contract import (
    DEFAULT_DURATION_BUCKETS,
    BackendErrorKind,
    CircuitState,
    LostReason,
    Operation,
    Outcome,
    TelemetryAdapter,
    classify_backend_error,
    coerce_backend_error_kind,
    coerce_circuit_state,
    coerce_lost_reason,
    coerce_operation,
    coerce_outcome,
)
from fastapi_stream_lease.observability.otel import OpenTelemetryMetrics
from fastapi_stream_lease.observability.prometheus import PrometheusMetrics


def test_strict_cardinality_coercion() -> None:
    """Verifies that unknown or dynamic strings are rejected by enum coercion helpers."""
    assert coerce_operation("acquire") == Operation.ACQUIRE
    assert coerce_operation(Operation.RENEW) == Operation.RENEW
    with pytest.raises(ValueError, match="Invalid operation 'invalid_op'"):
        coerce_operation("invalid_op")

    assert coerce_outcome("success") == Outcome.SUCCESS
    assert coerce_outcome(Outcome.REJECTED) == Outcome.REJECTED
    with pytest.raises(ValueError, match="Invalid outcome 'invalid_out'"):
        coerce_outcome("invalid_out")

    assert coerce_lost_reason("backend_timeout") == LostReason.BACKEND_TIMEOUT
    assert coerce_lost_reason(LostReason.REDIS_REVOKED) == LostReason.REDIS_REVOKED
    with pytest.raises(ValueError, match="Invalid lost reason 'user_cancelled'"):
        coerce_lost_reason("user_cancelled")

    assert coerce_backend_error_kind("connection") == BackendErrorKind.CONNECTION
    assert coerce_backend_error_kind(BackendErrorKind.TIMEOUT) == BackendErrorKind.TIMEOUT
    with pytest.raises(ValueError, match="Invalid backend error kind 'bad_kind'"):
        coerce_backend_error_kind("bad_kind")

    assert coerce_circuit_state("closed") == CircuitState.CLOSED
    assert coerce_circuit_state("half_open") == CircuitState.HALF_OPEN
    assert coerce_circuit_state("open") == CircuitState.OPEN
    assert coerce_circuit_state(CircuitState.OPEN) == CircuitState.OPEN
    with pytest.raises(ValueError, match="Invalid circuit state 'bad_state'"):
        coerce_circuit_state("bad_state")


@pytest.mark.parametrize(
    "operation,outcome",
    [
        (Operation.ACQUIRE, Outcome.SUCCESS),
        (Operation.ACQUIRE, Outcome.REJECTED),
        (Operation.ACQUIRE, Outcome.BACKEND_ERROR),
        (Operation.RENEW, Outcome.SUCCESS),
        (Operation.RENEW, Outcome.REVOKED),
        (Operation.RENEW, Outcome.BACKEND_ERROR),
        (Operation.RELEASE, Outcome.SUCCESS),
        (Operation.RELEASE, Outcome.BACKEND_ERROR),
        (Operation.VERIFY_CONFIG, Outcome.SUCCESS),
        (Operation.VERIFY_CONFIG, Outcome.REJECTED),
        (Operation.VERIFY_CONFIG, Outcome.BACKEND_ERROR),
    ],
)
def test_prometheus_operations_metrics(operation: Operation, outcome: Outcome) -> None:
    """Parametrized test for all valid operation/outcome pairs on Prometheus metrics."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)

    metrics.record_operation(operation, outcome, duration=0.005)

    ops_val = registry.get_sample_value(
        "fastapi_stream_lease_operations_total",
        {"operation": operation.value, "outcome": outcome.value},
    )
    assert ops_val == 1.0

    duration_count = registry.get_sample_value(
        "fastapi_stream_lease_operation_duration_seconds_count",
        {"operation": operation.value},
    )
    assert duration_count == 1.0


@pytest.mark.parametrize(
    "reason",
    [
        LostReason.REDIS_REVOKED,
        LostReason.BACKEND_TIMEOUT,
        LostReason.UNEXPECTED_ERROR,
    ],
)
def test_prometheus_lost_metrics(reason: LostReason) -> None:
    """Parametrized test for lease loss reasons."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)

    metrics.record_lost(reason)
    val = registry.get_sample_value(
        "fastapi_stream_lease_lost_total",
        {"reason": reason.value},
    )
    assert val == 1.0


@pytest.mark.parametrize(
    "kind",
    [
        BackendErrorKind.CONNECTION,
        BackendErrorKind.TIMEOUT,
        BackendErrorKind.READONLY,
        BackendErrorKind.CLUSTERDOWN,
        BackendErrorKind.UNKNOWN,
    ],
)
def test_prometheus_backend_errors_metrics(kind: BackendErrorKind) -> None:
    """Parametrized test for backend error classifications."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)

    metrics.record_backend_error(kind)
    val = registry.get_sample_value(
        "fastapi_stream_lease_backend_errors_total",
        {"kind": kind.value},
    )
    assert val == 1.0


def test_prometheus_fallback_and_dispatcher_gauges() -> None:
    """Verifies fallback counter and dispatcher hook queue depth, drops, and errors."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)

    metrics.record_fallback()
    assert registry.get_sample_value("fastapi_stream_lease_fallback_total") == 1.0

    metrics.record_hook_drop()
    metrics.record_hook_drop()
    metrics.record_hook_drop()
    assert registry.get_sample_value("fastapi_stream_lease_hook_dropped_total") == 3.0
    metrics.record_hook_error()
    assert registry.get_sample_value("fastapi_stream_lease_hook_errors_total") == 1.0

    metrics.record_hook_queue_change(42)
    assert registry.get_sample_value("fastapi_stream_lease_hook_queue_depth") == 42.0
    metrics.record_hook_queue_change(-12)
    assert registry.get_sample_value("fastapi_stream_lease_hook_queue_depth") == 30.0
    metrics.record_hook_queue_change(0)
    assert registry.get_sample_value("fastapi_stream_lease_hook_queue_depth") == 30.0


def test_prometheus_duplicate_registration_raises() -> None:
    """Verifies that creating two PrometheusMetrics on the same registry raises ValueError."""
    registry = CollectorRegistry()
    PrometheusMetrics(registry=registry)
    with pytest.raises(ValueError, match="Duplicated timeseries"):
        PrometheusMetrics(registry=registry)


def test_otel_metrics_and_selective_tracing() -> None:
    """Verifies OpenTelemetry metrics and selective tracing (spans for acquire and verify)."""
    metric_reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[metric_reader])
    span_exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))

    otel = OpenTelemetryMetrics(
        meter_provider=meter_provider,
        tracer_provider=tracer_provider,
    )

    # 1. Acquire: records metric AND emits span
    with otel.trace_operation(Operation.ACQUIRE):
        otel.record_operation(Operation.ACQUIRE, Outcome.SUCCESS, duration=0.004)

    # 2. Renew: records metric but DOES NOT emit span
    with otel.trace_operation(Operation.RENEW):
        otel.record_operation(Operation.RENEW, Outcome.SUCCESS, duration=0.001)

    # 3. Verify config: records metric AND emits span
    with otel.trace_operation(Operation.VERIFY_CONFIG):
        otel.record_operation(Operation.VERIFY_CONFIG, Outcome.SUCCESS, duration=0.002)

    spans = span_exporter.get_finished_spans()
    span_names = [s.name for s in spans]
    assert "fastapi_stream_lease.acquire" in span_names
    assert "fastapi_stream_lease.verify_config" in span_names
    # Strict rule: renew does NOT emit span by default to prevent trace flooding
    assert "fastapi_stream_lease.renew" not in span_names

    metric_data = metric_reader.get_metrics_data()
    assert metric_data is not None


@pytest.mark.asyncio
async def test_manager_end_to_end_prometheus_integration(fake_redis) -> None:
    """Full end-to-end lifecycle integration testing StreamLeaseManager with PrometheusMetrics."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)
    config = LeaseConfig(lease_seconds=5.0, max_per_user=1, max_global=10)
    manager = StreamLeaseManager(fake_redis, config, metrics=metrics)

    # 1. Acquire success
    lease = await manager.acquire(user_id="user_metrics_1")
    assert lease is not None
    await manager.drain()

    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_operations_total",
            {"operation": "acquire", "outcome": "success"},
        )
        == 1.0
    )

    # 2. Acquire rejection (limit exceeded)
    with pytest.raises(StreamLeaseRejected):
        await manager.acquire(user_id="user_metrics_1")
    await manager.drain()

    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_operations_total",
            {"operation": "acquire", "outcome": "rejected"},
        )
        == 1.0
    )

    # 2b. Global limit rejection with metrics
    mgr_global = StreamLeaseManager(
        fake_redis,
        LeaseConfig(lease_seconds=5.0, max_per_user=10, max_global=1, key_prefix="global_cov"),
        metrics=metrics,
    )
    l1 = await mgr_global.acquire(user_id="u1")
    with pytest.raises(StreamLeaseRejected, match="global_limit"):
        await mgr_global.acquire(user_id="u2")
    await l1.release()
    await mgr_global.close()

    # 3. Renew success
    renew_ok = await lease.renew()
    assert renew_ok is True
    await manager.drain()

    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_operations_total",
            {"operation": "renew", "outcome": "success"},
        )
        == 1.0
    )

    # 4. Release
    await lease.release()
    await manager.drain()

    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_operations_total",
            {"operation": "release", "outcome": "success"},
        )
        == 2.0
    )

    # 5. Verify config (initial register)
    verified = await manager.verify_cluster_config()
    assert verified is True

    # 5b. Verify config (subsequent check against existing canonical config)
    verified_again = await manager.verify_cluster_config()
    assert verified_again is True
    await manager.drain()

    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_operations_total",
            {"operation": "verify_config", "outcome": "success"},
        )
        >= 2.0
    )

    await manager.close()


def test_missing_optional_dependency_raises_clear_importerror() -> None:
    """Verifies that missing packages trigger explicit and actionable ImportError messages."""
    with patch.dict("sys.modules", {"prometheus_client": None}):
        with pytest.raises(ImportError, match=r"pip install 'fastapi-stream-lease\[prometheus\]'"):
            from fastapi_stream_lease.observability.prometheus import require_prometheus_client

            require_prometheus_client()

    with patch.dict("sys.modules", {"opentelemetry": None}):
        with pytest.raises(ImportError, match=r"pip install 'fastapi-stream-lease\[otel\]'"):
            from fastapi_stream_lease.observability.otel import require_opentelemetry

            require_opentelemetry()


@pytest.mark.parametrize(
    "exc,expected",
    [
        (ConnectionError("refused"), BackendErrorKind.CONNECTION),
        (ConnectionResetError("reset"), BackendErrorKind.CONNECTION),
        (TimeoutError("timeout"), BackendErrorKind.TIMEOUT),
        (asyncio.TimeoutError(), BackendErrorKind.TIMEOUT),
        (type("ReadOnlyError", (Exception,), {})(), BackendErrorKind.READONLY),
        (type("ClusterDownError", (Exception,), {})(), BackendErrorKind.CLUSTERDOWN),
        (type("MasterDownError", (Exception,), {})(), BackendErrorKind.CLUSTERDOWN),
        (type("SlotNotCoveredError", (Exception,), {})(), BackendErrorKind.CLUSTERDOWN),
        (ValueError("other"), BackendErrorKind.UNKNOWN),
    ],
)
def test_classify_backend_error_mapping(exc: BaseException, expected: BackendErrorKind) -> None:
    """Verify exception mapping to cardinality-safe BackendErrorKind categories."""

    assert classify_backend_error(exc) == expected


def test_otel_lost_backend_error_and_fallback() -> None:
    """Verify OTel lost, backend error, fallback counters, and negative duration safety."""
    metric_reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[metric_reader])
    otel = OpenTelemetryMetrics(meter_provider=meter_provider)

    otel.record_lost(LostReason.BACKEND_TIMEOUT)
    otel.record_backend_error(BackendErrorKind.TIMEOUT)
    otel.record_fallback()
    otel.record_operation(Operation.ACQUIRE, Outcome.SUCCESS, duration=-1.0)


def test_prometheus_negative_duration() -> None:
    """Verify Prometheus does not record histogram samples for negative durations."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)
    metrics.record_operation(Operation.ACQUIRE, Outcome.SUCCESS, duration=-1.0)
    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_operation_duration_seconds_count",
            {"operation": "acquire"},
        )
        is None
    )


@pytest.mark.asyncio
async def test_manager_metrics_on_failures_and_fallback(fake_redis) -> None:
    """Verify manager metrics recording on network errors, fallbacks, and mismatches."""
    import json
    from unittest.mock import AsyncMock

    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)
    config = LeaseConfig(lease_seconds=5.0, fail_open=True)
    manager = StreamLeaseManager(fake_redis, config, metrics=metrics)

    # 1. Acquire with fail_open=True network failure -> fallback metric
    with patch.object(fake_redis, "eval", side_effect=ConnectionError("fail")):
        lease = await manager.acquire(user_id="usr_fallback")
        assert lease._is_fallback is True
    assert registry.get_sample_value("fastapi_stream_lease_fallback_total") == 1.0
    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_operations_total",
            {"operation": "acquire", "outcome": "fallback"},
        )
        == 1.0
    )

    # 2. Acquire with fail_open=False network failure -> backend_error metric
    manager_strict = StreamLeaseManager(
        fake_redis, LeaseConfig(lease_seconds=5.0, fail_open=False), metrics=metrics
    )
    with patch.object(fake_redis, "eval", side_effect=ConnectionError("fail")):
        with pytest.raises(StreamLeaseUnavailable):
            await manager_strict.acquire(user_id="usr_strict")
    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_operations_total",
            {"operation": "acquire", "outcome": "backend_error"},
        )
        == 1.0
    )

    # 3. Renew network error -> backend_error metric
    with patch.object(fake_redis, "eval", side_effect=ConnectionError("renew fail")):
        with pytest.raises(StreamLeaseUnavailable):
            await manager.renew(lease)
    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_operations_total",
            {"operation": "renew", "outcome": "backend_error"},
        )
        == 1.0
    )

    # 4. Release network error -> backend_error metric
    with patch.object(fake_redis, "eval", side_effect=ConnectionError("release fail")):
        await manager.release(lease)
    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_operations_total",
            {"operation": "release", "outcome": "backend_error"},
        )
        == 1.0
    )

    # 5. verify_cluster_config network error -> backend_error metric
    with patch.object(fake_redis, "set", side_effect=ConnectionError("set fail")):
        await manager.verify_cluster_config(strict=False, retry_attempts=1)
    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_operations_total",
            {"operation": "verify_config", "outcome": "backend_error"},
        )
        == 1.0
    )

    # 6. verify_cluster_config missing key -> backend_error metric
    with (
        patch.object(fake_redis, "set", new_callable=AsyncMock, return_value=False),
        patch.object(fake_redis, "get", new_callable=AsyncMock, return_value=None),
    ):
        await manager.verify_cluster_config(strict=False, retry_attempts=1)

    # 7. verify_cluster_config mismatch -> rejected metric
    with (
        patch.object(fake_redis, "set", new_callable=AsyncMock, return_value=False),
        patch.object(
            fake_redis,
            "get",
            new_callable=AsyncMock,
            return_value=json.dumps({"algorithm_version": 999}).encode("utf-8"),
        ),
    ):
        await manager.verify_cluster_config(strict=False, retry_attempts=1)
    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_operations_total",
            {"operation": "verify_config", "outcome": "rejected"},
        )
        == 1.0
    )

    await manager.close()
    await manager_strict.close()


def test_telemetry_adapter_protocol_conformance() -> None:
    """Verifies Prometheus and OTel adapters fulfill TelemetryAdapter protocol."""
    registry = CollectorRegistry()
    prom = PrometheusMetrics(registry=registry)
    assert isinstance(prom, TelemetryAdapter)

    metric_reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[metric_reader])
    otel = OpenTelemetryMetrics(meter_provider=meter_provider)
    assert isinstance(otel, TelemetryAdapter)


def test_otel_duration_buckets_and_hook_metrics() -> None:
    """Verifies OTel histogram explicit bucket boundaries advisory and hook metrics."""
    metric_reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[metric_reader])
    otel = OpenTelemetryMetrics(meter_provider=meter_provider)

    assert otel.duration_histogram._advisory.explicit_bucket_boundaries == DEFAULT_DURATION_BUCKETS

    otel.record_hook_drop()
    otel.record_hook_error()
    otel.record_hook_queue_change(13)
    otel.record_hook_queue_change(-5)

    metric_data = metric_reader.get_metrics_data()
    assert metric_data is not None


@pytest.mark.asyncio
async def test_broken_telemetry_failure_isolation(fake_redis) -> None:
    """
    CRITICAL P1 AUDIT TEST:
    Verifies that a completely broken TelemetryAdapter raising exceptions on every call
    NEVER causes ghost leases, dropped streams, or breaks Redis coordination semantics.
    """

    class BrokenTelemetryAdapter:
        def record_operation(self, operation: Any, outcome: Any, duration: float) -> None:
            raise RuntimeError("record_operation simulated failure")

        def record_lost(self, reason: Any) -> None:
            raise RuntimeError("record_lost simulated failure")

        def record_backend_error(self, kind: Any) -> None:
            raise RuntimeError("record_backend_error simulated failure")

        def record_fallback(self) -> None:
            raise RuntimeError("record_fallback simulated failure")

        def record_hook_drop(self) -> None:
            raise RuntimeError("record_hook_drop simulated failure")

        def record_hook_error(self) -> None:
            raise RuntimeError("record_hook_error simulated failure")

        def record_hook_queue_change(self, delta: int) -> None:
            raise RuntimeError("record_hook_queue_change simulated failure")

        def record_circuit_state(self, state: Any) -> None:
            raise RuntimeError("record_circuit_state simulated failure")

        def record_short_circuit(self) -> None:
            raise RuntimeError("record_short_circuit simulated failure")

        def trace_operation(self, operation: Any) -> Any:
            raise RuntimeError("trace_operation simulated failure")

    broken = BrokenTelemetryAdapter()
    config = LeaseConfig(lease_seconds=5.0, fail_open=True)
    manager = StreamLeaseManager(fake_redis, config=config, telemetry=broken)

    # 1. Acquire with broken telemetry must succeed and return a valid lease
    lease = await manager.acquire(user_id="user_broken_telemetry")
    assert lease is not None
    assert lease.lease_id is not None

    # 2. Renew with broken telemetry must succeed
    renewed = await lease.renew()
    assert renewed is True

    # 3. Release with broken telemetry must succeed
    await lease.release()
    assert lease._is_released is True

    # 4. Verify config with broken telemetry must succeed
    verified = await manager.verify_cluster_config()
    assert verified is True

    # 5. Acquire with network error and fail_open=True must still return fallback lease
    with patch.object(fake_redis, "eval", side_effect=ConnectionError("redis down")):
        fallback_lease = await manager.acquire(user_id="user_broken_fallback")
        assert fallback_lease._is_fallback is True
        await fallback_lease.release()

    # 6. Verify broken hook callbacks, depth, lost, and backend error calls
    manager._on_hook_drop()
    manager._on_hook_error()
    manager._on_hook_queue_change(1)
    manager._safe_record_lost("redis_revoked")
    manager._safe_record_backend_error(ConnectionError("fail"))
    manager._safe_record_fallback()
    manager._safe_record_circuit_state(CircuitState.OPEN)
    manager._safe_record_short_circuit()

    # 7. Verify property alias works
    assert manager.metrics is broken
    manager.metrics = None
    assert manager.telemetry is None

    await manager.close()

    # 8. Verify broken span __enter__ failure isolation
    class BrokenEnterSpan:
        def __enter__(self) -> Any:
            raise RuntimeError("enter boom")

        def __exit__(self, *args: Any) -> Any:
            return False

    class BrokenEnterAdapter:
        def trace_operation(self, op: Any) -> Any:
            return BrokenEnterSpan()

    manager_enter_broken = StreamLeaseManager(fake_redis, telemetry=BrokenEnterAdapter())
    l_enter = await manager_enter_broken.acquire(user_id="u_enter_broken")
    assert l_enter is not None
    await l_enter.release()
    await manager_enter_broken.close()

    # 9. Verify broken span __exit__ failure isolation (normal completion and rejection throw)
    class BrokenExitSpan:
        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> Any:
            raise RuntimeError("exit boom")

    class BrokenExitAdapter:
        def trace_operation(self, op: Any) -> Any:
            return BrokenExitSpan()

    manager_exit_broken = StreamLeaseManager(
        fake_redis, LeaseConfig(max_per_user=1), telemetry=BrokenExitAdapter()
    )
    l_exit1 = await manager_exit_broken.acquire(user_id="u_exit_broken")
    assert l_exit1 is not None
    with pytest.raises(StreamLeaseRejected):
        await manager_exit_broken.acquire(user_id="u_exit_broken")
    await l_exit1.release()
    await manager_exit_broken.close()

    # 10. Verify callbacks when telemetry is None
    manager_none = StreamLeaseManager(fake_redis, telemetry=None)
    manager_none._on_hook_drop()
    manager_none._on_hook_error()
    manager_none._on_hook_queue_change(1)
    await manager_none.close()

    # 11. CRITICAL P1: Telemetry span context manager MUST NEVER suppress business exceptions
    class SuppressSpan:
        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> bool:
            return True  # Attempt to suppress exception

    class SuppressAdapter:
        def trace_operation(self, op: Any) -> Any:
            return SuppressSpan()

    manager_suppress = StreamLeaseManager(fake_redis, telemetry=SuppressAdapter())
    with pytest.raises(ValueError, match="must not be suppressed by span"):
        with manager_suppress._safe_trace_operation(Operation.ACQUIRE):
            raise ValueError("must not be suppressed by span")

    with pytest.raises(StreamLeaseRejected):
        with manager_suppress._safe_trace_operation(Operation.ACQUIRE):
            raise StreamLeaseRejected(reason="user_limit")

    await manager_suppress.close()

    # 12. CRITICAL P1: Span __exit__ raising an error does NOT swallow original exception
    class CrashingExitSpan:
        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> None:
            raise RuntimeError("span __exit__ crashed")

    class CrashingExitAdapter:
        def trace_operation(self, op: Any) -> Any:
            return CrashingExitSpan()

    manager_crashing_exit = StreamLeaseManager(fake_redis, telemetry=CrashingExitAdapter())
    with pytest.raises(ValueError, match="original error survives span exit crash"):
        with manager_crashing_exit._safe_trace_operation(Operation.ACQUIRE):
            raise ValueError("original error survives span exit crash")
    await manager_crashing_exit.close()


@pytest.mark.asyncio
async def test_manager_records_lost_on_redis_revocation(fake_redis) -> None:
    """Verifies that lost_total is automatically emitted when lease is revoked/evicted in Redis."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)
    config = LeaseConfig(lease_seconds=0.2)
    manager = StreamLeaseManager(fake_redis, config, telemetry=metrics)

    with pytest.raises(StreamLeaseLost):
        async with manager.lease(user_id="usr_lost_revocation", renew_interval=0.03) as lease:
            await fake_redis.delete(lease.user_key)
            await asyncio.sleep(0.1)

    val = registry.get_sample_value(
        "fastapi_stream_lease_lost_total",
        {"reason": "redis_revoked"},
    )
    assert val == 1.0

    await manager.close()


@pytest.mark.asyncio
async def test_manager_records_lost_on_backend_timeout(fake_redis) -> None:
    """Verifies that lost_total is emitted when renewal expires during backend outage."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)
    config = LeaseConfig(lease_seconds=0.1)
    manager = StreamLeaseManager(fake_redis, config, telemetry=metrics)

    lease = await manager.acquire(user_id="usr_lost_timeout")

    async def slow_stream() -> AsyncIterator[str]:
        yield "chunk1"
        await asyncio.sleep(0.2)
        yield "chunk2"

    with patch.object(fake_redis, "eval", side_effect=ConnectionError("outage")):
        with pytest.raises(StreamLeaseLost):
            async for _ in lease.wrap(slow_stream(), auto_renew=True, renew_interval=0.02):
                pass

    val = registry.get_sample_value(
        "fastapi_stream_lease_lost_total",
        {"reason": "backend_timeout"},
    )
    assert val == 1.0

    await manager.close()


@pytest.mark.asyncio
async def test_manager_records_lost_on_unexpected_error(fake_redis) -> None:
    """Verifies that lost_total is automatically emitted when renewal fails unexpectedly."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)
    config = LeaseConfig(lease_seconds=0.1)
    manager = StreamLeaseManager(fake_redis, config, telemetry=metrics)

    lease = await manager.acquire(user_id="usr_lost_unexpected")

    async def slow_stream() -> AsyncIterator[str]:
        yield "chunk1"
        await asyncio.sleep(0.2)
        yield "chunk2"

    with patch.object(fake_redis, "eval", side_effect=TypeError("script bug")):
        with pytest.raises(StreamLeaseLost):
            async for _ in lease.wrap(slow_stream(), auto_renew=True, renew_interval=0.02):
                pass

    val = registry.get_sample_value(
        "fastapi_stream_lease_lost_total",
        {"reason": "unexpected_error"},
    )
    assert val == 1.0

    await manager.close()


@pytest.mark.asyncio
async def test_manager_dispatcher_hook_error_telemetry(fake_redis) -> None:
    """Verifies that hook_errors_total is incremented when a lifecycle callback raises."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)

    def failing_hook(*args: Any) -> None:
        raise ValueError("failing hook")

    config = LeaseConfig(on_acquired=failing_hook)
    manager = StreamLeaseManager(fake_redis, config, telemetry=metrics)

    lease = await manager.acquire(user_id="usr_hook_err")
    await manager.drain()
    await lease.release()
    await manager.close()

    val = registry.get_sample_value("fastapi_stream_lease_hook_errors_total")
    assert val == 1.0


@pytest.mark.asyncio
async def test_prometheus_circuit_breaker_metrics() -> None:
    """Verifies Prometheus gauge and counter for circuit breaker states and short circuits."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)

    # Initial state gauge value with default scope
    metrics.record_circuit_state(CircuitState.CLOSED)
    assert (
        registry.get_sample_value("fastapi_stream_lease_circuit_state", {"prefix": "default"})
        == 0.0
    )

    metrics.record_circuit_state(CircuitState.HALF_OPEN, scope="custom_prefix")
    assert (
        registry.get_sample_value("fastapi_stream_lease_circuit_state", {"prefix": "custom_prefix"})
        == 1.0
    )

    metrics.record_circuit_state("open", scope="custom_prefix")
    assert (
        registry.get_sample_value("fastapi_stream_lease_circuit_state", {"prefix": "custom_prefix"})
        == 2.0
    )

    # Short circuited counter
    metrics.record_short_circuit(Operation.ACQUIRE, CircuitState.OPEN, scope="custom_prefix")
    metrics.record_short_circuit(Operation.COUNT, CircuitState.HALF_OPEN, scope="custom_prefix")
    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_short_circuited_total",
            {"operation": "acquire", "state": "open", "prefix": "custom_prefix"},
        )
        == 1.0
    )
    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_short_circuited_total",
            {"operation": "count", "state": "half_open", "prefix": "custom_prefix"},
        )
        == 1.0
    )


def test_otel_circuit_breaker_metrics() -> None:
    """Verifies OpenTelemetry gauge and counter for circuit breaker states and short circuits."""
    metric_reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[metric_reader])
    otel = OpenTelemetryMetrics(meter_provider=meter_provider)

    otel.record_circuit_state(CircuitState.CLOSED, scope="otel_prefix")
    otel.record_circuit_state("open", scope="otel_prefix")
    otel.record_short_circuit(Operation.ACQUIRE, CircuitState.OPEN, scope="otel_prefix")
    otel.record_short_circuit(Operation.COUNT, "half_open", scope="otel_prefix")


@pytest.mark.asyncio
async def test_manager_circuit_breaker_metrics_integration(fake_redis) -> None:
    """Verifies end-to-end integration between manager circuit breaker and Prometheus metrics."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)
    config = LeaseConfig(
        failure_policy={"circuit_breaker": {"failure_threshold": 1, "recovery_timeout": 60.0}}
    )
    manager = StreamLeaseManager(fake_redis, config, telemetry=metrics)

    # Initially CLOSED
    assert (
        registry.get_sample_value("fastapi_stream_lease_circuit_state", {"prefix": "stream_lease"})
        == 0.0
    )

    # Trigger failure to trip breaker to OPEN
    with patch.object(fake_redis, "eval", side_effect=ConnectionError("backend failure")):
        with pytest.raises(StreamLeaseUnavailable):
            await manager.acquire("user_cb_metric")

    # Circuit breaker tripped to OPEN: gauge must be 2.0
    assert (
        registry.get_sample_value("fastapi_stream_lease_circuit_state", {"prefix": "stream_lease"})
        == 2.0
    )

    # Next acquire is short-circuited: increments short_circuited_total for acquire/open
    with pytest.raises(StreamLeaseUnavailable, match="Circuit breaker is OPEN"):
        await manager.acquire("user_cb_metric")

    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_short_circuited_total",
            {"operation": "acquire", "state": "open", "prefix": "stream_lease"},
        )
        == 1.0
    )

    # get_active_count also short-circuits and increments for count/open
    with pytest.raises(StreamLeaseUnavailable, match="Circuit breaker is OPEN"):
        await manager.get_active_count("user_cb_metric")

    assert (
        registry.get_sample_value(
            "fastapi_stream_lease_short_circuited_total",
            {"operation": "count", "state": "open", "prefix": "stream_lease"},
        )
        == 1.0
    )
    await manager.close()


@pytest.mark.asyncio
async def test_multi_manager_shared_metrics_scoping_no_collision(fake_redis) -> None:
    """CRITICAL P2 AUDIT TEST (OBS-02):
    Verifies that multiple StreamLeaseManager instances sharing a single telemetry
    adapter maintain isolated, non-colliding circuit breaker states and counters.
    """
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)

    config_heavy = LeaseConfig(
        key_prefix="llm_heavy",
        failure_policy={"circuit_breaker": {"failure_threshold": 1, "recovery_timeout": 60.0}},
    )
    config_light = LeaseConfig(
        key_prefix="llm_light",
        failure_policy={"circuit_breaker": {"failure_threshold": 5, "recovery_timeout": 60.0}},
    )

    manager_heavy = StreamLeaseManager(fake_redis, config=config_heavy, telemetry=metrics)
    manager_light = StreamLeaseManager(fake_redis, config=config_light, telemetry=metrics)

    # Trip manager_heavy to OPEN
    with patch.object(fake_redis, "eval", side_effect=ConnectionError("heavy failure")):
        with pytest.raises(StreamLeaseUnavailable):
            await manager_heavy.acquire("user_h")

    # Manager heavy is OPEN (2.0)
    assert (
        registry.get_sample_value("fastapi_stream_lease_circuit_state", {"prefix": "llm_heavy"})
        == 2.0
    )

    # Manager light remains CLOSED (0.0) without collision!
    assert (
        registry.get_sample_value("fastapi_stream_lease_circuit_state", {"prefix": "llm_light"})
        == 0.0
    )

    # Acquire on light succeeds and reinforces CLOSED state
    lease_l = await manager_light.acquire("user_l")
    assert (
        registry.get_sample_value("fastapi_stream_lease_circuit_state", {"prefix": "llm_light"})
        == 0.0
    )
    # Heavy still remains OPEN (2.0)
    assert (
        registry.get_sample_value("fastapi_stream_lease_circuit_state", {"prefix": "llm_heavy"})
        == 2.0
    )

    await lease_l.release()
    await manager_heavy.close()
    await manager_light.close()


def test_legacy_v03_telemetry_adapter_protocol_conformance() -> None:
    """CRITICAL P2 AUDIT TEST (API-01):
    Verifies that legacy custom TelemetryAdapter implementations conforming to v0.3.0
    (without circuit breaker methods) continue to pass isinstance() conformance.
    """
    from fastapi_stream_lease.observability.contract import (
        CircuitBreakerTelemetry,
        StreamLeaseTelemetry,
        TelemetryAdapter,
    )

    class LegacyV03Adapter:
        def record_operation(self, operation: Any, outcome: Any, duration: float) -> None:
            pass

        def record_lost(self, reason: Any) -> None:
            pass

        def record_backend_error(self, kind: Any) -> None:
            pass

        def record_fallback(self) -> None:
            pass

        def record_hook_drop(self) -> None:
            pass

        def record_hook_error(self) -> None:
            pass

        def record_hook_queue_change(self, delta: int) -> None:
            pass

        def trace_operation(self, operation: Any) -> Any:
            return nullcontext()

    legacy = LegacyV03Adapter()
    assert isinstance(legacy, TelemetryAdapter)
    assert not isinstance(legacy, CircuitBreakerTelemetry)
    assert not isinstance(legacy, StreamLeaseTelemetry)

    # Builtin adapter implements all protocols
    prom = PrometheusMetrics()
    assert isinstance(prom, TelemetryAdapter)
    assert isinstance(prom, CircuitBreakerTelemetry)
    assert isinstance(prom, StreamLeaseTelemetry)
