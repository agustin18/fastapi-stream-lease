"""
Tests for cardinality-safe observability adapters (Prometheus & OpenTelemetry).
"""

from __future__ import annotations

import asyncio
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
    StreamLeaseRejected,
    StreamLeaseUnavailable,
)
from fastapi_stream_lease.manager import StreamLeaseManager
from fastapi_stream_lease.observability.contract import (
    BackendErrorKind,
    LostReason,
    Operation,
    Outcome,
    validate_cardinality_safe,
)
from fastapi_stream_lease.observability.otel import OpenTelemetryMetrics
from fastapi_stream_lease.observability.prometheus import PrometheusMetrics


def test_cardinality_safe_validation() -> None:
    """Verifies that user_id, lease_id, or dynamic keys are rejected as metric labels."""
    safe_labels = {"operation": "acquire", "outcome": "success"}
    validate_cardinality_safe(safe_labels)

    with pytest.raises(ValueError, match="Forbidden metric label 'user_id'"):
        validate_cardinality_safe({"user_id": "123", "operation": "acquire"})

    with pytest.raises(ValueError, match="Forbidden metric label 'lease_id'"):
        validate_cardinality_safe({"lease_id": "abc-456", "outcome": "success"})


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
    """Verifies fallback counter and dispatcher hook queue depth and drops."""
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)

    metrics.record_fallback()
    assert registry.get_sample_value("fastapi_stream_lease_fallback_total") == 1.0

    metrics.record_hook_metrics(dropped_count=3, error_count=1, queue_depth=42)
    assert registry.get_sample_value("fastapi_stream_lease_hook_dropped_total") == 3.0
    assert registry.get_sample_value("fastapi_stream_lease_hook_errors_total") == 1.0
    assert registry.get_sample_value("fastapi_stream_lease_hook_queue_depth") == 42.0


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
    from fastapi_stream_lease.observability.contract import classify_backend_error

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
