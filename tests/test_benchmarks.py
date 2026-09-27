"""Unit tests covering the algorithmic, statistical, and invariant logic of benchmarks."""

from __future__ import annotations

import pytest

from benchmarks.bench_soak import compute_linear_slope
from benchmarks.bench_wrapper_overhead import compute_distribution, percentile


def test_percentile_calculation():
    """Verify percentile calculation on empty, single, and multi-element sorted lists."""
    assert percentile([], 50.0) == 0.0
    assert percentile([42.0], 50.0) == 42.0
    assert percentile([42.0], 99.0) == 42.0

    data = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert percentile(data, 50.0) == 3.0
    assert percentile(data, 0.0) == 1.0
    assert percentile(data, 100.0) == 5.0


def test_compute_distribution_preserves_negative_deltas_no_clipping():
    """Verify that compute_distribution preserves negative deltas without clipping (B02)."""
    assert compute_distribution([])["p50_ms"] == 0.0

    # Negative deltas occur due to sub-millisecond network jitter
    deltas = [-0.15, -0.05, 0.0, 0.02, 0.10]
    dist = compute_distribution(deltas)
    assert dist["p50_ms"] == 0.0
    assert dist["min_ms"] == -0.15
    assert dist["max_ms"] == 0.10
    assert dist["p99_ms"] > 0.0


def test_linear_regression_slope_calculation():
    """Verify OLS linear regression slope calculation for plateau detection."""
    assert compute_linear_slope([], []) == 0.0
    assert compute_linear_slope([1.0], [50.0]) == 0.0
    assert compute_linear_slope([1.0, 1.0], [50.0, 50.0]) == 0.0

    # Flat line: zero slope
    times = [1.0, 2.0, 3.0, 4.0, 5.0]
    flat_rss = [50.0, 50.0, 50.0, 50.0, 50.0]
    assert compute_linear_slope(times, flat_rss) == 0.0

    # Linearly growing: positive slope
    growing_rss = [50.0, 51.0, 52.0, 53.0, 54.0]
    assert pytest.approx(compute_linear_slope(times, growing_rss), rel=1e-3) == 1.0

    # Falling RSS: negative slope
    falling_rss = [54.0, 53.0, 52.0, 51.0, 50.0]
    assert pytest.approx(compute_linear_slope(times, falling_rss), rel=1e-3) == -1.0


def test_task_leak_detection_identity_logic():
    """Verify task leak detection based on task identity rather than simple counts (S06)."""

    class DummyTask:
        def __init__(self, name: str):
            self.name = name

    t1 = DummyTask("baseline_1")
    t2 = DummyTask("baseline_2")
    baseline = {t1, t2}

    current = DummyTask("runner")
    # All clean
    final_clean = {t1, t2, current}
    extra_clean = final_clean - baseline - {current}
    assert len(extra_clean) == 0

    # Lingering orphaned task (count is same as baseline+current, but identity changed!)
    t_leak = DummyTask("orphaned_renew_worker")
    final_leaked = {t1, current, t_leak}
    extra_leaked = final_leaked - baseline - {current}
    assert extra_leaked == {t_leak}
    assert len(extra_leaked) == 1


@pytest.mark.asyncio
async def test_wrapper_overhead_input_validation():
    """Verify CLI input validation for bench_wrapper_overhead (V01)."""
    from benchmarks.bench_wrapper_overhead import run_comparative_benchmark

    with pytest.raises(ValueError, match="count must be at least 10"):
        await run_comparative_benchmark(
            "redis://localhost", count=5, concurrency=1, max_p99_overhead_ms=1.0
        )

    with pytest.raises(ValueError, match="concurrency must be at least 1"):
        await run_comparative_benchmark(
            "redis://localhost", count=100, concurrency=0, max_p99_overhead_ms=1.0
        )

    with pytest.raises(ValueError, match="max_p99_overhead_ms must be > 0"):
        await run_comparative_benchmark(
            "redis://localhost", count=100, concurrency=1, max_p99_overhead_ms=-1.0
        )


@pytest.mark.asyncio
async def test_soak_input_validation():
    """Verify CLI input validation for bench_soak (V01)."""
    from benchmarks.bench_soak import run_soak

    with pytest.raises(ValueError, match="duration_seconds must be > 0"):
        await run_soak("redis://localhost", duration_seconds=0, target_concurrency=10)

    with pytest.raises(ValueError, match="target_concurrency must be > 0"):
        await run_soak("redis://localhost", duration_seconds=10, target_concurrency=0)

    with pytest.raises(ValueError, match="renew_interval must be > 0 and < lease_seconds"):
        await run_soak(
            "redis://localhost",
            duration_seconds=10,
            target_concurrency=10,
            lease_seconds=5.0,
            renew_interval=10.0,
        )
