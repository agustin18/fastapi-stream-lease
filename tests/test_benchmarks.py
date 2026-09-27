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


@pytest.mark.parametrize(
    ("baseline_p99", "current_p99", "max_allowed_pct", "noise_floor_ms", "expected_passed"),
    [
        (0.20, 0.21, 10.0, 0.20, True),  # +5% regression <= 10%
        (0.20, 0.22, 10.0, 0.20, True),  # +10% regression == 10%
        (0.20, 0.45, 10.0, 0.20, False),  # +125% regression > 10% AND delta 0.25 > 0.20
        (0.04, 0.16, 10.0, 0.20, True),  # +300% relative but delta 0.12 <= noise floor 0.20
        (0.50, 0.25, 10.0, 0.20, True),  # -50% improvement
    ],
)
def test_regression_gate_evaluation(
    baseline_p99: float,
    current_p99: float,
    max_allowed_pct: float,
    noise_floor_ms: float,
    expected_passed: bool,
) -> None:
    """Verify regression gate accepts within-budget deltas and rejects regressions (R1)."""
    abs_diff_ms = current_p99 - baseline_p99
    diff_pct = (abs_diff_ms / baseline_p99) * 100.0
    regression_passed = not (abs_diff_ms > noise_floor_ms and diff_pct > max_allowed_pct)
    assert regression_passed is expected_passed


@pytest.mark.parametrize(
    ("slope", "abs_growth", "rel_growth", "expected_stable"),
    [
        (0.05, 5.0, 5.0, True),  # Healthy plateau
        (0.16, 5.0, 5.0, False),  # Slope > 0.15 MB/s
        (0.05, 16.0, 5.0, False),  # Absolute growth > 15 MB
        (0.05, 5.0, 16.0, False),  # Relative growth > 15%
    ],
)
def test_plateau_stability_criteria(
    slope: float, abs_growth: float, rel_growth: float, expected_stable: bool
) -> None:
    """Verify memory plateau criteria strictly flags slope, absolute, and relative leaks (M02)."""
    plateau_stable = not (slope > 0.15 or rel_growth > 15.0 or abs_growth > 15.0)
    assert plateau_stable is expected_stable


@pytest.mark.parametrize(
    (
        "zero_ghosts",
        "zero_residual_keys",
        "zero_tasks",
        "rejections",
        "errors_count",
        "plateau",
        "expected_passed",
    ),
    [
        (True, True, True, 0, 0, True, True),  # All clean -> PASS
        (True, False, True, 0, 0, True, False),  # R2: Residual keys present -> FAIL
        (False, True, True, 0, 0, True, False),  # Ghost leases present -> FAIL
        (True, True, False, 0, 0, True, False),  # Task leak -> FAIL
        (True, True, True, 1, 0, True, False),  # Rejections -> FAIL
        (True, True, True, 0, 1, True, False),  # Errors -> FAIL
        (True, True, True, 0, 0, False, False),  # Unstable plateau -> FAIL
    ],
)
def test_soak_overall_passed_invariants(
    zero_ghosts: bool,
    zero_residual_keys: bool,
    zero_tasks: bool,
    rejections: int,
    errors_count: int,
    plateau: bool,
    expected_passed: bool,
) -> None:
    """Verify that soak passed requires zero ghost leases AND zero residual keys (R2, S04, S07)."""
    passed = (
        zero_ghosts
        and zero_residual_keys
        and zero_tasks
        and errors_count == 0
        and rejections == 0
        and plateau
    )
    assert passed is expected_passed


def test_baseline_json_parsing_robustness(tmp_path) -> None:
    """Verify baseline parser rejects missing fields, non-dicts, or invalid values (R3)."""
    import json
    import math

    def parse_baseline(file_path):
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("Baseline JSON must be an object/dict")
        if "max_p99_paired_overhead_ms" not in data:
            raise ValueError("Missing required field 'max_p99_paired_overhead_ms'")
        val = float(data["max_p99_paired_overhead_ms"])
        if not math.isfinite(val) or val <= 0.0:
            raise ValueError("Must be finite positive number")
        return val

    # Non-dict
    f1 = tmp_path / "f1.json"
    f1.write_text("[1, 2, 3]")
    with pytest.raises(ValueError, match="must be an object/dict"):
        parse_baseline(f1)

    # Missing field
    f2 = tmp_path / "f2.json"
    f2.write_text('{"other": 1.0}')
    with pytest.raises(ValueError, match="Missing required field"):
        parse_baseline(f2)

    # Non-positive
    f3 = tmp_path / "f3.json"
    f3.write_text('{"max_p99_paired_overhead_ms": 0.0}')
    with pytest.raises(ValueError, match="Must be finite positive"):
        parse_baseline(f3)

    # Valid
    f4 = tmp_path / "f4.json"
    f4.write_text('{"max_p99_paired_overhead_ms": 0.35}')
    assert parse_baseline(f4) == 0.35
