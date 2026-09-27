#!/usr/bin/env python3
"""
Wrapper Overhead and Host Latency Benchmark for fastapi-stream-lease.

Measures:
1. Intrinsic host latency (raw Redis PING and minimal Lua EVAL)
2. Raw Redis Lua script latency vs. StreamLeaseManager wrapped latency
3. Exact wrapper overhead (p50, p95, p99, p99.9) for acquire, renew, and release
4. Regression budget enforcement against baseline (e.g. <= 10% regression)
5. Strict p99 overhead budget threshold (default <= 1.0 ms)

Usage:
    docker compose run --rm backend uv run python benchmarks/bench_wrapper_overhead.py \\
        --count 1000 --concurrency 20 --max-p99-overhead-ms 1.0
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import redis.asyncio as redis

from fastapi_stream_lease import LeaseConfig, StreamLease, StreamLeaseManager
from fastapi_stream_lease.lua import ACQUIRE_SCRIPT, RELEASE_SCRIPT, RENEW_SCRIPT


def percentile(data: list[float], pct: float) -> float:
    """Calculate percentile in milliseconds from sorted seconds float list."""
    if not data:
        return 0.0
    k = (len(data) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(data) - 1)
    d = k - f
    return (data[f] + d * (data[c] - data[f])) * 1000.0


def compute_stats(latencies_seconds: list[float]) -> dict[str, float]:
    """Compute distribution percentiles and statistics in milliseconds."""
    if not latencies_seconds:
        return {
            "p50_ms": 0.0,
            "p90_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "mean_ms": 0.0,
            "min_ms": 0.0,
            "max_ms": 0.0,
        }
    sorted_data = sorted(latencies_seconds)
    return {
        "p50_ms": round(percentile(sorted_data, 50), 3),
        "p90_ms": round(percentile(sorted_data, 90), 3),
        "p95_ms": round(percentile(sorted_data, 95), 3),
        "p99_ms": round(percentile(sorted_data, 99), 3),
        "mean_ms": round((sum(sorted_data) / len(sorted_data)) * 1000.0, 3),
        "min_ms": round(min(sorted_data) * 1000.0, 3),
        "max_ms": round(max(sorted_data) * 1000.0, 3),
    }


async def measure_host_latency(client: redis.Redis, samples: int = 200) -> dict[str, Any]:
    """Measure intrinsic host-to-Redis network roundtrip latency."""
    # Ping roundtrip
    ping_samples: list[float] = []
    for _ in range(samples):
        t0 = time.perf_counter()
        await client.ping()
        ping_samples.append(time.perf_counter() - t0)

    # Minimal Lua EVAL roundtrip (evaluating simple arithmetic)
    eval_samples: list[float] = []
    for _ in range(samples):
        t0 = time.perf_counter()
        await client.eval("return 1", 0)
        eval_samples.append(time.perf_counter() - t0)

    return {
        "ping": compute_stats(ping_samples),
        "minimal_eval": compute_stats(eval_samples),
    }


async def run_comparative_benchmark(
    redis_url: str,
    count: int,
    concurrency: int,
    max_p99_overhead_ms: float,
    baseline_p99: float | None = None,
    max_regression_pct: float = 10.0,
) -> dict[str, Any]:
    client = redis.from_url(redis_url)
    try:
        await client.ping()
    except Exception as exc:
        print(f"Error connecting to Redis at {redis_url}: {exc}", file=sys.stderr)
        return {"error": str(exc), "passed": False}

    run_uuid = uuid.uuid4().hex[:8]
    config = LeaseConfig(
        lease_seconds=60.0,
        max_per_user=count * 2,
        max_global=count * 4,
        key_prefix=f"bench_overhead_{run_uuid}",
    )
    manager = StreamLeaseManager(redis=client, config=config)

    print("=" * 70)
    print("FASTAPI-STREAM-LEASE WRAPPER OVERHEAD & LATENCY BENCHMARK")
    print(f"Sample Count: {count} | Concurrency: {concurrency}")
    print(f"Prefix: {config._cluster_prefix}")
    print(f"Overhead Target Budget: <= {max_p99_overhead_ms:.2f} ms at p99")
    print("=" * 70)

    # 1. Host Latency Calibration
    print("\n[Phase 1] Calibrating Intrinsic Host Latency...")
    host_stats = await measure_host_latency(client, samples=max(100, min(count // 5, 500)))
    print(
        f"    Redis PING:     p50={host_stats['ping']['p50_ms']}ms | "
        f"p99={host_stats['ping']['p99_ms']}ms"
    )
    print(
        f"    Minimal EVAL:   p50={host_stats['minimal_eval']['p50_ms']}ms | "
        f"p99={host_stats['minimal_eval']['p99_ms']}ms"
    )

    sem = asyncio.Semaphore(concurrency)

    # 2. Acquire Comparison: Raw EVAL vs. StreamLeaseManager.acquire
    print("\n[Phase 2] Measuring Acquire Latencies (Raw vs. Wrapped)...")
    raw_acq_times: list[float] = []
    wrapped_acq_times: list[float] = []
    raw_user_key = f"{config._cluster_prefix}:raw_user"
    raw_global_key = config.global_key

    async def raw_acquire_worker(idx: int) -> None:
        async with sem:
            lease_id = f"raw_lease_{idx}"
            t0 = time.perf_counter()
            await client.eval(
                ACQUIRE_SCRIPT,
                2,
                raw_user_key,
                raw_global_key,
                config.lease_seconds,
                lease_id,
                config.max_per_user,
                config.max_global,
                config.redis_ttl,
            )
            raw_acq_times.append(time.perf_counter() - t0)

    await asyncio.gather(*(raw_acquire_worker(i) for i in range(count)))

    async def wrapped_acquire_worker(idx: int) -> StreamLease:
        async with sem:
            t0 = time.perf_counter()
            lease = await manager.acquire(f"wrap_user_{idx % 50}")
            wrapped_acq_times.append(time.perf_counter() - t0)
            return lease

    acquired_leases: list[StreamLease] = list(
        await asyncio.gather(*(wrapped_acquire_worker(i) for i in range(count)))
    )

    acq_raw_stats = compute_stats(raw_acq_times)
    acq_wrapped_stats = compute_stats(wrapped_acq_times)
    acq_overhead_p99 = max(0.0, acq_wrapped_stats["p99_ms"] - acq_raw_stats["p99_ms"])
    acq_overhead_p50 = max(0.0, acq_wrapped_stats["p50_ms"] - acq_raw_stats["p50_ms"])

    print(
        f"    Raw Acquire:     p50={acq_raw_stats['p50_ms']}ms | "
        f"p95={acq_raw_stats['p95_ms']}ms | p99={acq_raw_stats['p99_ms']}ms"
    )
    print(
        f"    Wrapped Acquire: p50={acq_wrapped_stats['p50_ms']}ms | "
        f"p95={acq_wrapped_stats['p95_ms']}ms | p99={acq_wrapped_stats['p99_ms']}ms"
    )
    print(f"    --> ACQUIRE OVERHEAD: p50={acq_overhead_p50:.3f}ms | p99={acq_overhead_p99:.3f}ms")

    # 3. Renew Comparison: Raw EVAL vs. StreamLeaseManager.renew
    print("\n[Phase 3] Measuring Renew Latencies (Raw vs. Wrapped)...")
    raw_renew_times: list[float] = []
    wrapped_renew_times: list[float] = []

    async def raw_renew_worker(idx: int) -> None:
        async with sem:
            lease_id = f"raw_lease_{idx}"
            t0 = time.perf_counter()
            await client.eval(
                RENEW_SCRIPT,
                2,
                raw_user_key,
                raw_global_key,
                lease_id,
                config.lease_seconds,
                config.redis_ttl,
                1,
            )
            raw_renew_times.append(time.perf_counter() - t0)

    await asyncio.gather(*(raw_renew_worker(i) for i in range(count)))

    async def wrapped_renew_worker(lease_obj: StreamLease) -> None:
        async with sem:
            t0 = time.perf_counter()
            ok = await manager.renew(lease_obj)
            wrapped_renew_times.append(time.perf_counter() - t0)
            assert ok is True

    await asyncio.gather(*(wrapped_renew_worker(item) for item in acquired_leases))

    renew_raw_stats = compute_stats(raw_renew_times)
    renew_wrapped_stats = compute_stats(wrapped_renew_times)
    renew_overhead_p99 = max(0.0, renew_wrapped_stats["p99_ms"] - renew_raw_stats["p99_ms"])
    renew_overhead_p50 = max(0.0, renew_wrapped_stats["p50_ms"] - renew_raw_stats["p50_ms"])

    print(
        f"    Raw Renew:       p50={renew_raw_stats['p50_ms']}ms | "
        f"p95={renew_raw_stats['p95_ms']}ms | p99={renew_raw_stats['p99_ms']}ms"
    )
    print(
        f"    Wrapped Renew:   p50={renew_wrapped_stats['p50_ms']}ms | "
        f"p95={renew_wrapped_stats['p95_ms']}ms | p99={renew_wrapped_stats['p99_ms']}ms"
    )
    print(
        f"    --> RENEW OVERHEAD:   p50={renew_overhead_p50:.3f}ms | p99={renew_overhead_p99:.3f}ms"
    )

    # 4. Release Comparison: Raw EVAL vs. StreamLease.release
    print("\n[Phase 4] Measuring Release Latencies (Raw vs. Wrapped)...")
    raw_release_times: list[float] = []
    wrapped_release_times: list[float] = []

    async def raw_release_worker(idx: int) -> None:
        async with sem:
            lease_id = f"raw_lease_{idx}"
            t0 = time.perf_counter()
            await client.eval(RELEASE_SCRIPT, 2, raw_user_key, raw_global_key, lease_id)
            raw_release_times.append(time.perf_counter() - t0)

    await asyncio.gather(*(raw_release_worker(i) for i in range(count)))

    async def wrapped_release_worker(lease_obj: StreamLease) -> None:
        async with sem:
            t0 = time.perf_counter()
            await lease_obj.release()
            wrapped_release_times.append(time.perf_counter() - t0)

    await asyncio.gather(*(wrapped_release_worker(item) for item in acquired_leases))

    release_raw_stats = compute_stats(raw_release_times)
    release_wrapped_stats = compute_stats(wrapped_release_times)
    release_overhead_p99 = max(0.0, release_wrapped_stats["p99_ms"] - release_raw_stats["p99_ms"])
    release_overhead_p50 = max(0.0, release_wrapped_stats["p50_ms"] - release_raw_stats["p50_ms"])

    print(
        f"    Raw Release:     p50={release_raw_stats['p50_ms']}ms | "
        f"p95={release_raw_stats['p95_ms']}ms | p99={release_raw_stats['p99_ms']}ms"
    )
    print(
        f"    Wrapped Release: p50={release_wrapped_stats['p50_ms']}ms | "
        f"p95={release_wrapped_stats['p95_ms']}ms | p99={release_wrapped_stats['p99_ms']}ms"
    )
    print(
        f"    --> RELEASE OVERHEAD: p50={release_overhead_p50:.3f}ms | "
        f"p99={release_overhead_p99:.3f}ms"
    )

    # Cleanup Redis
    await client.delete(raw_user_key, raw_global_key)
    await manager.close(drain=True)
    await client.aclose()

    # 5. Budget and Regression Validation
    max_measured_p99_overhead = max(acq_overhead_p99, renew_overhead_p99, release_overhead_p99)
    budget_passed = max_measured_p99_overhead <= max_p99_overhead_ms

    regression_passed = True
    regression_details: dict[str, Any] = {}

    if baseline_p99 is not None and baseline_p99 > 0:
        diff_pct = ((max_measured_p99_overhead - baseline_p99) / baseline_p99) * 100.0
        regression_details = {
            "baseline_max_p99_ms": baseline_p99,
            "current_max_p99_ms": max_measured_p99_overhead,
            "delta_pct": round(diff_pct, 2),
            "max_allowed_regression_pct": max_regression_pct,
        }
        if diff_pct > max_regression_pct:
            regression_passed = False
            print(
                f"\n[Phase 5] REGRESSION DETECTED: +{diff_pct:.2f}% "
                f"(allowed: <= +{max_regression_pct:.1f}%)"
            )
        else:
            print(f"\n[Phase 5] Regression Check PASSED: delta={diff_pct:+.2f}% vs baseline")

    result_summary: dict[str, Any] = {
        "timestamp": time.time(),
        "count": count,
        "concurrency": concurrency,
        "host_latency": host_stats,
        "acquire": {
            "raw": acq_raw_stats,
            "wrapped": acq_wrapped_stats,
            "overhead_p50_ms": round(acq_overhead_p50, 3),
            "overhead_p99_ms": round(acq_overhead_p99, 3),
        },
        "renew": {
            "raw": renew_raw_stats,
            "wrapped": renew_wrapped_stats,
            "overhead_p50_ms": round(renew_overhead_p50, 3),
            "overhead_p99_ms": round(renew_overhead_p99, 3),
        },
        "release": {
            "raw": release_raw_stats,
            "wrapped": release_wrapped_stats,
            "overhead_p50_ms": round(release_overhead_p50, 3),
            "overhead_p99_ms": round(release_overhead_p99, 3),
        },
        "max_p99_overhead_ms": round(max_measured_p99_overhead, 3),
        "budget_threshold_ms": max_p99_overhead_ms,
        "budget_passed": budget_passed,
        "regression_passed": regression_passed,
        "regression_details": regression_details,
        "passed": budget_passed and regression_passed,
    }

    print("\n" + "=" * 70)
    print(f"BENCHMARK RESULT: {'PASSED [OK]' if result_summary['passed'] else 'FAILED [X]'}")
    print(
        f"Max p99 Overhead: {max_measured_p99_overhead:.3f} ms "
        f"(budget: <= {max_p99_overhead_ms:.2f} ms)"
    )
    print("=" * 70)

    return result_summary


def main() -> None:
    parser = argparse.ArgumentParser(description="fastapi-stream-lease wrapper overhead benchmark")
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("REDIS_URL", "redis://redis:6379/15"),
        help="Redis connection URL",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="Number of operations to benchmark (default: 1000)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=25,
        help="Concurrent coroutines (default: 25)",
    )
    parser.add_argument(
        "--max-p99-overhead-ms",
        type=float,
        default=1.0,
        help="Maximum allowable p99 wrapper overhead in milliseconds (default: 1.0)",
    )
    parser.add_argument(
        "--baseline",
        type=str,
        default=None,
        help="Path to previous JSON benchmark baseline to verify regression budget",
    )
    parser.add_argument(
        "--save-baseline",
        type=str,
        default=None,
        help="Save current benchmark results as JSON baseline to specified path",
    )
    parser.add_argument(
        "--max-regression-pct",
        type=float,
        default=10.0,
        help="Maximum regression percentage allowed vs baseline (default: 10.0)",
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Print machine-readable JSON output",
    )
    args = parser.parse_args()

    baseline_p99: float | None = None
    if args.baseline and Path(args.baseline).exists():
        try:
            with open(args.baseline, encoding="utf-8") as f:
                baseline_data = json.load(f)
            baseline_p99 = float(baseline_data.get("max_p99_overhead_ms", 1.0))
        except Exception as err:
            print(f"Warning: Failed to load baseline from {args.baseline}: {err}", file=sys.stderr)

    results = asyncio.run(
        run_comparative_benchmark(
            redis_url=args.redis_url,
            count=args.count,
            concurrency=args.concurrency,
            max_p99_overhead_ms=args.max_p99_overhead_ms,
            baseline_p99=baseline_p99,
            max_regression_pct=args.max_regression_pct,
        )
    )

    if args.save_baseline:
        with open(args.save_baseline, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        print(f"Saved benchmark baseline report to {args.save_baseline}")

    if args.json_output:
        print("\n" + json.dumps(results, indent=2))

    if not results.get("passed", False):
        sys.exit(1)


if __name__ == "__main__":
    main()
