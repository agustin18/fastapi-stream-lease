#!/usr/bin/env python3
"""
Wrapper Overhead and Host Latency Benchmark for fastapi-stream-lease.

Measures:
1. Intrinsic host latency (raw Redis PING and minimal Lua EVAL)
2. Paired, interleaved measurements between Raw Lua execution and StreamLeaseManager
3. Equivalent Redis state: independent prefixes with identical key cardinality & distribution
4. True paired delta distribution (p50, p90, p95, p99) without artificial zero-clipping
5. Regression budget enforcement against persistent baseline (e.g. <= 10% regression)
6. Strict p99 paired overhead budget threshold (default <= 1.0 ms)
7. Forensic teardown verifying zero residual keys in Redis

Usage:
    docker compose run --rm backend uv run python benchmarks/bench_wrapper_overhead.py \\
        --count 1000 --concurrency 25 --max-p99-overhead-ms 1.0
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
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
    """Calculate percentile in milliseconds from sorted float list."""
    if not data:
        return 0.0
    k = (len(data) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(data) - 1)
    d = k - f
    return data[f] + d * (data[c] - data[f])


def compute_distribution(latencies_ms: list[float]) -> dict[str, float]:
    """Compute distribution percentiles in milliseconds from float list."""
    if not latencies_ms:
        return {
            "p50_ms": 0.0,
            "p90_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "mean_ms": 0.0,
            "min_ms": 0.0,
            "max_ms": 0.0,
        }
    sorted_data = sorted(latencies_ms)
    return {
        "p50_ms": round(percentile(sorted_data, 50), 3),
        "p90_ms": round(percentile(sorted_data, 90), 3),
        "p95_ms": round(percentile(sorted_data, 95), 3),
        "p99_ms": round(percentile(sorted_data, 99), 3),
        "mean_ms": round(sum(sorted_data) / len(sorted_data), 3),
        "min_ms": round(min(sorted_data), 3),
        "max_ms": round(max(sorted_data), 3),
    }


async def measure_host_latency(client: redis.Redis, samples: int = 200) -> dict[str, Any]:
    """Measure intrinsic host-to-Redis network roundtrip latency."""
    ping_samples: list[float] = []
    for _ in range(samples):
        t0 = time.perf_counter()
        await client.ping()
        ping_samples.append((time.perf_counter() - t0) * 1000.0)

    eval_samples: list[float] = []
    for _ in range(samples):
        t0 = time.perf_counter()
        await client.eval("return 1", 0)
        eval_samples.append((time.perf_counter() - t0) * 1000.0)

    return {
        "ping": compute_distribution(ping_samples),
        "minimal_eval": compute_distribution(eval_samples),
    }


async def run_comparative_benchmark(
    redis_url: str,
    count: int,
    concurrency: int,
    max_p99_overhead_ms: float,
    baseline_p99: float | None = None,
    max_regression_pct: float = 10.0,
    noise_floor_ms: float = 0.20,
) -> dict[str, Any]:
    if count < 10:
        raise ValueError(f"count must be at least 10, got {count}")
    if concurrency < 1:
        raise ValueError(f"concurrency must be at least 1, got {concurrency}")
    if max_p99_overhead_ms <= 0:
        raise ValueError(f"max_p99_overhead_ms must be > 0, got {max_p99_overhead_ms}")

    client = redis.from_url(redis_url)
    try:
        await client.ping()
    except Exception as exc:
        print(f"Error connecting to Redis at {redis_url}: {exc}", file=sys.stderr)
        return {"error": str(exc), "passed": False}

    run_uuid = uuid.uuid4().hex[:8]
    # B01: Independent namespaces with identical cardinality to eliminate cross-talk
    raw_prefix = f"bench_raw_{run_uuid}"
    wrap_prefix = f"bench_wrap_{run_uuid}"

    raw_global_key = f"{{{raw_prefix}}}:global"
    wrap_config = LeaseConfig(
        lease_seconds=60.0,
        max_per_user=count * 2,
        max_global=count * 4,
        key_prefix=wrap_prefix,
    )
    manager = StreamLeaseManager(redis=client, config=wrap_config)

    print("=" * 70)
    print("FASTAPI-STREAM-LEASE PAIRED WRAPPER OVERHEAD BENCHMARK")
    print(f"Sample Count: {count} paired operations | Concurrency: {concurrency}")
    print(f"Namespaces: Raw={raw_prefix} | Wrapped={wrap_prefix}")
    print(f"Overhead Target Budget: <= {max_p99_overhead_ms:.2f} ms at p99")
    print("=" * 70)

    # 1. Host Latency Calibration
    print("\n[Phase 1] Calibrating Intrinsic Host Latency...")
    host_stats = await measure_host_latency(client, samples=max(50, min(count // 5, 300)))
    print(
        f"    Redis PING:     p50={host_stats['ping']['p50_ms']}ms | "
        f"p99={host_stats['ping']['p99_ms']}ms"
    )
    print(
        f"    Minimal EVAL:   p50={host_stats['minimal_eval']['p50_ms']}ms | "
        f"p99={host_stats['minimal_eval']['p99_ms']}ms"
    )

    # Pre-warm connection pool and Lua script cache
    await asyncio.gather(*(client.ping() for _ in range(concurrency)))
    dummy_raw = f"{{{raw_prefix}}}:warmup"
    await client.eval(ACQUIRE_SCRIPT, 2, dummy_raw, raw_global_key, 60.0, "warmup", 100, 100, 120.0)
    await client.eval(RELEASE_SCRIPT, 2, dummy_raw, raw_global_key, "warmup")
    await client.delete(dummy_raw)

    sem = asyncio.Semaphore(concurrency)
    num_users = max(1, min(50, count // 10))

    # 2. PAIRED INTERLEAVED ACQUIRE
    print("\n[Phase 2] Paired Interleaved Acquire (Raw vs. Wrapped)...")
    raw_acq_ms: list[float] = [0.0] * count
    wrap_acq_ms: list[float] = [0.0] * count
    delta_acq_ms: list[float] = [0.0] * count
    acquired_leases: list[StreamLease | None] = [None] * count

    async def paired_acquire_task(i: int) -> None:
        async with sem:
            user_id = f"user_{i % num_users}"
            raw_user_key = f"{{{raw_prefix}}}:user:{user_id}"
            lease_id_raw = f"raw_l_{i}"

            # Interleave order to balance scheduler and Redis latency variance
            if i % 2 == 0:
                # Raw first
                t0 = time.perf_counter()
                await client.eval(
                    ACQUIRE_SCRIPT,
                    2,
                    raw_user_key,
                    raw_global_key,
                    wrap_config.lease_seconds,
                    lease_id_raw,
                    wrap_config.max_per_user,
                    wrap_config.max_global,
                    wrap_config.redis_ttl,
                )
                r_time = (time.perf_counter() - t0) * 1000.0

                # Wrapped second
                t0 = time.perf_counter()
                lease = await manager.acquire(user_id)
                w_time = (time.perf_counter() - t0) * 1000.0
            else:
                # Wrapped first
                t0 = time.perf_counter()
                lease = await manager.acquire(user_id)
                w_time = (time.perf_counter() - t0) * 1000.0

                # Raw second
                t0 = time.perf_counter()
                await client.eval(
                    ACQUIRE_SCRIPT,
                    2,
                    raw_user_key,
                    raw_global_key,
                    wrap_config.lease_seconds,
                    lease_id_raw,
                    wrap_config.max_per_user,
                    wrap_config.max_global,
                    wrap_config.redis_ttl,
                )
                r_time = (time.perf_counter() - t0) * 1000.0

            raw_acq_ms[i] = r_time
            wrap_acq_ms[i] = w_time
            # B02: Pure paired delta without artificial zero-clipping
            delta_acq_ms[i] = w_time - r_time
            acquired_leases[i] = lease

    await asyncio.gather(*(paired_acquire_task(i) for i in range(count)))

    acq_raw_dist = compute_distribution(raw_acq_ms)
    acq_wrap_dist = compute_distribution(wrap_acq_ms)
    acq_delta_dist = compute_distribution(delta_acq_ms)

    print(f"    Raw Acquire:     p50={acq_raw_dist['p50_ms']}ms | p99={acq_raw_dist['p99_ms']}ms")
    print(f"    Wrapped Acquire: p50={acq_wrap_dist['p50_ms']}ms | p99={acq_wrap_dist['p99_ms']}ms")
    print(
        f"    --> PAIRED ACQUIRE OVERHEAD: p50={acq_delta_dist['p50_ms']}ms | "
        f"p95={acq_delta_dist['p95_ms']}ms | p99={acq_delta_dist['p99_ms']}ms"
    )

    # 3. PAIRED INTERLEAVED RENEW
    print("\n[Phase 3] Paired Interleaved Renew (Raw vs. Wrapped)...")
    raw_renew_ms: list[float] = [0.0] * count
    wrap_renew_ms: list[float] = [0.0] * count
    delta_renew_ms: list[float] = [0.0] * count

    async def paired_renew_task(i: int) -> None:
        async with sem:
            user_id = f"user_{i % num_users}"
            raw_user_key = f"{{{raw_prefix}}}:user:{user_id}"
            lease_id_raw = f"raw_l_{i}"
            lease = acquired_leases[i]
            assert lease is not None

            if i % 2 == 0:
                t0 = time.perf_counter()
                await client.eval(
                    RENEW_SCRIPT,
                    2,
                    raw_user_key,
                    raw_global_key,
                    lease_id_raw,
                    wrap_config.lease_seconds,
                    wrap_config.redis_ttl,
                    1,
                )
                r_time = (time.perf_counter() - t0) * 1000.0

                t0 = time.perf_counter()
                ok = await manager.renew(lease)
                w_time = (time.perf_counter() - t0) * 1000.0
                assert ok is True
            else:
                t0 = time.perf_counter()
                ok = await manager.renew(lease)
                w_time = (time.perf_counter() - t0) * 1000.0
                assert ok is True

                t0 = time.perf_counter()
                await client.eval(
                    RENEW_SCRIPT,
                    2,
                    raw_user_key,
                    raw_global_key,
                    lease_id_raw,
                    wrap_config.lease_seconds,
                    wrap_config.redis_ttl,
                    1,
                )
                r_time = (time.perf_counter() - t0) * 1000.0

            raw_renew_ms[i] = r_time
            wrap_renew_ms[i] = w_time
            delta_renew_ms[i] = w_time - r_time

    await asyncio.gather(*(paired_renew_task(i) for i in range(count)))

    renew_raw_dist = compute_distribution(raw_renew_ms)
    renew_wrap_dist = compute_distribution(wrap_renew_ms)
    renew_delta_dist = compute_distribution(delta_renew_ms)

    print(
        f"    Raw Renew:       p50={renew_raw_dist['p50_ms']}ms | p99={renew_raw_dist['p99_ms']}ms"
    )
    print(
        f"    Wrapped Renew:   p50={renew_wrap_dist['p50_ms']}ms | "
        f"p99={renew_wrap_dist['p99_ms']}ms"
    )
    print(
        f"    --> PAIRED RENEW OVERHEAD:   p50={renew_delta_dist['p50_ms']}ms | "
        f"p95={renew_delta_dist['p95_ms']}ms | p99={renew_delta_dist['p99_ms']}ms"
    )

    # 4. PAIRED INTERLEAVED RELEASE
    print("\n[Phase 4] Paired Interleaved Release (Raw vs. Wrapped)...")
    raw_rel_ms: list[float] = [0.0] * count
    wrap_rel_ms: list[float] = [0.0] * count
    delta_rel_ms: list[float] = [0.0] * count

    async def paired_release_task(i: int) -> None:
        async with sem:
            user_id = f"user_{i % num_users}"
            raw_user_key = f"{{{raw_prefix}}}:user:{user_id}"
            lease_id_raw = f"raw_l_{i}"
            lease = acquired_leases[i]
            assert lease is not None

            if i % 2 == 0:
                t0 = time.perf_counter()
                await client.eval(RELEASE_SCRIPT, 2, raw_user_key, raw_global_key, lease_id_raw)
                r_time = (time.perf_counter() - t0) * 1000.0

                t0 = time.perf_counter()
                await lease.release()
                w_time = (time.perf_counter() - t0) * 1000.0
            else:
                t0 = time.perf_counter()
                await lease.release()
                w_time = (time.perf_counter() - t0) * 1000.0

                t0 = time.perf_counter()
                await client.eval(RELEASE_SCRIPT, 2, raw_user_key, raw_global_key, lease_id_raw)
                r_time = (time.perf_counter() - t0) * 1000.0

            raw_rel_ms[i] = r_time
            wrap_rel_ms[i] = w_time
            delta_rel_ms[i] = w_time - r_time

    await asyncio.gather(*(paired_release_task(i) for i in range(count)))

    rel_raw_dist = compute_distribution(raw_rel_ms)
    rel_wrap_dist = compute_distribution(wrap_rel_ms)
    rel_delta_dist = compute_distribution(delta_rel_ms)

    print(f"    Raw Release:     p50={rel_raw_dist['p50_ms']}ms | p99={rel_raw_dist['p99_ms']}ms")
    print(f"    Wrapped Release: p50={rel_wrap_dist['p50_ms']}ms | p99={rel_wrap_dist['p99_ms']}ms")
    print(
        f"    --> PAIRED RELEASE OVERHEAD: p50={rel_delta_dist['p50_ms']}ms | "
        f"p95={rel_delta_dist['p95_ms']}ms | p99={rel_delta_dist['p99_ms']}ms"
    )

    # 5. FORENSIC TEARDOWN & RESIDUAL VERIFICATION
    # Delete test keys and verify zero residual keys
    for i in range(num_users):
        u_key = f"{{{raw_prefix}}}:user:user_{i}"
        await client.delete(u_key)
    await client.delete(raw_global_key)
    await manager.close(drain=True)

    # Scan for any lingering keys under raw or wrap prefix
    residual_raw_keys = [k async for k in client.scan_iter(match=f"*{raw_prefix}*")]
    residual_wrap_keys = [k async for k in client.scan_iter(match=f"*{wrap_prefix}*")]
    zero_residual_keys = len(residual_raw_keys) == 0 and len(residual_wrap_keys) == 0
    await client.aclose()

    # 6. BUDGET AND REGRESSION ANALYSIS
    max_p99_paired_overhead = max(
        acq_delta_dist["p99_ms"], renew_delta_dist["p99_ms"], rel_delta_dist["p99_ms"]
    )
    budget_passed = max_p99_paired_overhead <= max_p99_overhead_ms

    regression_passed = True
    regression_details: dict[str, Any] = {}

    if baseline_p99 is not None and baseline_p99 > 0:
        abs_diff_ms = max_p99_paired_overhead - baseline_p99
        diff_pct = (abs_diff_ms / baseline_p99) * 100.0
        regression_details = {
            "baseline_p99_ms": baseline_p99,
            "current_p99_ms": max_p99_paired_overhead,
            "delta_ms": round(abs_diff_ms, 3),
            "delta_pct": round(diff_pct, 2),
            "noise_floor_ms": noise_floor_ms,
            "max_allowed_regression_pct": max_regression_pct,
        }
        if abs_diff_ms > noise_floor_ms and diff_pct > max_regression_pct:
            regression_passed = False
            print(
                f"\n[Phase 5] REGRESSION DETECTED: +{diff_pct:.2f}% "
                f"(+{abs_diff_ms:.3f} ms > noise floor {noise_floor_ms:.2f} ms, "
                f"allowed: <= +{max_regression_pct:.1f}%)"
            )
        else:
            print(
                f"\n[Phase 5] Regression Check PASSED: delta={diff_pct:+.2f}% "
                f"({abs_diff_ms:+.3f} ms) vs baseline"
            )

    overall_passed = budget_passed and regression_passed and zero_residual_keys

    result_summary: dict[str, Any] = {
        "timestamp": time.time(),
        "count": count,
        "concurrency": concurrency,
        "host_latency": host_stats,
        "acquire": {
            "raw": acq_raw_dist,
            "wrapped": acq_wrap_dist,
            "paired_overhead": acq_delta_dist,
        },
        "renew": {
            "raw": renew_raw_dist,
            "wrapped": renew_wrap_dist,
            "paired_overhead": renew_delta_dist,
        },
        "release": {
            "raw": rel_raw_dist,
            "wrapped": rel_wrap_dist,
            "paired_overhead": rel_delta_dist,
        },
        "max_p99_paired_overhead_ms": round(max_p99_paired_overhead, 3),
        "budget_threshold_ms": max_p99_overhead_ms,
        "budget_passed": budget_passed,
        "regression_passed": regression_passed,
        "regression_details": regression_details,
        "zero_residual_keys": zero_residual_keys,
        "passed": overall_passed,
    }

    print("\n" + "=" * 70)
    print(f"BENCHMARK RESULT: {'PASSED [OK]' if overall_passed else 'FAILED [X]'}")
    print(
        f"Max p99 Paired Overhead: {max_p99_paired_overhead:.3f} ms "
        f"(budget: <= {max_p99_overhead_ms:.2f} ms)"
    )
    print(f"Zero Residual Keys: {'[OK]' if zero_residual_keys else '[FAIL]'}")
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
        help="Number of paired operations to benchmark (default: 1000)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Concurrent coroutines (default: 1)",
    )
    parser.add_argument(
        "--max-p99-overhead-ms",
        type=float,
        default=1.0,
        help="Maximum allowable p99 paired wrapper overhead in milliseconds (default: 1.0)",
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
        "--noise-floor-ms",
        type=float,
        default=0.20,
        help="Overhead noise floor in ms below which regressions are considered host jitter",
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Print machine-readable JSON output",
    )
    args = parser.parse_args()

    # Argument validation
    if args.count < 10:
        print(f"Error: --count must be >= 10, got {args.count}", file=sys.stderr)
        sys.exit(1)
    if args.concurrency < 1:
        print(f"Error: --concurrency must be >= 1, got {args.concurrency}", file=sys.stderr)
        sys.exit(1)

    baseline_p99: float | None = None
    if args.baseline:
        baseline_file = Path(args.baseline)
        if not baseline_file.exists():
            print(f"Error: Baseline file not found: {args.baseline}", file=sys.stderr)
            sys.exit(1)
        try:
            with open(baseline_file, encoding="utf-8") as f:
                baseline_data = json.load(f)
            if not isinstance(baseline_data, dict):
                raise ValueError("Baseline JSON must be an object/dict")
            if "max_p99_paired_overhead_ms" not in baseline_data:
                raise ValueError(
                    "Missing required field 'max_p99_paired_overhead_ms' in baseline JSON"
                )
            val = float(baseline_data["max_p99_paired_overhead_ms"])
            if not math.isfinite(val) or val <= 0.0:
                raise ValueError(
                    f"'max_p99_paired_overhead_ms' must be a finite positive number, got {val}"
                )
            baseline_p99 = val
        except Exception as err:
            print(
                f"Error: Failed to parse baseline JSON from {args.baseline}: {err}",
                file=sys.stderr,
            )
            sys.exit(1)

    results = asyncio.run(
        run_comparative_benchmark(
            redis_url=args.redis_url,
            count=args.count,
            concurrency=args.concurrency,
            max_p99_overhead_ms=args.max_p99_overhead_ms,
            baseline_p99=baseline_p99,
            max_regression_pct=args.max_regression_pct,
            noise_floor_ms=args.noise_floor_ms,
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
