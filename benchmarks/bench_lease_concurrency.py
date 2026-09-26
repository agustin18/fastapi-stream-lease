#!/usr/bin/env python3
"""
Reproducible Concurrency and Throughput Benchmark for fastapi-stream-lease.

Measures:
- Acquire throughput (leases/sec) and p50/p95/p99 latency
- Renewal throughput (renews/sec) and p50/p95/p99 latency
- Release throughput (releases/sec) and p50/p95/p99 latency
- Redis memory footprint per 1,000 active stream leases

Usage:
    docker compose run --rm backend python benchmarks/bench_lease_concurrency.py \\
        --count 1000 --concurrency 50
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
import uuid
from typing import Any

import redis.asyncio as redis

from fastapi_stream_lease import LeaseConfig, StreamLeaseManager


def percentile(data: list[float], pct: float) -> float:
    """Calculate percentile from sorted list in milliseconds."""
    if not data:
        return 0.0
    k = (len(data) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(data) - 1)
    d = k - f
    return (data[f] + d * (data[c] - data[f])) * 1000.0


async def get_redis_memory(client: redis.Redis) -> dict[str, Any]:
    """Fetch memory usage statistics from Redis if available."""
    try:
        info = await client.info("memory")
        return {
            "used_memory_human": info.get("used_memory_human", "N/A"),
            "used_memory_peak_human": info.get("used_memory_peak_human", "N/A"),
            "used_memory_bytes": int(info.get("used_memory", 0)),
        }
    except Exception:
        return {
            "used_memory_human": "N/A",
            "used_memory_peak_human": "N/A",
            "used_memory_bytes": 0,
        }


async def run_benchmark(
    redis_url: str,
    total_leases: int,
    concurrency: int,
    base_prefix: str,
    run_idx: int = 1,
    total_runs: int = 1,
) -> dict[str, Any]:
    client = redis.from_url(redis_url)
    try:
        await client.ping()
    except Exception as exc:
        print(f"Error connecting to Redis at {redis_url}: {exc}", file=sys.stderr)
        return {"error": str(exc)}

    # Dynamic run prefix to guarantee zero interference between runs or leftover data
    run_uuid = uuid.uuid4().hex[:8]
    key_prefix = f"{base_prefix}_{run_uuid}"

    config = LeaseConfig(
        lease_seconds=60.0,
        max_per_user=total_leases,
        max_global=total_leases * 2,
        key_prefix=key_prefix,
    )
    manager = StreamLeaseManager(redis=client, config=config)

    print("=" * 60)
    run_banner = f" (Run {run_idx}/{total_runs})" if total_runs > 1 else ""
    print(f"FASTAPI-STREAM-LEASE CONCURRENCY BENCHMARK{run_banner}")
    print(f"Total Leases: {total_leases} | Concurrency: {concurrency} concurrent coroutines")
    print(f"Redis URL: {redis_url} | Prefix: {config._cluster_prefix}")
    print("=" * 60)

    # 0. PRE-WARM CONNECTION POOL
    # Warms up TCP sockets so connection handshake latency does not skew acquire
    await asyncio.gather(*(client.ping() for _ in range(concurrency)))

    # Initial Memory
    mem_before = await get_redis_memory(client)

    sem = asyncio.Semaphore(concurrency)

    # 1. ACQUIRE PHASE
    acquire_latencies: list[float] = []

    async def acquire_task(idx: int) -> Any:
        async with sem:
            t0 = time.monotonic()
            lease = await manager.acquire(f"bench_user_{idx % 100}")
            dt = time.monotonic() - t0
            acquire_latencies.append(dt)
            return lease

    t_start_acq = time.monotonic()
    leases = await asyncio.gather(*(acquire_task(i) for i in range(total_leases)))
    t_total_acq = time.monotonic() - t_start_acq
    acq_rps = total_leases / t_total_acq if t_total_acq > 0 else 0

    acquire_latencies.sort()
    acq_p50 = percentile(acquire_latencies, 50)
    acq_p95 = percentile(acquire_latencies, 95)
    acq_p99 = percentile(acquire_latencies, 99)

    mem_during = await get_redis_memory(client)
    active_count = await manager.get_active_count()

    mem_delta_bytes = max(
        0, mem_during.get("used_memory_bytes", 0) - mem_before.get("used_memory_bytes", 0)
    )
    bytes_per_lease = mem_delta_bytes / total_leases if total_leases > 0 else 0

    print("\n[1] ACQUIRE PHASE:")
    print(f"    Completed:  {total_leases} leases in {t_total_acq:.3f}s ({acq_rps:.1f} ops/sec)")
    print(f"    Latency:    p50={acq_p50:.2f}ms | p95={acq_p95:.2f}ms | p99={acq_p99:.2f}ms")
    print(f"    Active:     {active_count} active leases verified in Redis")
    print(
        f"    Redis RAM:  {mem_before['used_memory_human']} -> {mem_during['used_memory_human']} "
        f"(+{mem_delta_bytes / 1024:.1f} KB, ~{bytes_per_lease:.0f} B/lease)"
    )

    # 2. RENEW PHASE
    renew_latencies: list[float] = []

    async def renew_task(lease: Any) -> None:
        async with sem:
            t0 = time.monotonic()
            ok = await manager.renew(lease)
            dt = time.monotonic() - t0
            assert ok is True
            renew_latencies.append(dt)

    t_start_renew = time.monotonic()
    await asyncio.gather(*(renew_task(lease_item) for lease_item in leases))
    t_total_renew = time.monotonic() - t_start_renew
    renew_rps = total_leases / t_total_renew if t_total_renew > 0 else 0

    renew_latencies.sort()
    renew_p50 = percentile(renew_latencies, 50)
    renew_p95 = percentile(renew_latencies, 95)
    renew_p99 = percentile(renew_latencies, 99)

    print("\n[2] RENEWAL HEARTBEAT PHASE:")
    print(
        f"    Completed:  {total_leases} renewals in {t_total_renew:.3f}s ({renew_rps:.1f} ops/sec)"
    )
    print(f"    Latency:    p50={renew_p50:.2f}ms | p95={renew_p95:.2f}ms | p99={renew_p99:.2f}ms")

    # 3. RELEASE PHASE
    release_latencies: list[float] = []

    async def release_task(lease: Any) -> None:
        async with sem:
            t0 = time.monotonic()
            await lease.release()
            dt = time.monotonic() - t0
            release_latencies.append(dt)

    t_start_rel = time.monotonic()
    await asyncio.gather(*(release_task(lease_item) for lease_item in leases))
    t_total_rel = time.monotonic() - t_start_rel
    rel_rps = total_leases / t_total_rel if t_total_rel > 0 else 0

    release_latencies.sort()
    rel_p50 = percentile(release_latencies, 50)
    rel_p95 = percentile(release_latencies, 95)
    rel_p99 = percentile(release_latencies, 99)

    count_after = await manager.get_active_count()
    mem_after = await get_redis_memory(client)

    print("\n[3] RELEASE PHASE:")
    print(f"    Completed:  {total_leases} releases in {t_total_rel:.3f}s ({rel_rps:.1f} ops/sec)")
    print(f"    Latency:    p50={rel_p50:.2f}ms | p95={rel_p95:.2f}ms | p99={rel_p99:.2f}ms")
    print(f"    Remaining:  {count_after} active leases (clean teardown)")
    print(f"    Redis RAM:  {mem_during['used_memory_human']} -> {mem_after['used_memory_human']}")
    print("=" * 60)

    await manager.close(drain=True)
    await client.aclose()

    return {
        "run_idx": run_idx,
        "total_leases": total_leases,
        "concurrency": concurrency,
        "acquire": {
            "throughput_rps": round(acq_rps, 1),
            "p50_ms": round(acq_p50, 2),
            "p95_ms": round(acq_p95, 2),
            "p99_ms": round(acq_p99, 2),
        },
        "renew": {
            "throughput_rps": round(renew_rps, 1),
            "p50_ms": round(renew_p50, 2),
            "p95_ms": round(renew_p95, 2),
            "p99_ms": round(renew_p99, 2),
        },
        "release": {
            "throughput_rps": round(rel_rps, 1),
            "p50_ms": round(rel_p50, 2),
            "p95_ms": round(rel_p95, 2),
            "p99_ms": round(rel_p99, 2),
        },
        "memory": {
            "before": mem_before["used_memory_human"],
            "during": mem_during["used_memory_human"],
            "after": mem_after["used_memory_human"],
            "delta_kb": round(mem_delta_bytes / 1024, 1),
            "bytes_per_lease": round(bytes_per_lease, 1),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="fastapi-stream-lease concurrency benchmark")
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("REDIS_URL", "redis://redis:6379/15"),
        help="Redis connection URL",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="Total number of stream leases to acquire, renew, and release (default: 1000)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=50,
        help="Number of concurrent coroutines (default: 50)",
    )
    parser.add_argument(
        "--prefix",
        default="bench_stream",
        help="Base Redis key prefix (default: bench_stream)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Number of benchmark repetitions to calculate median and dispersion (default: 1)",
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Print benchmark summary as JSON",
    )
    args = parser.parse_args()

    if args.runs < 1:
        print("Error: --runs must be >= 1", file=sys.stderr)
        sys.exit(1)

    all_results: list[dict[str, Any]] = []
    for r in range(1, args.runs + 1):
        res = asyncio.run(
            run_benchmark(
                redis_url=args.redis_url,
                total_leases=args.count,
                concurrency=args.concurrency,
                base_prefix=args.prefix,
                run_idx=r,
                total_runs=args.runs,
            )
        )
        all_results.append(res)

    if args.runs > 1:
        acq_rps = [r["acquire"]["throughput_rps"] for r in all_results]
        renew_rps = [r["renew"]["throughput_rps"] for r in all_results]
        rel_rps = [r["release"]["throughput_rps"] for r in all_results]
        acq_p95 = [r["acquire"]["p95_ms"] for r in all_results]

        print("\n" + "=" * 60)
        print(f"MULTI-RUN SUMMARY ({args.runs} runs):")
        print(
            f"  Acquire Throughput:  median={statistics.median(acq_rps):.1f} ops/s "
            f"(min={min(acq_rps):.1f}, max={max(acq_rps):.1f})"
        )
        print(
            f"  Renewal Throughput:  median={statistics.median(renew_rps):.1f} ops/s "
            f"(min={min(renew_rps):.1f}, max={max(renew_rps):.1f})"
        )
        print(
            f"  Release Throughput:  median={statistics.median(rel_rps):.1f} ops/s "
            f"(min={min(rel_rps):.1f}, max={max(rel_rps):.1f})"
        )
        print(
            f"  Acquire p95 Latency: median={statistics.median(acq_p95):.2f}ms "
            f"(min={min(acq_p95):.2f}, max={max(acq_p95):.2f})"
        )
        print("=" * 60)

    if args.json_output:
        output: Any = all_results if args.runs > 1 else all_results[0]
        print("\n" + json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
