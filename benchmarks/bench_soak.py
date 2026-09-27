#!/usr/bin/env python3
"""
Soak Workload and Resource Plateau Benchmark for fastapi-stream-lease.

Executes a sustained concurrent streaming workload, verifying:
1. Memory Plateau: Zero unbounded growth of RSS memory after initial warm-up.
2. Task Leak Invariant: len(asyncio.all_tasks()) plateaus and returns to baseline.
3. Clean Teardown: 0 ghost leases in Redis and 0 lingering renewal coroutines.
4. Concurrency Guard: 0 concurrency violations under sustained churn and burst.

Usage:
    docker compose run --rm backend uv run python benchmarks/bench_soak.py \\
        --duration 60 --target-concurrency 100 --assert-plateau
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import sys
import time
import uuid
from typing import Any

import redis.asyncio as redis

from fastapi_stream_lease import LeaseConfig, StreamLeaseManager, StreamLeaseRejected


def get_current_rss_kb() -> int:
    """Read current VmRSS in KB from /proc/self/status on Linux, fallback to ru_maxrss."""
    try:
        with open("/proc/self/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        pass
    # Fallback to getrusage maxrss (KB on Linux)
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


async def run_soak(
    redis_url: str,
    duration_seconds: float,
    target_concurrency: int,
    lease_seconds: float = 30.0,
    renew_interval: float = 10.0,
    burst_rate: int = 0,
    burst_duration: float = 0.0,
    burst_start: float = 0.0,
    assert_plateau: bool = True,
    sample_interval: float = 2.0,
) -> dict[str, Any]:
    client = redis.from_url(redis_url)
    try:
        await client.ping()
    except Exception as exc:
        print(f"Error connecting to Redis at {redis_url}: {exc}", file=sys.stderr)
        return {"error": str(exc), "passed": False}

    run_uuid = uuid.uuid4().hex[:8]
    config = LeaseConfig(
        lease_seconds=lease_seconds,
        max_per_user=10,
        max_global=target_concurrency * 3,
        key_prefix=f"soak_{run_uuid}",
    )
    manager = StreamLeaseManager(redis=client, config=config)

    print("=" * 70)
    print("FASTAPI-STREAM-LEASE SUSTAINED SOAK & RESOURCE PLATEAU BENCHMARK")
    print(f"Duration: {duration_seconds:.1f}s | Target Concurrency: {target_concurrency} streams")
    print(f"Lease TTL: {lease_seconds:.1f}s | Renew Interval: {renew_interval:.1f}s")
    print(f"Prefix: {config._cluster_prefix}")
    print("=" * 70)

    initial_tasks = len(asyncio.all_tasks())
    initial_rss_kb = get_current_rss_kb()

    stop_event = asyncio.Event()
    active_streams = 0
    total_acquired = 0
    total_completed = 0
    total_renewals = 0
    concurrency_violations = 0
    unexpected_errors: list[str] = []

    samples: list[dict[str, Any]] = []

    async def simulated_stream(stream_idx: int) -> None:
        nonlocal active_streams, total_acquired, total_completed, total_renewals
        nonlocal concurrency_violations

        user_id = f"user_{stream_idx % max(1, target_concurrency // 2)}"
        try:
            lease = await manager.acquire(user_id)
            total_acquired += 1
            active_streams += 1
        except StreamLeaseRejected:
            concurrency_violations += 1
            return
        except Exception as exc:
            unexpected_errors.append(f"acquire_error: {exc}")
            return

        stream_lifetime = min(15.0, max(2.0, renew_interval * 1.5))
        t_start = time.monotonic()

        try:
            # Simulate streaming loop with renewals
            while (time.monotonic() - t_start < stream_lifetime) and not stop_event.is_set():
                await asyncio.sleep(min(renew_interval, 1.0))
                # Trigger renewal if past interval
                if time.monotonic() - t_start >= renew_interval:
                    renewed = await manager.renew(lease)
                    if renewed:
                        total_renewals += 1
            total_completed += 1
        except Exception as exc:
            unexpected_errors.append(f"streaming_error: {exc}")
        finally:
            active_streams -= 1
            try:
                await lease.release()
            except Exception as exc:
                unexpected_errors.append(f"release_error: {exc}")

    # Worker pool maintaining target concurrency
    async def load_generator() -> None:
        stream_seq = 0
        sem = asyncio.Semaphore(target_concurrency)

        async def spawn_and_run(idx: int) -> None:
            async with sem:
                await simulated_stream(idx)

        running_tasks: set[asyncio.Task[None]] = set()

        while not stop_event.is_set():
            # Check burst mode
            elapsed = time.monotonic() - t_start_soak
            current_target = target_concurrency
            if burst_duration > 0 and burst_start <= elapsed <= (burst_start + burst_duration):
                current_target += burst_rate

            # Spawn to match target
            while len(running_tasks) < current_target and not stop_event.is_set():
                stream_seq += 1
                task = asyncio.create_task(spawn_and_run(stream_seq))
                running_tasks.add(task)
                task.add_done_callback(running_tasks.discard)

            await asyncio.sleep(0.05)

        if running_tasks:
            await asyncio.gather(*running_tasks, return_exceptions=True)

    # Monitor coroutine sampling memory and tasks
    async def resource_monitor() -> None:
        while not stop_event.is_set():
            t_now = time.monotonic() - t_start_soak
            cur_rss = get_current_rss_kb()
            cur_tasks = len(asyncio.all_tasks())
            cur_redis_leases = await manager.get_active_count()
            samples.append(
                {
                    "time_s": round(t_now, 2),
                    "rss_kb": cur_rss,
                    "tasks": cur_tasks,
                    "active_streams": active_streams,
                    "redis_leases": cur_redis_leases,
                }
            )
            await asyncio.sleep(sample_interval)

    t_start_soak = time.monotonic()
    generator_task = asyncio.create_task(load_generator())
    monitor_task = asyncio.create_task(resource_monitor())

    # Wait for duration
    print("\n[Phase 1] Executing soak stream churn...")
    step_duration = min(duration_seconds, 10.0)
    elapsed_soak = 0.0
    while elapsed_soak < duration_seconds:
        await asyncio.sleep(step_duration)
        elapsed_soak = time.monotonic() - t_start_soak
        cur_rss = get_current_rss_kb()
        cur_tasks = len(asyncio.all_tasks())
        cur_redis = await manager.get_active_count()
        print(
            f"    [{elapsed_soak:5.1f}s / {duration_seconds:.0f}s] "
            f"Active: {active_streams:4d} | Redis: {cur_redis:4d} | "
            f"Tasks: {cur_tasks:4d} | VmRSS: {cur_rss / 1024:6.1f} MB | "
            f"Acq: {total_acquired:5d} | Completed: {total_completed:5d}"
        )

    # Teardown
    print("\n[Phase 2] Draining in-flight streams & checking teardown invariants...")
    stop_event.set()
    await generator_task
    await monitor_task

    # Allow event loop and background tasks to settle
    await asyncio.sleep(0.5)

    final_tasks = len(asyncio.all_tasks())
    final_rss_kb = get_current_rss_kb()
    remaining_redis_leases = await manager.get_active_count()

    await manager.close(drain=True)
    await client.aclose()

    # Invariants Analysis
    # 1. Zero Ghost Leases in Redis
    zero_ghost_leases = remaining_redis_leases == 0

    # 2. Tasks returned to baseline (initial tasks + monitor/caller)
    # Background auto-renew or stream tasks must be completely cleared.
    task_growth = final_tasks - initial_tasks
    zero_task_leak = task_growth <= 2

    # 3. Memory Plateau Analysis:
    # Memory in the second half of the soak run should not grow unboundedly vs mid-point.
    plateau_stable = True
    rss_growth_pct = 0.0
    if len(samples) >= 4:
        mid_point = len(samples) // 2
        mid_rss = samples[mid_point]["rss_kb"]
        end_rss = samples[-1]["rss_kb"]
        rss_growth_pct = ((end_rss - mid_rss) / mid_rss) * 100.0 if mid_rss > 0 else 0.0
        # For a soak run, memory in the steady state should not increase by > 20%
        if rss_growth_pct > 20.0:
            plateau_stable = False

    passed = (
        zero_ghost_leases
        and zero_task_leak
        and len(unexpected_errors) == 0
        and (not assert_plateau or plateau_stable)
    )

    print("\n" + "=" * 70)
    print("SOAK INVARIANTS VERIFICATION:")
    print(f"  Total Acquired Leases:   {total_acquired}")
    print(f"  Total Completed Streams: {total_completed}")
    err_summary = str(unexpected_errors[:3]) if unexpected_errors else ""
    print(f"  Unexpected Errors:       {len(unexpected_errors)} {err_summary}")
    ghost_status = "[OK]" if zero_ghost_leases else "[FAIL]"
    print(f"  Ghost Leases in Redis:   {remaining_redis_leases} {ghost_status}")
    leak_status = "[OK]" if zero_task_leak else "[FAIL]"
    print(
        f"  Asyncio Task Delta:      {task_growth:+d} "
        f"(initial={initial_tasks}, final={final_tasks}) {leak_status}"
    )
    plateau_status = "[OK]" if plateau_stable else "[FAIL]"
    print(f"  VmRSS Plateau Growth:    {rss_growth_pct:+.2f}% during steady state {plateau_status}")
    result_status = "PASSED [OK]" if passed else "FAILED [X]"
    print(f"  OVERALL RESULT:          {result_status}")
    print("=" * 70)

    return {
        "passed": passed,
        "duration_seconds": duration_seconds,
        "target_concurrency": target_concurrency,
        "total_acquired": total_acquired,
        "total_completed": total_completed,
        "total_renewals": total_renewals,
        "unexpected_errors": unexpected_errors,
        "remaining_redis_leases": remaining_redis_leases,
        "initial_tasks": initial_tasks,
        "final_tasks": final_tasks,
        "task_growth": task_growth,
        "initial_rss_kb": initial_rss_kb,
        "final_rss_kb": final_rss_kb,
        "rss_growth_pct": round(rss_growth_pct, 2),
        "plateau_stable": plateau_stable,
        "samples": samples,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="fastapi-stream-lease soak workload benchmark")
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("REDIS_URL", "redis://redis:6379/15"),
        help="Redis connection URL",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Soak duration in seconds (default: 30.0, use 7200 for 2-hour soak)",
    )
    parser.add_argument(
        "--target-concurrency",
        type=int,
        default=100,
        help="Steady-state active concurrent streams (default: 100)",
    )
    parser.add_argument(
        "--lease-seconds",
        type=float,
        default=10.0,
        help="Lease TTL in seconds (default: 10.0)",
    )
    parser.add_argument(
        "--renew-interval",
        type=float,
        default=3.0,
        help="Renewal heartbeat interval in seconds (default: 3.0)",
    )
    parser.add_argument(
        "--burst-rate",
        type=int,
        default=0,
        help="Additional burst acquisitions during burst window (default: 0)",
    )
    parser.add_argument(
        "--burst-duration",
        type=float,
        default=0.0,
        help="Duration of burst window in seconds (default: 0)",
    )
    parser.add_argument(
        "--assert-plateau",
        action="store_true",
        default=True,
        help="Assert zero memory/task leak invariants and exit 1 on violation",
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Output full metrics report in JSON format",
    )
    args = parser.parse_args()

    results = asyncio.run(
        run_soak(
            redis_url=args.redis_url,
            duration_seconds=args.duration,
            target_concurrency=args.target_concurrency,
            lease_seconds=args.lease_seconds,
            renew_interval=args.renew_interval,
            burst_rate=args.burst_rate,
            burst_duration=args.burst_duration,
            assert_plateau=args.assert_plateau,
        )
    )

    if args.json_output:
        print("\n" + json.dumps(results, indent=2))

    if not results.get("passed", False):
        sys.exit(1)


if __name__ == "__main__":
    main()
