#!/usr/bin/env python3
"""
Soak Workload and Resource Plateau Benchmark for fastapi-stream-lease.

Executes a sustained concurrent streaming workload, verifying:
1. Real Auto-Renew Lifecycle: Uses lease.wrap(auto_renew=True) to exercise
   StreamLease background renewal tasks, cancellation, and cleanup.
2. Memory Plateau: Evaluates linear regression slope and growth on Linux VmRSS
   (/proc/self/status) to verify memory stability without unbounded growth.
3. Task Leak Invariant: Checks task identity against baseline_tasks to ensure
   strictly 0 lingering or orphaned asyncio tasks after teardown.
4. Concurrency Guard: 0 unexpected rejections under capacity and true burst concurrency.
5. Forensic Teardown: Deep scan of all Redis keys under the benchmark prefix,
   asserting 0 ghost leases and 0 lingering keys.

Usage:
    docker compose run --rm backend uv run python benchmarks/bench_soak.py \\
        --duration 30 --target-concurrency 50 --assert-plateau
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
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as redis

from fastapi_stream_lease import LeaseConfig, StreamLeaseManager, StreamLeaseRejected


def get_current_rss_kb() -> int:
    """
    Read current VmRSS in KB from /proc/self/status on Linux.

    Note: /proc/self/status VmRSS is used as an efficient, non-invasive signal
    of process resident memory trend under Linux/Docker environments.
    """
    try:
        with open("/proc/self/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        pass
    # Fallback to getrusage maxrss (KB on Linux)
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def compute_linear_slope(times: list[float], values: list[float]) -> float:
    """Compute ordinary least squares linear regression slope."""
    n = len(times)
    if n < 2:
        return 0.0
    mean_t = sum(times) / n
    mean_v = sum(values) / n
    numerator = sum((t - mean_t) * (v - mean_v) for t, v in zip(times, values, strict=True))
    denominator = sum((t - mean_t) ** 2 for t in times)
    if denominator == 0.0:
        return 0.0
    return numerator / denominator


async def run_soak(
    redis_url: str,
    duration_seconds: float,
    target_concurrency: int,
    lease_seconds: float = 30.0,
    renew_interval: float = 5.0,
    burst_rate: int = 0,
    burst_duration: float = 0.0,
    burst_start: float = 0.0,
    assert_plateau: bool = True,
    sample_interval: float = 1.0,
) -> dict[str, Any]:
    # V01: Robust argument validation
    if duration_seconds <= 0:
        raise ValueError(f"duration_seconds must be > 0, got {duration_seconds}")
    if target_concurrency <= 0:
        raise ValueError(f"target_concurrency must be > 0, got {target_concurrency}")
    if lease_seconds <= 0:
        raise ValueError(f"lease_seconds must be > 0, got {lease_seconds}")
    if renew_interval <= 0 or renew_interval >= lease_seconds:
        raise ValueError(f"renew_interval must be > 0 and < lease_seconds, got {renew_interval}")

    client = redis.from_url(redis_url)
    try:
        await client.ping()
    except Exception as exc:
        print(f"Error connecting to Redis at {redis_url}: {exc}", file=sys.stderr)
        return {"error": str(exc), "passed": False}

    run_uuid = uuid.uuid4().hex[:8]
    max_capacity = (target_concurrency + burst_rate) * 4
    config = LeaseConfig(
        lease_seconds=lease_seconds,
        max_per_user=10,
        max_global=max_capacity,
        key_prefix=f"soak_{run_uuid}",
    )
    manager = StreamLeaseManager(redis=client, config=config)

    print("=" * 70)
    print("FASTAPI-STREAM-LEASE SUSTAINED SOAK & RESOURCE PLATEAU BENCHMARK")
    print(f"Duration: {duration_seconds:.1f}s | Target Concurrency: {target_concurrency} streams")
    print(f"Burst Mode: +{burst_rate} streams (duration={burst_duration}s, start={burst_start}s)")
    print(f"Lease TTL: {lease_seconds:.1f}s | Renew Interval: {renew_interval:.1f}s")
    print(f"Prefix: {config._cluster_prefix}")
    print("=" * 70)

    # S06: Capture baseline task identity
    baseline_tasks = set(asyncio.all_tasks())
    initial_rss_kb = get_current_rss_kb()

    stop_event = asyncio.Event()
    active_streams = 0
    total_acquired = 0
    total_completed = 0
    unexpected_rejections = 0
    unexpected_errors: list[str] = []

    samples: list[dict[str, Any]] = []

    async def stream_generator(stream_duration: float) -> AsyncIterator[bytes]:
        """Simulate an active SSE/LLM stream generating data chunks periodically."""
        chunk_interval = min(0.5, renew_interval / 4.0)
        t_end = time.monotonic() + stream_duration
        chunk_idx = 0
        while time.monotonic() < t_end and not stop_event.is_set():
            chunk_idx += 1
            yield f"data: chunk_{chunk_idx}\n\n".encode()
            await asyncio.sleep(chunk_interval)

    # S01: Simulated stream exercises lease.wrap(auto_renew=True)
    async def simulated_stream(stream_idx: int) -> None:
        nonlocal active_streams, total_acquired, total_completed, unexpected_rejections

        user_id = f"user_{stream_idx % max(1, target_concurrency // 2)}"
        try:
            lease = await manager.acquire(user_id)
            total_acquired += 1
            active_streams += 1
        except StreamLeaseRejected:
            # S04: Concurrency rejections under normal capacity are unexpected
            unexpected_rejections += 1
            return
        except Exception as exc:
            unexpected_errors.append(f"acquire_error: {type(exc).__name__}: {exc}")
            return

        # Lifetime covers multiple renewal cycles
        stream_duration = max(renew_interval * 2.2, 2.0)
        try:
            # Exercise real auto_renew background task and cancellation
            async for _ in lease.wrap(
                stream_generator(stream_duration),
                auto_renew=True,
                renew_interval=renew_interval,
            ):
                pass
            total_completed += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            unexpected_errors.append(f"stream_error: {type(exc).__name__}: {exc}")
        finally:
            active_streams -= 1
            try:
                await lease.release()
            except Exception as exc:
                unexpected_errors.append(f"release_error: {type(exc).__name__}: {exc}")

    # S03: Load generator supporting dynamic burst capacity
    async def load_generator() -> None:
        stream_seq = 0
        running_tasks: set[asyncio.Task[None]] = set()

        def on_task_done(t: asyncio.Task[None]) -> None:
            running_tasks.discard(t)
            # S05: Do not drop task exceptions
            if not t.cancelled():
                exc = t.exception()
                if exc is not None:
                    unexpected_errors.append(f"task_exception: {type(exc).__name__}: {exc}")

        while not stop_event.is_set():
            elapsed = time.monotonic() - t_start_soak
            current_target = target_concurrency

            # S03: True burst elevation
            if burst_duration > 0 and burst_start <= elapsed <= (burst_start + burst_duration):
                current_target += burst_rate

            # Spawn tasks to maintain current_target
            while len(running_tasks) < current_target and not stop_event.is_set():
                stream_seq += 1
                task = asyncio.create_task(
                    simulated_stream(stream_seq), name=f"stream_{stream_seq}"
                )
                running_tasks.add(task)
                task.add_done_callback(on_task_done)

            await asyncio.sleep(0.02)

        # Teardown remaining running tasks
        if running_tasks:
            results = await asyncio.gather(*running_tasks, return_exceptions=True)
            unexpected_errors.extend(
                f"shutdown_error: {type(r).__name__}: {r}"
                for r in results
                if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError)
            )

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

    # Wait for soak duration
    print("\n[Phase 1] Executing sustained stream churn with real auto-renew...")
    step_duration = min(duration_seconds, 5.0)
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

    # Allow event loop and lingering socket callbacks to settle cleanly
    await asyncio.sleep(0.5)

    # S06: Strict task leak detection by identity
    current_task = asyncio.current_task()
    final_tasks = set(asyncio.all_tasks())
    extra_tasks = final_tasks - baseline_tasks - ({current_task} if current_task else set())
    zero_task_leak = len(extra_tasks) == 0

    if not zero_task_leak:
        print(f"WARNING: Lingering asyncio tasks detected ({len(extra_tasks)}):", file=sys.stderr)
        for t in extra_tasks:
            stack_frames = [
                f"{f.f_code.co_filename}:{f.f_lineno} in {f.f_code.co_name}" for f in t.get_stack()
            ]
            print(
                f"  - Task name={t.get_name()}, coro={t.get_coro()}, stack={stack_frames}",
                file=sys.stderr,
            )

    final_rss_kb = get_current_rss_kb()

    # S07: Forensic verification of zero ghost leases across ALL keys in prefix
    remaining_keys: list[str] = [
        k.decode() if isinstance(k, bytes) else k
        async for k in client.scan_iter(match=f"*{config.key_prefix}*")
    ]
    total_ghost_leases = 0
    for k in remaining_keys:
        k_type = await client.type(k)
        if (isinstance(k_type, bytes) and k_type == b"zset") or k_type == "zset":
            total_ghost_leases += await client.zcard(k)

    await manager.close(drain=True)
    # Wipe any keys after measurement
    if remaining_keys:
        await client.delete(*remaining_keys)
    await client.aclose()

    zero_ghost_leases = total_ghost_leases == 0
    zero_residual_keys = len(remaining_keys) == 0

    # M02: Robust linear regression slope & plateau calculation
    plateau_stable = True
    rss_growth_pct = 0.0
    rss_slope_mb_per_sec = 0.0
    abs_growth_mb = 0.0

    if len(samples) >= 2:
        # Evaluate steady-state phase (second half if >= 6 samples, otherwise all samples)
        steady_samples = samples[len(samples) // 2 :] if len(samples) >= 6 else samples
        times = [s["time_s"] for s in steady_samples]
        rss_mbs = [s["rss_kb"] / 1024.0 for s in steady_samples]

        rss_slope_mb_per_sec = compute_linear_slope(times, rss_mbs)
        abs_growth_mb = rss_mbs[-1] - rss_mbs[0]
        delta_rss = rss_mbs[-1] - rss_mbs[0]
        rss_growth_pct = (delta_rss / rss_mbs[0]) * 100.0 if rss_mbs[0] > 0 else 0.0

        # Memory plateau criteria:
        # 1. Slope of linear regression in steady state must be near zero (<= 0.15 MB/sec)
        # 2. Relative growth in steady state must be <= 15%
        # 3. Absolute growth in steady state must be <= 15 MB
        if rss_slope_mb_per_sec > 0.15 or rss_growth_pct > 15.0 or abs_growth_mb > 15.0:
            plateau_stable = False
    elif assert_plateau and len(samples) < 2:
        plateau_stable = False

    # S04 & S07: unexpected_rejections must be 0, zero ghost leases AND zero residual keys
    passed = (
        zero_ghost_leases
        and zero_residual_keys
        and zero_task_leak
        and len(unexpected_errors) == 0
        and unexpected_rejections == 0
        and (not assert_plateau or plateau_stable)
    )

    print("\n" + "=" * 70)
    print("SOAK INVARIANTS VERIFICATION:")
    print(f"  Total Acquired Leases:   {total_acquired}")
    print(f"  Total Completed Streams: {total_completed}")
    print(
        f"  Unexpected Rejections:   {unexpected_rejections} "
        f"{'[OK]' if unexpected_rejections == 0 else '[FAIL]'}"
    )
    print(
        f"  Unexpected Errors:       {len(unexpected_errors)} "
        f"{unexpected_errors[:3] if unexpected_errors else ''}"
    )
    print(
        f"  Ghost Leases in Redis:   {total_ghost_leases} "
        f"{'[OK]' if zero_ghost_leases else '[FAIL]'}"
    )
    print(
        f"  Residual Redis Keys:     {len(remaining_keys)} "
        f"{'[OK]' if zero_residual_keys else '[FAIL]'}"
    )
    print(f"  Lingering Asyncio Tasks: {len(extra_tasks)} {'[OK]' if zero_task_leak else '[FAIL]'}")
    print(
        f"  Steady-State RSS Slope:  {rss_slope_mb_per_sec:+.4f} MB/s "
        f"(abs={abs_growth_mb:+.2f} MB, rel={rss_growth_pct:+.1f}%) "
        f"{'[OK]' if plateau_stable else '[FAIL]'}"
    )
    print(f"  OVERALL RESULT:          {'PASSED [OK]' if passed else 'FAILED [X]'}")
    print("=" * 70)

    return {
        "passed": passed,
        "duration_seconds": duration_seconds,
        "target_concurrency": target_concurrency,
        "total_acquired": total_acquired,
        "total_completed": total_completed,
        "unexpected_rejections": unexpected_rejections,
        "unexpected_errors": unexpected_errors,
        "total_ghost_leases": total_ghost_leases,
        "zero_ghost_leases": zero_ghost_leases,
        "remaining_keys": remaining_keys,
        "zero_residual_keys": zero_residual_keys,
        "lingering_tasks_count": len(extra_tasks),
        "zero_task_leak": zero_task_leak,
        "initial_rss_kb": initial_rss_kb,
        "final_rss_kb": final_rss_kb,
        "rss_slope_mb_per_sec": round(rss_slope_mb_per_sec, 4),
        "abs_growth_mb": round(abs_growth_mb, 2),
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
        default=50,
        help="Steady-state active concurrent streams (default: 50)",
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
        default=2.5,
        help="Renewal heartbeat interval in seconds (default: 2.5)",
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
        "--burst-start",
        type=float,
        default=0.0,
        help="Start time of burst window in seconds (default: 0)",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=1.0,
        help="Resource sampling interval in seconds (default: 1.0)",
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

    # V01: Argument validation
    if args.duration <= 0:
        print(f"Error: --duration must be > 0, got {args.duration}", file=sys.stderr)
        sys.exit(1)
    if args.target_concurrency <= 0:
        print(
            f"Error: --target-concurrency must be > 0, got {args.target_concurrency}",
            file=sys.stderr,
        )
        sys.exit(1)
    if args.lease_seconds <= args.renew_interval:
        print(
            f"Error: --lease-seconds ({args.lease_seconds}) must be > "
            f"--renew-interval ({args.renew_interval})",
            file=sys.stderr,
        )
        sys.exit(1)

    results = asyncio.run(
        run_soak(
            redis_url=args.redis_url,
            duration_seconds=args.duration,
            target_concurrency=args.target_concurrency,
            lease_seconds=args.lease_seconds,
            renew_interval=args.renew_interval,
            burst_rate=args.burst_rate,
            burst_duration=args.burst_duration,
            burst_start=args.burst_start,
            assert_plateau=args.assert_plateau,
            sample_interval=args.sample_interval,
        )
    )

    if args.json_output:
        print("\n" + json.dumps(results, indent=2))

    if not results.get("passed", False):
        sys.exit(1)


if __name__ == "__main__":
    main()
