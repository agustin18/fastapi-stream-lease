---
description: Monitor active stream leases with Prometheus and OpenTelemetry metrics.
---

# Observability

The package ships optional Prometheus and OpenTelemetry adapters, plus zero-dependency
lifecycle hooks for custom telemetry. All adapters use bounded label sets; do not add user
IDs or request IDs as labels.

## Prometheus

Install the extra:

```bash
pip install 'fastapi-stream-lease[prometheus]'
```

Create one adapter per `CollectorRegistry` and share it across managers:

```python
from prometheus_client import CollectorRegistry, make_asgi_app
from fastapi import FastAPI
from fastapi_stream_lease import LeaseConfig, StreamLeaseManager
from fastapi_stream_lease.observability.prometheus import PrometheusMetrics

registry = CollectorRegistry()
metrics = PrometheusMetrics(registry=registry)
manager = StreamLeaseManager(
    redis=client,
    config=LeaseConfig(telemetry_scope="llm_streams"),
    telemetry=metrics,
)

app = FastAPI()
app.mount("/metrics", make_asgi_app(registry=registry))
```

The adapter registers:

| Metric | Type | Labels |
|---|---|---|
| `fastapi_stream_lease_operations_total` | Counter | `operation`, `outcome` |
| `fastapi_stream_lease_operation_duration_seconds` | Histogram | `operation` |
| `fastapi_stream_lease_lost_total` | Counter | `reason` |
| `fastapi_stream_lease_backend_errors_total` | Counter | `kind` |
| `fastapi_stream_lease_fallback_total` | Counter | none |
| `fastapi_stream_lease_hook_dropped_total` | Counter | none |
| `fastapi_stream_lease_hook_errors_total` | Counter | none |
| `fastapi_stream_lease_hook_queue_depth` | Gauge | none |
| `fastapi_stream_lease_circuit_state` | Gauge | `scope` |
| `fastapi_stream_lease_short_circuited_total` | Counter | `operation`, `state`, `scope` |
| `fastapi_stream_lease_active_leases` | Gauge | `scope` |

`active_leases` and `hook_queue_depth` are worker-local in-memory values. If you run
multiple workers behind one metrics endpoint, read the
[multi-worker Prometheus notes](production.md#prometheus-with-multiple-workers) before
writing alerts.

## OpenTelemetry

Install the API plus an SDK/provider for your application:

```bash
pip install 'fastapi-stream-lease[otel]' opentelemetry-sdk
```

```python
from opentelemetry import metrics as otel_metrics
from opentelemetry.sdk.metrics import MeterProvider
from fastapi_stream_lease.observability.otel import OpenTelemetryMetrics

provider = MeterProvider()
otel_metrics.set_meter_provider(provider)

metrics = OpenTelemetryMetrics(meter_provider=provider)
manager = StreamLeaseManager(redis=client, config=config, telemetry=metrics)
```

The OpenTelemetry adapter emits the same operations, lease losses, backend errors,
fallbacks, hook pressure, circuit state, and active lease gauge without creating a span
for every renewal. Spans are reserved for higher-value operations such as acquisition and
configuration verification.

## Lifecycle hooks

If you do not need an adapter, use callbacks on `LeaseConfig`:

```python
from fastapi_stream_lease import LeaseConfig

config = LeaseConfig(
    on_acquired=lambda lease: active.inc(),
    on_released=lambda lease, reason: active.dec(),
    on_rejected=lambda user_id, reason: rejected.labels(reason=reason).inc(),
    on_lost=lambda lease, reason: lost.labels(reason=reason).inc(),
    on_backend_error=lambda exc: backend_errors.inc(),
    hook_queue_size=1024,
)
```

Hooks run out-of-band on a bounded FIFO queue. Synchronous callbacks are moved to a worker
thread; asynchronous callbacks must stay cooperative. If the queue fills, new events are
dropped and `hook_dropped_total` increases. This protects the event loop, but it also means
hooks are **best-effort telemetry**. Do not use them for billing, credit deduction, or
security audits.

Inspect dispatcher pressure with `manager.dispatcher.queue_depth`,
`manager.dispatcher.queued_count`, `manager.dispatcher.dropped_count`, and
`manager.dispatcher.error_count`. On shutdown, `manager.close(drain=True, timeout=...)`
waits for in-flight callbacks within the timeout budget.

## Scope isolation

When several managers share one registry or meter, set a static `telemetry_scope` on each
`LeaseConfig`:

```python
heavy = LeaseConfig(key_prefix="app:heavy", telemetry_scope="heavy")
light = LeaseConfig(key_prefix="app:light", telemetry_scope="light")
```

`telemetry_scope` must be low-cardinality. It defaults to `key_prefix`. Circuit metrics are
labeled with `scope`; the active lease gauge is labeled with the resolved scope as well.

## What to alert on

- A sustained rise in `backend_errors_total` or `short_circuited_total`.
- `circuit_state` stuck at `2` (`OPEN`) for longer than the expected recovery window.
- `lost_total` growing without a corresponding deploy or Redis failover.
- `hook_queue_depth` near `hook_queue_size` or `hook_dropped_total` increasing.
- `active_leases` near `max_global` on a single worker, if that worker also enforces the
  global limit.

The exported metric names, label keys, and names in the README are treated as
compatibility contracts across minor releases.
