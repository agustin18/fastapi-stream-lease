---
description: Answers about concurrent stream limits, Redis leases, client disconnects, failure modes, and production use of fastapi-stream-lease.
---

# Frequently asked questions

## What is fastapi-stream-lease?

`fastapi-stream-lease` is a Python library for FastAPI and Starlette that limits
**active streams**, not requests per minute. It holds a Redis lease for the lifetime of
an SSE response, LLM token stream, or WebSocket session, and rejects excess connections
with HTTP 429 before upstream token generation starts.

## How is it different from a request rate limiter?

A request rate limiter counts requests inside a time window. A streaming connection can
stay open for 30 to 60 seconds, so one user can open several tabs and hold several worker
sockets while sending only a few requests. `fastapi-stream-lease` answers a different
question: **how many streams are open right now?**

The two approaches can be combined. Use a request limiter for burst protection and a
stream lease for concurrency and upstream cost control.

## How do I limit concurrent SSE connections in FastAPI?

Wrap the token generator with `manager.stream()`:

```python
return await manager.stream(user_id, token_stream())
```

The manager acquires the lease before the response starts, renews it while the stream is
open, and releases it on completion, cancellation, error, or client disconnect. If the
user or the global limit is reached, the registered exception handler returns HTTP 429
with a `Retry-After` header.

## Does it work with LLM streaming?

Yes. The lease wrapper accepts any async token generator, so it can wrap OpenAI,
Anthropic, LiteLLM, or a provider-specific SDK. The important part is that the lease is
acquired before the upstream request starts and the upstream iterator is closed when the
downstream client disconnects.

## What happens when a client disconnects?

The built-in response helper detects the ASGI disconnect and closes the upstream
iterator. The lease is released immediately. If the release cannot reach Redis, the lease
expires on its own after the configured TTL.

## What happens if Redis is unavailable?

The default is fail-closed: acquisition raises `StreamLeaseUnavailable`, which maps to
HTTP 503 instead of silently allowing unlimited streams. If availability matters more
than coordination during an outage, `LeaseConfig(fail_open=True)` grants worker-local
fallback leases. Fallback leases are not coordinated across workers until Redis recovers.

## Does it support multiple workers and Redis Cluster?

Yes. Lease state lives in Redis, so multiple FastAPI workers coordinate through the same
keys. The per-user and global keys share a Redis hash tag, which keeps them on one cluster
slot for atomic Lua execution. The project's CI also exercises Redis Cluster failover and
Sentinel.

## Is it production-ready?

The project is pre-1.0. Version `0.6.x` has production-focused failure handling, Redis
Cluster and Sentinel coverage, and 100% statement and branch coverage. Pin a version and
test your own failure modes before a broad rollout. `v1.0.0` is planned as the formal
public API stability declaration.

## How do I try it without an LLM key?

Run the local browser demo from a checkout:

```bash
docker compose up --build demo
```

Open <http://localhost:8080>. The demo uses a synthetic SSE stream and Redis only. Tab B
is rejected by the per-session lease while Tab A is streaming; a third session from the
same network hits the per-IP guard.

## Can I use it with WebSockets?

Yes. The built-in `manager.stream()` helper targets HTTP streaming responses, while
`manager.lease()` can guard a WebSocket session. See
[`examples/websocket_demo.py`](https://github.com/agustin18/fastapi-stream-lease/blob/main/examples/websocket_demo.py)
for the pattern.

<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "FAQPage",
  "mainEntity": [
    {
      "@type": "Question",
      "name": "What is fastapi-stream-lease?",
      "acceptedAnswer": {
        "@type": "Answer",
        "text": "fastapi-stream-lease is a Python library for FastAPI and Starlette that limits active streams, not requests per minute. It holds a Redis lease for the lifetime of an SSE response, LLM token stream, or WebSocket session."
      }
    },
    {
      "@type": "Question",
      "name": "How is it different from a request rate limiter?",
      "acceptedAnswer": {
        "@type": "Answer",
        "text": "A request rate limiter counts requests inside a time window. fastapi-stream-lease counts open streams, so it can reject a second long-lived connection before upstream token generation starts."
      }
    },
    {
      "@type": "Question",
      "name": "Does it work with LLM streaming?",
      "acceptedAnswer": {
        "@type": "Answer",
        "text": "Yes. The lease wrapper accepts any async token generator, including OpenAI, Anthropic, LiteLLM, or provider-specific SDKs."
      }
    },
    {
      "@type": "Question",
      "name": "What happens when a client disconnects?",
      "acceptedAnswer": {
        "@type": "Answer",
        "text": "The response helper closes the upstream iterator and releases the lease immediately. If Redis is unreachable during release, the lease expires after its TTL."
      }
    },
    {
      "@type": "Question",
      "name": "What happens if Redis is unavailable?",
      "acceptedAnswer": {
        "@type": "Answer",
        "text": "The default is fail-closed: acquisition raises StreamLeaseUnavailable and the API returns HTTP 503. LeaseConfig(fail_open=True) enables worker-local fallback leases during an outage."
      }
    }
  ]
}
</script>
