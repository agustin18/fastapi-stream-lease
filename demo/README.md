# Browser demo

A self-contained FastAPI application for showing the lease in a browser. It uses
a synthetic 20-second SSE stream, so it needs Redis but no LLM keys. The demo is
shipped in the repository source tree and is not part of the published wheel; run
it from a checkout or from the included Docker image.

Both panels share one signed HttpOnly session cookie. The first stream acquires
a Redis lease; the second stream for the same session receives `429` until the
first one finishes or the browser disconnects. A Redis-backed per-IP cap also
prevents one client from filling every global slot with forged sessions.

## Run locally

```bash
docker compose up --build demo
```

Open <http://localhost:8080>. The compose service starts Redis and the demo
together, with secure cookies disabled for local HTTP.

To run it without Docker:

```bash
uv sync --locked --extra fastapi --group demo
docker run --rm -p 6379:6379 redis:7-alpine
DEMO_COOKIE_SECURE=0 uv run uvicorn demo.app:app --reload
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string. |
| `DEMO_LEASE_SECONDS` | `25` | Redis lease TTL while the stream is open. |
| `DEMO_STREAM_SECONDS` | `20` | Synthetic stream duration. |
| `DEMO_TOKEN_INTERVAL` | `0.5` | Delay between synthetic tokens. |
| `DEMO_MAX_PER_USER` | `1` | Concurrent streams per session. |
| `DEMO_MAX_GLOBAL` | `20` | Concurrent streams across the process. |
| `DEMO_MAX_PER_IP` | `1` | Concurrent streams per client IP. |
| `DEMO_RATE_LIMIT_PER_MINUTE` | `30` | Stream attempts per IP per window. |
| `DEMO_RATE_LIMIT_WINDOW_SECONDS` | `60` | Rate-limit window. |
| `DEMO_COOKIE_SECURE` | `0` | Set to `1` if you run the demo behind HTTPS. |
| `DEMO_KEY_PREFIX` | `stream_lease_demo` | Redis key prefix. |
| `DEMO_SESSION_SECRET` | random per process | HMAC secret for sessions; set a stable value to keep sessions across restarts. |

## Endpoints

- `GET /` — browser demo.
- `GET /demo/stream` — protected synthetic SSE stream.
- `GET /demo/stats` — active global count and configured limits.
- `GET /healthz` — Redis-backed health check; returns `503` when Redis is unavailable.

## Test

```bash
uv run pytest tests/test_demo.py -q -o addopts=''
```

The demo is a local reference app, not a production template: it has no user
authentication, trusts the session cookie only as a stable demo identity, and
keeps its rate limiter and stats cache in process memory.
