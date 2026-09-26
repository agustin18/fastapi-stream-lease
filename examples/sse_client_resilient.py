"""
Resilient Server-Sent Events (SSE) Client Example

Demonstrates consuming an SSE stream protected by fastapi-stream-lease with:
- Automatic reconnection on disconnects with exponential backoff and jitter.
- Handling HTTP 429 (Too Many Requests) by backing off and not bombarding the server.
- Handling HTTP 503 (Service Unavailable) by honoring the Retry-After header.
- Clean cancellation and session termination.
"""

from __future__ import annotations

import asyncio
import logging
import random
import sys

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("sse-client")


async def consume_sse(
    url: str,
    api_key: str,
    max_retries: int = 5,
    base_backoff: float = 1.0,
    max_backoff: float = 30.0,
) -> None:
    headers = {"X-API-Key": api_key, "Accept": "text/event-stream"}
    retries = 0

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=5.0)) as client:
        while retries < max_retries:
            try:
                logger.info(
                    "Connecting to SSE stream: %s (attempt %d/%d)", url, retries + 1, max_retries
                )
                async with client.stream("GET", url, headers=headers) as response:
                    if response.status_code == 429:
                        logger.warning(
                            "Concurrency limit exceeded (HTTP 429). "
                            "User has too many active streams."
                        )
                        retry_after = float(response.headers.get("retry-after", base_backoff * 2))
                        logger.info("Backing off for %.2fs before retrying...", retry_after)
                        await asyncio.sleep(retry_after)
                        retries += 1
                        continue

                    if response.status_code == 503:
                        retry_after = float(response.headers.get("retry-after", 5.0))
                        logger.warning(
                            "Stream lease backend unavailable (HTTP 503). Retrying after %.2fs...",
                            retry_after,
                        )
                        await asyncio.sleep(retry_after)
                        retries += 1
                        continue

                    response.raise_for_status()
                    logger.info("Connected to stream. Receiving events...")
                    retries = 0

                    async for line in response.aiter_lines():
                        if line.startswith("data: "):
                            data = line[6:]
                            logger.info("Event data: %s", data)
                        elif line.startswith("event: "):
                            logger.info("Event type: %s", line[7:])

                    logger.info("Stream ended normally by server.")
                    return

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as err:
                retries += 1
                backoff = min(max_backoff, base_backoff * (2 ** (retries - 1)))
                jitter = random.uniform(0, 0.5 * backoff)
                sleep_time = backoff + jitter
                logger.warning(
                    "Stream connection dropped (%s). Reconnecting in %.2fs...",
                    type(err).__name__,
                    sleep_time,
                )
                await asyncio.sleep(sleep_time)

    logger.error("Exceeded maximum retries (%d). Aborting stream consumption.", max_retries)


if __name__ == "__main__":
    stream_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000/stream"
    demo_token = sys.argv[2] if len(sys.argv) > 2 else "local-secret"
    try:
        asyncio.run(consume_sse(stream_url, demo_token))
    except KeyboardInterrupt:
        logger.info("SSE client stopped by user.")
