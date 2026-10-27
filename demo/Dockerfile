FROM python:3.13-slim@sha256:57b29a7c050c4e679e4bea572e14b3965cf4b5839de2da49d55e49ffc4024f39

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.12.19@sha256:04d046b13e60d6bcec73cbc5e1cad25d680dea90c8573340950a0ac2d1aef424 /uv /bin/uv

COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --locked --extra fastapi --group demo --no-install-project

COPY src/ src/
COPY demo/ demo/
RUN uv sync --locked --extra fastapi --group demo

ENV PATH="/app/.venv/bin:$PATH"

RUN useradd --create-home --uid 10001 demo
USER demo

EXPOSE 8080

CMD ["uvicorn", "demo.app:app", "--host", "0.0.0.0", "--port", "8080"]
