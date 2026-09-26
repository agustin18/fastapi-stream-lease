FROM python:3.13-slim

WORKDIR /app

# Install uv binary
COPY --from=ghcr.io/astral-sh/uv:0.12.19 /uv /bin/uv

# Install project dependencies
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --extra dev

# Copy codebase
COPY src/ src/
COPY tests/ tests/
COPY examples/ examples/

ENV PATH="/app/.venv/bin:$PATH"
