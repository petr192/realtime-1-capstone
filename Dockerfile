FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_CACHE_DIR=/tmp/uv-cache

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --locked --no-dev --no-install-project

COPY api.py cache.py ./

ENV PATH="/app/.venv/bin:$PATH"

USER 75532:75532

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]