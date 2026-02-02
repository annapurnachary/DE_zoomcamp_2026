#FROM python:3.10
FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /code

ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml uv.lock .python-version ./

RUN uv sync --locked --no-dev

COPY ingest_data.py ingest_data.py

ENTRYPOINT ["uv", "run", "python", "ingest_data.py"]
