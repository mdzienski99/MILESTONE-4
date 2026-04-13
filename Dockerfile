FROM python:3.10-slim AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt


FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    curl \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*

COPY service ./service
COPY stream ./stream
COPY scripts ./scripts
COPY artifacts ./artifacts
COPY snapshots ./snapshots

RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

ENV PORT=8080
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/healthz || exit 1

CMD ["sh", "-c", "exec python -m uvicorn service.app:app --host 0.0.0.0 --port ${PORT:-8080}"]