FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/ingestion/batch

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tini \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip \
    && pip install -r /tmp/requirements.txt

COPY ingestion /app/ingestion

RUN useradd --create-home --shell /usr/sbin/nologin crawler \
    && chown -R crawler:crawler /app

USER crawler

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "/app/ingestion/batch/main.py", "--help"]
