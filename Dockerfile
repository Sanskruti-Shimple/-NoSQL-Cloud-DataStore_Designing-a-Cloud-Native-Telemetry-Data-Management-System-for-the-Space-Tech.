FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential libev-dev libev4 \
    && pip install --no-cache-dir . \
    && apt-get purge -y --auto-remove build-essential libev-dev \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3)"
CMD ["uvicorn", "mission_telemetry.api:app", "--host", "0.0.0.0", "--port", "8000"]
