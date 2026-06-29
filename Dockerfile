FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV APP_CONFIG=/app/config/collector.yaml

WORKDIR /app

COPY pyproject.toml README.md ./
COPY app ./app
COPY config ./config

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]

FROM base AS dxi
RUN pip install --no-cache-dir ".[dxi]"

FROM base AS dd
RUN pip install --no-cache-dir ".[dd]"

FROM base AS i6000
RUN pip install --no-cache-dir ".[i6000]"

FROM base AS networker
RUN pip install --no-cache-dir ".[networker]"

FROM base AS zfs
RUN pip install --no-cache-dir ".[zfs]"
