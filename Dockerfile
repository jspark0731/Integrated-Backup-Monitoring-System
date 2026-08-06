FROM python:3.12-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV APP_CONFIG=/apps/config/collector.yaml

WORKDIR /apps

COPY pyproject.toml README.md ./
COPY apps ./apps
COPY config ./config

EXPOSE 8080

CMD ["uvicorn", "apps.main:app", "--host", "0.0.0.0", "--port", "8080"]

FROM base AS dxi
COPY ./wheelhouse /wheelhouse
RUN python -m pip install \
      --no-cache-dir \
      --no-index \
      --find-links=/wheelhouse \
      ".[dxi]" \
    && rm -rf /wheelhouse

FROM base AS dd
COPY ./wheelhouse /wheelhouse
RUN python -m pip install \
      --no-cache-dir \
      --no-index \
      --find-links=/wheelhouse \
      ".[dd]" \
    && rm -rf /wheelhouse

FROM base AS i6000
COPY ./wheelhouse /wheelhouse
RUN python -m pip install \
      --no-cache-dir \
      --no-index \
      --find-links=/wheelhouse \
      ".[i6000]" \
    && rm -rf /wheelhouse

FROM base AS networker
COPY ./wheelhouse /wheelhouse
RUN python -m pip install \
      --no-cache-dir \
      --no-index \
      --find-links=/wheelhouse \
      ".[networker]" \
    && rm -rf /wheelhouse

FROM base AS zfs
COPY ./wheelhouse /wheelhouse
RUN python -m pip install \
      --no-cache-dir \
      --no-index \
      --find-links=/wheelhouse \
      ".[zfs]" \
    && rm -rf /wheelhouse
