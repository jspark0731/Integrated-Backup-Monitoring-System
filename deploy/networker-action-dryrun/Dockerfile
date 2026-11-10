# Preload the Collector's Python base, or pass an approved local image/digest.
ARG PYTHON_IMAGE=python:3.12-slim-bookworm
FROM ${PYTHON_IMAGE} AS dependencies
WORKDIR /build
COPY deploy/networker-action-dryrun/requirements.txt ./requirements.txt
COPY wheelhouse/ /wheelhouse/
RUN python -m venv /opt/venv \
    && /opt/venv/bin/python -m pip install --no-index --only-binary=:all: \
       --no-cache-dir --find-links=/wheelhouse -r requirements.txt \
    && /opt/venv/bin/python -m pip check

FROM ${PYTHON_IMAGE} AS runtime
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY --from=dependencies /opt/venv /opt/venv
# Only required code; no Collector config, operational data or credentials.
COPY apps/__init__.py ./apps/__init__.py
COPY apps/classifiers/__init__.py apps/classifiers/source_networker.py ./apps/classifiers/
COPY apps/lookups/__init__.py ./apps/lookups/__init__.py
COPY apps/lookups/networker_action/ ./apps/lookups/networker_action/
COPY apps/lookups/networker_action_shadow/ ./apps/lookups/networker_action_shadow/
COPY tools/networker_action_shadow.py tools/networker_action_dryrun_container.py ./tools/
RUN python -c "import httpx, sqlite3; from tools import networker_action_shadow; from tools import networker_action_dryrun_container; print('sqlite_and_cli_import_ok')" \
    && python tools/networker_action_shadow.py --help \
    && mkdir -p /results /tmp/runtime \
    && chown -R 10001:10001 /results /tmp/runtime
USER 10001:10001
ENTRYPOINT ["python", "/app/tools/networker_action_dryrun_container.py"]
