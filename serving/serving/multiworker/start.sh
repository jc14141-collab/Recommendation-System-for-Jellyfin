#!/bin/sh
set -e

PROM_DIR="${PROMETHEUS_MULTIPROC_DIR:-/tmp/prometheus_multiproc}"
rm -rf "${PROM_DIR:?}"
mkdir -p "${PROM_DIR}"

exec gunicorn app.main:app \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:8000 \
    --timeout 60
