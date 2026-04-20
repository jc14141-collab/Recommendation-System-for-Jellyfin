#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-mlops}"
POSTGRES_USER="${POSTGRES_USER:-recsys}"

if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
  echo "Set POSTGRES_PASSWORD before running this script." >&2
  exit 1
fi

if [[ -z "${S3_ACCESS_KEY:-}" ]]; then
  echo "Set S3_ACCESS_KEY before running this script." >&2
  exit 1
fi

if [[ -z "${S3_SECRET_KEY:-}" ]]; then
  echo "Set S3_SECRET_KEY before running this script." >&2
  exit 1
fi

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic data-pipeline-secrets \
  --namespace "${NAMESPACE}" \
  --from-literal=POSTGRES_USER="${POSTGRES_USER}" \
  --from-literal=POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
  --from-literal=S3_ACCESS_KEY="${S3_ACCESS_KEY}" \
  --from-literal=S3_SECRET_KEY="${S3_SECRET_KEY}" \
  --from-literal=ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin123}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Secret data-pipeline-secrets created in namespace ${NAMESPACE}."
