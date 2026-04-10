#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-mlops}"

if [[ -z "${MINIO_ROOT_USER:-}" ]]; then
  echo "Set MINIO_ROOT_USER before running this script."
  exit 1
fi

if [[ -z "${MINIO_ROOT_PASSWORD:-}" ]]; then
  echo "Set MINIO_ROOT_PASSWORD before running this script."
  exit 1
fi

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic minio-secret \
  --namespace "${NAMESPACE}" \
  --from-literal=MINIO_ROOT_USER="${MINIO_ROOT_USER}" \
  --from-literal=MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Secret minio-secret created in namespace ${NAMESPACE}."
