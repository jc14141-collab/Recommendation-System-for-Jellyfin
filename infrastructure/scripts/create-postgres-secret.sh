#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-mlops}"
POSTGRES_DB="${POSTGRES_DB:-recsys}"
POSTGRES_USER="${POSTGRES_USER:-recsys}"

if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
  echo "Set POSTGRES_PASSWORD before running this script."
  echo "Example:"
  echo '  export POSTGRES_PASSWORD="CHANGE_ME"'
  echo "You can also copy values from scripts/create-postgres-secret.example.env."
  exit 1
fi

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic postgres-secret \
  --namespace "${NAMESPACE}" \
  --from-literal=POSTGRES_DB="${POSTGRES_DB}" \
  --from-literal=POSTGRES_USER="${POSTGRES_USER}" \
  --from-literal=POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Secret postgres-secret created in namespace ${NAMESPACE}."
