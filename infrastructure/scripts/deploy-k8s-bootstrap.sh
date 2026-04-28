#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${NAMESPACE:-mlops}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"
SKIP_SECRET_SETUP="${SKIP_SECRET_SETUP:-false}"

usage() {
  cat <<'EOF'
Usage: ./scripts/deploy-k8s-bootstrap.sh [--skip-secret-setup] [--timeout 300s]

Apply the Kubernetes manifests in three phases:

Phase 1: Infrastructure (00-02, 04-06)
  00-namespace.yaml
  01-postgres-initdb.yaml
  01-postgres.yaml
  02-mlflow.yaml
  04-minio.yaml
  05-minio-init.yaml
  06-adminer.yaml

Phase 2: Config
  postgres-initdb.yaml
  data-configmap.yaml
  online-service-configmap.yaml
  simulator-configmap.yaml

Phase 3: Applications
  11-online-service.yaml
  12-simulator.yaml
  13-data-api.yaml

Environment variables:
  NAMESPACE            Target namespace. Default: mlops
  WAIT_TIMEOUT         Rollout/wait timeout. Default: 300s
  SKIP_SECRET_SETUP    Set to true to skip secret creation helpers

If secret setup is not skipped:
  - POSTGRES_DB defaults to recsys
  - POSTGRES_USER defaults to recsys
  - POSTGRES_PASSWORD is required
  - MINIO_ROOT_USER and MINIO_ROOT_PASSWORD are required
EOF
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command '$1' is not available in PATH." >&2
    exit 1
  fi
}

ensure_secret() {
  local name="$1"
  if ! kubectl get secret "$name" -n "$NAMESPACE" >/dev/null 2>&1; then
    echo "Required secret '$name' was not found in namespace '$NAMESPACE'." >&2
    exit 1
  fi
}

apply_manifest_phase() {
  local phase="$1"
  shift

  echo "========================================"
  echo " ${phase}"
  echo "========================================"
  for manifest in "$@"; do
    kubectl apply -f "$manifest"
  done
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-secret-setup)
      SKIP_SECRET_SETUP=true
      shift
      ;;
    --timeout)
      WAIT_TIMEOUT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_command kubectl

INFRA_MANIFESTS=(
  "$REPO_ROOT/k8s/00-namespace.yaml"
  "$REPO_ROOT/k8s/01-postgres-initdb.yaml"
  "$REPO_ROOT/k8s/01-postgres.yaml"
  "$REPO_ROOT/k8s/02-mlflow.yaml"
  "$REPO_ROOT/k8s/04-minio.yaml"
  "$REPO_ROOT/k8s/05-minio-init.yaml"
  "$REPO_ROOT/k8s/06-adminer.yaml"
)

CONFIG_MANIFESTS=(
  "$REPO_ROOT/k8s/postgres-initdb.yaml"
  "$REPO_ROOT/k8s/data-configmap.yaml"
  "$REPO_ROOT/k8s/online-service-configmap.yaml"
  "$REPO_ROOT/k8s/simulator-configmap.yaml"
)

APP_MANIFESTS=(
  "$REPO_ROOT/k8s/11-online-service.yaml"
  "$REPO_ROOT/k8s/12-simulator.yaml"
  "$REPO_ROOT/k8s/13-data-api.yaml"
)

if [[ "$SKIP_SECRET_SETUP" != "true" ]]; then
  if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
    echo "Set POSTGRES_PASSWORD before running this script, or use --skip-secret-setup." >&2
    exit 1
  fi
  if [[ -z "${MINIO_ROOT_USER:-}" || -z "${MINIO_ROOT_PASSWORD:-}" ]]; then
    echo "Set MINIO_ROOT_USER and MINIO_ROOT_PASSWORD before running this script, or use --skip-secret-setup." >&2
    exit 1
  fi

  NAMESPACE="$NAMESPACE" POSTGRES_DB="${POSTGRES_DB:-recsys}" POSTGRES_PASSWORD="$POSTGRES_PASSWORD" POSTGRES_USER="${POSTGRES_USER:-recsys}" \
    "$SCRIPT_DIR/create-postgres-secret.sh"
  NAMESPACE="$NAMESPACE" MINIO_ROOT_USER="$MINIO_ROOT_USER" MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
    "$SCRIPT_DIR/create-minio-secret.sh"
else
  kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
  ensure_secret postgres-secret
  ensure_secret minio-secret
fi

kubectl delete job minio-init -n "$NAMESPACE" --ignore-not-found >/dev/null

apply_manifest_phase "Phase 1: Infrastructure" "${INFRA_MANIFESTS[@]}"

kubectl rollout status statefulset/postgres -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/mlflow -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/minio -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/adminer -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl wait --for=condition=complete job/minio-init -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"

apply_manifest_phase "Phase 2: Config" "${CONFIG_MANIFESTS[@]}"
apply_manifest_phase "Phase 3: Applications" "${APP_MANIFESTS[@]}"

kubectl rollout status deployment/api -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/online-service-api -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/online-service-worker -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/simulator -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"

kubectl get pods -n "$NAMESPACE"
kubectl get svc -n "$NAMESPACE"
kubectl get pvc -n "$NAMESPACE"
