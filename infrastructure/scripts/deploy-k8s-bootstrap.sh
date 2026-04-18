#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BOOTSTRAP_DIR="${REPO_ROOT}/k8s/bootstrap"
NAMESPACE="${NAMESPACE:-mlops}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"
SKIP_SECRET_SETUP="${SKIP_SECRET_SETUP:-false}"

usage() {
  cat <<'EOF'
Usage: ./scripts/deploy-k8s-bootstrap.sh [--skip-secret-setup] [--timeout 300s]

Deploy the first six Kubernetes manifests in one pass:
  00-namespace.yaml
  01-postgres.yaml
  02-mlflow.yaml
  03-jellyfin.yaml
  04-minio.yaml
  05-minio-init.yaml

Environment variables:
  NAMESPACE            Target namespace. Default: mlops
  WAIT_TIMEOUT         Rollout/wait timeout. Default: 300s
  SKIP_SECRET_SETUP    Set to true to skip secret creation helpers

If secret setup is not skipped:
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

MANIFESTS=(
  "$REPO_ROOT/k8s/00-namespace.yaml"
  "$REPO_ROOT/k8s/01-postgres.yaml"
  "$REPO_ROOT/k8s/02-mlflow.yaml"
  "$REPO_ROOT/k8s/03-jellyfin.yaml"
  "$REPO_ROOT/k8s/04-minio.yaml"
  "$REPO_ROOT/k8s/05-minio-init.yaml"
)

if kubectl kustomize "$BOOTSTRAP_DIR" >/dev/null 2>&1; then
  APPLY_COMMAND=(kubectl apply -k "$BOOTSTRAP_DIR")
else
  APPLY_COMMAND=(kubectl apply)
  for manifest in "${MANIFESTS[@]}"; do
    APPLY_COMMAND+=(-f "$manifest")
  done
fi

if [[ "$SKIP_SECRET_SETUP" != "true" ]]; then
  if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
    echo "Set POSTGRES_PASSWORD before running this script, or use --skip-secret-setup." >&2
    exit 1
  fi
  if [[ -z "${MINIO_ROOT_USER:-}" || -z "${MINIO_ROOT_PASSWORD:-}" ]]; then
    echo "Set MINIO_ROOT_USER and MINIO_ROOT_PASSWORD before running this script, or use --skip-secret-setup." >&2
    exit 1
  fi

  NAMESPACE="$NAMESPACE" POSTGRES_PASSWORD="$POSTGRES_PASSWORD" POSTGRES_USER="${POSTGRES_USER:-postgres}" \
    "$SCRIPT_DIR/create-postgres-secret.sh"
  NAMESPACE="$NAMESPACE" MINIO_ROOT_USER="$MINIO_ROOT_USER" MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
    "$SCRIPT_DIR/create-minio-secret.sh"
else
  kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
  ensure_secret postgres-secret
  ensure_secret minio-secret
fi

kubectl delete job minio-init -n "$NAMESPACE" --ignore-not-found >/dev/null
"${APPLY_COMMAND[@]}"

kubectl rollout status statefulset/postgres -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/mlflow -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/jellyfin -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/minio -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl wait --for=condition=complete job/minio-init -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"

kubectl get pods -n "$NAMESPACE"
kubectl get svc -n "$NAMESPACE"
kubectl get pvc -n "$NAMESPACE"
