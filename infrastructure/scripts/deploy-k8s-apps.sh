#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
APPS_DIR="${REPO_ROOT}/k8s/apps"
NAMESPACE="${NAMESPACE:-mlops}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"
SKIP_SECRET_SETUP="${SKIP_SECRET_SETUP:-false}"

usage() {
  cat <<'EOF'
Usage: ./scripts/deploy-k8s-apps.sh [--skip-secret-setup] [--timeout 300s]

Deploy the app-layer services:
  11-data-pipeline-config.yaml
  12-online-service-config.yaml
  13-api.yaml
  14-online-service-api.yaml
  15-training-manager.yaml

If secret setup is not skipped:
  - POSTGRES_PASSWORD is required
  - S3_ACCESS_KEY is required
  - S3_SECRET_KEY is required
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

if [[ "$SKIP_SECRET_SETUP" != "true" ]]; then
  POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}" \
  POSTGRES_USER="${POSTGRES_USER:-recsys}" \
  S3_ACCESS_KEY="${S3_ACCESS_KEY:-}" \
  S3_SECRET_KEY="${S3_SECRET_KEY:-}" \
  ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin123}" \
  NAMESPACE="$NAMESPACE" \
  bash "$SCRIPT_DIR/create-data-pipeline-secret.sh"
else
  kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
  ensure_secret data-pipeline-secrets
fi

if kubectl kustomize "$APPS_DIR" >/dev/null 2>&1; then
  kubectl apply -k "$APPS_DIR"
else
  kubectl apply -f "$REPO_ROOT/k8s/11-data-pipeline-config.yaml" \
    -f "$REPO_ROOT/k8s/12-online-service-config.yaml" \
    -f "$REPO_ROOT/k8s/13-api.yaml" \
    -f "$REPO_ROOT/k8s/14-online-service-api.yaml" \
    -f "$REPO_ROOT/k8s/15-training-manager.yaml"
fi

kubectl rollout status deployment/api -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/online-service-api -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/training-manager -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"

kubectl get pods -n "$NAMESPACE"
kubectl get svc -n "$NAMESPACE"
