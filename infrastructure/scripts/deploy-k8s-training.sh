#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TRAINING_DIR="${REPO_ROOT}/k8s/training"
NAMESPACE="${NAMESPACE:-mlops}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"

usage() {
  cat <<'EOF'
Usage: ./scripts/deploy-k8s-training.sh [--timeout 300s]

Deploy the training-layer resources:
  14-training-config.yaml
  15-training-manager.yaml
  16-training-retrain-cronjob.yaml

Assumptions:
  - namespace mlops already exists
  - minio-secret already exists
  - the image docker.io/library/jellyfin-training:latest is already imported into the node runtime
EOF
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command '$1' is not available in PATH." >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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

kubectl get namespace "$NAMESPACE" >/dev/null
kubectl get secret minio-secret -n "$NAMESPACE" >/dev/null

if kubectl kustomize "$TRAINING_DIR" >/dev/null 2>&1; then
  kubectl apply -k "$TRAINING_DIR"
else
  kubectl apply -f "$REPO_ROOT/k8s/14-training-config.yaml" \
    -f "$REPO_ROOT/k8s/15-training-manager.yaml" \
    -f "$REPO_ROOT/k8s/16-training-retrain-cronjob.yaml"
fi

kubectl rollout status deployment/training-manager -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl get pods -n "$NAMESPACE"
kubectl get svc -n "$NAMESPACE"
kubectl get cronjob -n "$NAMESPACE"
