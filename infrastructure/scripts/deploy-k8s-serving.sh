#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVING_DIR="${REPO_ROOT}/k8s/serving"
NAMESPACE="${NAMESPACE:-mlops}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"
IMAGE_REF="${IMAGE_REF:-songchenxue/project25-serving-multiworker:latest}"

usage() {
  cat <<'EOF'
Usage: ./scripts/deploy-k8s-serving.sh [--timeout 300s]

Deploy the serving-layer resources:
  17-serving-config.yaml
  18-serving-multiworker.yaml

Recommended flow:
  1. ./scripts/import-serving-image.sh
  2. ./scripts/deploy-k8s-serving.sh

Assumptions:
  - namespace mlops already exists
  - minio-secret already exists
  - the image songchenxue/project25-serving-multiworker:latest is already pushed/imported
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

if command -v sudo >/dev/null 2>&1 && command -v crictl >/dev/null 2>&1; then
  if ! sudo crictl images | grep -q "project25-serving-multiworker"; then
    echo "Serving image '$IMAGE_REF' was not found in the node runtime." >&2
    echo "Run ./scripts/import-serving-image.sh first, then retry deployment." >&2
    exit 1
  fi
fi

kubectl get namespace "$NAMESPACE" >/dev/null
kubectl get secret minio-secret -n "$NAMESPACE" >/dev/null

if kubectl kustomize "$SERVING_DIR" >/dev/null 2>&1; then
  kubectl apply -k "$SERVING_DIR"
else
  kubectl apply -f "$REPO_ROOT/k8s/17-serving-config.yaml" \
    -f "$REPO_ROOT/k8s/18-serving-multiworker.yaml"
fi

kubectl rollout status deployment/serving-staging -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/serving-canary -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/recommender-serving -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/prometheus -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status deployment/grafana -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"
kubectl rollout status daemonset/node-exporter -n "$NAMESPACE" --timeout="$WAIT_TIMEOUT"

kubectl get pods -n "$NAMESPACE" -l 'app in (serving-staging,serving-canary,serving-prod,prometheus,grafana,node-exporter)'
kubectl get svc -n "$NAMESPACE" serving-staging serving-canary serving-prod recommender-serving prometheus grafana node-exporter
