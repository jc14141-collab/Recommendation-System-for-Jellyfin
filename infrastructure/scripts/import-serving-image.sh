#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVING_SOURCE_DIR="${REPO_ROOT}/../serving/serving/multiworker"
IMAGE_REF="${IMAGE_REF:-docker.io/library/project25-serving-multiworker:latest}"
ARCHIVE_PATH="${ARCHIVE_PATH:-/tmp/project25-serving-multiworker.tar}"

usage() {
  cat <<'EOF'
Usage: ./scripts/import-serving-image.sh

Build the multiworker serving image, tag it with the full containerd-visible
reference, and import it into the k3s/containerd runtime.

Environment variables:
  IMAGE_REF     Full image reference to import. Default: docker.io/library/project25-serving-multiworker:latest
  ARCHIVE_PATH  Temporary tar path for docker save. Default: /tmp/project25-serving-multiworker.tar
EOF
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command '$1' is not available in PATH." >&2
    exit 1
  fi
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

require_command docker
require_command sudo
require_command k3s

if [[ ! -d "$SERVING_SOURCE_DIR" ]]; then
  echo "Serving source directory was not found at '$SERVING_SOURCE_DIR'." >&2
  exit 1
fi

docker build -t project25-serving-multiworker:latest "$SERVING_SOURCE_DIR"
docker tag project25-serving-multiworker:latest "$IMAGE_REF"
docker save "$IMAGE_REF" -o "$ARCHIVE_PATH"
sudo k3s ctr images import "$ARCHIVE_PATH"
sudo crictl images | grep 'project25-serving-multiworker'

echo "Serving image is ready in the node runtime as $IMAGE_REF"
