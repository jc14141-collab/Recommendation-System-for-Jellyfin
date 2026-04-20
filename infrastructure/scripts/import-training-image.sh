#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TRAINING_SOURCE_DIR="${REPO_ROOT}/../training"
IMAGE_REF="${IMAGE_REF:-songchenxue/jellyfin-training:latest}"
ARCHIVE_PATH="${ARCHIVE_PATH:-/tmp/jellyfin-training.tar}"

usage() {
  cat <<'EOF'
Usage: ./scripts/import-training-image.sh

Build the training image from training/Dockerfile, tag it, push it, and import
it into the k3s/containerd runtime.

Environment variables:
  IMAGE_REF     Full image reference to tag/push/import. Default: songchenxue/jellyfin-training:latest
  ARCHIVE_PATH  Temporary tar path for docker save. Default: /tmp/jellyfin-training.tar
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

if [[ ! -d "$TRAINING_SOURCE_DIR" ]]; then
  echo "Training source directory was not found at '$TRAINING_SOURCE_DIR'." >&2
  exit 1
fi

docker build -t jellyfin-training:latest "$TRAINING_SOURCE_DIR"
docker tag jellyfin-training:latest "$IMAGE_REF"
docker push "$IMAGE_REF"
docker save "$IMAGE_REF" -o "$ARCHIVE_PATH"
sudo k3s ctr images import "$ARCHIVE_PATH"
sudo crictl images | grep 'jellyfin-training'

echo "Training image is ready in the node runtime as $IMAGE_REF"
