#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root or via sudo."
  exit 1
fi

cat /var/lib/rancher/k3s/server/node-token
