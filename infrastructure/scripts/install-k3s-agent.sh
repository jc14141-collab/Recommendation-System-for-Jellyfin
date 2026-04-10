#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root or via sudo."
  exit 1
fi

if [[ $# -ne 2 ]]; then
  echo "Usage: sudo ./install-k3s-agent.sh <SERVER_IP> <NODE_TOKEN>"
  exit 1
fi

SERVER_IP="$1"
NODE_TOKEN="$2"

export K3S_URL="https://${SERVER_IP}:6443"
export K3S_TOKEN="${NODE_TOKEN}"

curl -sfL https://get.k3s.io | sh -

echo
echo "K3s agent installation finished."
echo "Confirm from the server with: kubectl get nodes -o wide"
