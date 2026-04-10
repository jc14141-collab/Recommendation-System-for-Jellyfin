#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root or via sudo."
  exit 1
fi

export INSTALL_K3S_EXEC="server --write-kubeconfig-mode 644"

curl -sfL https://get.k3s.io | sh -

echo
echo "K3s server installation finished."
echo "Check status with: systemctl status k3s --no-pager"
echo "Check nodes with: kubectl get nodes"
