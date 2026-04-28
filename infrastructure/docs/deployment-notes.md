# Deployment and Provisioning Notes

## What Is Covered by This Repository

This repository covers the Kubernetes-side materials for the initial project deployment on Chameleon:

- K3s installation and cluster setup support through scripts
- Kubernetes namespace creation
- Persistent storage declarations through PVCs
- Network exposure using `ClusterIP` and `NodePort`
- Application deployment manifests for PostgreSQL and MLflow, plus a formal host-level Jellyfin deployment script

## Manual Steps Versus Repository-Driven Steps

### Manual or Environment-Driven Steps

Some infrastructure access steps may be performed manually in the Chameleon environment, including:

- launching or accessing the Chameleon instance
- assigning a floating IP
- confirming SSH access
- confirming security group/network access for SSH and NodePort-based browser access

### Script-Driven Steps

The following tasks are supported by scripts in `scripts/`:

- K3s server installation
- K3s agent installation for worker-node join
- retrieval of the K3s node token
- creation of the PostgreSQL Kubernetes secret from environment variables

### Manifest-Driven Steps

The following tasks are represented by manifests in `k8s/`:

- namespace creation
- PostgreSQL deployment and PVC-backed state
- MLflow deployment with PostgreSQL-backed metadata and PVC-backed artifact storage
- Jellyfin deployment through `scripts/deploy-formal-custom-jellyfin.sh`

## Recommended Local-to-Chameleon Workflow

For a Windows-based local terminal workflow, the recommended path is:

1. confirm the Chameleon instance floating IP is reachable over SSH
2. run `scripts/deploy-chameleon.ps1` from the local repository
3. allow the script to sync the repository, install K3s, create the PostgreSQL secret, and apply the Kubernetes manifests
4. validate access to MLflow through the main node floating IP and Jellyfin through the dedicated Jellyfin node on port `8096`

This workflow is useful when `kubectl` is not installed on the local machine and cluster administration is performed directly on the Chameleon node.

## Initial Deployment Note

This repository reflects an initial deployment for project integration and course demonstration.

- resource sizing is preliminary
- network exposure is intentionally simple
- storage uses the default K3s `local-path` behavior
- service configuration may evolve later as workload and team integration requirements become clearer

The current materials are intended to demonstrate a working, reproducible initial deployment rather than a final optimized platform configuration.
