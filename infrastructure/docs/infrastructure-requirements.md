# Infrastructure Requirements and Initial Resource Sizing

This document summarizes the initial Kubernetes resource configuration used by the current deployment manifests. The values below should be treated as preliminary sizing for the initial deployment stage and may be adjusted later based on observed workload behavior.

## Service Resource Table

| Service | CPU Request | CPU Limit | Memory Request | Memory Limit | GPU Request | GPU Limit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| PostgreSQL | `500m` | `1` | `1Gi` | `2Gi` | `0` | `0` | Initial sizing for a single-replica relational database used as a shared platform service. |
| MLflow | `250m` | `1` | `512Mi` | `1Gi` | `0` | `0` | Initial sizing for low-concurrency experiment tracking and artifact management. |
| Jellyfin | host-level service | host-level service | host-level service | host-level service | `0` | `0` | Final live Jellyfin is managed outside Kubernetes through `custom-jellyfin.service` on the dedicated Jellyfin node. |

## GPU Usage Note

GPU resources are not requested by any of the services in the current initial deployment.

This is intentional:

- PostgreSQL does not require GPU resources.
- MLflow does not require GPU resources as deployed here.
- Jellyfin is deployed for application availability and demonstration on a dedicated node, not GPU-backed media processing in this repository state.

GPU capacity can be reserved for future training or model-related workloads if the project later requires it.

## Initial Deployment Sizing Note

The current requests and limits are preliminary values chosen to support:

- a readable and reviewable initial deployment
- stable startup of the required platform services
- a working course demonstration on Chameleon

These values may be refined later after observing actual usage, restart behavior, memory pressure, CPU pressure, or service-specific workload changes.

## Evidence to Include in Submission PDF

When exporting this document to PDF for submission, it is helpful to include:

- Chameleon instance or node specification screenshot
- `kubectl describe pod` output for PostgreSQL and MLflow, plus `systemctl status custom-jellyfin` for Jellyfin
- `kubectl get pods -n mlops` and `kubectl get pvc -n mlops`
- browser-access evidence for MLflow and Jellyfin where applicable
