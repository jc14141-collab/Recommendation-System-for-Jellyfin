# Infrastructure Repository for Jellyfin Recommender Initial Deployment

This repository contains the infrastructure and deployment materials used for the initial Chameleon-based deployment of the Jellyfin recommender project. It is intended to serve as an infrastructure repository for course submission and documents how the team configured a K3s-based Kubernetes environment and deployed the initial shared services required by the project.

## Repository Purpose

This repository is focused on:

- Chameleon infrastructure access and initial cluster bring-up
- K3s installation and Kubernetes cluster setup
- Kubernetes namespace and service deployment materials
- Persistent storage configuration through PVC-backed services
- Deployment manifests for shared platform services and the open-source service used by the project

The repository is intentionally scoped to the initial deployment stage. It is designed to be readable, reproducible, and suitable for packaging as course infrastructure submission material.

## Environment Summary

- Target environment: Chameleon Cloud
- Kubernetes distribution: K3s
- Primary namespace: `mlops`
- Deployment stage: initial deployment
- Resource sizing: preliminary and subject to later refinement based on observed workload behavior

The current repository reflects a working initial deployment for early integration and demonstration. It is not presented as a final production-hardened platform configuration.

## Services Deployed

- PostgreSQL: platform service for persistent relational storage.
- MLflow: platform service for experiment tracking and artifact management.
- Jellyfin: open-source service used by the project and deployed in Kubernetes for demonstration.
- MinIO: S3-compatible object storage used by data engineering services.
- Adminer: lightweight database UI for PostgreSQL debugging.

## Networking Model

- PostgreSQL uses `ClusterIP` and is intended for internal cluster access only.
- MLflow uses `NodePort` `30500`.
- Jellyfin uses `NodePort` `30096`.
- No dedicated Ingress manifest is included in this initial deployment. NodePort is used for demonstration access.

## Persistent Storage

Persistent storage is implemented using the default K3s `local-path` storage class.

- PostgreSQL defines a PVC for database state.
- MLflow defines a PVC for artifact storage while experiment metadata is stored in PostgreSQL.
- Jellyfin defines a PVC for configuration retention.
- MinIO defines a PVC for object storage data.

These services rely on persistent volumes so that state and configuration survive pod restart events. In this initial deployment, persistence is scoped to node-local storage behavior provided by `local-path`.

## Repository Structure

The full infrastructure package is attached at:

- `infrastructure/infrastructure-submission.zip`

Inside the package:

- `k8s/` contains Kubernetes manifests for namespace creation and service deployment.
- `scripts/` contains K3s, deployment, and secret creation helpers.
- `docs/` contains container inventory, sizing notes, and deployment notes.

## Deployment Order

1. Provision or access a Chameleon node.
2. Install K3s on the primary node using `scripts/install-k3s-server.sh`.
3. Create the Kubernetes namespace with `k8s/00-namespace.yaml`.
4. Create the PostgreSQL secret separately using `scripts/create-postgres-secret.sh`.
5. Apply PostgreSQL, MLflow, and Jellyfin manifests.
6. If needed, create the MinIO secret and apply MinIO/Adminer manifests.

Some Chameleon instance provisioning actions, such as launching the instance, assigning a floating IP, and confirming security group rules, may be performed manually through the Chameleon environment.

## Security Note

Real secrets are not stored in this repository. PostgreSQL and MinIO credentials must be created separately at deployment time using environment variables or equivalent secret management workflows.

## Submission Note

This infrastructure package is intended to support course submission as initial Chameleon + K3s deployment material for the DevOps / Platform role.
