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

- PostgreSQL
  Platform service for persistent relational storage used by project components that require online state.
- MLflow
  Platform service for experiment tracking and artifact management. In this repository, MLflow stores metadata in PostgreSQL and artifacts on a PVC-backed filesystem path.
- Jellyfin
  Open-source service used by the project and deployed in Kubernetes for demonstration.
- MinIO
  S3-compatible object storage used by the data engineering services defined in the compose file.
- Adminer
  Lightweight database UI for inspecting PostgreSQL during integration and debugging.

## Networking Model

The current initial deployment uses a simple service exposure model:

- PostgreSQL uses `ClusterIP` and is intended for internal cluster access only.
- MLflow uses `NodePort` `30500`.
- Jellyfin uses `NodePort` `30096`.

No dedicated Ingress manifest is included in this initial deployment. `NodePort` is used as the external access method where browser access is needed for demonstration and validation.

## Persistent Storage

Persistent storage is implemented using the default K3s `local-path` storage class.

- PostgreSQL defines a PVC for database state.
- MLflow defines a PVC for artifact storage while experiment metadata is stored in PostgreSQL.
- Jellyfin defines a PVC for configuration retention.
- MinIO defines a PVC for object storage data.

These services rely on persistent volumes so that state and configuration survive pod restart events. In this initial deployment, persistence is scoped to the node-local storage behavior provided by `local-path`.

## Repository Structure

- `k8s/`
  Kubernetes manifests for namespace creation and service deployment.
- `scripts/`
  Utility scripts for K3s installation, worker join, token lookup, and Kubernetes secret creation.
- `docs/`
  Submission-oriented documentation, including container inventory, infrastructure sizing notes, and deployment/provisioning notes.

## Deployment Order

The repository supports the following initial deployment workflow:

1. Provision or access a Chameleon node.
2. Install K3s on the primary node using `scripts/install-k3s-server.sh`.
3. If additional nodes are used, join them with `scripts/install-k3s-agent.sh`.
4. Deploy the base shared services by applying `k8s/bootstrap`, which bundles `00-namespace.yaml` through `06-adminer.yaml`.

The Chameleon instance provisioning and access steps may include manual actions in the Chameleon environment, such as launching the instance, assigning a floating IP, and confirming security group rules. This repository documents and supports the Kubernetes-side deployment after node access is available.

## Security Note

Real secrets are not stored in this repository.

- PostgreSQL credentials must be created separately at deployment time.
- `scripts/create-postgres-secret.sh` provides an example mechanism for creating the required Kubernetes secret from environment variables.
- No real password or sensitive credential should be committed into Git.

## Submission Note

This repository is intended to be packaged and uploaded as infrastructure and deployment material for course submission. It includes the cluster setup scripts, Kubernetes manifests, and supporting documentation needed to explain the initial Chameleon + K3s deployment of the project services.

## Automated Bootstrap

For day-to-day operations, the recommended path is to deploy the first seven manifests as one unit through the bootstrap kustomization.

From a Linux shell with `kubectl` configured for the target cluster:

```bash
chmod +x scripts/*.sh
export POSTGRES_USER="recsys"
export POSTGRES_PASSWORD="replace-with-a-real-password"
export MINIO_ROOT_USER="minioadmin"
export MINIO_ROOT_PASSWORD="replace-with-a-real-password"
./scripts/deploy-k8s-bootstrap.sh
```

The bootstrap script:

- creates or updates `postgres-secret` and `minio-secret`
- applies `k8s/bootstrap`, which includes `00-namespace.yaml` through `06-adminer.yaml`
- recreates the one-shot `minio-init` job cleanly
- waits for PostgreSQL, MLflow, Jellyfin, MinIO, and Adminer to become ready

## App Deployment

Application services are managed separately from the infrastructure bootstrap so that redeploying API or training UI containers does not force a database or object-store restart.

The app-layer manifests currently included in this repository are:

- `11-data-pipeline-config.yaml`
- `12-online-service-config.yaml`
- `13-api.yaml`
- `14-online-service-api.yaml`
- `15-training-manager.yaml`
- `16-retrain-job.yaml` as a manual job template for one-off retraining

From a Linux shell with `kubectl` configured for the target cluster:

```bash
export POSTGRES_USER="recsys"
export POSTGRES_PASSWORD="replace-with-a-real-password"
export S3_ACCESS_KEY="minioadmin"
export S3_SECRET_KEY="replace-with-a-real-password"
export ADMIN_PASSWORD="replace-with-a-real-password"
bash ./scripts/deploy-k8s-apps.sh
```

From Windows PowerShell:

```powershell
.\scripts\deploy-k8s-apps.ps1 `
  -PostgresUser "recsys" `
  -PostgresPassword "replace-with-a-real-password" `
  -S3AccessKey "minioadmin" `
  -S3SecretKey "replace-with-a-real-password" `
  -AdminPassword "replace-with-a-real-password"
```

Notes:

- `training-manager` uses the image `project25-training-manager:latest`, which should be built from `training/Dockerfile` and loaded onto the node or published to a registry before deployment.
- `retrain-mlp` is intentionally not included in the app-layer kustomization because retraining should be triggered manually. Apply `k8s/16-retrain-job.yaml` only when you want a new model run.

If the secrets already exist in the cluster, you can skip secret creation:

```bash
./scripts/deploy-k8s-bootstrap.sh --skip-secret-setup
```

From Windows PowerShell:

```powershell
.\scripts\deploy-k8s-bootstrap.ps1 `
  -PostgresUser "recsys" `
  -PostgresPassword "replace-with-a-real-password" `
  -MinioRootUser "minioadmin" `
  -MinioRootPassword "replace-with-a-real-password"
```

## Quick Deployment Example

After SSH access to a Chameleon node is available:

```bash
chmod +x scripts/*.sh
sudo ./scripts/install-k3s-server.sh
export POSTGRES_USER="recsys"
export POSTGRES_PASSWORD="replace-with-a-real-password"
export MINIO_ROOT_USER="minioadmin"
export MINIO_ROOT_PASSWORD="replace-with-a-real-password"
./scripts/deploy-k8s-bootstrap.sh
kubectl get pods -n mlops
kubectl get svc -n mlops
kubectl get pvc -n mlops
```

If you are deploying from a local Windows PowerShell terminal to a Chameleon instance, you can also run:

```powershell
.\scripts\deploy-chameleon.ps1 -FloatingIp 129.114.25.219 -PostgresPassword "replace-with-a-real-password"
```

This helper script copies the repository to the remote node, installs K3s unless `-SkipK3sInstall` is supplied, creates the PostgreSQL secret, applies the manifests, and waits for PostgreSQL, MLflow, and Jellyfin to become ready.

Expected external endpoints for the initial deployment:

- MLflow: `http://<floating-ip>:30500`
- Jellyfin: `http://<floating-ip>:30096`
- MinIO API: `http://<floating-ip>:30900`
- MinIO Console: `http://<floating-ip>:30901`
- Adminer: `http://<floating-ip>:30050`

PostgreSQL is intended for internal service access inside the cluster.
