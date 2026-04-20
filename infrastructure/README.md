# Infrastructure Repository for Jellyfin Recommender Initial Deployment

This repository contains the infrastructure and deployment materials used for the initial Chameleon-based deployment of the Jellyfin recommender project. It is intended to serve as an infrastructure repository for course submission and documents how the team configured a K3s-based Kubernetes environment and deployed the initial shared services required by the project.

## Repository Purpose

This repository is focused on:

- Chameleon infrastructure access and initial cluster bring-up
- K3s installation and Kubernetes cluster setup
- Kubernetes namespace and service deployment materials
- Persistent storage configuration for node-local services
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

Persistent storage is implemented with a mix of K3s `local-path` PVCs and direct node-local host storage.

- PostgreSQL defines a PVC for database state.
- MLflow defines a PVC for artifact storage while experiment metadata is stored in PostgreSQL.
- Jellyfin defines a PVC for configuration retention.
- MinIO stores object data under `/mnt/block/minio_data` on the node.

These services rely on persistent storage so that state and configuration survive pod restart events. In this initial deployment, persistence remains node-local, using `local-path` PVCs where convenient and direct host mounts where the team wants stable host directories.

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
4. Deploy the base shared services by applying `k8s/bootstrap`, which bundles the namespace, PostgreSQL init configmap, infrastructure services, and the role reference ConfigMaps.

The Chameleon instance provisioning and access steps may include manual actions in the Chameleon environment, such as launching the instance, assigning a floating IP, and confirming security group rules. This repository documents and supports the Kubernetes-side deployment after node access is available.

## Security Note

Real secrets are not stored in this repository.

- PostgreSQL credentials must be created separately at deployment time.
- `scripts/create-postgres-secret.sh` provides an example mechanism for creating the required Kubernetes secret from environment variables.
- No real password or sensitive credential should be committed into Git.

## Submission Note

This repository is intended to be packaged and uploaded as infrastructure and deployment material for course submission. It includes the cluster setup scripts, Kubernetes manifests, and supporting documentation needed to explain the initial Chameleon + K3s deployment of the project services.

## Automated Bootstrap

For day-to-day operations, the recommended path is to deploy the bootstrap manifest set as one unit through the bootstrap kustomization.

From a Linux shell with `kubectl` configured for the target cluster:

```bash
chmod +x scripts/*.sh
export POSTGRES_USER="recsys"
export POSTGRES_DB="recsys"
export POSTGRES_PASSWORD="replace-with-a-real-password"
export MINIO_ROOT_USER="minioadmin"
export MINIO_ROOT_PASSWORD="replace-with-a-real-password"
./scripts/deploy-k8s-bootstrap.sh
```

The bootstrap script:

- creates or updates `postgres-secret` and `minio-secret`
- applies the infrastructure manifests `00-06`
- recreates the one-shot `minio-init` job cleanly
- waits for PostgreSQL, MLflow, Jellyfin, MinIO, and Adminer to become ready
- applies the config manifests after `16`:
  - `postgres-initdb.yaml`
  - `data-configmap.yaml`
  - `online-service-configmap.yaml`
  - `simulator-configmap.yaml`
- applies the application manifests:
  - `11-online-service.yaml`
  - `12-simulator.yaml`
  - `13-data-api.yaml`
- waits for `api`, `online-service-api`, `online-service-worker`, and `simulator`

## Training Deployment

Training is deployed separately from the infrastructure bootstrap. The current training-layer resources are:

- `14-training-config.yaml`
- `15-training-manager.yaml`
- `16-training-retrain-cronjob.yaml`

The training deployment assumes:

- infrastructure bootstrap has already completed
- `minio-secret` already exists in `mlops`
- the image `songchenxue/jellyfin-training:latest` has already been built from `training/Dockerfile`, pushed, and imported into the node runtime

On the Linux node, prepare the image with:

```bash
./scripts/import-training-image.sh
```

From a Linux shell with `kubectl` configured for the target cluster:

```bash
bash ./scripts/deploy-k8s-training.sh
```

From Windows PowerShell:

```powershell
.\scripts\deploy-k8s-training.ps1
```

The training manager service is exposed on NodePort `30089`, and the scheduled retraining CronJob runs daily at `02:00` UTC.

The end-to-end ops flow is now:

1. Run `bash ./scripts/deploy-k8s-bootstrap.sh`
2. Run `./scripts/import-training-image.sh`
3. Run `bash ./scripts/deploy-k8s-training.sh`

If the secrets already exist in the cluster, you can skip secret creation:

```bash
./scripts/deploy-k8s-bootstrap.sh --skip-secret-setup
```

## Serving Deployment

Serving is deployed separately from the infrastructure bootstrap and CI/CD placeholders. The current serving-layer resources are:

- `17-serving-config.yaml`
- `18-serving-multiworker.yaml`

On the Linux node, prepare the image with:

```bash
./scripts/import-serving-image.sh
```

Then deploy the serving layer:

```bash
bash ./scripts/deploy-k8s-serving.sh
```

From Windows PowerShell:

```powershell
.\scripts\deploy-k8s-serving.ps1
```

This first serving deployment targets the prod multiworker recommender only and exposes:

- internal service: `http://recommender-serving:8000`
- external NodePort: `http://<node-ip>:30080`

From Windows PowerShell:

```powershell
.\scripts\deploy-k8s-bootstrap.ps1 `
  -PostgresUser "recsys" `
  -PostgresDb "recsys" `
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
export POSTGRES_DB="recsys"
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
