# Infrastructure Repository for Jellyfin Recommender Initial Deployment

This repository contains the infrastructure and deployment materials used for the Chameleon-based deployment of the Jellyfin recommender project. It documents both the K3s-based shared platform on the main node and the dedicated host-level Jellyfin deployment used by the current live demo.

## Repository Purpose

This repository is focused on:

- Chameleon infrastructure access and initial cluster bring-up
- K3s installation and Kubernetes cluster setup
- Kubernetes namespace and service deployment materials
- Persistent storage configuration for node-local services
- Deployment manifests for shared platform services
- A formal deployment script for the custom Jellyfin frontend currently used by the project

The repository is intentionally scoped to the initial deployment stage. It is designed to be readable, reproducible, and suitable for packaging as course infrastructure submission material.

## Environment Summary

- Target environment: Chameleon Cloud
- Kubernetes distribution: K3s
- Primary namespace: `mlops`
- Deployment stage: initial deployment
- Resource sizing: preliminary and subject to later refinement based on observed workload behavior

The current repository reflects a working initial deployment for early integration and demonstration. It is not presented as a final production-hardened platform configuration.

## Current Two-Node Layout

The final demo deployment uses two nodes with different responsibilities.

- Main platform node
  Runs the shared K3s platform and the Kubernetes-managed services:
  - PostgreSQL
  - MLflow
  - MinIO
  - Adminer
  - Airflow
  - training manager / retraining
  - serving / monitoring
- Dedicated Jellyfin node
  Runs only the final Jellyfin service through `custom-jellyfin.service` using:
  - frontend: `Teqqquila/JF-frontend`
  - backend: official `jellyfin/jellyfin`

This means the main platform deployment and the Jellyfin deployment are intentionally executed on different nodes.

## Services Deployed

- PostgreSQL
  Platform service for persistent relational storage used by project components that require online state.
- MLflow
  Platform service for experiment tracking and artifact management. In this repository, MLflow stores metadata in PostgreSQL and artifacts on a PVC-backed filesystem path.
- Jellyfin
  Custom Jellyfin frontend plus official Jellyfin backend, deployed as a dedicated host-level service on the Jellyfin node.
- MinIO
  S3-compatible object storage used by the data engineering services defined in the compose file.
- Adminer
  Lightweight database UI for inspecting PostgreSQL during integration and debugging.

## Networking Model

The current shared-platform deployment uses a simple service exposure model:

- PostgreSQL uses `ClusterIP` and is intended for internal cluster access only.
- MLflow uses `NodePort` `30500`.
- Jellyfin is not part of the Kubernetes bootstrap flow. The final live Jellyfin runs as a host-level service and listens on `8096` on the dedicated Jellyfin node.

No dedicated Ingress manifest is included in this initial deployment. `NodePort` is used as the external access method where browser access is needed for demonstration and validation.

## Persistent Storage

Persistent storage is implemented with a mix of K3s `local-path` PVCs and direct node-local host storage.

- PostgreSQL stores database state in the node-local directory `/mnt/block/postgres_data`.
- MLflow defines a PVC for artifact storage while experiment metadata is stored in PostgreSQL.
- Jellyfin watches `/mnt/block/movies` on the dedicated Jellyfin node and stores runtime state under the service user's home directory.
- MinIO stores object data under `/mnt/object/minio_data` on the node.

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
5. On the dedicated Jellyfin node, run `scripts/deploy-formal-custom-jellyfin.sh`.

The Chameleon instance provisioning and access steps may include manual actions in the Chameleon environment, such as launching the instance, assigning a floating IP, and confirming security group rules. This repository documents and supports the Kubernetes-side deployment after node access is available.

## New Node Preparation

When deploying onto a brand-new Chameleon node, perform these preparation steps before running the bootstrap script. This is the part that is easy to overlook when moving from one node to another.

1. Make sure the repository exists on the node and enter the infrastructure directory.
2. Install K3s on the node.
3. Create the host directories used by node-local storage.
4. Make sure the mount points and permissions are ready before applying the manifests.

Example Linux node preparation sequence for a brand-new node without the repository yet:

```bash
sudo -n true
git clone https://github.com/jc14141-collab/Recommendation-System-for-Jellyfin.git
cd ~/Recommendation-System-for-Jellyfin/infrastructure
chmod +x scripts/*.sh

sudo ./scripts/install-k3s-server.sh

mkdir -p /mnt/object/minio_data
mkdir -p /mnt/block/postgres_data

sudo chown -R 999:999 /mnt/block/postgres_data
sudo chmod 700 /mnt/block/postgres_data
```

If the repository is already present on the node, update it instead of cloning again:

```bash
cd ~/Recommendation-System-for-Jellyfin
git pull origin main
cd ~/Recommendation-System-for-Jellyfin/infrastructure
chmod +x scripts/*.sh
```

If the node does not already have the expected storage mounts, verify them before continuing:

```bash
df -h
ls -la /mnt/block
ls -la /mnt/object
```

After this preparation, continue with the bootstrap deployment.

## Dedicated Jellyfin Node Preparation

The final Jellyfin used by the project is deployed separately from the Kubernetes bootstrap manifests.

- frontend source: `https://github.com/Teqqquila/JF-frontend.git`
- backend source: `https://github.com/jellyfin/jellyfin.git`
- deployment entrypoint in this repo: `scripts/deploy-formal-custom-jellyfin.sh`

This script installs dependencies, prepares `/mnt/block/movies`, clones or updates both upstream repositories into managed directories under `~/custom-jellyfin-managed`, builds the frontend, stops any older hand-run Jellyfin process from the legacy root-directory workflow, and installs a `systemd` service named `custom-jellyfin`.

If the repository is not present yet on the dedicated Jellyfin node:

```bash
git clone https://github.com/jc14141-collab/Recommendation-System-for-Jellyfin.git
cd ~/Recommendation-System-for-Jellyfin/infrastructure
chmod +x scripts/*.sh
```

If the repository already exists on the Jellyfin node, do not clone again. Update it in place:

```bash
cd ~/Recommendation-System-for-Jellyfin
git pull origin main
cd ~/Recommendation-System-for-Jellyfin/infrastructure
chmod +x scripts/*.sh
```

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
- applies the infrastructure manifests `00-02`, `04-06`
- recreates the one-shot `minio-init` job cleanly
- waits for PostgreSQL, MLflow, MinIO, and Adminer to become ready
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

## Full End-to-End Deployment

Use this section when starting from a new Chameleon lease or after cleaning the `mlops` namespace. The full deployment is intentionally split into four layers:

1. Bootstrap infrastructure and application support services
2. Deploy Airflow
3. Build/import/deploy the training layer
4. Build/import/deploy the serving layer
5. Deploy the formal Jellyfin service on the dedicated Jellyfin node

Before running the scripts, make sure the node has K3s, Docker, Git, Python, and Docker Hub login configured. Also make sure the node-local directories `/mnt/block/postgres_data` and `/mnt/object/minio_data` exist. If the node only has K3s' bundled kubectl, you can use:

```bash
echo 'alias kubectl="sudo k3s kubectl"' >> ~/.bashrc
source ~/.bashrc
```

Then run the complete deployment sequence from the repository root on the main platform node:

```bash
cd ~/Recommendation-System-for-Jellyfin
git pull origin main

cd ~/Recommendation-System-for-Jellyfin/infrastructure
chmod +x scripts/*.sh

mkdir -p /mnt/object/minio_data
mkdir -p /mnt/block/postgres_data
sudo chown -R 999:999 /mnt/block/postgres_data
sudo chmod 700 /mnt/block/postgres_data

export POSTGRES_DB="recsys"
export POSTGRES_USER="recsys"
export POSTGRES_PASSWORD="recsys123"
export MINIO_ROOT_USER="minioadmin"
export MINIO_ROOT_PASSWORD="minioadmin123"

# 1. Infrastructure, shared platform services, config, and app support services.
bash ./scripts/deploy-k8s-bootstrap.sh

# 2. Airflow Helm and Kubernetes resources.
helm repo add apache-airflow https://airflow.apache.org
helm repo update
echo 'export KUBECONFIG=/etc/rancher/k3s/k3s.yaml' >> ~/.bashrc
source ~/.bashrc
cd ~/Recommendation-System-for-Jellyfin
helm install airflow apache-airflow/airflow \
  -n airflow \
  --create-namespace
kubectl apply -f rbac/ -R
helm upgrade airflow apache-airflow/airflow \
  -n airflow \
  -f airflow/values.yaml

# 3. Training image and Kubernetes resources.
bash ./scripts/import-training-image.sh
bash ./scripts/deploy-k8s-training.sh

# 4. Serving image and Kubernetes resources.
bash ./scripts/import-serving-image.sh
bash ./scripts/deploy-k8s-serving.sh
```

On the dedicated Jellyfin node, run from the repository infrastructure directory:

```bash
cd ~/Recommendation-System-for-Jellyfin/infrastructure
chmod +x scripts/*.sh
bash ./scripts/deploy-formal-custom-jellyfin.sh
```

Validate the full deployment:

```bash
kubectl get pods -n mlops
kubectl get svc -n mlops
kubectl get cronjob -n mlops
kubectl get pods -n airflow
kubectl get svc -n airflow
```

Core local health checks from the node:

```bash
curl -i http://127.0.0.1:30500
curl -i http://127.0.0.1:30900/minio/health/live
curl -i http://127.0.0.1:30089/api/status
curl -i http://127.0.0.1:30082/health
curl -i http://127.0.0.1:31080/health
```

Expected external endpoints use the node or floating IP:

- MLflow: `http://<node-ip>:30500`
- Airflow: `http://<node-ip>:30080/dags`
- Jellyfin: `http://<jellyfin-node-ip>:8096/web/#/home`
- MinIO API: `http://<node-ip>:30900`
- MinIO Console: `http://<node-ip>:30901`
- Training Manager: `http://<node-ip>:30089`
- Serving prod direct: `http://<node-ip>:30082`
- Serving prod alias: `http://<node-ip>:31080`
- Serving staging: `http://<node-ip>:30083`
- Serving canary: `http://<node-ip>:30084`
- Prometheus: `http://<node-ip>:30090`
- Grafana: `http://<node-ip>:30030`

## Formal Jellyfin Deployment

The current live Jellyfin is intentionally managed outside the Kubernetes bootstrap flow. The old Kubernetes manifest `k8s/03-jellyfin.yaml` is retained only as a legacy reference and is no longer applied by the main bootstrap scripts.

The formal deployment script is:

```bash
./scripts/deploy-formal-custom-jellyfin.sh
```

It performs the following steps:

1. installs Linux dependencies
2. ensures Node.js 24 is available
3. installs the matching `.NET` SDK channel required by the current official Jellyfin backend
4. clones or updates:
   - `https://github.com/Teqqquila/JF-frontend.git`
   - `https://github.com/jellyfin/jellyfin.git`
5. builds the frontend with `npm ci` and `npm run build:production`
6. prepares `/mnt/block/movies` for the demo media library
7. stops older hand-run Jellyfin processes from the legacy root-directory workflow if they are still running
8. installs and starts a host-level `systemd` service named `custom-jellyfin`

To avoid overwriting ad-hoc working copies, the script uses managed directories:

- `~/custom-jellyfin-managed/jellyfin-web`
- `~/custom-jellyfin-managed/jellyfin`

Useful commands after deployment:

```bash
sudo systemctl status custom-jellyfin
sudo systemctl restart custom-jellyfin
journalctl -u custom-jellyfin -f
```

## Airflow Deployment

Airflow is deployed on the main platform node after the bootstrap layer is ready.

The current repository-managed deployment flow is:

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
echo 'export KUBECONFIG=/etc/rancher/k3s/k3s.yaml' >> ~/.bashrc
source ~/.bashrc

cd ~/Recommendation-System-for-Jellyfin

helm install airflow apache-airflow/airflow \
  -n airflow \
  --create-namespace

kubectl apply -f rbac/ -R

helm upgrade airflow apache-airflow/airflow \
  -n airflow \
  -f airflow/values.yaml
```

This flow uses:

- `airflow/Dockerfile`
- `airflow/values.yaml`
- `airflow/dags/`
- `rbac/airflow-sa.yaml`
- `rbac/airflow-job-trigger.yaml`
- `rbac/airflow-secrets.yaml`
- `rbac/airflow-api-nodeport.yaml`

After deployment, the expected external UI is:

- Airflow: `http://<node-ip>:30080/dags`

Default login:

- username: `admin`
- password: `admin123`

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
2. Deploy Airflow with Helm and the `rbac/` manifests
3. Run `./scripts/import-training-image.sh`
4. Run `bash ./scripts/deploy-k8s-training.sh`

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

This serving deployment includes six runtime components aligned with the multiworker compose setup:

- `serving-staging`
- `serving-canary`
- `recommender-serving` / `serving-prod`
- `node-exporter`
- `prometheus`
- `grafana`

The serving endpoints are:

- prod alias for training and app consumers: `http://recommender-serving:8000`
- prod direct service: `http://serving-prod:8000`
- staging direct service: `http://serving-staging:8000`
- canary direct service: `http://serving-canary:8000`

The external NodePorts are:

- prod alias: `http://<node-ip>:31080`
- prod direct: `http://<node-ip>:30082`
- staging: `http://<node-ip>:30083`
- canary: `http://<node-ip>:30084`
- Prometheus: `http://<node-ip>:30090`
- Grafana: `http://<node-ip>:30030`
- node exporter metrics: `http://<node-ip>:30100/metrics`

The serving image is:

- `songchenxue/project25-serving-multiworker:latest`

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

After SSH access to a Chameleon node is available, use the full sequence in [Full End-to-End Deployment](#full-end-to-end-deployment) for the current project stack. The minimal bootstrap-only example below is useful when validating only the base infrastructure layer:

```bash
if [ ! -d ~/Recommendation-System-for-Jellyfin/.git ]; then
  git clone https://github.com/jc14141-collab/Recommendation-System-for-Jellyfin.git
else
  cd ~/Recommendation-System-for-Jellyfin
  git pull origin main
fi

cd ~/Recommendation-System-for-Jellyfin/infrastructure
chmod +x scripts/*.sh
sudo ./scripts/install-k3s-server.sh
mkdir -p /mnt/object/minio_data
mkdir -p /mnt/block/postgres_data
sudo chown -R 999:999 /mnt/block/postgres_data
sudo chmod 700 /mnt/block/postgres_data
export POSTGRES_USER="recsys"
export POSTGRES_DB="recsys"
export POSTGRES_PASSWORD="recsys123"
export MINIO_ROOT_USER="minioadmin"
export MINIO_ROOT_PASSWORD="minioadmin123"
./scripts/deploy-k8s-bootstrap.sh
kubectl get pods -n mlops
kubectl get svc -n mlops
kubectl get pvc -n mlops
```

If you are deploying from a local Windows PowerShell terminal to a Chameleon instance, you can also run:

```powershell
.\scripts\deploy-chameleon.ps1 -FloatingIp 129.114.25.219 -PostgresPassword "replace-with-a-real-password"
```

This helper script copies the repository to the remote node, installs K3s unless `-SkipK3sInstall` is supplied, creates the PostgreSQL secret, applies the bootstrap manifests, and waits for the base infrastructure services to become ready. It does not deploy Jellyfin.

Expected external endpoints for the full deployment:

- MLflow: `http://<floating-ip>:30500`
- Airflow: `http://<floating-ip>:30080/dags`
- Jellyfin: `http://<jellyfin-floating-ip>:8096/web/#/home`
- MinIO API: `http://<floating-ip>:30900`
- MinIO Console: `http://<floating-ip>:30901`
- Adminer: `http://<floating-ip>:5050`
- Training Manager: `http://<floating-ip>:30089`
- Serving prod direct: `http://<floating-ip>:30082`
- Serving prod alias: `http://<floating-ip>:31080`
- Serving staging: `http://<floating-ip>:30083`
- Serving canary: `http://<floating-ip>:30084`
- Prometheus: `http://<floating-ip>:30090`
- Grafana: `http://<floating-ip>:30030`

PostgreSQL is intended for internal service access inside the cluster.
