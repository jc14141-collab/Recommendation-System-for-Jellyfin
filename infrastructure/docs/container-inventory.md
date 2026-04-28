# Container Inventory and Role Mapping

This document enumerates the containers and containerized systems currently involved across the independent team roles. It is intended to show that the DevOps/Platform role can support each role-owned system with an equivalent Kubernetes-side deployment path, even while some role-owned components are still being developed independently.

## Cross-Role Inventory

| Role | Container / System | Purpose | Docker source | Equivalent Kubernetes manifest | Current integration status |
| --- | --- | --- | --- | --- | --- |
| DevOps / Platform | Namespace | Shared Kubernetes namespace for the project platform | Repository-native platform design | [`k8s/10-devops-platform-components.yaml`](../k8s/10-devops-platform-components.yaml) | Implemented in `mlops` |
| DevOps / Platform | PostgreSQL | Shared relational database for platform state and application data | [`postgres` in `docker-data.yml`](../../docker-data.yml) | [`k8s/01-postgres.yaml`](../k8s/01-postgres.yaml) | Implemented in `mlops` |
| DevOps / Platform | MLflow | Experiment tracking and model artifact metadata | Repository-native deployment in this infrastructure repo | [`k8s/02-mlflow.yaml`](../k8s/02-mlflow.yaml) | Implemented in `mlops` |
| DevOps / Platform | Jellyfin | Media application used by the project demo | Frontend: `Teqqquila/JF-frontend`, backend: official `jellyfin/jellyfin` | [`scripts/deploy-formal-custom-jellyfin.sh`](../scripts/deploy-formal-custom-jellyfin.sh) | Implemented as host-level `systemd` service on the dedicated Jellyfin node |
| DevOps / Platform | MinIO | Shared object storage platform service used by data and training workflows | [`minio` in `docker-data.yml`](../../docker-data.yml) | [`k8s/04-minio.yaml`](../k8s/04-minio.yaml) | Kubernetes manifest prepared |
| DevOps / Platform | MinIO bucket init | Platform-side bucket initialization support for object storage | [`minio-init` in `docker-data.yml`](../../docker-data.yml) | [`k8s/05-minio-init.yaml`](../k8s/05-minio-init.yaml) | Kubernetes manifest prepared |
| DevOps / Platform | Adminer | Shared PostgreSQL inspection UI for debugging and validation | [`adminer` in `docker-data.yml`](../../docker-data.yml) | [`k8s/06-adminer.yaml`](../k8s/06-adminer.yaml) | Kubernetes manifest prepared |
| Data | MinIO | Object storage for raw, cleaned, warehouse, artifact, and embedding data | [`minio` in `docker-data.yml`](../../docker-data.yml) | [`k8s/04-minio.yaml`](../k8s/04-minio.yaml) | Kubernetes manifest prepared |
| Data | MinIO bucket init | One-time creation of buckets such as `warehouse`, `raw`, and `artifacts` | [`minio-init` in `docker-data.yml`](../../docker-data.yml) | [`k8s/05-minio-init.yaml`](../k8s/05-minio-init.yaml) | Kubernetes manifest prepared |
| Data | Bootstraper | Data engineering utility container for embedding and catalog bootstrap tasks | [`bootstraper` in `docker-data.yml`](../../docker-data.yml) | [`k8s/07-data-role-components.yaml`](../k8s/07-data-role-components.yaml) | Planned Kubernetes equivalent |
| Data | Pipeline | Data ingestion / transformation container | [`pipeline` in `docker-data.yml`](../../docker-data.yml) | [`k8s/07-data-role-components.yaml`](../k8s/07-data-role-components.yaml) | Planned Kubernetes equivalent |
| Data | API | Application-side API backed by PostgreSQL | [`api` in `docker-data.yml`](../../docker-data.yml) | [`k8s/07-data-role-components.yaml`](../k8s/07-data-role-components.yaml) | Planned Kubernetes equivalent |
| Data | Online service | Candidate / online recommendation service backed by PostgreSQL and MinIO | [`online_service` in `docker-data.yml`](../../docker-data.yml) | [`k8s/07-data-role-components.yaml`](../k8s/07-data-role-components.yaml) | Planned Kubernetes equivalent |
| Data | Simulator | Traffic / workload simulation container | [`simulator` in `docker-data.yml`](../../docker-data.yml) | [`k8s/07-data-role-components.yaml`](../k8s/07-data-role-components.yaml) | Planned Kubernetes equivalent |
| Data | Adminer | Database inspection UI for PostgreSQL | [`adminer` in `docker-data.yml`](../../docker-data.yml) | [`k8s/06-adminer.yaml`](../k8s/06-adminer.yaml) | Kubernetes manifest prepared |
| Data | DB initialization SQL | Business table creation for user events, sessions, checkpoints, and popular movies | [`db/init/create_table.sql`](../../db_unzipped/db/init/create_table.sql) | [`k8s/01-postgres.yaml`](../k8s/01-postgres.yaml) | Ready to be merged into Postgres init flow |
| Training | Trainer image | Offline model training image with MLflow logging | [`docker-training`](../../docker-training) | [`k8s/08-training-role-components.yaml`](../k8s/08-training-role-components.yaml) | Planned Kubernetes Job / CronJob |
| Training | Training script | Main training entry point | [`train.py`](../../train.py) | [`k8s/08-training-role-components.yaml`](../k8s/08-training-role-components.yaml) | Updated to support MLflow and env overrides |
| Training | Training config | Runtime config for data paths and MLflow tracking URI | [`config.yaml`](../../config.yaml) | [`k8s/08-training-role-components.yaml`](../k8s/08-training-role-components.yaml) | Updated for current Chameleon MLflow endpoint |
| Serving | Baseline server | CPU baseline recommendation API | [`serving/baseline/Dockerfile`](../../serving-repo/serving/baseline/Dockerfile) | [`k8s/09-serving-role-components.yaml`](../k8s/09-serving-role-components.yaml) | Planned Kubernetes Deployment + Service |
| Serving | ONNX server | ONNX-based recommendation API | [`serving/onnx/Dockerfile`](../../serving-repo/serving/onnx/Dockerfile) | [`k8s/09-serving-role-components.yaml`](../k8s/09-serving-role-components.yaml) | Planned Kubernetes Deployment + Service |
| Serving | Multiworker server | Multiworker serving variant | [`serving/multiworker/Dockerfile`](../../serving-repo/serving/multiworker/Dockerfile) | [`k8s/09-serving-role-components.yaml`](../k8s/09-serving-role-components.yaml) | Planned Kubernetes Deployment + Service |
| Serving | ONNX multiworker server | Multiworker ONNX serving variant | [`serving/onnx_multiworker/Dockerfile`](../../serving-repo/serving/onnx_multiworker/Dockerfile) | [`k8s/09-serving-role-components.yaml`](../k8s/09-serving-role-components.yaml) | Planned Kubernetes Deployment + Service |
| Serving | Torch model server | PyTorch model serving variant | [`serving/torch_model/Dockerfile`](../../serving-repo/serving/torch_model/Dockerfile) | [`k8s/09-serving-role-components.yaml`](../k8s/09-serving-role-components.yaml) | Planned Kubernetes Deployment + Service |
| Serving | Torch multiworker server | Multiworker PyTorch serving variant | [`serving/torch_multiworker/Dockerfile`](../../serving-repo/serving/torch_multiworker/Dockerfile) | [`k8s/09-serving-role-components.yaml`](../k8s/09-serving-role-components.yaml) | Planned Kubernetes Deployment + Service |

## Summary Notes

- The current DevOps / Platform deployment already implements the shared namespace and core platform services: PostgreSQL and MLflow in Kubernetes, plus Jellyfin as a dedicated host-level service on the Jellyfin node.
- The DevOps / Platform role mapping is summarized separately in [`k8s/10-devops-platform-components.yaml`](../k8s/10-devops-platform-components.yaml) so that platform-owned services are visually separated from role-owned application containers.
- The data-role infrastructure pieces with clear platform ownership boundaries, especially MinIO, MinIO initialization, and Adminer, already have direct Kubernetes-side equivalents in this repository.
- The remaining data-role, training-role, and serving-role systems have valid Docker-side sources and now have explicit Kubernetes-equivalent reference manifests documenting how the platform layer will support them.
- Some role-owned services are still marked as planned because their final production image publication, data mounting strategy, or runtime integration details are still being finalized by the owning team members.
