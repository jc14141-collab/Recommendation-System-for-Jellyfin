# Container Inventory and Role Mapping

This document enumerates the containers and containerized systems currently involved across the independent team roles. It is intended to show that the DevOps/Platform role can support each role-owned system with an equivalent Kubernetes-side deployment path, even while some role-owned components are still being developed independently.

## Cross-Role Inventory

| Role | Container / System | Purpose | Docker source | Equivalent Kubernetes manifest | Current integration status |
| --- | --- | --- | --- | --- | --- |
| DevOps / Platform | Namespace | Shared Kubernetes namespace for the project platform | Repository-native platform design | [`k8s/10-devops-platform-components.yaml`](../k8s/10-devops-platform-components.yaml) | Implemented in `mlops` |
| DevOps / Platform | PostgreSQL | Shared relational database for platform state and application data | `docker-compose.yml` / platform manifest | [`k8s/01-postgres.yaml`](../k8s/01-postgres.yaml) | Implemented in `mlops` |
| DevOps / Platform | MLflow | Experiment tracking and model artifact metadata | Repository-native deployment in this infrastructure repo | [`k8s/02-mlflow.yaml`](../k8s/02-mlflow.yaml) | Implemented in `mlops` |
| DevOps / Platform | Jellyfin | Media application used by the project demo | Repository-native deployment in this infrastructure repo | [`k8s/03-jellyfin.yaml`](../k8s/03-jellyfin.yaml) | Implemented in `mlops` |
| DevOps / Platform | MinIO | Shared object storage platform service used by data and training workflows | `docker-compose.yml` | [`k8s/04-minio.yaml`](../k8s/04-minio.yaml) | Kubernetes manifest prepared |
| DevOps / Platform | MinIO bucket init | Platform-side bucket initialization support for object storage | `docker-compose.yml` | [`k8s/05-minio-init.yaml`](../k8s/05-minio-init.yaml) | Kubernetes manifest prepared |
| DevOps / Platform | Adminer | Shared PostgreSQL inspection UI for debugging and validation | `docker-compose.yml` | [`k8s/06-adminer.yaml`](../k8s/06-adminer.yaml) | Kubernetes manifest prepared |
| Data | Bootstraper | Data engineering bootstrap tasks | `docker-compose.yml` and role-owned Dockerfile | [`k8s/07-data-role-components.yaml`](../k8s/07-data-role-components.yaml) | Planned Kubernetes equivalent |
| Data | Pipeline | Data ingestion / transformation container | `docker-compose.yml` and role-owned Dockerfile | [`k8s/07-data-role-components.yaml`](../k8s/07-data-role-components.yaml) | Planned Kubernetes equivalent |
| Data | API | Application-side API backed by PostgreSQL | `docker-compose.yml` and role-owned Dockerfile | [`k8s/07-data-role-components.yaml`](../k8s/07-data-role-components.yaml) | Planned Kubernetes equivalent |
| Data | Online service | Candidate / online recommendation service backed by PostgreSQL and MinIO | `docker-compose.yml` and role-owned Dockerfile | [`k8s/07-data-role-components.yaml`](../k8s/07-data-role-components.yaml) | Planned Kubernetes equivalent |
| Data | Simulator | Traffic / workload simulation container | `docker-compose.yml` and role-owned Dockerfile | [`k8s/07-data-role-components.yaml`](../k8s/07-data-role-components.yaml) | Planned Kubernetes equivalent |
| Training | Trainer image | Offline model training image with MLflow logging | role-owned training Dockerfile | [`k8s/08-training-role-components.yaml`](../k8s/08-training-role-components.yaml) | Planned Kubernetes Job / CronJob |
| Serving | Baseline server | CPU baseline recommendation API | role-owned serving Dockerfile | [`k8s/09-serving-role-components.yaml`](../k8s/09-serving-role-components.yaml) | Planned Kubernetes Deployment + Service |
| Serving | ONNX / Torch serving variants | Model serving variants owned by serving role | role-owned serving Dockerfiles | [`k8s/09-serving-role-components.yaml`](../k8s/09-serving-role-components.yaml) | Planned Kubernetes Deployment + Service |

## Summary Notes

- This is a draft cross-role inventory, not the final integrated production system inventory.
- Team members are still working independently and may update image names, Dockerfile locations, and runtime details later.
- The current DevOps / Platform deployment already implements the shared namespace and core platform services: PostgreSQL, MLflow, and Jellyfin.
- Data, training, and serving role-owned systems have explicit Kubernetes-equivalent reference manifests documenting how the platform layer will support them.
