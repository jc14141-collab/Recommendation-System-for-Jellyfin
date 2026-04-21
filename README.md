# Recommendation-System-for-Jellyfin

## Infrastructure / DevOps Materials

The DevOps / Platform infrastructure repository materials are in:

- [`infrastructure/README.md`](infrastructure/README.md)
- [`infrastructure/infrastructure-submission.zip`](infrastructure/infrastructure-submission.zip)

This directory contains the Chameleon + K3s/Kubernetes deployment materials, including K3s setup scripts, Kubernetes manifests, deployment notes, resource documentation, and submission-oriented container inventory materials.

Current deployed services covered by the infrastructure materials include PostgreSQL, MLflow, MinIO, Jellyfin, Adminer, data/API services, simulator, online-service components, training-manager, scheduled retraining, serving staging/canary/prod, Prometheus, Grafana, and node-exporter.

For the complete end-to-end deployment command sequence, see [`infrastructure/README.md`](infrastructure/README.md#full-end-to-end-deployment).
