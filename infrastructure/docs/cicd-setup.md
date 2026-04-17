# CI/CD Setup Notes

This repository uses GitHub Actions as the first-stage CI/CD mechanism for the integrated system.

## Workflows

- `ci.yml`
  Validates Python syntax and YAML syntax on every push and pull request.
- `platform-deploy.yml`
  Applies the shared Kubernetes platform manifests and waits for rollouts.
- `serving-ci-cd.yml`
  Intended to build a serving image, push it to GHCR, and deploy it to Kubernetes.
- `training-ci.yml`
  Intended to build a training image, push it to GHCR, and trigger a Kubernetes training job.

## Required GitHub Secrets

- `KUBECONFIG_B64`
  Base64-encoded kubeconfig for the target K3s cluster.
- `GHCR_USERNAME`
  GitHub username used to push images to GHCR.
- `GHCR_TOKEN`
  GitHub token or PAT with package write permissions.

## Current Activation State

`platform-deploy.yml` is immediately usable because this repository already contains the shared infrastructure manifests under `infrastructure/k8s/`.

`serving-ci-cd.yml` and `training-ci.yml` are scaffolded to support the next integration stage. They intentionally skip themselves until the repository contains:

- `serving/onnx/Dockerfile`
- `infrastructure/k8s/09-serving-onnx.yaml`
- `training/Dockerfile`
- `infrastructure/k8s/08-training-job.yaml`

This lets the team commit the CI/CD skeleton now, while progressively moving role-owned components into the integrated repository.
