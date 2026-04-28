# Formal Custom Jellyfin Deployment

This document describes the final Jellyfin deployment path used by the project.

- frontend: [`Teqqquila/JF-frontend`](https://github.com/Teqqquila/JF-frontend)
- backend: official [`jellyfin/jellyfin`](https://github.com/jellyfin/jellyfin)

This deployment is intentionally separate from the old Kubernetes `k8s/03-jellyfin.yaml` path.

## Deployment entrypoint

Use:

```bash
./scripts/deploy-formal-custom-jellyfin.sh
```

## What the script does

The script:

1. installs Linux dependencies
2. installs or verifies Node.js 24
3. installs the matching `.NET` SDK channel required by the current official Jellyfin backend
4. clones or updates the frontend and backend repositories
5. builds the frontend with `npm ci` and `npm run build:production`
6. prepares `/mnt/block/movies` for the media library
7. stops older hand-run Jellyfin processes from the legacy root-directory workflow if needed
8. installs a `systemd` service named `custom-jellyfin`
9. starts Jellyfin on port `8096`

## Managed directories

The deployment uses separate managed directories so it does not overwrite ad-hoc working copies:

- `~/custom-jellyfin-managed/jellyfin-web`
- `~/custom-jellyfin-managed/jellyfin`

## Service form

This deployment is a host-level `systemd` service, not a Kubernetes Pod.

- service name: `custom-jellyfin.service`

Useful commands:

```bash
sudo systemctl status custom-jellyfin
sudo systemctl restart custom-jellyfin
sudo systemctl stop custom-jellyfin
journalctl -u custom-jellyfin -f
```

## Expected external URL

```text
http://<server-ip>:8096/web/#/home
```
