# Local Terminal Launch via python-chi

This note explains how the workflow used in the course notebook `2_create_server_nvidia.ipynb` can be adapted to a local terminal environment.

## Conclusion

Yes, the same overall approach can be used from a local terminal.

The notebook relies on `python-chi`, not on Jupyter alone. The notebook-specific parts are mainly:

- interactive project selection
- interactive site selection
- notebook widgets for progress display

Those parts can be replaced in a local script by explicit arguments and environment-based authentication.

## What the Notebook Method Actually Does

The notebook performs the following high-level actions:

1. select a Chameleon project and site
2. look up an existing active lease
3. retrieve the node reservation ID from that lease
4. create a server attached to that reservation
5. associate a floating IP
6. optionally check SSH connectivity

The local script in `scripts/launch_chameleon_instance.py` follows the same model.

## Local Prerequisites

To use this workflow from a local terminal, the following are required:

- a Python environment with `python-chi` installed
- Chameleon/OpenStack authentication variables available locally
- the correct project name and site name
- an active lease with a node reservation

`python-chi` uses the same authentication environment variables as the OpenStack CLI/OpenRC workflow.

## Recommended Authentication Pattern

Use the same credentials flow that works for your local Chameleon CLI usage, then run the local launch script in that authenticated shell.

For example, after your authentication environment is loaded, run:

```bash
python scripts/launch_chameleon_instance.py \
  --project-name CHI-251409 \
  --site CHI@TACC \
  --lease-name YOUR_LEASE_NAME \
  --server-name YOUR_SERVER_NAME \
  --image-name CC-Ubuntu22.04 \
  --key-name YOUR_KEYPAIR_NAME \
  --network-name fabnetv4
```

## Important Differences from the Notebook

- `context.choose_project()` is a notebook-friendly helper. In local scripts, explicit project arguments are preferable.
- `context.choose_site()` is also notebook-oriented. In local scripts, the site should be passed explicitly.
- The local script prints JSON output instead of notebook widgets.

## Current Limitation to Keep in Mind

This script assumes:

- the lease already exists
- the lease is already active
- the selected project and site match the lease

It automates instance launch and floating IP association after the lease is available; it does not create the lease itself.
