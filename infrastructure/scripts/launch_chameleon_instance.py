#!/usr/bin/env python
"""
Launch a Chameleon instance from an active lease using python-chi.

This script adapts the workflow used in the course notebook
`2_create_server_nvidia.ipynb` into a local terminal-friendly form.

Prerequisites:
- python-chi installed in the local Python environment
- OpenRC or equivalent OpenStack/Chameleon authentication variables available
- correct project/site selected via arguments

Example:
  python scripts/launch_chameleon_instance.py ^
    --project-name CHI-251409 ^
    --site CHI@TACC ^
    --lease-name jc14141-test ^
    --server-name devopsjc14141 ^
    --image-name CC-Ubuntu22.04 ^
    --key-name id_rsa_chameleon ^
    --network-name fabnetv4
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Launch a Chameleon instance from an active lease using python-chi."
    )
    parser.add_argument("--project-name", required=True, help="Chameleon project name.")
    parser.add_argument("--site", required=True, help="Chameleon site, e.g. CHI@TACC.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--lease-name", help="Lease name.")
    group.add_argument("--lease-id", help="Lease UUID.")
    parser.add_argument("--server-name", required=True, help="Desired server name.")
    parser.add_argument(
        "--image-name",
        default="CC-Ubuntu22.04",
        help="Image name to use for the server.",
    )
    parser.add_argument(
        "--flavor-name",
        default="baremetal",
        help="Flavor name to use for the server.",
    )
    parser.add_argument("--key-name", help="Keypair name to inject into the server.")
    parser.add_argument(
        "--network-name",
        default="sharednet1",
        help="Network name to attach to the instance.",
    )
    parser.add_argument(
        "--reservation-index",
        type=int,
        default=0,
        help="Index of the node reservation to use from the lease.",
    )
    parser.add_argument(
        "--project-domain-name",
        default=os.environ.get("OS_PROJECT_DOMAIN_NAME", "chameleon"),
        help="Project domain name for authentication context.",
    )
    parser.add_argument(
        "--skip-connectivity-check",
        action="store_true",
        help="Skip the post-launch TCP connectivity check.",
    )
    return parser


def fail(message: str, exit_code: int = 1) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(exit_code)


def require_module_imports() -> tuple[Any, Any]:
    try:
        import chi
        from chi import context
    except Exception as exc:
        fail(
            "python-chi is not installed in the current environment. "
            "Install it with: pip install python-chi\n"
            f"Import error: {exc}"
        )
    return chi, context


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    chi, context = require_module_imports()
    context.version = "1.0"

    # python-chi local authentication relies on the same environment variables
    # used by the OpenStack CLI/OpenRC workflow.
    chi.set("project_name", args.project_name)
    chi.set("project_domain_name", args.project_domain_name)
    chi.use_site(args.site)

    lease_obj = (
        chi.lease.get_lease(args.lease_name)
        if args.lease_name
        else chi.lease.get_lease(args.lease_id)
    )

    if not lease_obj:
        fail("Unable to resolve lease from the provided lease name or lease ID.")

    if str(lease_obj.status).upper() != "ACTIVE":
        fail(
            f"Lease is not ACTIVE. Current status: {lease_obj.status}. "
            "Wait until the lease start time is reached."
        )

    if not getattr(lease_obj, "node_reservations", None):
        fail("This lease does not include any node reservations.")

    if args.reservation_index >= len(lease_obj.node_reservations):
        fail(
            f"reservation-index {args.reservation_index} is out of range for "
            f"{len(lease_obj.node_reservations)} node reservation(s)."
        )

    reservation_id = lease_obj.node_reservations[args.reservation_index]["id"]

    srv = chi.server.Server(
        args.server_name,
        reservation_id=reservation_id,
        image_name=args.image_name,
        flavor_name=args.flavor_name,
        key_name=args.key_name,
        network_name=args.network_name,
    )

    srv.submit(idempotent=True)
    srv.refresh()

    floating_ip = srv.associate_floating_ip()
    srv.refresh()

    connectivity_ok = None
    if not args.skip_connectivity_check:
        connectivity_ok = srv.check_connectivity(wait=True, show="text")

    result = {
        "server_name": args.server_name,
        "server_id": srv.id,
        "status": srv.status,
        "image_name": args.image_name,
        "flavor_name": args.flavor_name,
        "network_name": args.network_name,
        "reservation_id": reservation_id,
        "floating_ip": floating_ip or srv.get_floating_ip(),
        "connectivity_ok": connectivity_ok,
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
