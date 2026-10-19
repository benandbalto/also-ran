#!/usr/bin/env python3
"""Generate a credential-free deployment directory; never contact a provider."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--project-number", required=True)
    parser.add_argument("--resource-prefix", default="also-ran")
    parser.add_argument("--fleet", default="also-ran-ci")
    parser.add_argument("--github-repository", action="append", required=True)
    parser.add_argument("--image-source-commit", required=True)
    parser.add_argument("--ssh-source-cidr", action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--warm-pool", action="store_true", help="opt into the example peak-hours warm-pool schedule; review its cost before activation")
    parser.add_argument("--enable-telemetry", action="store_true", help="opt into guest Axiom telemetry; requires an ingest secret")
    args = parser.parse_args(argv)
    identity = {key: getattr(args, key) for key in ("project_id", "project_number", "resource_prefix", "fleet")}
    if not re.fullmatch(r"[0-9a-f]{40}", args.image_source_commit):
        parser.error("--image-source-commit must be a full immutable Git commit")
    repos = args.github_repository
    if any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}/[A-Za-z0-9._-]{1,100}", value) for value in repos):
        parser.error("repositories must have the form owner/repository")
    owners = {value.split("/")[0].lower() for value in repos}
    if len(owners) != 1 or len({value.lower() for value in repos}) != len(repos):
        parser.error("use unique repositories belonging to one GitHub organization")
    cidrs = []
    for value in args.ssh_source_cidr:
        try:
            network = ipaddress.ip_network(value, strict=True)
        except ValueError:
            parser.error(f"invalid SSH source CIDR: {value}")
        if network.version != 4 or network.prefixlen == 0:
            parser.error("SSH sources must be explicit IPv4 networks, never 0.0.0.0/0")
        cidrs.append(str(network))
    output = args.output_dir.resolve()
    # Refuse overwrites, including attempts to use an existing symlink.
    if args.output_dir.exists() or args.output_dir.is_symlink():
        parser.error("output directory must not already exist")
    # Validate before creating any output.
    for key, pattern in {
        "project_id": r"[a-z][a-z0-9-]{4,28}[a-z0-9]",
        "project_number": r"[1-9][0-9]{5,19}",
        "resource_prefix": r"[a-z][a-z0-9-]{0,19}[a-z0-9]",
        "fleet": r"[a-z][a-z0-9-]{0,61}[a-z0-9]",
    }.items():
        if not re.fullmatch(pattern, identity[key]):
            parser.error(f"invalid {key}")

    template = json.loads((ROOT / "controller/also-ran/gcp-acceptance-policy.json").read_text())
    previous_commit = template["image_source_commit"]

    def render(value):
        if isinstance(value, dict):
            return {key: render(item) for key, item in value.items()}
        if isinstance(value, list):
            return [render(item) for item in value]
        if not isinstance(value, str):
            return value
        replacements = {
            previous_commit: args.image_source_commit,
            previous_commit[:12]: args.image_source_commit[:12],
            "also-ran-ci-runners": args.project_id,
            "123456789012": args.project_number,
            "also-ran-ci": args.fleet,
            "also-ran-": args.resource_prefix + "-",
        }
        pattern = "|".join(re.escape(key) for key in sorted(replacements, key=len, reverse=True))
        return re.sub(pattern, lambda match: replacements[match.group()], value)


    policy = render(template)
    # These values are exact identities, not strings to interpolate recursively.
    policy["project_id"] = args.project_id
    policy["project_number"] = args.project_number
    policy["github_repositories"] = repos
    policy["builder"]["labels"]["managed-by"] = "also-ran-image-builder"
    policy["network"]["ssh_source_cidrs"] = cidrs
    policy["network"]["egress"]["public_ingress_allowlist"] = sorted(set(cidrs + ["35.235.240.0/20"]))
    for key in ("production", "general_production"):
        policy[key]["labels"]["fleet"] = args.fleet
        policy[key]["automatic_delete"] = False
        policy[key]["legacy_image_source_commits"] = []
        if not args.warm_pool:
            policy[key]["retention_mode"] = "demand_only"
            policy[key].pop("warm_pool", None)
    for region in policy["market"]["regions"]:
        region["canary_valid"] = False
    if not args.enable_telemetry:
        policy["secrets"] = [name for name in policy.get("secrets", []) if name != "axiom-api-token"]
    image_vars = json.loads((ROOT / "examples/image-vars.json").read_text())
    image_vars.update({
        "gcp_runner_bootstrap_project_id": args.project_id,
        "gcp_runner_bootstrap_project_number": args.project_number,
        "outrunner_github_url": "https://github.com/" + repos[0].split("/")[0],
        "telemetry_fleet_name": args.fleet,
        "telemetry_enabled": args.enable_telemetry,
        "runner_image_finalize_source_commit": args.image_source_commit,
    })
    files = {
        "deployment.json": identity,
        "gcp-policy.json": policy,
        "image-vars.json": image_vars,
        "controller-vars.json": {
            "also_ran_deployment_source": str(output / "deployment.json"),
            "also_ran_gcp_policy_source": str(output / "gcp-policy.json"),
            "also_ran_gcp_broker_enabled": False,
            "also_ran_gcp_broker_apply": False,
            "also_ran_gcp_general_broker_apply": False,
            "also_ran_github_app_id": "",
            "also_ran_github_app_installation_id": "",
            "also_ran_github_app_key_source": "",
            "also_ran_github_credential_source": "",
            "also_ran_gcp_auth_mode": "attached_service_account",
            "also_ran_gcp_controller_credential_source": "",
        },
    }
    output.mkdir(mode=0o700)
    for name, content in files.items():
        with open(output / name, "x", encoding="utf-8", opener=lambda path, flags: os.open(path, flags, 0o600)) as handle:
            handle.write(json.dumps(content, indent=2) + "\n")
    print(f"Generated credential-free configuration in {output}; provider mutations remain disabled.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
