#!/usr/bin/env python3
"""Read-only GCP readiness check for an also-ran runner acceptance test."""

from __future__ import annotations

import pathlib as _pathlib
import sys as _sys
_sys.path.insert(0, str(_pathlib.Path(__file__).resolve().parents[1] / "controller" / "also-ran"))
from deployment_identity import PROJECT_ID, PROJECT_NUMBER, RESOURCE_PREFIX, FLEET

import argparse
import json
import re
import subprocess
import sys
from typing import Any


class PreflightError(RuntimeError):
    pass


def load_policy(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        policy = json.load(handle)
    validate_policy(policy)
    return policy


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("policy_version") != 1:
        raise PreflightError("unsupported GCP policy version")
    if (
        policy.get("project_id") != PROJECT_ID
        or policy.get("project_number") != PROJECT_NUMBER
    ):
        raise PreflightError("unexpected GCP project boundary")
    if not re.fullmatch(r"[0-9a-f]{40}", policy.get("image_source_commit", "")):
        raise PreflightError("image source commit must be immutable")
    if "/images/" not in policy.get("base_image", "") or "/families/" in policy.get(
        "base_image", ""
    ):
        raise PreflightError("base image must use an exact image name")
    acceptance = policy.get("acceptance", {})
    if (
        acceptance.get("provisioning_model") != "SPOT"
        or acceptance.get("maximum_hosts") != 1
        or acceptance.get("outrunner_max_runners") != 1
        or acceptance.get("automatic_delete") is not False
        or acceptance.get("labels", {}).get("hold") != "true"
    ):
        raise PreflightError("unexpected GCP acceptance safety rails")
    target = policy.get("target", {})
    if (
        target.get("lifecycle") != "broker-created-disposable-spot"
        or target.get("provisioning_model") != "SPOT"
        or target.get("desired_burst_hosts") != 20
        or target.get("vcpus_per_host") != 8
        or target.get("requested_regional_vcpus") < 160
        or target.get("automatic_delete_after_drain") is not True
    ):
        raise PreflightError("unexpected GCP target architecture")
    spend = policy.get("spend", {})
    if (
        spend.get("maximum_acceptance_runtime_minutes", 0) > 60
        or spend.get("maximum_campaign_runtime_minutes", 0) > 60
        or spend.get("maximum_aggregate_hourly_usd", 0) > 5
    ):
        raise PreflightError("GCP acceptance spend ceiling is too broad")
    cidrs = policy.get("network", {}).get("ssh_source_cidrs", [])
    if not cidrs or any(value in {"0.0.0.0/0", "::/0", "*"} for value in cidrs):
        raise PreflightError("GCP SSH sources must be explicit and non-public")


def gcloud(arguments: list[str]) -> Any:
    command = ["gcloud", *arguments, "--quiet", "--format=json"]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        raise PreflightError(
            f"gcloud command failed ({' '.join(command[:4])}): {result.stderr.strip()}"
        )
    return json.loads(result.stdout) if result.stdout.strip() else {}


def region_from_zone(zone: str) -> str:
    parts = zone.rsplit("-", 1)
    if len(parts) != 2 or not parts[1].isalpha() or len(parts[1]) != 1:
        raise PreflightError(f"invalid GCP zone: {zone}")
    return parts[0]


def relevant_quotas(quotas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    wanted = ("CPU", "INSTANCE", "ADDRESS", "DISK", "SSD")
    return [
        {
            "metric": quota.get("metric"),
            "usage": quota.get("usage", 0),
            "limit": quota.get("limit", 0),
        }
        for quota in quotas
        if any(token in str(quota.get("metric", "")).upper() for token in wanted)
    ]


def available(quota: dict[str, Any]) -> float:
    return float(quota.get("limit", 0)) - float(quota.get("usage", 0))


def quota_capacity(
    quotas: list[dict[str, Any]], *, metric: str, units_per_host: int
) -> int | None:
    match = next((quota for quota in quotas if quota.get("metric") == metric), None)
    if match is None:
        return None
    return max(0, int(available(match) // units_per_host))


def spot_quota_capacity(
    quotas: list[dict[str, Any]], *, vcpus_per_host: int
) -> dict[str, Any]:
    dedicated = quota_capacity(
        quotas, metric="PREEMPTIBLE_CPUS", units_per_host=vcpus_per_host
    )
    regular = quota_capacity(quotas, metric="N2_CPUS", units_per_host=vcpus_per_host)
    if dedicated is not None and dedicated > 0:
        return {"hosts": dedicated, "quota_pool": "PREEMPTIBLE_CPUS"}
    return {
        "hosts": regular,
        "quota_pool": "N2_CPUS",
        "note": "Dedicated preemptible quota is not granted; Spot uses standard quota.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--policy", required=True
    )
    parser.add_argument("--project", required=True)
    parser.add_argument("--machine-type", default="n2-standard-8")
    parser.add_argument("--vcpus-per-host", type=int, default=8)
    parser.add_argument(
        "--zone",
        action="append",
        required=True,
        help="Candidate zone; repeat for multiple zones.",
    )
    parser.add_argument("--image-project", default="ubuntu-os-cloud")
    parser.add_argument("--image-family", default="ubuntu-2404-lts-amd64")
    args = parser.parse_args(argv)
    policy = load_policy(args.policy)
    if args.project != PROJECT_ID:
        raise PreflightError("--project must match the trusted deployment identity")

    accounts = gcloud(["auth", "list", "--filter=status:ACTIVE"])
    if not accounts:
        raise PreflightError("no active gcloud identity")

    services = gcloud(
        [
            "services",
            "list",
            f"--project={args.project}",
            "--enabled",
            "--filter=config.name:compute.googleapis.com",
        ]
    )
    project_info = gcloud(
        ["compute", "project-info", "describe", f"--project={args.project}"]
    )
    global_quotas = relevant_quotas(project_info.get("quotas", []))
    global_cpu_capacity = quota_capacity(
        global_quotas,
        metric="CPUS_ALL_REGIONS",
        units_per_host=args.vcpus_per_host,
    )
    image = gcloud(
        [
            "compute",
            "images",
            "describe-from-family",
            args.image_family,
            f"--project={args.image_project}",
        ]
    )

    regions: dict[str, Any] = {}
    zones: list[dict[str, Any]] = []
    for zone in args.zone:
        region = region_from_zone(zone)
        machine = gcloud(
            [
                "compute",
                "machine-types",
                "describe",
                args.machine_type,
                f"--zone={zone}",
                f"--project={args.project}",
            ]
        )
        zones.append(
            {
                "zone": zone,
                "machine_type": machine.get("name"),
                "guest_cpus": machine.get("guestCpus"),
                "memory_mb": machine.get("memoryMb"),
            }
        )
        if region not in regions:
            region_data = gcloud(
                [
                    "compute",
                    "regions",
                    "describe",
                    region,
                    f"--project={args.project}",
                ]
            )
            quotas = relevant_quotas(region_data.get("quotas", []))
            regions[region] = {
                "quotas": quotas,
                "spot_capacity": spot_quota_capacity(
                    quotas, vcpus_per_host=args.vcpus_per_host
                ),
                "regular_host_capacity": quota_capacity(
                    quotas,
                    metric="CPUS",
                    units_per_host=args.vcpus_per_host,
                ),
                "instance_host_capacity": quota_capacity(
                    quotas, metric="INSTANCES", units_per_host=1
                ),
                "ssd_disk_host_capacity": quota_capacity(
                    quotas,
                    metric="SSD_TOTAL_GB",
                    units_per_host=policy["campaign"]["boot_disk_gb"],
                ),
            }
            regional_spot = regions[region]["spot_capacity"].get("hosts")
            limits = [
                regional_spot,
                global_cpu_capacity,
                regions[region]["instance_host_capacity"],
                regions[region]["ssd_disk_host_capacity"],
            ]
            known_limits = [value for value in limits if value is not None]
            if known_limits:
                regions[region]["effective_spot_host_capacity"] = min(known_limits)

    result = {
        "project": args.project,
        "active_account": accounts[0].get("account"),
        "compute_api_enabled": bool(services),
        "global_quotas": global_quotas,
        "global_cpu_host_capacity": global_cpu_capacity,
        "resolved_image": {
            "name": image.get("name"),
            "self_link": image.get("selfLink"),
            "creation_timestamp": image.get("creationTimestamp"),
        },
        "zones": zones,
        "regions": regions,
        "mutation_performed": False,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        raise SystemExit(1)
