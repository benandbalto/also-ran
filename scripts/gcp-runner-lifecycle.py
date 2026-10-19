#!/usr/bin/env python3
"""Guarded GCP golden-image and held Spot-runner lifecycle for also-ran."""

from __future__ import annotations

import pathlib as _pathlib
import sys as _sys
_sys.path.insert(0, str(_pathlib.Path(__file__).resolve().parents[1] / "controller" / "also-ran"))
from deployment_identity import PROJECT_ID, PROJECT_NUMBER, RESOURCE_PREFIX, FLEET, valid_legacy_images

import argparse
import base64
import contextlib
import datetime
import ipaddress
import json
import pathlib
import re
import subprocess
import shlex
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

POOL_CONFIG_KEYS = {"heavy": "production", "general": "general_production"}
# IAP TCP forwarding originates here. It is a Google-owned range rather than one
# of ours, so it is named separately from the reviewed SSH sources.
IAP_SSH_CIDRS = ("35.235.240.0/20",)
EGRESS_MODES = ("nat", "direct")


class LifecycleError(RuntimeError):
    pass


def load_policy(path: str, *, require_production_regions: bool = True) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        policy = json.load(handle)
    if not isinstance(policy, dict):
        raise LifecycleError("policy must be a JSON object")
    validate_policy(policy, require_production_regions=require_production_regions)
    return policy


def retention_shape_ok(config: dict[str, Any]) -> bool:
    """The lifecycle helper never reads the warm pool; the broker validates it."""
    if config.get("retention_mode") == "demand_only":
        return "warm_pool" not in config
    return config.get("retention_mode") == "peak_warm_pool" and isinstance(
        config.get("warm_pool"), dict
    )


def validate_policy(policy: dict[str, Any], *, require_production_regions: bool = True) -> None:
    if policy.get("policy_version") != 1:
        raise LifecycleError("unsupported policy version")
    if (
        policy.get("project_id") != PROJECT_ID
        or policy.get("project_number") != PROJECT_NUMBER
    ):
        raise LifecycleError("unexpected GCP project boundary")
    if not re.fullmatch(r"[0-9a-f]{40}", str(policy.get("image_source_commit", ""))):
        raise LifecycleError("image_source_commit must be a full Git commit")
    if not re.fullmatch(
        r"projects/ubuntu-os-cloud/global/images/[a-z0-9-]+",
        str(policy.get("base_image", "")),
    ):
        raise LifecycleError("base image must be an exact ubuntu-os-cloud image")
    if not re.fullmatch(
        f"{RESOURCE_PREFIX}-runner-ubuntu2404-[0-9a-f]{{12}}", str(policy.get("golden_image", ""))
    ):
        raise LifecycleError("golden image name must encode the source commit")
    if not policy["golden_image"].endswith(policy["image_source_commit"][:12]):
        raise LifecycleError("golden image and source commit do not match")
    if policy.get("machine_type") != "n2-standard-8":
        raise LifecycleError("unexpected GCP machine type")

    network = policy.get("network", {})
    if (
        network.get("name") != PROJECT_ID
        or network.get("subnet") != "runner-hosts-us-central1"
        or network.get("subnet_region") != "us-central1"
    ):
        raise LifecycleError("unexpected GCP network boundary")
    cidrs = network.get("ssh_source_cidrs", [])
    if not cidrs or any(value in {"0.0.0.0/0", "::/0", "*"} for value in cidrs):
        raise LifecycleError("SSH sources must be explicit and non-public")
    validate_egress_policy(network, cidrs)

    builder = policy.get("builder", {})
    expected_builder_labels = {
        "managed-by": "also-ran-image-builder",
        "fleet": FLEET,
        "lifecycle": "image-build",
        "provider": "gcp",
    }
    if (
        builder.get("name") != f"{RESOURCE_PREFIX}-runner-image-builder-gcp"
        or builder.get("zone") != "us-central1-c"
        or builder.get("machine_type") != "e2-standard-4"
        or builder.get("boot_disk_gb") != 32
        or builder.get("labels") != expected_builder_labels
    ):
        raise LifecycleError("unexpected GCP image builder")

    acceptance = policy.get("acceptance", {})
    expected_acceptance_labels = {
        "managed-by": "runner-autoscaler",
        "fleet": FLEET,
        "lifecycle": "acceptance",
        "autoscale-slot": "gcp-drain-01",
        "provider": "gcp",
        "hold": "true",
    }
    if (
        acceptance.get("name") != f"{RESOURCE_PREFIX}-runner-acceptance-gcp-drain-01"
        or acceptance.get("zone") != "us-central1-a"
        or acceptance.get("machine_type") != "c4-standard-8-lssd"
        or acceptance.get("boot_disk_gb") != 32
        or acceptance.get("boot_disk_type") != "hyperdisk-balanced"
        or acceptance.get("boot_disk_provisioned_iops") != 3000
        or acceptance.get("boot_disk_provisioned_throughput") != 140
        or acceptance.get("use_local_ssd") is not True
        or acceptance.get("provisioning_model") != "SPOT"
        or acceptance.get("maximum_hosts") != 1
        or acceptance.get("outrunner_max_runners") != 1
        or acceptance.get("automatic_delete") is not False
        or acceptance.get("include_shared_label") is not False
        or acceptance.get("labels") != expected_acceptance_labels
        or acceptance.get("scale_set") != f"{RESOURCE_PREFIX}-gcp-drain-01"
        or acceptance.get("diagnostic_label") != f"{RESOURCE_PREFIX}-gcp-drain-01"
    ):
        raise LifecycleError("unexpected held GCP Spot acceptance slot")

    campaign = policy.get("campaign", {})
    if (
        campaign.get("name_prefix") != f"{RESOURCE_PREFIX}-runner-campaign-gcp"
        or campaign.get("maximum_hosts") != 20
        or campaign.get("boot_disk_gb") != 100
        or campaign.get("include_shared_label") is not False
        or len(campaign.get("zone_cycle", [])) < 2
        or any(
            not zone.startswith("us-central1-")
            for zone in campaign.get("zone_cycle", [])
        )
    ):
        raise LifecycleError("unexpected GCP burst campaign boundary")

    production = policy.get("production", {})
    expected_production_labels = {
        "managed-by": "runner-autoscaler",
        "fleet": FLEET,
        "lifecycle": "burst",
        "provider": "gcp",
        "hold": "true",
    }
    if (
        production.get("name_prefix") != f"{RESOURCE_PREFIX}-runner-gcp-prod"
        or production.get("maximum_hosts") != 28
        or production.get("minimum_hosts") != 0
        or not retention_shape_ok(production)
        or production.get("max_jobs_per_host") != 5
        or production.get("vcpus_per_host") != 8
        or production.get("autoscale_slot_prefix") != "gcp-prod"
        or production.get("boot_disk_gb") != 32
        or production.get("boot_disk_type") != "hyperdisk-balanced"
        or production.get("boot_disk_provisioned_iops") != 3000
        or production.get("boot_disk_provisioned_throughput") != 140
        or production.get("machine_type") != "c4-standard-8-lssd"
        or production.get("use_local_ssd") is not True
        or not valid_legacy_images(production)
        or production.get("scale_set_prefix") != f"{RESOURCE_PREFIX}-gcp-prod"
        or production.get("workflow_label") != "outrunner-gcp-linux-x64"
        or production.get("include_shared_label") is not False
        or production.get("regional_placement_enabled") is not True
        or production.get("create_enabled") is not True
        or not isinstance(production.get("automatic_delete"), bool)
        or production.get("labels") != expected_production_labels
    ):
        raise LifecycleError("unexpected GCP production boundary")
    expected_zone_counts = {
        "northamerica-northeast1-b": 10,
        "europe-west9-a": 10,
        "us-south1-a": 4,
        "us-south1-b": 4,
    }
    zone_cycle = production.get("zone_cycle", [])
    if (
        len(zone_cycle) != production["maximum_hosts"]
        or {zone: zone_cycle.count(zone) for zone in set(zone_cycle)}
        != expected_zone_counts
    ):
        raise LifecycleError("unexpected GCP production regional placement")

    general = policy.get("general_production", {})
    expected_general_labels = {
        "managed-by": "runner-autoscaler",
        "fleet": FLEET,
        "lifecycle": "general",
        "provider": "gcp",
        "hold": "true",
    }
    if (
        general.get("name_prefix") != f"{RESOURCE_PREFIX}-runner-gcp-general"
        or general.get("maximum_hosts") != 12
        or general.get("minimum_hosts") != 0
        or not retention_shape_ok(general)
        or general.get("max_jobs_per_host") != 10
        or general.get("vcpus_per_host") != 4
        or general.get("autoscale_slot_prefix") != "gcp-general"
        or general.get("boot_disk_gb") != 32
        or general.get("boot_disk_type") != "hyperdisk-balanced"
        or general.get("boot_disk_provisioned_iops") != 3000
        or general.get("boot_disk_provisioned_throughput") != 140
        or general.get("machine_type") != "c4-standard-4-lssd"
        or general.get("use_local_ssd") is not True
        or not valid_legacy_images(general)
        or general.get("scale_set_prefix") != f"{RESOURCE_PREFIX}-gcp-general"
        or general.get("workflow_label") != "outrunner-gcp-general-linux-x64"
        or general.get("include_shared_label") is not False
        or general.get("regional_placement_enabled") is not True
        or general.get("create_enabled") is not True
        or not isinstance(general.get("automatic_delete"), bool)
        or general.get("labels") != expected_general_labels
    ):
        raise LifecycleError("unexpected GCP general production boundary")
    expected_general_zone_counts = {
        "northamerica-northeast1-b": 4,
        "europe-west9-a": 4,
        "us-south1-a": 2,
        "us-south1-b": 2,
    }
    zone_cycle = general.get("zone_cycle", [])
    if (
        len(zone_cycle) != general["maximum_hosts"]
        or {zone: zone_cycle.count(zone) for zone in set(zone_cycle)}
        != expected_general_zone_counts
    ):
        raise LifecycleError("unexpected GCP general production regional placement")

    expected_regional_canaries = {
        "maximum_concurrent_hosts": 3,
        "machine_type": "c4-standard-8-lssd",
        "boot_disk_gb": 32,
        "boot_disk_type": "hyperdisk-balanced",
        "boot_disk_provisioned_iops": 3000,
        "boot_disk_provisioned_throughput": 140,
        "use_local_ssd": True,
        "include_shared_label": False,
        "labels": {
            "managed-by": "runner-autoscaler",
            "fleet": FLEET,
            "lifecycle": "regional-canary",
            "provider": "gcp",
            "hold": "true",
        },
        "regions": {
            "europe-west9": {
                "short_name": "paris",
                "zones": ["europe-west9-a"],
                "drain_slots": {"europe-west9-a": "02"},
                "subnet": "runner-hosts-europe-west9",
            },
            "us-south1": {
                "short_name": "dallas",
                "zones": ["us-south1-a", "us-south1-b"],
                "drain_slots": {"us-south1-a": "03", "us-south1-b": "04"},
                "subnet": "runner-hosts-us-south1",
            },
            "northamerica-northeast1": {
                "short_name": "montreal",
                "zones": ["northamerica-northeast1-b"],
                "drain_slots": {"northamerica-northeast1-b": "05"},
                "subnet": "runner-hosts-northamerica-northeast1",
            },
        },
    }
    regional_canaries = policy.get("regional_canaries")
    approved_us_west2 = {
        "short_name": "losangeles",
        "zones": ["us-west2-c"],
        "drain_slots": {"us-west2-c": "06"},
        "subnet": "runner-hosts-us-west2",
    }
    expected_regional_canaries_with_us_west2 = {
        **expected_regional_canaries,
        "regions": {
            **expected_regional_canaries["regions"],
            "us-west2": approved_us_west2,
        },
    }
    expected_regional_canaries_with_jakarta = {
        **expected_regional_canaries_with_us_west2,
        "regions": {
            **expected_regional_canaries_with_us_west2["regions"],
            "asia-southeast2": {
                "short_name": "jakarta",
                "zones": ["asia-southeast2-a"],
                "drain_slots": {"asia-southeast2-a": "09"},
                "subnet": "runner-hosts-asia-southeast2",
            },
        },
    }
    if regional_canaries not in (
        expected_regional_canaries,
        expected_regional_canaries_with_us_west2,
        expected_regional_canaries_with_jakarta,
    ):
        raise LifecycleError("unexpected GCP regional canary boundary")

    market_regions = {
        item.get("name"): item
        for item in policy.get("market", {}).get("regions", [])
        if isinstance(item, dict)
    }
    for region in ("northamerica-northeast1", "europe-west9", "us-south1"):
        config = market_regions.get(region, {})
        if (
            (require_production_regions and config.get("canary_valid") is not True)
            or not isinstance(config.get("zones"), list)
            or not isinstance(config.get("foundation", {}).get("subnet"), str)
        ):
            raise LifecycleError("production region is not validated")

    target = policy.get("target", {})
    if (
        target.get("lifecycle") != "broker-created-disposable-spot"
        or target.get("provisioning_model") != "SPOT"
        or target.get("desired_burst_hosts") != 20
        or target.get("vcpus_per_host") != 8
        or target.get("requested_regional_vcpus") != 160
        or target.get("automatic_delete_after_drain") is not True
    ):
        raise LifecycleError("target must remain broker-created disposable Spot")
    spend = policy.get("spend", {})
    if (
        spend.get("maximum_acceptance_runtime_minutes", 0) > 60
        or spend.get("maximum_campaign_runtime_minutes", 0) > 60
        or spend.get("maximum_aggregate_hourly_usd", 0) > 5
    ):
        raise LifecycleError("GCP acceptance spend ceiling is too broad")


def validate_egress_policy(network: dict[str, Any], ssh_cidrs: list[str]) -> None:
    """Pin how runner VMs reach the internet, and what may reach them back.

    `nat` routes every byte through Cloud NAT, which bills data processing in both
    directions. `direct` gives each Spot VM an ephemeral external address, which
    makes internet ingress free and leaves only metered egress — but it also makes
    the VM addressable, so the mode cannot be selected without also pinning the
    ingress surface it is allowed to expose.
    """
    egress = network.get("egress", {})
    if egress.get("mode") not in EGRESS_MODES:
        raise LifecycleError("egress mode must be one of: " + ", ".join(EGRESS_MODES))
    if egress.get("network_tier") != "PREMIUM":
        raise LifecycleError("egress network tier must be PREMIUM")
    allowlist = egress.get("public_ingress_allowlist")
    if not isinstance(allowlist, list) or sorted(allowlist) != sorted(
        [*ssh_cidrs, *IAP_SSH_CIDRS]
    ):
        raise LifecycleError(
            "public ingress allowlist must equal the reviewed SSH sources plus IAP"
        )


def egress_create_arguments(policy: dict[str, Any], *, spot: bool) -> list[str]:
    """gcloud address arguments for one instance.

    The image builder has always carried an external address and keeps it. Only the
    disposable Spot fleet moves, because it is the fleet that pulls ~935 MB per boot.
    """
    egress = policy["network"]["egress"]
    arguments = [f"--network-tier={egress['network_tier']}"]
    if spot and egress["mode"] == "nat":
        arguments.append("--no-address")
    return arguments


def assert_closed_public_ingress(policy: dict[str, Any]) -> dict[str, Any]:
    """Refuse to hand out public addresses while the runner VPC accepts public ingress.

    With `--no-address` the VPC's ingress rules were unreachable from the internet
    whatever they said, so nothing forced them to be narrow. A direct-egress runner
    answers on a public address, which makes those rules load-bearing for the first
    time. This runs before the first such VM exists rather than after.
    """
    network = policy["network"]
    allowed = [
        ipaddress.ip_network(item)
        for item in network["egress"]["public_ingress_allowlist"]
    ]
    # `firewall-rules list` shows only VPC rules. The effective view is scoped to
    # this network and also carries any hierarchical or network firewall policy, so
    # a policy attached above us cannot open a port this gate never looked at.
    effective = gcloud(
        [
            "compute",
            "networks",
            "get-effective-firewalls",
            network["name"],
            f"--project={policy['project_id']}",
        ]
    )
    if not isinstance(effective, dict):
        raise LifecycleError("effective firewall inventory was not an object")
    owned = effective.get("firewalls")
    if not isinstance(owned, list):
        raise LifecycleError("effective firewall inventory was not an array")
    attached_policies: list[str] = []
    for key, value in effective.items():
        if key == "firewalls" or not value:
            continue
        for item in value if isinstance(value, list) else [value]:
            name = (
                item.get("name") or item.get("displayName")
                if isinstance(item, dict)
                else None
            )
            attached_policies.append(str(name or key))
    attached_policies.sort()
    if attached_policies:
        # Policy rules carry a different match shape. Rather than half-read them,
        # refuse and make someone review the surface by hand. The way out is to
        # extend this gate to evaluate them, or to set egress mode back to "nat"
        # while that happens.
        raise LifecycleError(
            "runner VPC has attached firewall policies that this gate cannot "
            "evaluate, so public runner addresses are unsafe: "
            + ", ".join(attached_policies)
        )
    offenders: list[str] = []
    for rule in owned:
        if rule.get("direction") != "INGRESS" or rule.get("disabled") is True:
            continue
        if not rule.get("allowed"):
            continue
        sources = rule.get("sourceRanges") or []
        if not sources:
            # A rule with neither source ranges nor source identities defaults to
            # the whole internet; one scoped by tag or service account cannot be
            # reached from outside the VPC at all.
            if rule.get("sourceTags") or rule.get("sourceServiceAccounts"):
                continue
            offenders.append(f"{rule.get('name')} (0.0.0.0/0)")
            continue
        for source in sources:
            try:
                candidate = ipaddress.ip_network(source, strict=False)
            except ValueError as error:
                raise LifecycleError(
                    f"firewall rule has an unreadable source range: {rule.get('name')}"
                ) from error
            if candidate.is_private:
                continue
            if any(
                candidate.version == item.version and candidate.subnet_of(item)
                for item in allowed
            ):
                continue
            offenders.append(f"{rule.get('name')} ({source})")
    if offenders:
        raise LifecycleError(
            "runner VPC accepts ingress from outside the reviewed allowlist, so "
            "public runner addresses are unsafe: " + ", ".join(sorted(offenders))
        )
    return {
        "network": network["name"],
        "mode": network["egress"]["mode"],
        "public_ingress_allowlist": list(network["egress"]["public_ingress_allowlist"]),
        "ingress_rules_checked": len(owned),
    }


def require_apply(args: argparse.Namespace) -> None:
    if not args.apply:
        raise LifecycleError("mutation requires --apply")


def gcloud(arguments: list[str], *, allow_missing: bool = False) -> Any:
    command = ["gcloud", *arguments, "--quiet", "--format=json"]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        missing = (
            "was not found" in result.stderr or "could not be found" in result.stderr
        )
        if allow_missing and missing:
            return None
        raise LifecycleError(
            f"gcloud command failed ({' '.join(command[:5])}): {result.stderr.strip()}"
        )
    return json.loads(result.stdout) if result.stdout.strip() else {}


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def label_args(labels: dict[str, str], source_commit: str) -> list[str]:
    values = {**labels, "source-commit": source_commit[:12]}
    return [
        "--labels="
        + ",".join(f"{key}={value}" for key, value in sorted(values.items()))
    ]


def instance(
    policy: dict[str, Any], config: dict[str, Any], *, allow_missing: bool = False
) -> dict[str, Any] | None:
    return gcloud(
        [
            "compute",
            "instances",
            "describe",
            config["name"],
            f"--zone={config['zone']}",
            f"--project={policy['project_id']}",
        ],
        allow_missing=allow_missing,
    )


def refuse_existing(policy: dict[str, Any], config: dict[str, Any]) -> None:
    if instance(policy, config, allow_missing=True) is not None:
        raise LifecycleError(f"instance already exists: {config['name']}")


def expected_labels(config: dict[str, Any], source_commit: str) -> dict[str, str]:
    return {**config["labels"], "source-commit": source_commit[:12]}


def owned_instance(
    policy: dict[str, Any],
    config: dict[str, Any],
    *,
    require_addressing: bool = True,
) -> dict[str, Any]:
    """The full ownership guard: this VM is ours, in the shape policy describes.

    `require_addressing` drops the public-address half of that check, and only
    deletion passes it. Addressing is a statement about a host we intend to keep
    running; a host on its way out fails it for exactly the reason we are
    deleting it. An egress mode switch leaves a fleet whose addressing matches
    the previous mode, in either direction, and asserting here would make those
    hosts unreapable -- they can neither be corrected in place nor replaced.
    Every other ownership check still applies, so this never widens what may be
    deleted.
    """
    vm = instance(policy, config)
    if vm is None:
        raise LifecycleError(f"instance does not exist: {config['name']}")
    expected = config["labels"]
    if any(vm.get("labels", {}).get(key) != value for key, value in expected.items()):
        raise LifecycleError("instance labels do not match policy ownership")
    allowed_source_commits = {policy["image_source_commit"][:12]}
    allowed_source_commits.update(
        commit[:12] for commit in config.get("legacy_image_source_commits", [])
    )
    if vm.get("labels", {}).get("source-commit") not in allowed_source_commits:
        raise LifecycleError("instance labels do not match policy ownership")
    if str(vm.get("zone", "")).rsplit("/", 1)[-1] != config["zone"]:
        raise LifecycleError("instance is outside the approved zone")
    service_accounts = {item.get("email") for item in vm.get("serviceAccounts", [])}
    identity = policy["identities"][
        "builder" if config is policy["builder"] else "runtime"
    ]
    if service_accounts != {identity}:
        raise LifecycleError("instance service account does not match policy")
    disks = vm.get("disks", [])
    boot_disks = [disk for disk in disks if disk.get("boot")]
    scratch_disks = [disk for disk in disks if not disk.get("boot")]
    if len(boot_disks) != 1 or not boot_disks[0].get("autoDelete"):
        raise LifecycleError("instance must have one auto-deleted boot disk")
    if str(boot_disks[0].get("source", "")).rsplit("/", 1)[-1] != config["name"]:
        raise LifecycleError("instance boot disk does not match its owned name")
    if scratch_disks and (
        not config.get("use_local_ssd", False)
        or len(scratch_disks) != 1
        or scratch_disks[0].get("type") != "SCRATCH"
        or scratch_disks[0].get("interface") != "NVME"
        or scratch_disks[0].get("deviceName") != "local-ssd-0"
        or not scratch_disks[0].get("autoDelete")
    ):
        raise LifecycleError("instance has an unexpected non-boot disk")
    subnet = config.get("subnet", policy["network"]["subnet"])
    interfaces = vm.get("networkInterfaces", [])
    if (
        len(interfaces) != 1
        or str(interfaces[0].get("subnetwork", "")).rsplit("/", 1)[-1] != subnet
    ):
        raise LifecycleError("instance network does not match policy")
    if require_addressing and config is not policy.get("builder"):
        assert_policy_addressing(policy, interfaces[0])
    return vm


def assert_policy_addressing(
    policy: dict[str, Any], interface: dict[str, Any], *, require_address: bool = True
) -> None:
    """A runner's address is policy, in both directions.

    In `direct` mode a missing address means the VM is silently back on the NAT
    meter; in `nat` mode an unexpected one means it is reachable from the internet.
    Both are wrong, so both raise. `require_address` is relaxed on the create
    response alone, where the allocated address can lag the returned resource.
    Deletion skips this check entirely -- see `owned_instance`.
    """
    egress = policy["network"]["egress"]
    access = interface.get("accessConfigs") or []
    if egress["mode"] != "direct":
        if access:
            raise LifecycleError("instance carries a public address outside policy")
        return
    if (
        len(access) != 1
        or access[0].get("type") != "ONE_TO_ONE_NAT"
        or access[0].get("networkTier") != egress["network_tier"]
        or (require_address and not access[0].get("natIP"))
    ):
        raise LifecycleError(
            "instance does not carry the policy's direct egress address"
        )


def ssh_metadata_file(public_key_path: str) -> tempfile.NamedTemporaryFile[str]:
    key = pathlib.Path(public_key_path).expanduser().read_text(encoding="utf-8").strip()
    if not re.fullmatch(r"ssh-ed25519 [A-Za-z0-9+/=]+(?: .*)?", key):
        raise LifecycleError("SSH public key must be an Ed25519 public key")
    handle = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8")
    handle.write(f"runner-admin:{key}\n")
    handle.flush()
    return handle


def reaper_startup_file() -> tempfile.NamedTemporaryFile[str]:
    source = pathlib.Path(__file__).with_name("gcp-runner-reaper.sh")
    if not source.exists():
        source = (pathlib.Path(__file__).resolve().parents[1] / "ansible/roles/"
                  "gcp_runner_bootstrap/templates/gcp-runner-reaper.sh.j2")
    rendered = source.read_text().replace("{{ gcp_runner_bootstrap_project_id | quote }}", shlex.quote(PROJECT_ID))
    encoded = base64.b64encode(rendered.encode()).decode("ascii")
    handle = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8")
    handle.write("""#!/usr/bin/env bash
set -euo pipefail
umask 077
mkdir -p /var/lib/outrunner /etc/systemd/system/gcp-runner-reaper.service.d /etc/systemd/system/gcp-runner-reaper.timer.d /etc/systemd/system/gcp-runner-bootstrap.service.d
printf '%s' '""" + encoded + """' | base64 --decode >/usr/local/sbin/gcp-runner-reaper.new
chmod 0755 /usr/local/sbin/gcp-runner-reaper.new
mv /usr/local/sbin/gcp-runner-reaper.new /usr/local/sbin/gcp-runner-reaper
cat >/etc/systemd/system/gcp-runner-reaper.service.d/orphan-recovery.conf <<'EOF'
[Unit]
ConditionPathExists=
[Service]
TimeoutStartSec=5min
ReadWritePaths=/var/lib/outrunner
EOF
cat >/etc/systemd/system/gcp-runner-bootstrap.service.d/orphan-recovery.conf <<'EOF'
[Unit]
ConditionPathExists=!/var/lib/outrunner/admission-closed.json
[Service]
TimeoutStartSec=15min
ExecStartPre=/usr/bin/test ! -e /var/lib/outrunner/admission-closed.json
EOF
# Answer a drain request within ~15s rather than a minute. Images baked
# before this cadence still poll every minute; this drop-in moves them too.
# An empty assignment resets every timer expression, OnBootSec included, so
# the boot trigger is restated; without it the timer would never first fire.
cat >/etc/systemd/system/gcp-runner-reaper.timer.d/cadence.conf <<'EOF'
[Timer]
OnUnitActiveSec=
OnBootSec=1min
OnUnitActiveSec=15s
AccuracySec=1s
EOF
systemctl daemon-reload
systemctl enable --now gcp-runner-reaper.timer
# Re-arm an already running timer on the new cadence.
systemctl restart gcp-runner-reaper.timer
""")
    handle.flush()
    return handle


def create_instance(
    policy: dict[str, Any],
    config: dict[str, Any],
    image: str,
    ssh_key_file: str | None,
    *,
    spot: bool,
) -> dict[str, Any]:
    refuse_existing(policy, config)
    requested_at = utc_now()
    identity_key = "runtime" if spot else "builder"
    metadata = []
    if spot:
        metadata = [
            "--metadata="
            + ",".join(
                [
                    f"runner-scale-set={config['scale_set']}",
                    f"runner-label={config['diagnostic_label']}",
                    f"runner-slot={config['labels']['autoscale-slot']}",
                    f"runner-image-source-commit={policy['image_source_commit'][:12]}",
                    f"runner-max-jobs={config.get('max_jobs_per_host', 1)}",
                    "runner-include-shared-label="
                    + (
                        "enabled"
                        if config.get("include_shared_label", True)
                        else "disabled"
                    ),
                    "runner-use-local-ssd="
                    + ("enabled" if config.get("use_local_ssd", False) else "disabled"),
                    # The drain receipt publisher also writes guest attributes,
                    # the one receipt channel still readable after the guest
                    # powers itself off (shutdown regression).
                    "enable-guest-attributes=TRUE",
                ]
            )
        ]
    ssh_context = (
        ssh_metadata_file(ssh_key_file)
        if ssh_key_file is not None
        else contextlib.nullcontext(None)
    )
    with contextlib.ExitStack() as stack:
        ssh_keys = stack.enter_context(ssh_context)
        startup = stack.enter_context(reaper_startup_file()) if spot else None
        arguments = [
            "compute",
            "instances",
            "create",
            config["name"],
            f"--project={policy['project_id']}",
            f"--zone={config['zone']}",
            f"--machine-type={config.get('machine_type', policy['machine_type'])}",
            f"--image={image}",
            f"--boot-disk-size={config.get('boot_disk_gb', policy['builder']['boot_disk_gb'])}GB",
            f"--boot-disk-type={config.get('boot_disk_type', 'pd-balanced')}",
            "--boot-disk-auto-delete",
            f"--subnet={config.get('subnet', policy['network']['subnet'])}",
            *egress_create_arguments(policy, spot=spot),
            "--tags=runner-admin",
            f"--service-account={policy['identities'][identity_key]}",
            "--scopes=https://www.googleapis.com/auth/cloud-platform",
            "--shielded-secure-boot",
            "--shielded-vtpm",
            "--shielded-integrity-monitoring",
            *metadata,
            *label_args(config["labels"], policy["image_source_commit"]),
        ]
        if "boot_disk_provisioned_iops" in config:
            arguments.append(
                "--boot-disk-provisioned-iops="
                + str(config["boot_disk_provisioned_iops"])
            )
        if "boot_disk_provisioned_throughput" in config:
            arguments.append(
                "--boot-disk-provisioned-throughput="
                + str(config["boot_disk_provisioned_throughput"])
            )
        file_metadata = []
        if ssh_keys is not None:
            file_metadata.append(f"ssh-keys={ssh_keys.name}")
        if startup is not None:
            file_metadata.append(f"startup-script={startup.name}")
        if file_metadata:
            arguments.append("--metadata-from-file=" + ",".join(file_metadata))
        if spot:
            arguments.extend(
                [
                    "--provisioning-model=SPOT",
                    "--instance-termination-action=DELETE",
                    "--maintenance-policy=TERMINATE",
                ]
            )
        created = gcloud(arguments)
    vm = created[0] if isinstance(created, list) else created
    if spot:
        assert_policy_addressing(
            policy,
            (vm.get("networkInterfaces") or [{}])[0],
            require_address=False,
        )
    return {
        "id": str(vm.get("id", "")),
        "name": config["name"],
        "requested_at": requested_at,
        "created_at": vm.get("creationTimestamp"),
        "api_completed_at": utc_now(),
        "status": vm.get("status"),
        "spot": spot,
        "public_ip": (
            (vm.get("networkInterfaces") or [{}])[0].get("accessConfigs") or [{}]
        )[0].get("natIP"),
    }


def approved_image(policy: dict[str, Any]) -> dict[str, Any]:
    image = gcloud(
        [
            "compute",
            "images",
            "describe",
            policy["golden_image"],
            f"--project={policy['project_id']}",
        ]
    )
    expected = {
        "managed-by": "runner-autoscaler",
        "image-purpose": f"{RESOURCE_PREFIX}-runner",
        "credential-free": "true",
        "source-commit": policy["image_source_commit"][:12],
    }
    if any(
        image.get("labels", {}).get(key) != value for key, value in expected.items()
    ):
        raise LifecycleError("golden image labels do not match approved inputs")
    return image


def campaign_config(policy: dict[str, Any], slot: int) -> dict[str, Any]:
    campaign = policy["campaign"]
    if slot < 1 or slot > campaign["maximum_hosts"]:
        raise LifecycleError("campaign slot is outside policy")
    suffix = f"{slot:02d}"
    return {
        "name": f"{campaign['name_prefix']}-{suffix}",
        "zone": campaign["zone_cycle"][(slot - 1) % len(campaign["zone_cycle"])],
        "boot_disk_gb": campaign["boot_disk_gb"],
        "scale_set": f"{campaign['scale_set_prefix']}-{suffix}",
        "diagnostic_label": f"{campaign['diagnostic_label_prefix']}-{suffix}",
        "include_shared_label": campaign["include_shared_label"],
        "labels": {
            **campaign["labels"],
            "autoscale-slot": f"campaign-{suffix}",
        },
    }


def production_region(policy: dict[str, Any], zone: str) -> dict[str, Any]:
    matches = [
        item
        for item in policy.get("market", {}).get("regions", [])
        if isinstance(item, dict) and zone in item.get("zones", [])
    ]
    if len(matches) != 1 or matches[0].get("canary_valid") is not True:
        raise LifecycleError("production zone is outside validated market policy")
    return matches[0]


def production_config(
    policy: dict[str, Any],
    slot: int,
    *,
    pool: str = "heavy",
    zone: str | None = None,
) -> dict[str, Any]:
    try:
        production = policy[POOL_CONFIG_KEYS[pool]]
    except KeyError as error:
        raise LifecycleError(f"unexpected GCP pool: {pool!r}") from error
    if slot < 1 or slot > production["maximum_hosts"]:
        raise LifecycleError("production slot is outside policy")
    suffix = f"{slot:02d}"
    selected_zone = zone or production["zone_cycle"][slot - 1]
    region = production_region(policy, selected_zone)
    return {
        "name": f"{production['name_prefix']}-{suffix}",
        "zone": selected_zone,
        "subnet": region["foundation"]["subnet"],
        "boot_disk_gb": production["boot_disk_gb"],
        "boot_disk_type": production["boot_disk_type"],
        "boot_disk_provisioned_iops": production["boot_disk_provisioned_iops"],
        "boot_disk_provisioned_throughput": production[
            "boot_disk_provisioned_throughput"
        ],
        "machine_type": production["machine_type"],
        "use_local_ssd": production["use_local_ssd"],
        "legacy_image_source_commits": production["legacy_image_source_commits"],
        "max_jobs_per_host": production["max_jobs_per_host"],
        "scale_set": f"{production['scale_set_prefix']}-{suffix}",
        "diagnostic_label": production["workflow_label"],
        "include_shared_label": production["include_shared_label"],
        "labels": {
            **production["labels"],
            "autoscale-slot": (f"{production['autoscale_slot_prefix']}-{suffix}"),
        },
    }


def regional_canary_config(
    policy: dict[str, Any], region: str, zone: str
) -> dict[str, Any]:
    canaries = policy["regional_canaries"]
    region_config = canaries["regions"].get(region)
    if region_config is None or zone not in region_config["zones"]:
        raise LifecycleError("regional canary zone is outside policy")
    zone_suffix = zone.rsplit("-", 1)[-1]
    short_name = region_config["short_name"]
    suffix = f"{short_name}-{zone_suffix}"
    drain_slot = region_config["drain_slots"][zone]
    return {
        "name": f"{RESOURCE_PREFIX}-runner-gcp-canary-{suffix}",
        "zone": zone,
        "subnet": region_config["subnet"],
        "boot_disk_gb": canaries["boot_disk_gb"],
        "boot_disk_type": canaries["boot_disk_type"],
        "boot_disk_provisioned_iops": canaries["boot_disk_provisioned_iops"],
        "boot_disk_provisioned_throughput": canaries[
            "boot_disk_provisioned_throughput"
        ],
        "machine_type": canaries["machine_type"],
        "use_local_ssd": canaries["use_local_ssd"],
        "legacy_image_source_commits": [],
        "scale_set": f"{RESOURCE_PREFIX}-gcp-canary-{suffix}",
        "diagnostic_label": f"{RESOURCE_PREFIX}-gcp-canary-{suffix}",
        "include_shared_label": canaries["include_shared_label"],
        "labels": {
            **canaries["labels"],
            "autoscale-slot": f"gcp-drain-{drain_slot}",
            "canary-region": region,
        },
    }


def create_regional_canary(
    policy: dict[str, Any], config: dict[str, Any], image: str
) -> dict[str, Any]:
    canaries = policy["regional_canaries"]
    payload = gcloud(
        [
            "compute",
            "instances",
            "list",
            f"--project={policy['project_id']}",
            "--filter=labels.lifecycle=regional-canary",
        ]
    )
    if not isinstance(payload, list):
        raise LifecycleError("regional canary inventory was not an array")
    allowed_names = {
        regional_canary_config(policy, region, zone)["name"]
        for region, item in canaries["regions"].items()
        for zone in item["zones"]
    }
    expected_source = policy["image_source_commit"][:12]
    for vm in payload:
        if (
            vm.get("name") not in allowed_names
            or vm.get("labels", {}).get("managed-by") != "runner-autoscaler"
            or vm.get("labels", {}).get("fleet") != FLEET
            or vm.get("labels", {}).get("provider") != "gcp"
            or vm.get("labels", {}).get("hold") != "true"
            or vm.get("labels", {}).get("source-commit") != expected_source
        ):
            raise LifecycleError("unexpected regional canary inventory")
    if len(payload) >= canaries["maximum_concurrent_hosts"]:
        raise LifecycleError("regional canary concurrency limit reached")
    target_region = config["labels"]["canary-region"]
    if any(
        vm.get("labels", {}).get("canary-region") == target_region for vm in payload
    ):
        raise LifecycleError("regional canary already exists in target region")
    return create_instance(policy, config, image, None, spot=True)


def parse_slots(value: str, maximum: int) -> list[int]:
    try:
        slots = [int(item) for item in value.split(",") if item]
    except ValueError as error:
        raise LifecycleError("slots must be comma-separated integers") from error
    if not slots or len(slots) != len(set(slots)):
        raise LifecycleError("slots must be a non-empty unique list")
    if any(slot < 1 or slot > maximum for slot in slots):
        raise LifecycleError("production slot is outside policy")
    return sorted(slots)


def run_parallel(slots: list[int], function: Any) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=len(slots)) as executor:
        futures = {executor.submit(function, slot): slot for slot in slots}
        for future in as_completed(futures):
            slot = futures[future]
            try:
                results.append({"slot": slot, "ok": True, "result": future.result()})
            except Exception as error:
                results.append({"slot": slot, "ok": False, "error": str(error)})
    return sorted(results, key=lambda item: item["slot"])


def ensure_private_egress(policy: dict[str, Any]) -> dict[str, Any]:
    network = policy["network"]
    project = policy["project_id"]
    region = network["subnet_region"]
    router = gcloud(
        [
            "compute",
            "routers",
            "describe",
            network["router"],
            f"--region={region}",
            f"--project={project}",
        ],
        allow_missing=True,
    )
    if router is None:
        gcloud(
            [
                "compute",
                "routers",
                "create",
                network["router"],
                f"--network={network['name']}",
                f"--region={region}",
                f"--project={project}",
            ]
        )
    nats = gcloud(
        [
            "compute",
            "routers",
            "nats",
            "list",
            f"--router={network['router']}",
            f"--router-region={region}",
            f"--project={project}",
        ]
    )
    if not any(item.get("name") == network["nat"] for item in nats):
        gcloud(
            [
                "compute",
                "routers",
                "nats",
                "create",
                network["nat"],
                f"--router={network['router']}",
                f"--region={region}",
                "--auto-allocate-nat-external-ips",
                "--nat-all-subnet-ip-ranges",
                "--enable-logging",
                "--log-filter=ERRORS_ONLY",
                f"--project={project}",
            ]
        )
    firewall = gcloud(
        [
            "compute",
            "firewall-rules",
            "describe",
            network["iap_ssh_firewall"],
            f"--project={project}",
        ],
        allow_missing=True,
    )
    if firewall is None:
        gcloud(
            [
                "compute",
                "firewall-rules",
                "create",
                network["iap_ssh_firewall"],
                f"--network={network['name']}",
                "--direction=INGRESS",
                "--action=ALLOW",
                "--rules=tcp:22",
                "--source-ranges=35.235.240.0/20",
                "--target-tags=runner-admin",
                f"--project={project}",
            ]
        )
    return {
        "router": network["router"],
        "nat": network["nat"],
        "iap_ssh_firewall": network["iap_ssh_firewall"],
    }


def delete_owned_instance(
    policy: dict[str, Any],
    config: dict[str, Any],
    *,
    expected_instance_id: str | None = None,
) -> dict[str, Any]:
    first = owned_instance(policy, config, require_addressing=False)
    instance_id = str(first.get("id", ""))
    if not instance_id.isdigit():
        raise LifecycleError("instance numeric ID is missing")
    if expected_instance_id is not None and instance_id != expected_instance_id:
        raise LifecycleError("instance numeric ID does not match drain receipt")
    second = owned_instance(policy, config, require_addressing=False)
    if str(second.get("id", "")) != instance_id:
        raise LifecycleError("instance identity changed during deletion guard")
    delete_requested_at = utc_now()
    gcloud(
        [
            "compute",
            "instances",
            "delete",
            config["name"],
            f"--zone={config['zone']}",
            "--delete-disks=all",
            f"--project={policy['project_id']}",
        ]
    )
    delete_completed_at = utc_now()
    return {
        "deleted": config["name"],
        "instance_id": instance_id,
        "delete_requested_at": delete_requested_at,
        "delete_completed_at": delete_completed_at,
    }


def parse_expected_instance_ids(value: str, slots: list[int]) -> dict[int, str]:
    result: dict[int, str] = {}
    for item in value.split(","):
        parts = item.split("=", 1)
        if len(parts) != 2:
            raise LifecycleError(
                "expected instance IDs must use SLOT=NUMERIC_ID entries"
            )
        try:
            slot = int(parts[0])
        except ValueError as error:
            raise LifecycleError(
                "expected instance ID slot must be an integer"
            ) from error
        instance_id = parts[1]
        if slot in result or not instance_id.isdigit():
            raise LifecycleError("expected instance IDs must be unique and numeric")
        result[slot] = instance_id
    if set(result) != set(slots):
        raise LifecycleError("expected instance IDs must exactly cover delete slots")
    return result


def parse_slot_zones(value: str, slots: list[int]) -> dict[int, str]:
    result: dict[int, str] = {}
    for item in value.split(","):
        parts = item.split("=", 1)
        if len(parts) != 2:
            raise LifecycleError("slot zones must use SLOT=ZONE entries")
        try:
            slot = int(parts[0])
        except ValueError as error:
            raise LifecycleError("slot zone slot must be an integer") from error
        zone = parts[1]
        if slot in result or not re.fullmatch(r"[a-z0-9-]+-[a-z]", zone):
            raise LifecycleError("slot zones must be unique valid zones")
        result[slot] = zone
    if set(result) != set(slots):
        raise LifecycleError("slot zones must exactly cover production slots")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--policy", required=True
    )
    parser.add_argument("--ssh-key-file", default="~/.ssh/id_ed25519.pub")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status")
    for name in (
        "create-builder",
        "stop-builder",
        "capture-image",
        "delete-builder",
        "create-acceptance",
        "delete-acceptance",
        "foundation-private-egress",
        "egress-posture",
        "create-campaign",
        "delete-campaign",
        "create-production",
        "delete-production",
        "create-regional-canary",
        "delete-regional-canary",
    ):
        child = subparsers.add_parser(name)
        child.add_argument("--apply", action="store_true")
        if name in {"create-campaign", "delete-campaign"}:
            child.add_argument("--count", type=int, required=True)
        if name in {"create-production", "delete-production"}:
            child.add_argument(
                "--pool", choices=sorted(POOL_CONFIG_KEYS), default="heavy"
            )
            child.add_argument("--slots", required=True)
            child.add_argument("--slot-zones", required=True)
        if name == "delete-production":
            child.add_argument("--expected-instance-ids", required=True)
        if name in {"create-regional-canary", "delete-regional-canary"}:
            child.add_argument("--region", required=True)
            child.add_argument("--zone", required=True)
        if name == "delete-regional-canary":
            child.add_argument("--expected-instance-id", required=True)
    args = parser.parse_args(argv)
    policy = load_policy(args.policy, require_production_regions=args.command in {"create-production", "delete-production", "create-campaign", "delete-campaign"})

    # Reading the firewall costs one API call per broker batch, and it is the only
    # thing standing between a policy edit and a fleet of publicly reachable VMs.
    if args.command.startswith("create-") and (
        policy["network"]["egress"]["mode"] == "direct"
    ):
        assert_closed_public_ingress(policy)

    if args.command == "status":
        result = gcloud(
            [
                "compute",
                "instances",
                "list",
                f"--project={policy['project_id']}",
                f"--filter=labels.fleet={FLEET}",
            ]
        )
    elif args.command == "create-builder":
        require_apply(args)
        result = create_instance(
            policy,
            policy["builder"],
            policy["base_image"],
            args.ssh_key_file,
            spot=False,
        )
    elif args.command == "stop-builder":
        require_apply(args)
        owned_instance(policy, policy["builder"])
        result = gcloud(
            [
                "compute",
                "instances",
                "stop",
                policy["builder"]["name"],
                f"--zone={policy['builder']['zone']}",
                f"--project={policy['project_id']}",
            ]
        )
    elif args.command == "capture-image":
        require_apply(args)
        vm = owned_instance(policy, policy["builder"])
        if vm.get("status") != "TERMINATED":
            raise LifecycleError("builder must be stopped before image capture")
        existing = gcloud(
            [
                "compute",
                "images",
                "describe",
                policy["golden_image"],
                f"--project={policy['project_id']}",
            ],
            allow_missing=True,
        )
        if existing is not None:
            raise LifecycleError(
                f"golden image already exists: {policy['golden_image']}"
            )
        result = gcloud(
            [
                "compute",
                "images",
                "create",
                policy["golden_image"],
                f"--project={policy['project_id']}",
                f"--source-disk={policy['builder']['name']}",
                f"--source-disk-zone={policy['builder']['zone']}",
                "--storage-location=us",
                *label_args(
                    {
                        "managed-by": "runner-autoscaler",
                        "image-purpose": f"{RESOURCE_PREFIX}-runner",
                        "credential-free": "true",
                    },
                    policy["image_source_commit"],
                ),
            ]
        )
    elif args.command == "delete-builder":
        require_apply(args)
        result = delete_owned_instance(policy, policy["builder"])
    elif args.command == "create-acceptance":
        require_apply(args)
        image = approved_image(policy)
        result = create_instance(
            policy,
            policy["acceptance"],
            str(image["selfLink"]),
            args.ssh_key_file,
            spot=True,
        )
    elif args.command == "delete-acceptance":
        require_apply(args)
        result = delete_owned_instance(policy, policy["acceptance"])
    elif args.command == "egress-posture":
        result = assert_closed_public_ingress(policy)
    elif args.command == "foundation-private-egress":
        require_apply(args)
        result = ensure_private_egress(policy)
    elif args.command in {"create-regional-canary", "delete-regional-canary"}:
        require_apply(args)
        config = regional_canary_config(policy, args.region, args.zone)
        if args.command == "create-regional-canary":
            image = approved_image(policy)
            result = create_regional_canary(policy, config, str(image["selfLink"]))
        else:
            if not args.expected_instance_id.isdigit():
                raise LifecycleError("expected instance ID must be numeric")
            result = delete_owned_instance(
                policy,
                config,
                expected_instance_id=args.expected_instance_id,
            )
    elif args.command == "create-campaign":
        require_apply(args)
        if args.count < 1 or args.count > policy["campaign"]["maximum_hosts"]:
            raise LifecycleError("campaign count is outside policy")
        image = approved_image(policy)
        result = run_parallel(
            list(range(1, args.count + 1)),
            lambda slot: create_instance(
                policy,
                campaign_config(policy, slot),
                str(image["selfLink"]),
                args.ssh_key_file,
                spot=True,
            ),
        )
    elif args.command == "delete-campaign":
        require_apply(args)
        if args.count < 1 or args.count > policy["campaign"]["maximum_hosts"]:
            raise LifecycleError("campaign count is outside policy")
        result = run_parallel(
            list(range(1, args.count + 1)),
            lambda slot: delete_owned_instance(policy, campaign_config(policy, slot)),
        )
    elif args.command in {"create-production", "delete-production"}:
        require_apply(args)
        production = policy[POOL_CONFIG_KEYS[args.pool]]
        slots = parse_slots(args.slots, production["maximum_hosts"])
        slot_zones = parse_slot_zones(args.slot_zones, slots)
        configs = {
            slot: production_config(policy, slot, pool=args.pool, zone=slot_zones[slot])
            for slot in slots
        }
        if args.command == "create-production":
            image = approved_image(policy)
            result = run_parallel(
                slots,
                lambda slot: create_instance(
                    policy,
                    configs[slot],
                    str(image["selfLink"]),
                    None,
                    spot=True,
                ),
            )
        else:
            expected_ids = parse_expected_instance_ids(
                args.expected_instance_ids, slots
            )
            result = run_parallel(
                slots,
                lambda slot: delete_owned_instance(
                    policy,
                    configs[slot],
                    expected_instance_id=expected_ids[slot],
                ),
            )
    else:
        raise LifecycleError(f"unsupported command: {args.command}")

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        raise SystemExit(1)
