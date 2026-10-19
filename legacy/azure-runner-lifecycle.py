#!/usr/bin/env python3
"""Guarded Azure image-builder and held Spot-runner lifecycle for also-ran."""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys
from typing import Any


class LifecycleError(RuntimeError):
    pass


def load_policy(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        policy = json.load(handle)
    if not isinstance(policy, dict):
        raise LifecycleError("policy must be a JSON object")
    validate_policy(policy)
    return policy


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("policy_version") != 1:
        raise LifecycleError("unsupported policy version")
    if policy.get("resource_group") != "also-ran-ci-runners":
        raise LifecycleError("unexpected resource group")
    if policy.get("location") != "eastus":
        raise LifecycleError("unexpected Azure location")
    if not re.fullmatch(r"[0-9a-f]{40}", str(policy.get("image_source_commit", ""))):
        raise LifecycleError("image_source_commit must be a full Git commit")
    if policy.get("builder", {}).get("name") != "also-ran-runner-image-builder-azure":
        raise LifecycleError("unexpected builder name")
    if (
        policy.get("builder", {}).get("public_ip_name")
        != "also-ran-runner-image-builder-azurePublicIP"
    ):
        raise LifecycleError("unexpected builder public IP name")
    if (
        policy.get("builder", {}).get("nic_name")
        != "also-ran-runner-image-builder-azureVMNic"
    ):
        raise LifecycleError("unexpected builder NIC name")
    if policy.get("burst", {}).get("name") != "also-ran-runner-burst-azure-01":
        raise LifecycleError("unexpected burst name")
    if (
        policy.get("burst", {}).get("public_ip_name")
        != "also-ran-runner-burst-azure-01PublicIP"
    ):
        raise LifecycleError("unexpected burst public IP name")
    if policy.get("burst", {}).get("nic_name") != "also-ran-runner-burst-azure-01VMNic":
        raise LifecycleError("unexpected burst NIC name")
    rollout = policy.get("rollout", {})
    if (
        rollout.get("acceptance_region") != policy["location"]
        or rollout.get("acceptance_size") != policy["burst"].get("size")
        or rollout.get("maximum_hosts") != 1
        or rollout.get("outrunner_max_runners") != 1
        or rollout.get("automatic_delete") is not False
    ):
        raise LifecycleError("unexpected Phase 1 acceptance safety rails")
    target = policy.get("target", {})
    if (
        target.get("lifecycle") != "broker-created-disposable-spot"
        or target.get("size") != "Standard_F8as_v6"
        or target.get("automatic_delete_after_drain") is not True
        or policy["location"] not in target.get("candidate_regions", [])
    ):
        raise LifecycleError("target must remain region-selecting disposable Spot")
    expected = {
        "managed-by": "runner-autoscaler",
        "fleet": "also-ran-ci",
        "lifecycle": "burst",
        "autoscale-slot": "burst-01",
        "provider": "azure",
        "hold": "true",
    }
    if policy["burst"].get("tags") != expected:
        raise LifecycleError("burst tags do not match the held Azure slot")
    paid = policy.get("paid_acceptance", {})
    expected_paid = {
        "managed-by": "runner-autoscaler",
        "fleet": "also-ran-ci",
        "lifecycle": "acceptance",
        "autoscale-slot": "paid-acceptance-01",
        "provider": "azure",
        "billing": "standard",
        "hold": "true",
    }
    if (
        paid.get("name") != "also-ran-runner-paid-azure-01"
        or paid.get("nic_name") != "also-ran-runner-paid-azure-01VMNic"
        or paid.get("public_ip_name") != "also-ran-runner-paid-azure-01PublicIP"
        or paid.get("location") not in target.get("candidate_regions", [])
        or paid.get("size") != "Standard_F8s_v2"
        or paid.get("tags") != expected_paid
    ):
        raise LifecycleError("unexpected paid acceptance slot")
    for network in (policy.get("network", {}), paid.get("network", {})):
        cidrs = network.get("ssh_source_cidrs", [])
        if not cidrs or any(
            value in {"0.0.0.0/0", "::/0", "*"} for value in cidrs
        ):
            raise LifecycleError("SSH sources must be explicit and non-public")


def require_apply(args: argparse.Namespace) -> None:
    if not args.apply:
        raise LifecycleError("mutation requires --apply")


def az(arguments: list[str], *, allow_missing: bool = False) -> Any:
    command = ["az", *arguments, "--only-show-errors", "-o", "json"]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        if allow_missing and "NotFound" in result.stderr:
            return None
        raise LifecycleError(
            f"Azure command failed ({' '.join(command[:4])}): {result.stderr.strip()}"
        )
    return json.loads(result.stdout) if result.stdout.strip() else {}


def source_commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()


def tag_args(tags: dict[str, str], commit: str) -> list[str]:
    return [
        "--tags",
        *[
            f"{key}={value}"
            for key, value in {**tags, "source-commit": commit[:12]}.items()
        ],
    ]


def image_version(commit: str) -> str:
    return f"1.{int(commit[:6], 16)}.{int(commit[6:12], 16)}"


def owned_vm(policy: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    vm = az(
        [
            "vm",
            "show",
            "--resource-group",
            policy["resource_group"],
            "--name",
            config["name"],
            "--show-details",
        ],
        allow_missing=True,
    )
    if vm is None:
        raise LifecycleError(f"VM does not exist: {config['name']}")
    if any(
        vm.get("tags", {}).get(key) != value for key, value in config["tags"].items()
    ):
        raise LifecycleError("VM tags do not match policy ownership")
    return vm


def refuse_existing(policy: dict[str, Any], name: str) -> None:
    vm = az(
        ["vm", "show", "--resource-group", policy["resource_group"], "--name", name],
        allow_missing=True,
    )
    if vm is not None:
        raise LifecycleError(f"VM already exists: {name}")


def owned_public_ip(
    policy: dict[str, Any], config: dict[str, Any], *, allow_missing: bool = False
) -> dict[str, Any] | None:
    public_ip = az(
        [
            "network",
            "public-ip",
            "show",
            "--resource-group",
            policy["resource_group"],
            "--name",
            config["public_ip_name"],
        ],
        allow_missing=True,
    )
    if public_ip is None:
        if allow_missing:
            return None
        raise LifecycleError(f"public IP does not exist: {config['public_ip_name']}")
    if any(
        public_ip.get("tags", {}).get(key) != value
        for key, value in config["tags"].items()
    ):
        raise LifecycleError("public IP tags do not match policy ownership")
    return public_ip


def delete_owned_public_ip(policy: dict[str, Any], config: dict[str, Any]) -> bool:
    public_ip = owned_public_ip(policy, config, allow_missing=True)
    if public_ip is None:
        return False
    if public_ip.get("ipConfiguration") is not None:
        raise LifecycleError("refusing to delete an attached public IP")
    az(
        [
            "network",
            "public-ip",
            "delete",
            "--resource-group",
            policy["resource_group"],
            "--name",
            config["public_ip_name"],
        ]
    )
    return True


def owned_nic(
    policy: dict[str, Any], config: dict[str, Any], *, allow_missing: bool = False
) -> dict[str, Any] | None:
    nic = az(
        [
            "network",
            "nic",
            "show",
            "--resource-group",
            policy["resource_group"],
            "--name",
            config["nic_name"],
        ],
        allow_missing=True,
    )
    if nic is None:
        if allow_missing:
            return None
        raise LifecycleError(f"NIC does not exist: {config['nic_name']}")
    if any(
        nic.get("tags", {}).get(key) != value for key, value in config["tags"].items()
    ):
        raise LifecycleError("NIC tags do not match policy ownership")
    return nic


def cleanup_failed_create(
    policy: dict[str, Any], config: dict[str, Any]
) -> dict[str, Any]:
    vm = az(
        [
            "vm",
            "show",
            "--resource-group",
            policy["resource_group"],
            "--name",
            config["name"],
        ],
        allow_missing=True,
    )
    if vm is not None:
        raise LifecycleError("refusing partial cleanup while VM exists")
    nic = owned_nic(policy, config, allow_missing=True)
    public_ip = owned_public_ip(policy, config, allow_missing=True)
    if nic is not None:
        expected_public_ip_id = (public_ip or {}).get("id", "").lower()
        attached_public_ip_ids = {
            (item.get("publicIPAddress") or {}).get("id", "").lower()
            for item in nic.get("ipConfigurations", [])
        }
        attached_public_ip_ids.discard("")
        if attached_public_ip_ids and attached_public_ip_ids != {expected_public_ip_id}:
            raise LifecycleError("NIC is attached to an unexpected public IP")
        az(
            [
                "network",
                "nic",
                "delete",
                "--resource-group",
                policy["resource_group"],
                "--name",
                config["nic_name"],
            ]
        )
    deleted_public_ip = delete_owned_public_ip(policy, config)
    return {"nic_deleted": nic is not None, "public_ip_deleted": deleted_public_ip}


def delete_owned_vm(policy: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    vm = owned_vm(policy, config)
    public_ip = owned_public_ip(policy, config)
    nic_ids = {
        item.get("id", "").lower()
        for item in vm.get("networkProfile", {}).get("networkInterfaces", [])
    }
    ip_configuration_id = (public_ip.get("ipConfiguration") or {}).get("id", "")
    if ip_configuration_id and not any(
        ip_configuration_id.lower().startswith(f"{nic_id}/ipconfigurations/")
        for nic_id in nic_ids
    ):
        raise LifecycleError("public IP is attached outside the owned VM")
    az(
        [
            "vm",
            "delete",
            "--resource-group",
            policy["resource_group"],
            "--name",
            config["name"],
            "--yes",
        ]
    )
    deleted_public_ip = delete_owned_public_ip(policy, config)
    return {"vm": config["name"], "public_ip_deleted": deleted_public_ip}


def ensure_foundation(
    policy: dict[str, Any],
    commit: str,
    *,
    location: str | None = None,
    network: dict[str, Any] | None = None,
) -> dict[str, Any]:
    group = policy["resource_group"]
    location = location or policy["location"]
    network = network or policy["network"]
    common = {"managed-by": "runner-autoscaler", "fleet": "also-ran-ci"}
    az(
        [
            "group",
            "create",
            "--name",
            group,
            "--location",
            policy["location"],
            *tag_args(common, commit),
        ]
    )
    az(
        [
            "network",
            "nsg",
            "create",
            "--resource-group",
            group,
            "--name",
            network["nsg"],
            "--location",
            location,
            *tag_args(common, commit),
        ]
    )
    for index, cidr in enumerate(network["ssh_source_cidrs"]):
        az(
            [
                "network",
                "nsg",
                "rule",
                "create",
                "--resource-group",
                group,
                "--nsg-name",
                network["nsg"],
                "--name",
                f"ssh-admin-{index + 1}",
                "--priority",
                str(100 + index),
                "--direction",
                "Inbound",
                "--access",
                "Allow",
                "--protocol",
                "Tcp",
                "--source-address-prefixes",
                cidr,
                "--destination-port-ranges",
                "22",
            ]
        )
    az(
        [
            "network",
            "vnet",
            "create",
            "--resource-group",
            group,
            "--name",
            network["vnet"],
            "--location",
            location,
            "--address-prefixes",
            network["address_prefix"],
            "--subnet-name",
            network["subnet"],
            "--subnet-prefixes",
            network["subnet_prefix"],
            *tag_args(common, commit),
        ]
    )
    subnet = az(
        [
            "network",
            "vnet",
            "subnet",
            "update",
            "--resource-group",
            group,
            "--vnet-name",
            network["vnet"],
            "--name",
            network["subnet"],
            "--network-security-group",
            network["nsg"],
        ]
    )
    return {"resource_group": group, "subnet_id": subnet["id"]}


def create_vm(
    policy: dict[str, Any],
    config: dict[str, Any],
    image: str,
    ssh_key_file: str,
    commit: str,
    *,
    spot: bool,
) -> dict[str, Any]:
    refuse_existing(policy, config["name"])
    network = config.get("network", policy["network"])
    location = config.get("location", policy["location"])
    arguments = [
        "vm",
        "create",
        "--resource-group",
        policy["resource_group"],
        "--name",
        config["name"],
        "--location",
        location,
        "--image",
        image,
        "--security-type",
        "TrustedLaunch",
        "--enable-secure-boot",
        "true",
        "--enable-vtpm",
        "true",
        "--size",
        config["size"],
        "--admin-username",
        "runner-admin",
        "--ssh-key-values",
        ssh_key_file,
        "--vnet-name",
        network["vnet"],
        "--subnet",
        network["subnet"],
        "--nsg",
        "",
        "--public-ip-sku",
        "Standard",
        "--public-ip-address",
        config["public_ip_name"],
        "--public-ip-address-allocation",
        "static",
        "--os-disk-size-gb",
        str(config["os_disk_size_gb"]),
        "--os-disk-delete-option",
        "Delete",
        "--nic-delete-option",
        "Delete",
        *tag_args(config["tags"], commit),
    ]
    if spot:
        arguments.extend(
            ["--priority", "Spot", "--eviction-policy", "Delete", "--max-price", "-1"]
        )
    try:
        created = az(arguments)
    except LifecycleError as create_error:
        try:
            cleanup_failed_create(policy, config)
        except LifecycleError as cleanup_error:
            raise LifecycleError(
                f"{create_error}; partial resource cleanup failed: {cleanup_error}"
            ) from create_error
        raise
    return {
        "id": created["id"],
        "name": config["name"],
        "public_ip": created.get("publicIpAddress"),
        "spot": spot,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--policy", required=True
    )
    parser.add_argument("--ssh-key-file", default="~/.ssh/id_ed25519.pub")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status")
    for name in (
        "foundation",
        "create-builder",
        "capture-image",
        "delete-builder",
        "cleanup-builder-public-ip",
        "cleanup-burst-resources",
        "create-burst",
        "delete-burst",
        "foundation-paid-acceptance",
        "cleanup-paid-acceptance-resources",
        "create-paid-acceptance",
        "delete-paid-acceptance",
    ):
        child = subparsers.add_parser(name)
        child.add_argument("--apply", action="store_true")
        if name in {"create-burst", "create-paid-acceptance"}:
            child.add_argument("--image-id", required=True)
    args = parser.parse_args(argv)
    policy = load_policy(args.policy)
    commit = source_commit()

    if args.command == "status":
        result = az(
            [
                "vm",
                "list",
                "--resource-group",
                policy["resource_group"],
                "--show-details",
            ],
            allow_missing=True,
        )
        result = result or []
    elif args.command == "foundation":
        require_apply(args)
        result = ensure_foundation(policy, commit)
    elif args.command == "foundation-paid-acceptance":
        require_apply(args)
        paid = policy["paid_acceptance"]
        result = ensure_foundation(
            policy,
            commit,
            location=paid["location"],
            network=paid["network"],
        )
    elif args.command == "create-builder":
        require_apply(args)
        result = create_vm(
            policy,
            policy["builder"],
            policy["builder"]["base_image"],
            args.ssh_key_file,
            commit,
            spot=False,
        )
    elif args.command == "capture-image":
        require_apply(args)
        image_commit = policy["image_source_commit"]
        vm = owned_vm(policy, policy["builder"])
        if vm.get("powerState") != "VM deallocated":
            raise LifecycleError("builder must be deallocated before image capture")
        gallery = policy["gallery"]
        version = image_version(image_commit)
        existing = az(
            [
                "sig",
                "show",
                "--resource-group",
                policy["resource_group"],
                "--gallery-name",
                gallery["name"],
            ],
            allow_missing=True,
        )
        if existing is None:
            az(
                [
                    "sig",
                    "create",
                    "--resource-group",
                    policy["resource_group"],
                    "--gallery-name",
                    gallery["name"],
                    "--location",
                    policy["location"],
                    *tag_args(
                        {"managed-by": "runner-autoscaler", "fleet": "also-ran-ci"},
                        image_commit,
                    ),
                ]
            )
        definition = az(
            [
                "sig",
                "image-definition",
                "show",
                "--resource-group",
                policy["resource_group"],
                "--gallery-name",
                gallery["name"],
                "--gallery-image-definition",
                gallery["image_definition"],
            ],
            allow_missing=True,
        )
        if definition is None:
            az(
                [
                    "sig",
                    "image-definition",
                    "create",
                    "--resource-group",
                    policy["resource_group"],
                    "--gallery-name",
                    gallery["name"],
                    "--gallery-image-definition",
                    gallery["image_definition"],
                    "--location",
                    policy["location"],
                    "--publisher",
                    "example-org",
                    "--offer",
                    "also-ranRunner",
                    "--sku",
                    "ubuntu2404",
                    "--os-type",
                    "Linux",
                    "--os-state",
                    "Generalized",
                    "--hyper-v-generation",
                    "V2",
                    "--features",
                    "SecurityType=TrustedLaunch",
                    *tag_args(
                        {"managed-by": "runner-autoscaler", "fleet": "also-ran-ci"},
                        image_commit,
                    ),
                ]
            )
        version_resource = az(
            [
                "sig",
                "image-version",
                "show",
                "--resource-group",
                policy["resource_group"],
                "--gallery-name",
                gallery["name"],
                "--gallery-image-definition",
                gallery["image_definition"],
                "--gallery-image-version",
                version,
            ],
            allow_missing=True,
        )
        if version_resource is not None:
            raise LifecycleError(f"image version already exists: {version}")
        instance = az(
            [
                "vm",
                "get-instance-view",
                "--resource-group",
                policy["resource_group"],
                "--name",
                policy["builder"]["name"],
            ]
        )
        status_codes = [
            item.get("code")
            for item in instance.get("instanceView", {}).get("statuses", [])
        ]
        if "OSState/generalized" not in status_codes:
            az(
                [
                    "vm",
                    "generalize",
                    "--resource-group",
                    policy["resource_group"],
                    "--name",
                    policy["builder"]["name"],
                ]
            )
        result = az(
            [
                "sig",
                "image-version",
                "create",
                "--resource-group",
                policy["resource_group"],
                "--gallery-name",
                gallery["name"],
                "--gallery-image-definition",
                gallery["image_definition"],
                "--gallery-image-version",
                version,
                "--virtual-machine",
                vm["id"],
                "--target-regions",
                policy["location"],
                "--replica-count",
                "1",
                "--storage-account-type",
                "Standard_LRS",
                *tag_args(
                    {
                        "managed-by": "runner-autoscaler",
                        "image-purpose": "also-ran-runner",
                        "credential-free": "true",
                    },
                    image_commit,
                ),
            ]
        )
    elif args.command == "delete-builder":
        require_apply(args)
        result = delete_owned_vm(policy, policy["builder"])
    elif args.command == "cleanup-builder-public-ip":
        require_apply(args)
        result = {
            "public_ip": policy["builder"]["public_ip_name"],
            "deleted": delete_owned_public_ip(policy, policy["builder"]),
        }
    elif args.command == "cleanup-burst-resources":
        require_apply(args)
        result = cleanup_failed_create(policy, policy["burst"])
    elif args.command == "create-burst":
        require_apply(args)
        image = az(["sig", "image-version", "show", "--ids", args.image_id])
        tags = image.get("tags", {})
        if (
            tags.get("credential-free") != "true"
            or tags.get("source-commit") != policy["image_source_commit"][:12]
        ):
            raise LifecycleError(
                "image is not credential-free or from the approved source commit"
            )
        result = create_vm(
            policy,
            policy["burst"],
            args.image_id,
            args.ssh_key_file,
            commit,
            spot=True,
        )
    elif args.command == "delete-burst":
        require_apply(args)
        result = delete_owned_vm(policy, policy["burst"])
    elif args.command == "cleanup-paid-acceptance-resources":
        require_apply(args)
        result = cleanup_failed_create(policy, policy["paid_acceptance"])
    elif args.command == "create-paid-acceptance":
        require_apply(args)
        image = az(["sig", "image-version", "show", "--ids", args.image_id])
        tags = image.get("tags", {})
        if (
            tags.get("credential-free") != "true"
            or tags.get("source-commit") != policy["image_source_commit"][:12]
        ):
            raise LifecycleError(
                "image is not credential-free or from the approved source commit"
            )
        result = create_vm(
            policy,
            policy["paid_acceptance"],
            args.image_id,
            args.ssh_key_file,
            commit,
            spot=False,
        )
    elif args.command == "delete-paid-acceptance":
        require_apply(args)
        result = delete_owned_vm(policy, policy["paid_acceptance"])
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
