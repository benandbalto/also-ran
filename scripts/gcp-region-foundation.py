#!/usr/bin/env python3
"""Plan or apply reviewed GCP runner subnet, router, and NAT foundations."""

from __future__ import annotations

import pathlib as _pathlib
import sys as _sys
_sys.path.insert(0, str(_pathlib.Path(__file__).resolve().parents[1] / "controller" / "also-ran"))
from deployment_identity import PROJECT_ID, PROJECT_NUMBER, RESOURCE_PREFIX, FLEET

import argparse
import ipaddress
import json
import pathlib
import subprocess
import sys
from typing import Any


NETWORK = PROJECT_ID
ALLOWED_REGIONS = {
    "europe-west9",
    "us-south1",
    "northamerica-northeast1",
    "us-west2",
    "asia-southeast2",
}



class FoundationError(RuntimeError):
    """Raised when foundation state is unsafe or outside reviewed policy."""


def gcloud(arguments: list[str], *, allow_missing: bool = False) -> Any:
    command = ["gcloud", *arguments, "--quiet", "--format=json"]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        if allow_missing and "was not found" in result.stderr.lower():
            return None
        raise FoundationError(
            f"gcloud failed ({' '.join(command[:6])}): {result.stderr.strip()}"
        )
    return json.loads(result.stdout) if result.stdout.strip() else {}


def load_policy(path: str) -> dict[str, Any]:
    with pathlib.Path(path).open(encoding="utf-8") as handle:
        policy = json.load(handle)
    market = policy.get("market", {})
    if (
        policy.get("policy_version") != 1
        or policy.get("project_id") != PROJECT_ID
        or policy.get("project_number") != PROJECT_NUMBER
        or market.get("policy_version") != 1
        or market.get("mutation_enabled") is not False
    ):
        raise FoundationError("unexpected GCP market policy boundary")
    return policy


def region_policy(policy: dict[str, Any], name: str) -> dict[str, Any]:
    if name not in ALLOWED_REGIONS:
        raise FoundationError(f"region is not approved for this foundation wave: {name}")
    matches = [item for item in policy["market"]["regions"] if item.get("name") == name]
    if len(matches) != 1:
        raise FoundationError(f"region policy is not unique: {name}")
    foundation = matches[0].get("foundation", {})
    expected = {
        "network": NETWORK,
        "subnet": f"runner-hosts-{name}",
        "router": f"{PROJECT_ID}-{name}",
        "nat": f"{PROJECT_ID}-{name}",
    }
    if any(foundation.get(key) != value for key, value in expected.items()):
        raise FoundationError(f"unexpected foundation identity for {name}")
    try:
        subnet = ipaddress.ip_network(foundation["subnet_cidr"], strict=True)
    except (KeyError, ValueError) as error:
        raise FoundationError(f"invalid subnet CIDR for {name}") from error
    if subnet.version != 4 or subnet.prefixlen != 24 or not subnet.is_private:
        raise FoundationError(f"subnet for {name} must be a private IPv4 /24")
    return foundation


def network_suffix(name: str) -> str:
    return f"/global/networks/{name}"


def validate_shared_boundary(policy: dict[str, Any]) -> None:
    network = gcloud(
        ["compute", "networks", "describe", NETWORK, f"--project={PROJECT_ID}"]
    )
    if network.get("name") != NETWORK or network.get("autoCreateSubnetworks") is not False:
        raise FoundationError("runner VPC identity is unexpected")
    cidrs = policy.get("network", {}).get("ssh_source_cidrs", [])
    if not cidrs or any(ipaddress.ip_network(value).prefixlen == 0 for value in cidrs):
        raise FoundationError("SSH sources must be explicit non-public networks")
    firewalls = {f"{PROJECT_ID}-ssh-admin": set(cidrs), f"{PROJECT_ID}-ssh-iap": {"35.235.240.0/20"}}
    for name, source_ranges in firewalls.items():
        rule = gcloud(
            ["compute", "firewall-rules", "describe", name, f"--project={PROJECT_ID}"]
        )
        allowed = rule.get("allowed") or []
        if (
            not str(rule.get("network", "")).endswith(network_suffix(NETWORK))
            or rule.get("direction") != "INGRESS"
            or rule.get("disabled") is True
            or set(rule.get("sourceRanges") or []) != source_ranges
            or set(rule.get("targetTags") or []) != {"runner-admin"}
            or allowed != [{"IPProtocol": "tcp", "ports": ["22"]}]
        ):
            raise FoundationError(f"shared SSH firewall identity mismatch: {name}")


def existing_subnets() -> list[dict[str, Any]]:
    payload = gcloud(
        [
            "compute",
            "networks",
            "subnets",
            "list",
            f"--project={PROJECT_ID}",
            f"--filter=network:{NETWORK}",
        ]
    )
    if not isinstance(payload, list):
        raise FoundationError("subnet inventory was not an array")
    return payload


def assert_no_cidr_conflict(
    foundation: dict[str, Any], subnets: list[dict[str, Any]]
) -> None:
    desired = ipaddress.ip_network(foundation["subnet_cidr"], strict=True)
    for subnet in subnets:
        name = subnet.get("name")
        cidr = subnet.get("ipCidrRange")
        if name == foundation["subnet"]:
            if cidr != foundation["subnet_cidr"]:
                raise FoundationError(f"existing subnet CIDR mismatch: {name}")
            continue
        try:
            existing = ipaddress.ip_network(str(cidr), strict=True)
        except ValueError as error:
            raise FoundationError(f"existing subnet has invalid CIDR: {name}") from error
        if desired.overlaps(existing):
            raise FoundationError(
                f"subnet CIDR {desired} overlaps existing {name} ({existing})"
            )


def subnet_valid(subnet: dict[str, Any], foundation: dict[str, Any]) -> bool:
    return (
        subnet.get("name") == foundation["subnet"]
        and str(subnet.get("network", "")).endswith(network_suffix(NETWORK))
        and subnet.get("ipCidrRange") == foundation["subnet_cidr"]
        and subnet.get("privateIpGoogleAccess") is True
        and subnet.get("purpose") == "PRIVATE"
        and subnet.get("stackType") == "IPV4_ONLY"
    )


def router_valid(router: dict[str, Any], foundation: dict[str, Any]) -> bool:
    return router.get("name") == foundation["router"] and str(
        router.get("network", "")
    ).endswith(network_suffix(NETWORK))


def nat_valid(nat: dict[str, Any], foundation: dict[str, Any]) -> bool:
    return (
        nat.get("name") == foundation["nat"]
        and nat.get("natIpAllocateOption") == "AUTO_ONLY"
        and nat.get("sourceSubnetworkIpRangesToNat")
        == "ALL_SUBNETWORKS_ALL_IP_RANGES"
        and nat.get("logConfig", {}).get("enable") is True
        and nat.get("logConfig", {}).get("filter") == "ERRORS_ONLY"
        and nat.get("type", "PUBLIC") == "PUBLIC"
    )


def observe_foundation(name: str, foundation: dict[str, Any]) -> dict[str, Any]:
    subnet = gcloud(
        [
            "compute",
            "networks",
            "subnets",
            "describe",
            foundation["subnet"],
            f"--region={name}",
            f"--project={PROJECT_ID}",
        ],
        allow_missing=True,
    )
    router = gcloud(
        [
            "compute",
            "routers",
            "describe",
            foundation["router"],
            f"--region={name}",
            f"--project={PROJECT_ID}",
        ],
        allow_missing=True,
    )
    nat = None
    if router is not None:
        nat = next(
            (
                item
                for item in router.get("nats", [])
                if item.get("name") == foundation["nat"]
            ),
            None,
        )
    if subnet is not None and not subnet_valid(subnet, foundation):
        raise FoundationError(f"subnet identity mismatch in {name}")
    if router is not None and not router_valid(router, foundation):
        raise FoundationError(f"router identity mismatch in {name}")
    if nat is not None and not nat_valid(nat, foundation):
        raise FoundationError(f"NAT identity mismatch in {name}")
    return {"subnet": subnet, "router": router, "nat": nat}


def plan_region(
    name: str, foundation: dict[str, Any], subnets: list[dict[str, Any]]
) -> dict[str, Any]:
    assert_no_cidr_conflict(foundation, subnets)
    observed = observe_foundation(name, foundation)
    actions = []
    if observed["subnet"] is None:
        actions.append("create_subnet")
    if observed["router"] is None:
        actions.append("create_router")
    if observed["nat"] is None:
        actions.append("create_nat")
    return {"region": name, "actions": actions, "ready": not actions}


def apply_region(name: str, foundation: dict[str, Any], actions: list[str]) -> None:
    if "create_subnet" in actions:
        gcloud(
            [
                "compute",
                "networks",
                "subnets",
                "create",
                foundation["subnet"],
                f"--network={NETWORK}",
                f"--region={name}",
                f"--range={foundation['subnet_cidr']}",
                "--enable-private-ip-google-access",
                "--stack-type=IPV4_ONLY",
                f"--project={PROJECT_ID}",
            ]
        )
    if "create_router" in actions:
        gcloud(
            [
                "compute",
                "routers",
                "create",
                foundation["router"],
                f"--network={NETWORK}",
                f"--region={name}",
                f"--project={PROJECT_ID}",
            ]
        )
    if "create_nat" in actions:
        gcloud(
            [
                "compute",
                "routers",
                "nats",
                "create",
                foundation["nat"],
                f"--router={foundation['router']}",
                f"--region={name}",
                "--auto-allocate-nat-external-ips",
                "--nat-all-subnet-ip-ranges",
                "--enable-logging",
                "--log-filter=ERRORS_ONLY",
                f"--project={PROJECT_ID}",
            ]
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--policy", required=True
    )
    parser.add_argument("--region", action="append", required=True)
    parser.add_argument("--apply", action="store_true")
    arguments = parser.parse_args(argv)
    if len(arguments.region) != len(set(arguments.region)):
        raise FoundationError("regions must be unique")
    policy = load_policy(arguments.policy)
    validate_shared_boundary(policy)
    plans = []
    for name in arguments.region:
        foundation = region_policy(policy, name)
        plan = plan_region(name, foundation, existing_subnets())
        if arguments.apply:
            apply_region(name, foundation, plan["actions"])
            verified = plan_region(name, foundation, existing_subnets())
            if not verified["ready"]:
                raise FoundationError(f"foundation verification failed in {name}")
            plan["applied"] = plan["actions"]
            plan["actions"] = []
            plan["ready"] = True
        plans.append(plan)
    print(
        json.dumps(
            {"mode": "apply" if arguments.apply else "plan", "regions": plans},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        raise SystemExit(1)
