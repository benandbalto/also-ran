#!/usr/bin/env python3
"""Read-only regional market observer for the GCP runner fleet."""

from __future__ import annotations

from deployment_identity import PROJECT_ID, PROJECT_NUMBER, RESOURCE_PREFIX, FLEET

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from decimal import Decimal
from typing import Any

from also_ran import ObservationError, atomic_write_json, exclusive_lock, load_json
from gcp_broker import (
    c4_family_quota_vcpus,
    c4_family_usage_vcpus,
    regional_host_cap,
    shared_regional_capacity,
    gcloud,
    hyperdisk_quota_gb,
    hyperdisk_usage_gb,
    local_ssd_capacity,
    quota_capacity,
    shared_admission_reads,
)


VERSION = "0.1.0"
COMPUTE_SERVICE = "services/6F81-5844-456A"
PRICE_COMPONENTS = {
    "core": (re.compile(r"^Spot Preemptible C4 Instance Core "), "h", Decimal(8)),
    "ram": (re.compile(r"^Spot Preemptible C4 Instance Ram "), "GiBy.h", Decimal(30)),
    "local_ssd": (
        re.compile(r"^Spot Preemptible C4 Instance Local SSD "),
        "GiBy.mo",
        Decimal(375) / Decimal(730),
    ),
}


class MarketPolicyError(RuntimeError):
    """Raised when the reviewed market boundary is not exact."""


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def isoformat(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def validate_market_policy(policy: dict[str, Any]) -> dict[str, Any]:
    shared_regional_capacity(policy)
    market = policy.get("market", {})
    regions = market.get("regions")
    if (
        policy.get("policy_version") != 1
        or policy.get("project_id") != PROJECT_ID
        or policy.get("project_number") != PROJECT_NUMBER
        or policy.get("production", {}).get("machine_type")
        != "c4-standard-8-lssd"
        or policy.get("production", {}).get("boot_disk_gb") != 32
        or policy.get("production", {}).get("boot_disk_type")
        != "hyperdisk-balanced"
        or market.get("policy_version") != 1
        or market.get("observer_enabled") is not True
        or market.get("mutation_enabled") is not False
        or market.get("heavy_global_maximum_hosts") != 28
        or market.get("heavy_secondary_region_maximum_hosts") != 10
        or market.get("general_global_maximum_hosts") != 12
        or market.get("general_secondary_region_maximum_hosts") != 4
        or market.get("vcpus_per_heavy_host") != 8
        or market.get("global_vcpu_headroom_ratio") != 1.25
        or market.get("global_vcpu_reserve") != 12
        or market.get("maximum_heavy_hourly_usd") != 8.5
        or market.get("maximum_combined_hourly_usd") != 10.0
        or market.get("maximum_price_age_hours") != 72
        or not isinstance(regions, list)
    ):
        raise MarketPolicyError("unexpected GCP market policy")
    expected = {
        "us-central1",
        "northamerica-northeast1",
        "europe-west9",
        "asia-southeast2",
        "us-west2",
        "us-south1",
        "europe-west8",
    }
    names = [item.get("name") for item in regions if isinstance(item, dict)]
    if len(names) != len(set(names)) or set(names) != expected:
        raise MarketPolicyError("unexpected GCP market region allowlist")
    for region in regions:
        foundation = region.get("foundation")
        if (
            not isinstance(region.get("zones"), list)
            or not region["zones"]
            or any(not zone.startswith(region["name"] + "-") for zone in region["zones"])
            or not isinstance(region.get("regional_maximum_hosts"), int)
            or region["regional_maximum_hosts"] < 1
            or region["regional_maximum_hosts"]
            > (20 if region["name"] == "us-central1" else 10)
            or not isinstance(region.get("maximum_hourly_usd"), (int, float))
            or region["maximum_hourly_usd"] <= 0
            or not isinstance(region.get("canary_valid"), bool)
            or not isinstance(foundation, dict)
            or any(
                not isinstance(foundation.get(key), str) or not foundation[key]
                for key in ("network", "subnet", "subnet_cidr", "router", "nat")
            )
        ):
            raise MarketPolicyError(f"invalid market region {region.get('name')}")
    return market


def access_token() -> str:
    result = subprocess.run(
        ["gcloud", "auth", "print-access-token"], capture_output=True, text=True
    )
    if result.returncode or not result.stdout.strip():
        raise ObservationError("could not obtain GCP access token for catalog readback")
    return result.stdout.strip()


def catalog_skus(token: str) -> list[dict[str, Any]]:
    skus: list[dict[str, Any]] = []
    page_token = ""
    for _ in range(20):
        query = {"pageSize": "5000", "currencyCode": "USD"}
        if page_token:
            query["pageToken"] = page_token
        endpoint = (
            "https://cloudbilling.googleapis.com/v1/"
            f"{COMPUTE_SERVICE}/skus?{urllib.parse.urlencode(query)}"
        )
        request = urllib.request.Request(
            endpoint, headers={"Authorization": f"Bearer {token}"}
        )
        payload = None
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    payload = json.load(response)
                break
            except urllib.error.HTTPError as error:
                last_error = error
                retryable = error.code == 429 or error.code >= 500
                if error.code == 403:
                    try:
                        body = json.loads(error.read())
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        body = {}
                    message = str(body.get("error", {}).get("message", ""))
                    retryable = "has not been used" in message or "disabled" in message
                if not retryable or attempt == 2:
                    break
                time.sleep(2**attempt)
            except Exception as error:
                last_error = error
                if attempt == 2:
                    break
                time.sleep(2**attempt)
        if payload is None:
            raise ObservationError("could not read Cloud Billing Catalog") from last_error
        page = payload.get("skus")
        if not isinstance(page, list):
            raise ObservationError("Cloud Billing Catalog response omitted skus")
        skus.extend(page)
        page_token = payload.get("nextPageToken", "")
        if not page_token:
            return skus
    raise ObservationError("Cloud Billing Catalog pagination exceeded safe limit")


def money_decimal(value: Any) -> Decimal:
    if not isinstance(value, dict) or value.get("currencyCode") != "USD":
        raise ObservationError("Cloud Billing Catalog returned non-USD price")
    try:
        return Decimal(str(value.get("units", "0"))) + Decimal(
            int(value.get("nanos", 0))
        ) / Decimal(1_000_000_000)
    except (TypeError, ValueError) as error:
        raise ObservationError("Cloud Billing Catalog returned malformed price") from error


def latest_unit_price(sku: dict[str, Any], now: dt.datetime) -> tuple[Decimal, str]:
    candidates: list[tuple[dt.datetime, Decimal, str]] = []
    for item in sku.get("pricingInfo", []):
        try:
            effective = dt.datetime.fromisoformat(
                str(item["effectiveTime"]).replace("Z", "+00:00")
            )
            expression = item["pricingExpression"]
            rates = expression["tieredRates"]
            if len(rates) != 1 or Decimal(str(rates[0]["startUsageAmount"])) != 0:
                continue
            price = money_decimal(rates[0]["unitPrice"])
            unit = expression["usageUnit"]
        except (KeyError, TypeError, ValueError):
            continue
        if effective <= now:
            candidates.append((effective, price, unit))
    if not candidates:
        raise ObservationError("Cloud Billing Catalog SKU has no current simple rate")
    _, price, unit = max(candidates, key=lambda item: item[0])
    return price, unit


def price_is_fresh(
    observed_at: str, now: dt.datetime, maximum_age_hours: int
) -> bool:
    try:
        observed = dt.datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return False
    return (
        observed.tzinfo is not None
        and dt.timedelta(0)
        <= now - observed.astimezone(dt.timezone.utc)
        <= dt.timedelta(hours=maximum_age_hours)
    )


def regional_prices(
    skus: list[dict[str, Any]], regions: list[str], now: dt.datetime
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for region in regions:
        components: dict[str, Decimal] = {}
        sku_ids: dict[str, str] = {}
        for component, (pattern, expected_unit, quantity) in PRICE_COMPONENTS.items():
            matches = [
                sku
                for sku in skus
                if region in (sku.get("serviceRegions") or [])
                and pattern.match(str(sku.get("description", "")))
            ]
            if len(matches) != 1:
                raise ObservationError(
                    f"expected one {component} Spot C4 SKU for {region}, got {len(matches)}"
                )
            unit_price, unit = latest_unit_price(matches[0], now)
            if unit != expected_unit:
                raise ObservationError(
                    f"unexpected {component} price unit for {region}: {unit}"
                )
            components[component] = unit_price * quantity
            sku_ids[component] = str(matches[0].get("skuId", ""))
        result[region] = {
            "hourly_usd": float(sum(components.values())),
            "components_hourly_usd": {
                key: float(value) for key, value in components.items()
            },
            "sku_ids": sku_ids,
        }
    return result


def owned_instances(policy: dict[str, Any]) -> list[dict[str, Any]]:
    production = policy["production"]
    payload = gcloud(
        [
            "compute",
            "instances",
            "list",
            f"--project={policy['project_id']}",
            f"--filter=name~'^{production['name_prefix']}-[0-9]+$'",
        ]
    )
    if not isinstance(payload, list):
        raise ObservationError("GCP instance list was not an array")
    instances = []
    pattern = re.compile(rf"^{re.escape(production['name_prefix'])}-(\d{{2}})$")
    for item in payload:
        match = pattern.fullmatch(str(item.get("name", "")))
        if not match:
            continue
        labels = item.get("labels")
        required_labels = production.get("labels", {})
        if not isinstance(labels, dict) or any(
            labels.get(key) != value for key, value in required_labels.items()
        ):
            raise ObservationError(
                f"instance {item.get('name')} has unexpected ownership labels"
            )
        zone = str(item.get("zone", "")).rsplit("/", 1)[-1]
        instances.append(
            {
                "slot": int(match.group(1)),
                "name": item["name"],
                "zone": zone,
                "region": zone.rsplit("-", 1)[0],
                "status": item.get("status"),
                "id": str(item.get("id", "")),
            }
        )
    return instances


def quota_limit_usage(
    quotas: list[dict[str, Any]], metric: str
) -> tuple[int, int]:
    quota = next((item for item in quotas if item.get("metric") == metric), None)
    if quota is None:
        raise ObservationError(f"GCP quota {metric} is missing")
    try:
        limit = int(float(quota["limit"]))
        usage = int(float(quota.get("usage", 0)))
    except (KeyError, TypeError, ValueError) as error:
        raise ObservationError(f"GCP quota {metric} is malformed") from error
    if limit < 0 or usage < 0:
        raise ObservationError(f"GCP quota {metric} is malformed")
    return limit, usage


def global_cpu_budget(
    market: dict[str, Any], quotas: list[dict[str, Any]], runner_hosts: int
) -> dict[str, int]:
    limit, usage = quota_limit_usage(quotas, "CPUS_ALL_REGIONS")
    runner_vcpus = runner_hosts * market["vcpus_per_heavy_host"]
    auxiliary_vcpus = max(0, usage - runner_vcpus)
    reserved_vcpus = max(auxiliary_vcpus, market["global_vcpu_reserve"])
    admissible_vcpus = int(limit / market["global_vcpu_headroom_ratio"])
    runner_budget_vcpus = max(0, admissible_vcpus - reserved_vcpus)
    additional_heavy_hosts = max(
        0,
        (runner_budget_vcpus - runner_vcpus)
        // market["vcpus_per_heavy_host"],
    )
    return {
        "quota_vcpus": limit,
        "used_vcpus": usage,
        "runner_vcpus": runner_vcpus,
        "auxiliary_vcpus": auxiliary_vcpus,
        "reserved_vcpus": reserved_vcpus,
        "admissible_vcpus": admissible_vcpus,
        "runner_budget_vcpus": runner_budget_vcpus,
        "additional_heavy_hosts": additional_heavy_hosts,
    }


def foundation_reasons(policy: dict[str, Any], region: dict[str, Any]) -> list[str]:
    project = policy["project_id"]
    foundation = region["foundation"]
    reasons: list[str] = []
    checks = (
        (
            "subnet_missing",
            [
                "compute",
                "networks",
                "subnets",
                "describe",
                foundation["subnet"],
                f"--region={region['name']}",
                f"--project={project}",
            ],
        ),
        (
            "router_missing",
            [
                "compute",
                "routers",
                "describe",
                foundation["router"],
                f"--region={region['name']}",
                f"--project={project}",
            ],
        ),
    )
    router = None
    subnet = None
    for reason, arguments in checks:
        try:
            payload = gcloud(arguments)
            if reason == "router_missing":
                router = payload
            else:
                subnet = payload
        except ObservationError:
            reasons.append(reason)
    expected_network = f"/global/networks/{foundation['network']}"
    if subnet is not None and (
        not str(subnet.get("network", "")).endswith(expected_network)
        or subnet.get("ipCidrRange") != foundation["subnet_cidr"]
        or subnet.get("privateIpGoogleAccess") is not True
        or subnet.get("purpose") != "PRIVATE"
    ):
        reasons.append("subnet_identity_mismatch")
    if router is not None and not str(router.get("network", "")).endswith(
        expected_network
    ):
        reasons.append("router_identity_mismatch")
    if router is not None and foundation["nat"] not in {
        item.get("name") for item in router.get("nats", [])
    }:
        reasons.append("nat_missing")
    if router is not None:
        nat = next(
            (
                item
                for item in router.get("nats", [])
                if item.get("name") == foundation["nat"]
            ),
            None,
        )
        if nat is not None and (
            nat.get("natIpAllocateOption") != "AUTO_ONLY"
            or nat.get("sourceSubnetworkIpRangesToNat")
            != "ALL_SUBNETWORKS_ALL_IP_RANGES"
            or nat.get("logConfig", {}).get("enable") is not True
        ):
            reasons.append("nat_identity_mismatch")
    return reasons


def observe_region(
    policy: dict[str, Any],
    region: dict[str, Any],
    price: dict[str, Any],
    instances: list[dict[str, Any]],
    global_cpu_hosts: int,
    price_observed_at: str,
    now: dt.datetime,
    maximum_price_age_hours: int,
) -> dict[str, Any]:
    name = region["name"]
    project = policy["project_id"]
    info = gcloud(["compute", "regions", "describe", name, f"--project={project}"])
    quotas = info.get("quotas")
    if not isinstance(quotas, list):
        raise ObservationError(f"GCP regional quotas missing for {name}")
    existing = sum(item["region"] == name for item in instances)
    family_vcpus = c4_family_quota_vcpus(policy, name)
    sharing = shared_regional_capacity(policy)
    reserve = sharing["reserved_vcpus"] if sharing and name in sharing["regions"] else 0
    family_usage = c4_family_usage_vcpus(policy, name) if sharing else existing * 8
    hdb_quota = hyperdisk_quota_gb(policy, name)
    hdb_usage = hyperdisk_usage_gb(policy, name)
    local_ssd_hosts, _, _ = local_ssd_capacity(
        policy, name, policy["production"]["machine_type"]
    )
    capacities = {
        "global_cpu_hosts": global_cpu_hosts,
        "family_cpu_hosts": max(0, (family_vcpus - family_usage - reserve) // 8),
        "instance_hosts": quota_capacity(quotas, "INSTANCES", 1),
        "disk_hosts": max(0, (hdb_quota - hdb_usage) // 32),
    }
    if local_ssd_hosts is not None:
        # The per-family limit GCP enforces, not the regional
        # LOCAL_SSD_TOTAL_GB metric, which is unlimited for C4.
        capacities["local_ssd_hosts"] = local_ssd_hosts
    if any(value is None for value in capacities.values()):
        raise ObservationError(f"required GCP quota missing for {name}")
    reasons = foundation_reasons(policy, region)
    supported_zones = []
    for zone in region["zones"]:
        try:
            gcloud(
                [
                    "compute",
                    "machine-types",
                    "describe",
                    policy["production"]["machine_type"],
                    f"--zone={zone}",
                    f"--project={project}",
                ]
            )
            supported_zones.append(zone)
        except ObservationError:
            pass
    if not supported_zones:
        reasons.append("machine_type_unavailable")
    if not region["canary_valid"]:
        reasons.append("canary_not_valid")
    if not price_is_fresh(price_observed_at, now, maximum_price_age_hours):
        reasons.append("price_stale")
    if price["hourly_usd"] > float(region["maximum_hourly_usd"]):
        reasons.append("price_ceiling_exceeded")
    if min(int(value) for value in capacities.values()) < 1:
        reasons.append("quota_exhausted")
    return {
        "name": name,
        "eligible": not reasons,
        "rejection_reasons": sorted(set(reasons)),
        "configured_zones": region["zones"],
        "supported_zones": supported_zones,
        "existing_hosts": existing,
        "regional_maximum_hosts": regional_host_cap(policy, name, "heavy"),
        "available_hosts": min(int(value) for value in capacities.values()),
        "hyperdisk_quota_gb": hdb_quota,
        "hyperdisk_usage_gb": hdb_usage,
        "price": price,
        **capacities,
    }


def proposed_placements(
    market: dict[str, Any],
    desired_hosts: int,
    instances: list[dict[str, Any]],
    regions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    slots = Counter(item["slot"] for item in instances)
    duplicates = sorted(slot for slot, count in slots.items() if count > 1)
    if duplicates:
        raise ObservationError(f"duplicate global slots: {duplicates}")
    maximum = market["heavy_global_maximum_hosts"]
    retained = {slot for slot in slots if 1 <= slot <= maximum}
    missing = max(0, min(desired_hosts, maximum) - len(retained))
    absent = [slot for slot in range(1, maximum + 1) if slot not in retained]
    placements: list[dict[str, Any]] = []
    counts = Counter(item["region"] for item in instances)
    by_name = {item["name"]: item for item in regions}
    unknown_regions = sorted(set(counts) - set(by_name))
    if unknown_regions:
        raise ObservationError(f"owned instances outside allowlist: {unknown_regions}")
    projected_hourly_usd = sum(
        by_name[item["region"]]["price"]["hourly_usd"] for item in instances
    )
    capacity = {
        item["name"]: min(
            item["available_hosts"],
            max(0, item["regional_maximum_hosts"] - counts[item["name"]]),
        )
        for item in regions
        if item["eligible"]
    }
    ranked = sorted(
        (item for item in regions if item["eligible"]),
        key=lambda item: (item["price"]["hourly_usd"], item["name"]),
    )
    global_capacity = (
        min(item["global_cpu_hosts"] for item in ranked) if ranked else 0
    )
    for slot in absent[:missing]:
        if len(placements) >= global_capacity:
            break
        candidate = next((item for item in ranked if capacity[item["name"]] > 0), None)
        if candidate is None:
            break
        if (
            projected_hourly_usd + candidate["price"]["hourly_usd"]
            > market["maximum_heavy_hourly_usd"]
        ):
            break
        placements.append(
            {
                "slot": slot,
                "region": candidate["name"],
                "zone": candidate["supported_zones"][
                    counts[candidate["name"]] % len(candidate["supported_zones"])
                ],
                "hourly_usd": candidate["price"]["hourly_usd"],
            }
        )
        capacity[candidate["name"]] -= 1
        counts[candidate["name"]] += 1
        projected_hourly_usd += candidate["price"]["hourly_usd"]
    return placements


def observe(policy: dict[str, Any], broker_state: dict[str, Any]) -> dict[str, Any]:
    market = validate_market_policy(policy)
    now = utc_now()
    region_configs = market["regions"]
    prices = regional_prices(
        catalog_skus(access_token()), [item["name"] for item in region_configs], now
    )
    price_observed_at = isoformat(now)
    instances = owned_instances(policy)
    project_info = gcloud(
        ["compute", "project-info", "describe", f"--project={policy['project_id']}"]
    )
    quotas = project_info.get("quotas")
    if not isinstance(quotas, list):
        raise ObservationError("GCP global quota response is incomplete")
    cpu_budget = global_cpu_budget(market, quotas, len(instances))
    global_cpu_hosts = cpu_budget["additional_heavy_hosts"]
    # One project-wide instance and disk list serves every region's usage.
    with shared_admission_reads():
        regions = [
            observe_region(
                policy,
                config,
                prices[config["name"]],
                instances,
                global_cpu_hosts,
                price_observed_at,
                now,
                market["maximum_price_age_hours"],
            )
            for config in region_configs
        ]
    desired = int(broker_state.get("desired_hosts", len(instances)))
    placements = proposed_placements(market, desired, instances, regions)
    price_by_region = {
        item["name"]: item["price"]["hourly_usd"] for item in regions
    }
    current_hourly_usd = sum(
        price_by_region[item["region"]] for item in instances
    )
    additional_hourly_usd = sum(item["hourly_usd"] for item in placements)
    return {
        "observer_version": VERSION,
        "mode": "dry-run",
        "mutation_enabled": False,
        "observed_at": isoformat(now),
        "price_source": "Cloud Billing Catalog",
        "price_observed_at": price_observed_at,
        "desired_hosts": desired,
        "existing_hosts": len(instances),
        "global_cpu_budget": cpu_budget,
        "global_slots": instances,
        "proposed_placements": placements,
        "current_hourly_usd": current_hourly_usd,
        "projected_additional_hourly_usd": additional_hourly_usd,
        "projected_total_hourly_usd": current_hourly_usd
        + additional_hourly_usd,
        "regions": regions,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--broker-state", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--lock", required=True)
    arguments = parser.parse_args()
    try:
        with exclusive_lock(arguments.lock):
            state = observe(load_json(arguments.policy), load_json(arguments.broker_state))
            atomic_write_json(arguments.state, state)
            print(json.dumps(state, sort_keys=True))
        return 0
    except (MarketPolicyError, ObservationError, OSError, ValueError) as error:
        print(json.dumps({"observer_version": VERSION, "error": str(error)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
