import datetime as dt
import importlib.util
import pathlib
import sys
import unittest
from decimal import Decimal
from unittest import mock


CONTROLLER_DIR = pathlib.Path(__file__).parents[1]
sys.path.insert(0, str(CONTROLLER_DIR))
CONTROLLER = CONTROLLER_DIR / "gcp_market_observer.py"
SPEC = importlib.util.spec_from_file_location("gcp_market_observer", CONTROLLER)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


NOW = dt.datetime(2026, 9, 17, 19, 0, tzinfo=dt.timezone.utc)


def sku(component, region, unit, nanos):
    labels = {
        "core": "Core",
        "ram": "Ram",
        "local_ssd": "Local SSD",
    }
    return {
        "skuId": component,
        "description": f"Spot Preemptible C4 Instance {labels[component]} running in Test",
        "serviceRegions": [region],
        "pricingInfo": [
            {
                "effectiveTime": "2026-09-17T07:00:00Z",
                "pricingExpression": {
                    "usageUnit": unit,
                    "tieredRates": [
                        {
                            "startUsageAmount": 0,
                            "unitPrice": {
                                "currencyCode": "USD",
                                "units": "0",
                                "nanos": nanos,
                            },
                        }
                    ],
                },
            }
        ],
    }


def region(name, price, available=8, eligible=True, maximum=8, zones=None):
    return {
        "name": name,
        "eligible": eligible,
        "available_hosts": available,
        "regional_maximum_hosts": maximum,
        "global_cpu_hosts": 100,
        "supported_zones": zones or [name + "-a"],
        "price": {"hourly_usd": price},
    }


class PriceTests(unittest.TestCase):
    def test_observer_shared_capacity_counts_other_pool_and_reserves_replacement(self):
        import json
        candidate = json.loads((CONTROLLER_DIR / "gcp-acceptance-policy.json").read_text())
        config = next(item for item in candidate["market"]["regions"] if item["name"] == "europe-west9")
        quotas = {"quotas": [{"metric": metric, "limit": limit, "usage": 0}
            for metric, limit in (("INSTANCES", 24), ("LOCAL_SSD_TOTAL_GB", 9000))]}
        with mock.patch.object(MODULE, "gcloud", return_value=quotas), \
             mock.patch.object(MODULE, "c4_family_quota_vcpus", return_value=100), \
             mock.patch.object(MODULE, "c4_family_usage_vcpus", return_value=80), \
             mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=640), \
             mock.patch.object(MODULE, "hyperdisk_usage_gb", return_value=0), \
             mock.patch.object(MODULE, "local_ssd_capacity", return_value=(16, 6000, 0)), \
             mock.patch.object(MODULE, "foundation_reasons", return_value=[]):
            result = MODULE.observe_region(candidate, config, {"hourly_usd": 0.08}, [],
                28, "2026-09-17T19:00:00Z", NOW, 72)
        self.assertEqual(result["family_cpu_hosts"], 1)
        self.assertEqual(result["regional_maximum_hosts"], 12)


    def observe_with_local_ssd(self, local_ssd):
        import json
        candidate = json.loads((CONTROLLER_DIR / "gcp-acceptance-policy.json").read_text())
        config = next(item for item in candidate["market"]["regions"] if item["name"] == "europe-west9")
        quotas = {"quotas": [{"metric": "INSTANCES", "limit": 24, "usage": 0}]}
        with mock.patch.object(MODULE, "gcloud", return_value=quotas), \
             mock.patch.object(MODULE, "c4_family_quota_vcpus", return_value=100), \
             mock.patch.object(MODULE, "c4_family_usage_vcpus", return_value=0), \
             mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=640), \
             mock.patch.object(MODULE, "hyperdisk_usage_gb", return_value=0), \
             mock.patch.object(MODULE, "local_ssd_capacity", return_value=local_ssd), \
             mock.patch.object(MODULE, "foundation_reasons", return_value=[]):
            return MODULE.observe_region(candidate, config, {"hourly_usd": 0.08}, [],
                28, "2026-09-17T19:00:00Z", NOW, 72)

    def test_full_local_ssd_quota_makes_the_region_ineligible(self):
        result = self.observe_with_local_ssd((0, 6000, 6000))

        self.assertEqual(result["local_ssd_hosts"], 0)
        self.assertFalse(result["eligible"])
        self.assertIn("quota_exhausted", result["rejection_reasons"])

    def test_unlimited_local_ssd_quota_does_not_cap_the_region(self):
        result = self.observe_with_local_ssd((None, -1, 0))

        self.assertNotIn("local_ssd_hosts", result)
        self.assertNotIn("quota_exhausted", result["rejection_reasons"])
        self.assertGreater(result["available_hosts"], 0)

    def test_exact_shape_price_combines_cpu_ram_and_monthly_local_ssd(self):
        prices = MODULE.regional_prices(
            [
                sku("core", "europe-west9", "h", 6_240_000),
                sku("ram", "europe-west9", "GiBy.h", 720_000),
                sku("local_ssd", "europe-west9", "GiBy.mo", 28_700_000),
            ],
            ["europe-west9"],
            NOW,
        )

        expected = (
            Decimal("0.00624") * 8
            + Decimal("0.00072") * 30
            + Decimal("0.0287") * Decimal(375) / Decimal(730)
        )
        self.assertAlmostEqual(prices["europe-west9"]["hourly_usd"], float(expected))

    def test_missing_component_fails_closed(self):
        with self.assertRaisesRegex(Exception, "expected one ram"):
            MODULE.regional_prices(
                [
                    sku("core", "us-south1", "h", 1),
                    sku("local_ssd", "us-south1", "GiBy.mo", 1),
                ],
                ["us-south1"],
                NOW,
            )

    def test_stale_and_future_prices_are_rejected(self):
        self.assertFalse(
            MODULE.price_is_fresh("2026-09-14T18:59:59Z", NOW, 72)
        )
        self.assertFalse(
            MODULE.price_is_fresh("2026-09-17T19:00:01Z", NOW, 72)
        )
        self.assertTrue(
            MODULE.price_is_fresh("2026-09-14T19:00:00Z", NOW, 72)
        )


class PlacementTests(unittest.TestCase):
    market = {
        "heavy_global_maximum_hosts": 28,
        "maximum_heavy_hourly_usd": 8.5,
    }

    def test_duplicate_global_slot_fails_closed(self):
        instances = [
            {"slot": 1, "region": "us-central1"},
            {"slot": 1, "region": "europe-west9"},
        ]
        with self.assertRaisesRegex(Exception, "duplicate global slots"):
            MODULE.proposed_placements(
                self.market, 2, instances, [region("us-central1", 0.2)]
            )

    def test_capacity_exhaustion_falls_through_to_next_eligible_region(self):
        regions = [
            region("europe-west9", 0.08, available=0),
            region("us-south1", 0.15, available=2),
        ]
        placements = MODULE.proposed_placements(self.market, 2, [], regions)

        self.assertEqual([item["slot"] for item in placements], [1, 2])
        self.assertEqual(
            [item["region"] for item in placements], ["us-south1", "us-south1"]
        )

    def test_unvalidated_region_is_never_selected_even_when_cheapest(self):
        regions = [
            region("europe-west9", 0.08, eligible=False),
            region("us-central1", 0.28, available=1, maximum=20),
        ]

        placements = MODULE.proposed_placements(self.market, 1, [], regions)

        self.assertEqual(placements[0]["region"], "us-central1")

    def test_regional_cap_is_enforced(self):
        instances = [
            {"slot": slot, "region": "europe-west9"} for slot in range(1, 11)
        ]
        regions = [
            region("europe-west9", 0.08, available=10, maximum=10),
            region("us-south1", 0.15, available=1),
        ]

        placements = MODULE.proposed_placements(self.market, 11, instances, regions)

        self.assertEqual(placements[0]["region"], "us-south1")
        self.assertEqual(placements[0]["slot"], 11)

    def test_aggregate_price_ceiling_suppresses_placement(self):
        expensive_market = {
            "heavy_global_maximum_hosts": 28,
            "maximum_heavy_hourly_usd": 0.1,
        }

        placements = MODULE.proposed_placements(
            expensive_market, 1, [], [region("us-central1", 0.28, maximum=20)]
        )

        self.assertEqual(placements, [])

    def test_global_cpu_budget_is_shared_across_regions(self):
        regions = [
            region("europe-west9", 0.08, available=8),
            region("us-south1", 0.15, available=8),
        ]
        for item in regions:
            item["global_cpu_hosts"] = 1

        placements = MODULE.proposed_placements(self.market, 10, [], regions)

        self.assertEqual(len(placements), 1)


class BudgetTests(unittest.TestCase):
    def test_global_budget_preserves_headroom_and_auxiliary_reserve(self):
        market = {
            "vcpus_per_heavy_host": 8,
            "global_vcpu_headroom_ratio": 1.25,
            "global_vcpu_reserve": 12,
        }
        quotas = [{"metric": "CPUS_ALL_REGIONS", "limit": 384, "usage": 36}]

        budget = MODULE.global_cpu_budget(market, quotas, runner_hosts=3)

        self.assertEqual(budget["admissible_vcpus"], 307)
        self.assertEqual(budget["auxiliary_vcpus"], 12)
        self.assertEqual(budget["runner_budget_vcpus"], 295)
        self.assertEqual(budget["additional_heavy_hosts"], 33)


class FoundationTests(unittest.TestCase):
    def test_foundation_identity_mismatch_is_ineligible(self):
        policy = {"project_id": "also-ran-ci-runners"}
        candidate = {
            "name": "europe-west9",
            "foundation": {
                "network": "also-ran-ci-runners",
                "subnet": "runner-hosts-europe-west9",
                "subnet_cidr": "10.42.2.0/24",
                "router": "also-ran-ci-runners-europe-west9",
                "nat": "also-ran-ci-runners-europe-west9",
            },
        }
        subnet = {
            "network": "global/networks/wrong-network",
            "ipCidrRange": "10.200.0.0/24",
            "privateIpGoogleAccess": False,
            "purpose": "PRIVATE",
        }
        router = {
            "network": "global/networks/wrong-network",
            "nats": [
                {
                    "name": "also-ran-ci-runners-europe-west9",
                    "natIpAllocateOption": "MANUAL_ONLY",
                    "sourceSubnetworkIpRangesToNat": "LIST_OF_SUBNETWORKS",
                    "logConfig": {"enable": False},
                }
            ],
        }

        with mock.patch.object(MODULE, "gcloud", side_effect=[subnet, router]):
            reasons = MODULE.foundation_reasons(policy, candidate)

        self.assertEqual(
            reasons,
            [
                "subnet_identity_mismatch",
                "router_identity_mismatch",
                "nat_identity_mismatch",
            ],
        )


if __name__ == "__main__":
    unittest.main()
