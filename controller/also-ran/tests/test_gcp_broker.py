import copy
import importlib.util
import base64
import datetime as dt
import json
import os
import tempfile
import threading
import time
import io
import contextlib
import pathlib
import sys
import unittest
from unittest import mock


CONTROLLER = pathlib.Path(__file__).parents[1] / "gcp_broker.py"
SPEC = importlib.util.spec_from_file_location("gcp_broker", CONTROLLER)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

ZONE_CYCLE = [
    "northamerica-northeast1-b",
    "europe-west9-a",
    "us-south1-a",
    "northamerica-northeast1-b",
    "europe-west9-a",
    "us-south1-b",
    "northamerica-northeast1-b",
    "europe-west9-a",
    "us-south1-a",
    "northamerica-northeast1-b",
    "europe-west9-a",
    "us-south1-b",
    "northamerica-northeast1-b",
    "europe-west9-a",
    "northamerica-northeast1-b",
    "europe-west9-a",
    "northamerica-northeast1-b",
    "europe-west9-a",
    "northamerica-northeast1-b",
    "europe-west9-a",
    "us-south1-a",
    "northamerica-northeast1-b",
    "europe-west9-a",
    "us-south1-b",
    "northamerica-northeast1-b",
    "europe-west9-a",
    "us-south1-a",
    "us-south1-b",
]



def serve_compute(api, listing, project_info, region_info, instances, disks):
    """Answer the broker's Compute REST reads the way the API would."""
    api.side_effect = lambda path, query=None: (
        region_info if "/regions/" in path else project_info
    )
    listing.side_effect = lambda project, resource, filter_expression=None: (
        instances if resource == "instances" else disks
    )


def production():
    return {
        "name_prefix": "also-ran-runner-gcp-prod",
        "scale_set_prefix": "also-ran-gcp-prod",
        "workflow_label": "outrunner-gcp-linux-x64",
        "maximum_hosts": 28,
        "minimum_hosts": 0,
        "retention_mode": "demand_only",
        "max_jobs_per_host": 5,
        "quarantine_grace_minutes": 45,
        "scale_down_hold_minutes": 0,
        "vcpus_per_host": 8,
        "autoscale_slot_prefix": "gcp-prod",
        "machine_type": "c4-standard-8-lssd",
        "boot_disk_gb": 32,
        "boot_disk_type": "hyperdisk-balanced",
        "boot_disk_provisioned_iops": 3000,
        "boot_disk_provisioned_throughput": 140,
        "use_local_ssd": True,
        "legacy_image_source_commits": [
            "0224b641773acf0f109e57f49266fe2d21ccf8cb",
            "3ecfb35f41ab507d2911b430e6dcc462166002b0",
            "802c6e4d92d803f11a0917a9738e2b06d6824d5f",
            "345a7c9ea4a29d78d5c2a9525303f54bb8a47e09",
            "b9b708a73ebf0d875ea6cb3365712482a5851ac7",
            "34096fbe83b155118c4ae7c40015028cc8c75c4d",
            "ff320dd63dbbaff632112f44913c42ef242fcedf",
            "b51c5c3c8d9fddfd4b8e8956aea76caa26b76a70",
            "d214de87c1413f742e68274f29f6f666cf4792fb",
            "d0c6c120b154bcddba6e6b9bcca467d04389eece",
            "11eb72580a70f5a16ceffbe51c7a1d3a1f9ead57",
            "4c5d0f5957e3f73c187603a9d3a11a34ccec8d97",
            "c37c1ddeebbb6a70315aeea13ffaaa2ab4400273",
            "e6212447bbcf1c9ba22d6801511552d85c09afd0",
            "516c610baa768bb9671d3a7be7f5d1f267be989f",
            "d6710c63ff47dd19bd4fba201f4ff418293e6615",
            "9e2e3badb634cf91fa4c9a1fc406d3f69d06a468",
            "98b42a6777c89bf85ccf6193ceee15a945ddb2b8",
            "0be0ea585f3a60b9c90dfdadc8e566e3447100f2",
            "9584a725ccc31ce39bc4a6bf456e44eb78ffa314",
            "2d6eb86500960801432a1b0de0baf5cf89234c04",
            "dc0561b75639783085ddf8f5b9b25f60d028eb2b",
            "ad56297e0b69fdf5ed1b5150217e582684745438",
            "c1cf893f1bea3b6d16b29d1e78b41369c92699d9",
            "4dd9c63990aa1d6561809942c9f5378629e61874",
            "8f9ea5b08b7393f3e84f2a7cecd9288d978ca2e0",
            "c32d2fdd7a46643cfe91b1b1bf2f6334dd28672c",
            "5a96761e971eca8d8dfd4698b64f4a0c71b9d2ee",
            "947f52435d756a25cff3784182160306be7f8c2c",
            "3205ccbaeb3e50b665a385a206bcba32f6f885d9",
            "3f30f1824821d53f0f542e9d1914117715348456",
        ],
        "labels": {
            "managed-by": "runner-autoscaler",
            "fleet": "also-ran-ci",
            "lifecycle": "burst",
            "provider": "gcp",
            "hold": "true",
        },
        "automatic_delete": False,
        "regional_placement_enabled": True,
    }


def general_production():
    return {
        "name_prefix": "also-ran-runner-gcp-general",
        "scale_set_prefix": "also-ran-gcp-general",
        "workflow_label": "outrunner-gcp-general-linux-x64",
        "maximum_hosts": 12,
        "minimum_hosts": 0,
        "retention_mode": "demand_only",
        "max_jobs_per_host": 10,
        "quarantine_grace_minutes": 45,
        "scale_down_hold_minutes": 0,
        "vcpus_per_host": 4,
        "autoscale_slot_prefix": "gcp-general",
        "machine_type": "c4-standard-4-lssd",
        "boot_disk_gb": 32,
        "boot_disk_type": "hyperdisk-balanced",
        "boot_disk_provisioned_iops": 3000,
        "boot_disk_provisioned_throughput": 140,
        "use_local_ssd": True,
        "legacy_image_source_commits": [
            "0224b641773acf0f109e57f49266fe2d21ccf8cb",
            "3ecfb35f41ab507d2911b430e6dcc462166002b0",
            "802c6e4d92d803f11a0917a9738e2b06d6824d5f",
            "345a7c9ea4a29d78d5c2a9525303f54bb8a47e09",
            "b9b708a73ebf0d875ea6cb3365712482a5851ac7",
            "34096fbe83b155118c4ae7c40015028cc8c75c4d",
            "ff320dd63dbbaff632112f44913c42ef242fcedf",
            "b51c5c3c8d9fddfd4b8e8956aea76caa26b76a70",
            "d214de87c1413f742e68274f29f6f666cf4792fb",
            "d0c6c120b154bcddba6e6b9bcca467d04389eece",
            "11eb72580a70f5a16ceffbe51c7a1d3a1f9ead57",
            "4c5d0f5957e3f73c187603a9d3a11a34ccec8d97",
            "d6710c63ff47dd19bd4fba201f4ff418293e6615",
            "9e2e3badb634cf91fa4c9a1fc406d3f69d06a468",
            "98b42a6777c89bf85ccf6193ceee15a945ddb2b8",
            "0be0ea585f3a60b9c90dfdadc8e566e3447100f2",
            "9584a725ccc31ce39bc4a6bf456e44eb78ffa314",
            "2d6eb86500960801432a1b0de0baf5cf89234c04",
            "dc0561b75639783085ddf8f5b9b25f60d028eb2b",
            "ad56297e0b69fdf5ed1b5150217e582684745438",
            "c1cf893f1bea3b6d16b29d1e78b41369c92699d9",
            "4dd9c63990aa1d6561809942c9f5378629e61874",
            "8f9ea5b08b7393f3e84f2a7cecd9288d978ca2e0",
            "c32d2fdd7a46643cfe91b1b1bf2f6334dd28672c",
            "5a96761e971eca8d8dfd4698b64f4a0c71b9d2ee",
            "947f52435d756a25cff3784182160306be7f8c2c",
            "3205ccbaeb3e50b665a385a206bcba32f6f885d9",
            "3f30f1824821d53f0f542e9d1914117715348456",
        ],
        "zone_cycle": ZONE_CYCLE[:12],
        "labels": {
            "managed-by": "runner-autoscaler",
            "fleet": "also-ran-ci",
            "lifecycle": "general",
            "provider": "gcp",
            "hold": "true",
        },
        "automatic_delete": True,
        "regional_placement_enabled": True,
        "create_enabled": True,
        "include_shared_label": False,
    }


def instance(slot, metadata=None, status="RUNNING"):
    return MODULE.Instance(
        slot=slot,
        instance_id=str(1000 + slot),
        name=f"also-ran-runner-gcp-prod-{slot:02d}",
        zone="us-central1-a",
        status=status,
        labels={"source-commit": "a" * 12},
        metadata=metadata or {},
    )


def broker_policy(automatic_delete=True):
    return {
        "policy_version": 1,
        "project_id": "also-ran-ci-runners",
        "project_number": "123456789012",
        "image_source_commit": "a" * 40,
        "machine_type": "n2-standard-8",
        "production": {
            **production(),
            "zone_cycle": ZONE_CYCLE,
            "create_enabled": True,
            "automatic_delete": automatic_delete,
            "include_shared_label": False,
        },
        "general_production": general_production(),
        "network": {
            "name": "also-ran-ci-runners",
            "egress": {
                "mode": "direct",
                "network_tier": "PREMIUM",
                "public_ingress_allowlist": ["192.0.2.1/32", "35.235.240.0/20"],
            },
        },
        "market": {
            "regions": [
                {
                    "name": region,
                    "zones": [
                        zone for zone in ZONE_CYCLE if zone.startswith(region + "-")
                    ],
                    "canary_valid": True,
                    "foundation": {"subnet": "runner-hosts-" + region},
                }
                for region in (
                    "northamerica-northeast1",
                    "europe-west9",
                    "us-south1",
                )
            ]
        },
    }


def receipt(slot=1, instance_id="1001"):
    scale_set = f"also-ran-gcp-prod-{slot:02d}"
    return {
        "version": 1,
        "status": "drained",
        "scale_set": scale_set,
        "completed_jobs": 1,
        "max_jobs": 1,
        "drained_at": "2026-09-16T22:56:59.634649Z",
        "identity": {
            "autoscale_slot": f"gcp-prod-{slot:02d}",
            "boot_id": "33a83c22-b38c-4b20-a560-4f40f5da7307",
            "image_source_commit": "a" * 12,
            "instance_id": instance_id,
            "instance_name": f"also-ran-runner-gcp-prod-{slot:02d}",
            "project_id": "also-ran-ci-runners",
            "provider": "gcp",
            "zone": "us-central1-a",
        },
        "jobs": [
            {
                "runner_name": scale_set + "-2baa95cf",
                "runner_id": 761,
                "runner_request_id": 0,
                "job_id": "a419e6cd-0dba-5474-a3e7-573eca92c397",
                "workflow_run_id": 35159242947,
                "repository": "example-org/example-repo",
                "result": "succeeded",
                "finished_at": "2026-09-16T22:56:58.128215Z",
            }
        ],
    }


def admission_closed(slot=1, instance_id="1001"):
    drained = receipt(slot, instance_id)
    return {
        "version": 1,
        "status": "admission_closed",
        "scale_set": drained["scale_set"],
        "closed_at": drained["drained_at"],
        "identity": drained["identity"],
    }


def receipt_v3(slot=1, instance_id="1001"):
    value = receipt(slot, instance_id)
    value["version"] = 3
    value["drained_at"] = "2026-09-18T13:45:03.547136Z"
    value["jobs"][0].update(
        {
            "job_workflow_ref": (
                "example-org/example-repo/.github/workflows/tests.yml@"
                "refs/pull/1/merge"
            ),
            "job_display_name": "Migration replay (clean slate)",
            "request_labels": ["outrunner-gcp-linux-x64"],
            "result": "canceled",
            "queue_time": "2026-09-18T13:34:02Z",
            "scale_set_assign_time": "2026-09-18T13:44:55Z",
            "runner_assign_time": "2026-09-18T13:44:58Z",
            "finished_at": "0001-01-01T00:00:00Z",
        }
    )
    return value


class DecisionTests(unittest.TestCase):
    def test_general_pool_is_cold_when_inactive_and_caps_at_twelve(self):
        config = general_production()
        idle = MODULE.decide(
            config,
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (),
        )
        busy = MODULE.decide(
            config,
            MODULE.Demand(queued=15, active_owned=0, rate_remaining=1000),
            (),
        )
        self.assertEqual(idle["desired_hosts"], 0)
        self.assertEqual(idle["create_slots"], [])
        self.assertEqual(busy["desired_hosts"], 12)
        self.assertEqual(len(busy["create_slots"]), 12)

    def test_one_general_job_creates_only_one_host(self):
        result = MODULE.decide(
            general_production(),
            MODULE.Demand(queued=1, active_owned=0, rate_remaining=1000),
            (),
        )

        self.assertEqual(result["desired_hosts"], 1)
        self.assertEqual(result["create_slots"], [1])
        self.assertEqual(result["retention_mode"], "demand_only")

    def test_both_pools_are_zero_demand_only(self):
        for config in (production(), general_production()):
            with self.subTest(pool=config["scale_set_prefix"]):
                result = MODULE.decide(
                    config,
                    MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
                    (),
                )
                self.assertEqual(result["desired_hosts"], 0)
                self.assertEqual(result["retention_mode"], "demand_only")

    def test_idle_hosts_enter_identity_bound_drain_immediately(self):
        config = general_production()
        config["automatic_delete"] = True
        hosts = tuple(instance(slot) for slot in range(1, 5))

        result = MODULE.decide(
            config,
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            hosts,
        )

        self.assertEqual(result["action"], "drain")
        self.assertEqual(result["desired_hosts"], 0)
        self.assertEqual(result["drain_request_slots"], [1, 2, 3, 4])

    def test_policy_rejects_idle_retention(self):
        candidate = broker_policy()
        candidate["general_production"]["retention_mode"] = "warm_pool"

        with self.assertRaisesRegex(MODULE.BrokerError, "general production policy"):
            MODULE.validate_policy(candidate, "general")

    def test_policy_rejects_legacy_warm_pool_configuration(self):
        candidate = broker_policy()
        candidate["production"]["warm_pool"] = {
            "active_minimum_hosts": 1,
            "idle_timeout_minutes": 15,
        }

        with self.assertRaisesRegex(MODULE.BrokerError, "heavy production policy"):
            MODULE.validate_policy(candidate)

    def test_policy_selects_distinct_general_pool(self):
        selected = MODULE.validate_policy(broker_policy(), "general")
        self.assertEqual(selected["machine_type"], "c4-standard-4-lssd")
        self.assertEqual(selected["workflow_label"], "outrunner-gcp-general-linux-x64")

    def test_repository_policy_validates_for_both_pools(self):
        policy_path = CONTROLLER.with_name("gcp-acceptance-policy.json")
        policy = json.loads(policy_path.read_text())

        heavy = MODULE.validate_policy(policy, "heavy")
        general = MODULE.validate_policy(policy, "general")

        self.assertEqual(heavy["scale_set_prefix"], "also-ran-gcp-prod")
        self.assertEqual(general["scale_set_prefix"], "also-ran-gcp-general")

    def test_queue_creates_one_host_per_missing_job(self):
        result = MODULE.decide(
            production(),
            MODULE.Demand(queued=7, active_owned=0, rate_remaining=1000),
            (),
        )
        self.assertEqual(result["desired_hosts"], 7)
        self.assertEqual(result["create_slots"], list(range(1, 8)))

    def test_active_and_queued_jobs_preserve_busy_capacity(self):
        result = MODULE.decide(
            production(),
            MODULE.Demand(queued=5, active_owned=3, rate_remaining=1000),
            tuple(instance(slot) for slot in range(1, 6)),
        )
        self.assertEqual(result["desired_hosts"], 8)
        self.assertEqual(result["create_slots"], [6, 7, 8])

    def test_cap_is_twenty_eight_and_absent_slots_are_stable(self):
        existing = (instance(1), instance(3), instance(28))
        result = MODULE.decide(
            production(),
            MODULE.Demand(queued=50, active_owned=0, rate_remaining=1000),
            existing,
        )
        self.assertEqual(result["desired_hosts"], 28)
        self.assertEqual(result["create_slots"][:3], [2, 4, 5])
        self.assertEqual(len(result["create_slots"]), 25)

    def test_surplus_idle_capacity_drains_while_jobs_are_queued(self):
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=2, active_owned=0, rate_remaining=1000),
            (instance(1), instance(2), instance(3)),
        )
        self.assertEqual(result["action"], "drain")
        self.assertEqual(result["create_slots"], [])
        self.assertEqual(result["drain_request_slots"], [3])

    def test_active_slots_are_kept_and_idle_surplus_drains(self):
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(
                queued=0,
                active_owned=2,
                rate_remaining=1000,
                active_slots=frozenset({1, 3}),
            ),
            (instance(1), instance(2), instance(3)),
        )

        self.assertEqual(result["desired_hosts"], 2)
        self.assertEqual(result["drain_request_slots"], [2])

    def test_active_job_on_draining_host_does_not_keep_or_create_idle_capacity(self):
        config = production()
        config["automatic_delete"] = True
        demand = MODULE.Demand(
            queued=0,
            active_owned=1,
            rate_remaining=1000,
            active_slots=frozenset({1}),
        )

        result = MODULE.decide(
            config,
            demand,
            (drain_requested(1), instance(2)),
        )
        fast = MODULE.fast_scale_up_decision(
            config,
            demand,
            (drain_requested(1),),
        )

        self.assertEqual(result["active_nonserving_slots"], [1])
        self.assertEqual(result["desired_serving_hosts"], 0)
        self.assertEqual(result["drain_request_slots"], [2])
        self.assertEqual(result["create_slots"], [])
        self.assertEqual(fast["active_nonserving_slots"], [1])
        self.assertEqual(fast["desired_serving_hosts"], 0)
        self.assertEqual(fast["create_slots"], [])

    def test_queue_still_gets_capacity_when_active_job_is_draining(self):
        demand = MODULE.Demand(
            queued=1,
            active_owned=1,
            rate_remaining=1000,
            active_slots=frozenset({1}),
        )

        result = MODULE.fast_scale_up_decision(
            production(), demand, (drain_requested(1),)
        )

        self.assertEqual(result["desired_serving_hosts"], 1)
        self.assertEqual(result["create_slots"], [2])

    def test_unattributed_active_job_suppresses_scale_down_for_safety(self):
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(
                queued=0,
                active_owned=1,
                rate_remaining=1000,
                unattributed_active=1,
            ),
            (instance(1), instance(2), instance(3)),
        )

        self.assertEqual(result["desired_hosts"], 1)
        self.assertEqual(result["drain_request_slots"], [])
        self.assertEqual(
            result["scale_down_suppressed_reason"],
            "active_runner_attribution_incomplete",
        )

    def test_fast_scale_up_fills_absent_slots_before_drained_hosts_are_reaped(self):
        decision = MODULE.fast_scale_up_decision(
            production(),
            MODULE.Demand(queued=4, active_owned=1, rate_remaining=1000),
            (instance(1), drain_requested(4)),
        )

        self.assertEqual(decision["serving_hosts"], 1)
        self.assertEqual(decision["create_slots"], [2, 3, 5, 6])
        self.assertEqual(decision["action"], "create")

    def test_market_refresh_during_queue_scan_uses_current_clock(self):
        candidate = json.loads((CONTROLLER.parent / "gcp-acceptance-policy.json").read_text())
        start = dt.datetime(2026, 9, 27, 6, 50, tzinfo=dt.timezone.utc)
        times = iter([start, start + dt.timedelta(minutes=6)])
        class Clock(dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return next(times)
        market = {"mode": "dry-run", "mutation_enabled": False,
                  "price_source": "Cloud Billing Catalog",
                  "price_observed_at": (start + dt.timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                  "regions": [{"name": item["name"], "eligible": True,
                               "supported_zones": item["zones"],
                               "price": {"hourly_usd": 0.01}}
                              for item in candidate["market"]["regions"]]}
        with tempfile.TemporaryDirectory() as root:
            policy_path = pathlib.Path(root) / "policy.json"
            market_path = pathlib.Path(root) / "market.json"
            state_path = pathlib.Path(root) / "state.json"
            policy_path.write_text(json.dumps(candidate))
            market_path.write_text(json.dumps(market))
            with (mock.patch.object(MODULE.dt, "datetime", Clock),
                  mock.patch.object(MODULE, "read_secret", return_value="test-only"),
                  mock.patch.object(MODULE.GitHubDemandObserver, "observe",
                                    return_value=MODULE.Demand(queued=1, active_owned=0, rate_remaining=4000)),
                  mock.patch.object(MODULE, "observe_instances", return_value=()),
                  mock.patch.object(MODULE, "observe_create_capacity",
                                    return_value={"global_cpu_hosts": 10, "available_hosts": 10}),
                  contextlib.redirect_stdout(io.StringIO())):
                self.assertEqual(MODULE.main(["--policy", str(policy_path), "--market-state", str(market_path),
                    "--state", str(state_path), "--lock", root + "/lock", "--github-token-file", "unused",
                    "--once", "--dry-run", "--scale-up-only"]), 0)
            self.assertTrue(json.loads(state_path.read_text())["create_placements"])

    def test_fast_scale_up_follows_current_demand_only(self):
        busy = MODULE.fast_scale_up_decision(
            general_production(),
            MODULE.Demand(queued=1, active_owned=0, rate_remaining=1000),
            (),
        )
        idle = MODULE.fast_scale_up_decision(
            general_production(),
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (),
        )

        self.assertEqual(busy["desired_hosts"], 1)
        self.assertEqual(busy["create_slots"], [1])
        self.assertEqual(idle["desired_hosts"], 0)
        self.assertEqual(idle["create_slots"], [])
        self.assertEqual(busy["retention_mode"], "demand_only")

    def test_fast_scale_up_persists_slot_attribution_for_safe_cleanup(self):
        decision = MODULE.fast_scale_up_decision(
            production(),
            MODULE.Demand(
                queued=0,
                active_owned=2,
                rate_remaining=1000,
                active_slots=frozenset({2}),
                unattributed_active=1,
            ),
            (instance(1), instance(2)),
        )

        self.assertEqual(decision["active_slots"], [2])
        self.assertEqual(decision["unattributed_active_jobs"], 1)

    def test_newer_fast_observation_suppresses_only_this_stale_drain(self):
        decision = {
            "observed_at": "2026-09-20T12:00:00Z",
            "drain_request_slots": [1],
            "delete_slots": [],
            "create_slots": [],
            "action": "drain",
            "scale_down_suppressed_reason": None,
        }
        peer = {
            "observed_at": "2026-09-20T12:00:01Z",
            "queued_jobs": 1,
            "active_owned_jobs": 0,
        }

        suppressed = MODULE.suppress_stale_scale_down(decision, peer)

        self.assertTrue(suppressed)
        self.assertEqual(decision["drain_request_slots"], [])
        self.assertEqual(decision["action"], "none")
        self.assertEqual(
            decision["scale_down_suppressed_reason"],
            "newer_scale_up_demand",
        )

    def test_older_or_zero_fast_observation_does_not_retain_capacity(self):
        for peer in (
            {
                "observed_at": "2026-09-20T11:59:59Z",
                "queued_jobs": 1,
                "active_owned_jobs": 0,
            },
            {
                "observed_at": "2026-09-20T12:00:01Z",
                "queued_jobs": 0,
                "active_owned_jobs": 0,
            },
        ):
            with self.subTest(peer=peer):
                decision = {
                    "observed_at": "2026-09-20T12:00:00Z",
                    "drain_request_slots": [1],
                    "delete_slots": [],
                    "create_slots": [],
                    "action": "drain",
                    "scale_down_suppressed_reason": None,
                }

                self.assertFalse(MODULE.suppress_stale_scale_down(decision, peer))
                self.assertEqual(decision["drain_request_slots"], [1])

    def test_fast_scale_up_never_reuses_an_occupied_reclaimable_slot(self):
        decision = MODULE.fast_scale_up_decision(
            production(),
            MODULE.Demand(queued=1, active_owned=1, rate_remaining=1000),
            (instance(1), instance(2, status="TERMINATED")),
        )

        self.assertEqual(decision["create_slots"], [3])

    def test_delete_disabled_also_disables_external_drain_requests(self):
        result = MODULE.decide(
            production(),
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (instance(1),),
        )
        self.assertEqual(result["action"], "none")
        self.assertEqual(result["drain_request_slots"], [])

    def test_identity_bound_drain_request_is_not_serving_capacity(self):
        draining = instance(2, {"runner-drain-request": "1002"})
        result = MODULE.decide(
            production(),
            MODULE.Demand(queued=2, active_owned=0, rate_remaining=1000),
            (instance(1), draining),
        )
        self.assertEqual(result["serving_hosts"], 1)
        self.assertEqual(result["drain_requested_slots"], [2])
        self.assertEqual(result["create_slots"], [3])

    def test_unverifiable_drain_quarantines_only_its_slot(self):
        result = MODULE.decide(
            production(),
            MODULE.Demand(queued=3, active_owned=0, rate_remaining=1000),
            (instance(1), instance(3)),
            quarantined={1: "drain receipt runner is ambiguous in GitHub"},
        )

        self.assertEqual(result["serving_hosts"], 1)
        self.assertEqual(
            result["quarantined_slots"],
            {1: "drain receipt runner is ambiguous in GitHub"},
        )
        self.assertEqual(result["delete_slots"], [])
        self.assertEqual(result["create_slots"], [2, 4])

    def test_idle_unreadable_quarantine_is_drain_requested(self):
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (instance(1),),
            quarantined={1: "GCP serial drain receipt is malformed"},
        )

        self.assertEqual(result["drain_request_slots"], [1])
        self.assertEqual(result["delete_slots"], [])
        self.assertEqual(result["action"], "drain")

    def test_busy_unreadable_quarantine_is_not_drain_requested(self):
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(
                queued=0,
                active_owned=1,
                rate_remaining=1000,
                active_slots=frozenset({1}),
            ),
            (instance(1),),
            quarantined={1: "GCP serial drain receipt is malformed"},
        )

        self.assertEqual(result["drain_request_slots"], [])
        self.assertEqual(result["delete_slots"], [])

    def test_unattributed_job_blocks_unreadable_quarantine_drain(self):
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(
                queued=0,
                active_owned=1,
                rate_remaining=1000,
                unattributed_active=1,
            ),
            (instance(1),),
            quarantined={1: "GCP serial drain receipt is malformed"},
        )

        self.assertEqual(result["drain_request_slots"], [])
        self.assertEqual(
            result["scale_down_suppressed_reason"],
            "active_runner_attribution_incomplete",
        )

    def test_only_a_parsed_receipt_marks_a_quarantined_slot_idle(self):
        # The expiry safety argument rests entirely on this split: a slot is
        # idle_confirmed only when its receipt parsed, which means Outrunner
        # already finished. A slot we could not read stays ineligible for the
        # direct quarantine-expiry path; the drain-stall path must first close
        # admission and prove continuous GitHub idleness.
        hosts = (instance(1), instance(2), instance(3), instance(4))
        receipts = {
            1: {"jobs": []},
            2: None,
            4: {"jobs": []},
        }

        def fake_receipt(item, policy, repository, production, contents=None, guest_cache=None):
            if item.slot == 3:
                raise MODULE.ObservationError(
                    "GCP serial output response is incomplete"
                )
            return receipts[item.slot]

        def fake_verify(receipt, observer, label):
            if receipt is receipts[4]:
                raise MODULE.ObservationError(
                    "drain receipt runner is ambiguous in GitHub"
                )

        observer = mock.Mock(repository="example-org/example-repo")
        with mock.patch.object(
            MODULE, "serial_console_contents", return_value=""
        ), mock.patch.object(
            MODULE, "serial_drain_receipt", fake_receipt
        ), mock.patch.object(
            MODULE, "verify_receipt_jobs", fake_verify
        ), mock.patch.object(MODULE, "guest_attributes", return_value=None):
            drained, quarantined, idle_confirmed, admission_closed_slots = (
                MODULE.observe_drained_instances(
                    hosts, broker_policy(), production(), observer
                )
            )

        self.assertEqual(sorted(drained), [1])
        self.assertEqual(
            quarantined,
            {
                3: "GCP serial output response is incomplete",
                4: "drain receipt runner is ambiguous in GitHub",
            },
        )
        # slot 3 could not be read, so it is never eligible for expiry
        self.assertEqual(idle_confirmed, {4})
        self.assertEqual(admission_closed_slots, set())

    def test_stopping_host_holds_its_slot_but_does_not_serve(self):
        # A STOPPING host still exists in GCP, so creating into its slot
        # would collide on the instance name. It keeps the slot and the
        # replacement goes somewhere else.
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=2, active_owned=0, rate_remaining=1000),
            (instance(1), instance(2, status="STOPPING")),
        )

        self.assertEqual(result["transitional_slots"], [2])
        self.assertEqual(result["serving_hosts"], 1)
        self.assertEqual(result["delete_slots"], [])
        self.assertNotIn(2, result["create_slots"])
        self.assertEqual(result["create_slots"], [3])

    def test_terminated_host_is_reclaimed_so_its_slot_returns(self):
        # A preempted Spot host stays TERMINATED forever. It runs nothing,
        # so deleting it is safe and is the only way the slot comes back.
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=1, active_owned=0, rate_remaining=1000),
            (instance(1, status="TERMINATED"),),
        )

        self.assertEqual(result["reclaimable_slots"], [1])
        self.assertEqual(result["delete_slots"], [1])
        self.assertEqual(result["serving_hosts"], 0)
        self.assertEqual(result["action"], "replace")

    def test_departing_host_is_never_drain_requested(self):
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (instance(1, status="STOPPING"),),
        )

        self.assertEqual(result["drain_request_slots"], [])
        self.assertEqual(result["action"], "none")

    def test_expired_quarantine_is_deleted_so_the_fleet_can_reach_zero(self):
        # An idle host whose receipt GitHub will never confirm must not
        # outlive the grace window: it bills, holds its slot against
        # maximum_hosts, and keeps desired=0 from ever being reached.
        now = dt.datetime(2026, 9, 18, 7, 0, tzinfo=dt.timezone.utc)
        ledger = {
            "1": {
                "instance_id": "1001",
                "since": "2026-09-18T06:10:00Z",
                "reason": "drain receipt runner is ambiguous in GitHub",
                "idle_confirmed": True,
            }
        }
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (instance(1),),
            quarantined={1: "drain receipt runner is ambiguous in GitHub"},
            quarantine_ledger=ledger,
            now=now,
        )

        self.assertEqual(result["expired_quarantine_slots"], [1])
        self.assertEqual(result["delete_slots"], [1])
        self.assertEqual(result["drain_request_slots"], [])
        self.assertEqual(result["action"], "delete")
        self.assertEqual(result["create_slots"], [])

    def test_quarantine_inside_the_grace_window_is_left_alone(self):
        now = dt.datetime(2026, 9, 18, 6, 50, tzinfo=dt.timezone.utc)
        ledger = {
            "1": {
                "instance_id": "1001",
                "since": "2026-09-18T06:10:00Z",
                "reason": "drain receipt runner is ambiguous in GitHub",
                "idle_confirmed": True,
            }
        }
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (instance(1),),
            quarantined={1: "drain receipt runner is ambiguous in GitHub"},
            quarantine_ledger=ledger,
            now=now,
        )

        self.assertEqual(result["expired_quarantine_slots"], [])
        self.assertEqual(result["delete_slots"], [])

    def test_unreadable_receipt_never_uses_direct_quarantine_expiry(self):
        # idle_confirmed is False when the receipt could not be read at all,
        # so the host might still hold a job. Quarantine age alone must not
        # delete it; the separate drain-stall proof owns this case.
        now = dt.datetime(2026, 9, 18, 12, 0, tzinfo=dt.timezone.utc)
        ledger = {
            "1": {
                "instance_id": "1001",
                "since": "2026-09-18T06:10:00Z",
                "reason": "GCP serial output response is incomplete",
                "idle_confirmed": False,
            }
        }
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (instance(1),),
            quarantined={1: "GCP serial output response is incomplete"},
            quarantine_ledger=ledger,
            now=now,
        )

        self.assertEqual(result["expired_quarantine_slots"], [])
        self.assertEqual(result["delete_slots"], [])

    def test_quarantine_clock_restarts_when_a_slot_is_recycled(self):
        # Slot 1 previously held instance 999; the live VM is a different
        # host, so it must not inherit the dead one's age and be deleted
        # while it is running a job.
        now = dt.datetime(2026, 9, 18, 7, 0, tzinfo=dt.timezone.utc)
        prior = {
            "quarantine_since": {
                "1": {
                    "instance_id": "999",
                    "since": "2026-09-18T06:10:00Z",
                    "reason": "drain receipt runner is ambiguous in GitHub",
                    "idle_confirmed": True,
                }
            }
        }
        ledger = MODULE.track_quarantine(
            prior,
            (instance(1),),
            {1: "drain receipt runner is ambiguous in GitHub"},
            {1},
            now,
        )

        self.assertEqual(ledger["1"]["instance_id"], "1001")
        self.assertEqual(ledger["1"]["since"], "2026-09-18T07:00:00Z")
        self.assertEqual(MODULE.expired_quarantine_slots(ledger, 45, now), [])

    def test_quarantine_clock_carries_across_ticks_for_the_same_host(self):
        now = dt.datetime(2026, 9, 18, 7, 0, tzinfo=dt.timezone.utc)
        prior = {
            "quarantine_since": {
                "1": {
                    "instance_id": "1001",
                    "since": "2026-09-18T06:10:00Z",
                    "reason": "drain receipt runner is ambiguous in GitHub",
                    "idle_confirmed": True,
                }
            }
        }
        ledger = MODULE.track_quarantine(
            prior,
            (instance(1),),
            {1: "drain receipt runner is ambiguous in GitHub"},
            {1},
            now,
        )

        self.assertEqual(ledger["1"]["since"], "2026-09-18T06:10:00Z")
        self.assertEqual(MODULE.expired_quarantine_slots(ledger, 45, now), [1])

    def test_quarantine_expiry_is_off_when_automatic_delete_is_off(self):
        now = dt.datetime(2026, 9, 18, 12, 0, tzinfo=dt.timezone.utc)
        ledger = {
            "1": {
                "instance_id": "1001",
                "since": "2026-09-18T06:10:00Z",
                "reason": "drain receipt runner is ambiguous in GitHub",
                "idle_confirmed": True,
            }
        }
        result = MODULE.decide(
            production(),
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (instance(1),),
            quarantined={1: "drain receipt runner is ambiguous in GitHub"},
            quarantine_ledger=ledger,
            now=now,
        )

        self.assertEqual(result["expired_quarantine_slots"], [])
        self.assertEqual(result["delete_slots"], [])

    def test_low_github_rate_limit_suppresses_create(self):
        result = MODULE.decide(
            production(),
            MODULE.Demand(queued=1, active_owned=0, rate_remaining=499),
            (),
        )
        self.assertEqual(result["action"], "none")
        self.assertEqual(result["create_slots"], [])
        self.assertEqual(result["suppressed_reason"], "github_rate_limit_low")

    @mock.patch.object(MODULE, "compute_aggregated_list")
    def test_reviewed_legacy_image_is_owned_during_rollout(self, listing):
        candidate = broker_policy()
        listing.return_value = [
            {
                "name": "also-ran-runner-gcp-prod-03",
                "zone": "zones/us-central1-c",
                "status": "RUNNING",
                "id": "123456789",
                "labels": {
                    **candidate["production"]["labels"],
                    "autoscale-slot": "gcp-prod-03",
                    "source-commit": "c37c1ddeebbb",
                },
            }
        ]

        observed = MODULE.observe_instances(candidate)

        self.assertEqual(observed[0].slot, 3)

    @mock.patch.object(MODULE, "compute_aggregated_list")
    def test_departing_host_does_not_kill_the_whole_tick(self, listing):
        # A single STOPPING or TERMINATED host used to raise out of
        # observe_instances and abort reconciliation for the entire pool,
        # so no other host drained or was deleted that tick.
        candidate = broker_policy()
        listing.return_value = [
            {
                "name": f"also-ran-runner-gcp-prod-0{slot}",
                "zone": "zones/us-central1-c",
                "status": status,
                "id": f"12345678{slot}",
                "labels": {
                    **candidate["production"]["labels"],
                    "autoscale-slot": f"gcp-prod-0{slot}",
                    "source-commit": candidate["image_source_commit"][:12],
                },
            }
            for slot, status in ((1, "RUNNING"), (2, "STOPPING"), (3, "TERMINATED"))
        ]

        observed = MODULE.observe_instances(candidate)

        self.assertEqual([item.slot for item in observed], [1, 2, 3])
        self.assertEqual(
            [item.status for item in observed],
            ["RUNNING", "STOPPING", "TERMINATED"],
        )

    @mock.patch.object(MODULE, "compute_aggregated_list")
    def test_genuinely_unknown_status_still_stops_the_tick(self, listing):
        candidate = broker_policy()
        listing.return_value = [
            {
                "name": "also-ran-runner-gcp-prod-01",
                "zone": "zones/us-central1-c",
                "status": "NONSENSE",
                "id": "123456789",
                "labels": {
                    **candidate["production"]["labels"],
                    "autoscale-slot": "gcp-prod-01",
                    "source-commit": candidate["image_source_commit"][:12],
                },
            }
        ]

        with self.assertRaises(MODULE.ObservationError):
            MODULE.observe_instances(candidate)

    def test_verified_drained_slot_uses_a_vacant_replacement_when_available(self):
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=1, active_owned=0, rate_remaining=1000),
            (instance(1),),
            {1: receipt()},
        )
        self.assertEqual(result["action"], "replace")
        self.assertEqual(result["delete_slots"], [1])
        self.assertEqual(result["create_slots"], [2])

    def test_full_pool_reuses_a_verified_drained_slot_after_delete(self):
        config = production()
        config["automatic_delete"] = True
        hosts = tuple(instance(slot) for slot in range(1, 29))
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=1, active_owned=27, rate_remaining=1000),
            hosts,
            {28: receipt(slot=28, instance_id="1028")},
        )

        self.assertEqual(result["delete_slots"], [28])
        self.assertEqual(result["create_slots"], [28])

    def test_verified_drained_slot_is_deleted_when_demand_is_zero(self):
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (instance(1),),
            {1: receipt()},
        )
        self.assertEqual(result["action"], "delete")
        self.assertEqual(result["delete_slots"], [1])
        self.assertEqual(result["create_slots"], [])


class DrainReceiptTests(unittest.TestCase):
    def test_host_evidence_is_read_concurrently_with_per_slot_outcomes(self):
        hosts = tuple(instance(slot) for slot in range(1, 9))
        proven = receipt(slot=1)
        unverified = receipt(slot=3)
        lock = threading.Lock()
        active = [0, 0]  # current, peak

        def fake_serial(item, policy):
            with lock:
                active[0] += 1
                active[1] = max(active[1], active[0])
            time.sleep(0.05)
            with lock:
                active[0] -= 1
            if item.slot == 2:
                raise MODULE.ObservationError("serial unavailable")
            return str(item.slot)

        def fake_receipt(item, policy, repositories, production, contents=None, guest_cache=None):
            return {"1": proven, "3": unverified}.get(contents)

        def fake_verify(candidate, observer, label):
            if candidate is unverified:
                raise MODULE.ObservationError("drain receipt runner is ambiguous in GitHub")

        observer = mock.Mock(known_repositories=("example-org/example-repo",))
        timer = MODULE.PhaseTimer()
        with mock.patch.object(MODULE, "serial_console_contents", fake_serial), \
                mock.patch.object(MODULE, "serial_drain_receipt", fake_receipt), \
                mock.patch.object(MODULE, "guest_drain_receipt", return_value=None), \
                mock.patch.object(MODULE, "serial_admission_closed", return_value=None), \
                mock.patch.object(MODULE, "verify_receipt_jobs", fake_verify):
            drained, quarantined, idle_confirmed, closed = MODULE.observe_drained_instances(
                hosts, broker_policy(), production(), observer, timer
            )

        self.assertGreater(active[1], 1)
        self.assertLessEqual(active[1], MODULE.RECEIPT_OBSERVATION_WORKERS)
        self.assertEqual(drained, {1: proven})
        self.assertEqual(
            quarantined,
            {2: "serial unavailable", 3: "drain receipt runner is ambiguous in GitHub"},
        )
        self.assertEqual(idle_confirmed, {3})
        self.assertEqual(closed, set())
        self.assertEqual(set(timer.phases), {"receipts_gcp", "receipts_github"})

    def test_phase_timer_attributes_github_requests_to_phase(self):
        client = mock.Mock(requests=0, request_ms=0)
        timer = MODULE.PhaseTimer(client)
        with timer.phase("github_demand"):
            client.requests += 3
            client.request_ms += 120
        with timer.phase("gcp_instances"):
            pass
        with timer.phase("github_demand"):
            client.requests += 2
            client.request_ms += 30

        self.assertEqual(timer.phases["github_demand"]["github_requests"], 5)
        self.assertEqual(timer.phases["github_demand"]["github_ms"], 150)
        self.assertEqual(timer.phases["gcp_instances"]["github_requests"], 0)

    def test_counting_client_counts_failed_requests(self):
        client = MODULE.CountingJsonClient("test-only", "https://example.invalid")
        with mock.patch.object(
            MODULE.JsonClient, "get", side_effect=[{"ok": True}, MODULE.ObservationError("boom")]
        ):
            client.get("/one")
            with self.assertRaises(MODULE.ObservationError):
                client.get("/two")
        self.assertEqual(client.requests, 2)

    def test_maintenance_tick_records_phases(self):
        candidate = json.loads((CONTROLLER.parent / "gcp-acceptance-policy.json").read_text())
        with tempfile.TemporaryDirectory() as root:
            policy_path = pathlib.Path(root) / "policy.json"
            market_path = pathlib.Path(root) / "market.json"
            state_path = pathlib.Path(root) / "state.json"
            policy_path.write_text(json.dumps(candidate))
            market_path.write_text("{}")
            with (mock.patch.object(MODULE, "read_secret", return_value="test-only"),
                  mock.patch.object(MODULE.GitHubDemandObserver, "observe",
                                    return_value=MODULE.Demand(queued=0, active_owned=0, rate_remaining=4000)),
                  mock.patch.object(MODULE, "observe_instances", return_value=()),
                  contextlib.redirect_stdout(io.StringIO())):
                self.assertEqual(MODULE.main(["--policy", str(policy_path), "--market-state", str(market_path),
                    "--state", str(state_path), "--lock", root + "/lock", "--github-token-file", "unused",
                    "--once", "--dry-run", "--maintenance-only"]), 0)
            phases = json.loads(state_path.read_text())["phases"]
        self.assertLessEqual(
            {"github_demand", "gcp_instances", "github_demand_refresh"}, set(phases)
        )
        for entry in phases.values():
            self.assertEqual(set(entry), {"ms", "github_requests", "github_ms"})

    def test_admission_closed_ack_binds_exact_instance_generation(self):
        acknowledged = MODULE.validate_admission_closed(
            admission_closed(), instance(1), broker_policy(), production()
        )
        self.assertEqual(acknowledged["status"], "admission_closed")

        stale = admission_closed(instance_id="9999")
        with self.assertRaisesRegex(MODULE.ObservationError, "does not match GCP slot"):
            MODULE.validate_admission_closed(
                stale, instance(1), broker_policy(), production()
            )

    def test_malformed_receipt_with_valid_ack_is_recoverable(self):
        encoded_ack = base64.b64encode(json.dumps(admission_closed()).encode()).decode()
        contents = (
            MODULE.ADMISSION_CLOSED_PREFIX
            + encoded_ack
            + "\n"
            + MODULE.RECEIPT_PREFIX
            + "not-base64\n"
        )
        observer = mock.Mock(repository="example-org/example-repo")

        with mock.patch.object(
            MODULE, "serial_console_contents", return_value=contents
        ), mock.patch.object(MODULE, "guest_drain_receipt", return_value=None):
            drained, quarantined, idle_confirmed, closed = (
                MODULE.observe_drained_instances(
                    (instance(1),), broker_policy(), production(), observer
                )
            )

        self.assertEqual(drained, {})
        self.assertEqual(quarantined, {1: "GCP serial drain receipt is malformed"})
        self.assertEqual(idle_confirmed, set())
        self.assertEqual(closed, {1})

    def test_missing_guest_namespace_does_not_quarantine_serving_host(self):
        missing = MODULE.ObservationError("GCP API .../getGuestAttributes failed: HTTP 404 The resource 'also-ran/' of type 'Guest Attribute' was not found")
        with mock.patch.object(MODULE, "compute_api", side_effect=missing):
            self.assertIsNone(MODULE.serial_drain_receipt(
                instance(1), broker_policy(), "example-org/example-repo",
                production(), contents="",
            ))

    def test_optional_guest_transport_failure_does_not_quarantine_serving_host(self):
        with mock.patch.object(MODULE, "compute_api", side_effect=MODULE.ObservationError("network timeout")):
            self.assertIsNone(MODULE.serial_drain_receipt(
                instance(1), broker_policy(), "example-org/example-repo",
                production(), contents="",
            ))

    def test_guest_receipt_survives_unreadable_serial_transport(self):
        candidate = receipt()
        observer = mock.Mock(known_repositories=("example-org/example-repo",))
        with mock.patch.object(MODULE, "serial_console_contents", side_effect=MODULE.ObservationError("serial unavailable")), mock.patch.object(
            MODULE, "guest_drain_receipt", return_value=candidate
        ), mock.patch.object(MODULE, "verify_receipt_jobs") as verify:
            drained, quarantined, _, _ = MODULE.observe_drained_instances(
                (instance(1),), broker_policy(), production(), observer
            )
        self.assertEqual(drained, {1: candidate})
        self.assertEqual(quarantined, {})
        verify.assert_called_once_with(candidate, observer, production()["workflow_label"])

    def test_interleaved_serial_uses_validated_guest_receipt(self):
        candidate = receipt()
        encoded = base64.b64encode(json.dumps(candidate).encode()).decode()
        corrupted = MODULE.RECEIPT_PREFIX + encoded[:80] + "kernel: docker0 lost carrier\n"
        with mock.patch.object(MODULE, "compute_api", return_value={"queryValue": {"items": [{
            "namespace": "also-ran", "key": "receipt", "value": encoded,
        }]}}):
            result = MODULE.serial_drain_receipt(
                instance(1), broker_policy(), "example-org/example-repo",
                production(), contents=corrupted,
            )
        self.assertEqual(result, candidate)

    def test_guest_receipt_cannot_bypass_generation_guard(self):
        encoded = base64.b64encode(json.dumps(receipt(instance_id="9999")).encode()).decode()
        with mock.patch.object(MODULE, "compute_api", return_value={"queryValue": {"items": [{
            "namespace": "also-ran", "key": "receipt", "value": encoded,
        }]}}), self.assertRaises(MODULE.ObservationError):
            MODULE.serial_drain_receipt(
                instance(1), broker_policy(), "example-org/example-repo",
                production(), contents=MODULE.RECEIPT_PREFIX + "broken",
            )

    def test_conflicting_serial_records_do_not_fall_back(self):
        with mock.patch.object(MODULE, "guest_drain_receipt") as fallback:
            with self.assertRaisesRegex(MODULE.ObservationError, "conflicting"):
                MODULE.serial_drain_receipt(
                    instance(1), broker_policy(), "example-org/example-repo",
                    production(), contents=MODULE.RECEIPT_PREFIX + "one\n" + MODULE.RECEIPT_PREFIX + "two",
                )
            fallback.assert_not_called()

    def test_valid_receipt_binds_gcp_and_job_identity(self):
        result = MODULE.validate_drain_receipt(
            receipt(), instance(1), broker_policy(), "example-org/example-repo"
        )
        self.assertEqual(result["identity"]["instance_id"], "1001")

    def test_stale_instance_receipt_fails_closed(self):
        candidate = receipt(instance_id="9999")
        with self.assertRaisesRegex(MODULE.ObservationError, "does not match"):
            MODULE.validate_drain_receipt(
                candidate, instance(1), broker_policy(), "example-org/example-repo"
            )

    def test_external_zero_job_receipt_is_valid(self):
        candidate = receipt()
        candidate["version"] = 2
        candidate["reason"] = "external"
        candidate["completed_jobs"] = 0
        candidate["jobs"] = []
        result = MODULE.validate_drain_receipt(
            candidate, instance(1), broker_policy(), "example-org/example-repo"
        )
        self.assertEqual(result["completed_jobs"], 0)

    def test_v4_synchronized_drain_preserves_full_job_proof(self):
        candidate = receipt_v3()
        candidate.update(version=4, reason="max_jobs")
        # A job accepted by an in-flight poll must finish even when the drain
        # threshold has just been reached. Every execution remains in proof.
        extra = copy.deepcopy(candidate["jobs"][0])
        extra.update(runner_name="also-ran-gcp-prod-01-12345678", runner_id=762,
                     job_id="b419e6cd-0dba-5474-a3e7-573eca92c397")
        candidate["jobs"].append(extra)
        candidate["completed_jobs"] = 2
        MODULE.validate_drain_receipt(candidate, instance(1), broker_policy(), "example-org/example-repo")
        for reason in ("idle", "external"):
            empty = copy.deepcopy(candidate)
            empty.update(reason=reason, completed_jobs=0, jobs=[])
            MODULE.validate_drain_receipt(empty, instance(1), broker_policy(), "example-org/example-repo")
        for changes in ({"reason": "unknown"}, {"max_jobs": 3}, {"completed_jobs": 1}, {"version": 3}):
            with self.subTest(changes=changes), self.assertRaises(MODULE.ObservationError):
                MODULE.validate_drain_receipt({**candidate, **changes}, instance(1), broker_policy(), "example-org/example-repo")
        extra["request_labels"] = ["wrong-pool"]
        with self.assertRaises(MODULE.ObservationError):
            MODULE.validate_drain_receipt(candidate, instance(1), broker_policy(), "example-org/example-repo")

    def test_v3_canceled_receipt_preserves_requeue_identity(self):
        result = MODULE.validate_drain_receipt(
            receipt_v3(),
            instance(1),
            broker_policy(),
            "example-org/example-repo",
        )

        self.assertEqual(
            result["jobs"][0]["job_display_name"],
            "Migration replay (clean slate)",
        )

    def test_v3_receipt_rejects_wrong_label_or_timestamp_order(self):
        wrong_label = receipt_v3()
        wrong_label["jobs"][0]["request_labels"] = ["ubuntu-latest"]
        wrong_order = receipt_v3()
        wrong_order["jobs"][0]["runner_assign_time"] = "2026-09-18T13:44:54Z"

        for candidate in (wrong_label, wrong_order):
            with self.subTest(candidate=candidate):
                with self.assertRaisesRegex(MODULE.ObservationError, "v3 job identity"):
                    MODULE.validate_drain_receipt(
                        candidate,
                        instance(1),
                        broker_policy(),
                        "example-org/example-repo",
                    )

    def test_v3_max_jobs_receipt_requires_completed_limit(self):
        candidate = receipt_v3()
        candidate["max_jobs"] = 2

        with self.assertRaisesRegex(MODULE.ObservationError, "completion fields"):
            MODULE.validate_drain_receipt(
                candidate,
                instance(1),
                broker_policy(),
                "example-org/example-repo",
            )

    def test_zero_job_v1_receipt_fails_closed(self):
        candidate = receipt()
        candidate["completed_jobs"] = 0
        candidate["jobs"] = []
        with self.assertRaisesRegex(MODULE.ObservationError, "completion fields"):
            MODULE.validate_drain_receipt(
                candidate, instance(1), broker_policy(), "example-org/example-repo"
            )

    def test_v3_receipt_accepts_a_bounded_batch_of_jobs(self):
        candidate = receipt_v3()
        second = candidate["jobs"][0].copy()
        second.update(
            {
                "runner_name": "also-ran-gcp-prod-01-second",
                "runner_id": 762,
                "runner_request_id": 901,
                "job_id": "6a18672d-0c01-4bc8-9aa5-ff4d57cd3c24",
                "finished_at": "2026-09-18T13:45:02Z",
            }
        )
        candidate["max_jobs"] = 2
        candidate["completed_jobs"] = 2
        candidate["jobs"].append(second)

        result = MODULE.validate_drain_receipt(
            candidate, instance(1), broker_policy(), "example-org/example-repo"
        )

        self.assertEqual(result["completed_jobs"], 2)
        self.assertEqual(result["max_jobs"], 2)

        decision = MODULE.decide(
            production(),
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (instance(1),),
            drained={1: result},
        )
        self.assertEqual(
            decision["drained_host_jobs"]["1"],
            {"completed_jobs": 2, "max_jobs": 2, "reason": "maximum_jobs",
             "host_instance_id": "1001", "drained_at": candidate["drained_at"]},
        )

    def test_malformed_or_unknown_receipt_fields_fail_closed(self):
        candidates = []
        malformed = receipt()
        malformed["identity"]["boot_id"] = "not-a-uuid"
        candidates.append(malformed)
        unknown = receipt()
        unknown["unexpected"] = True
        candidates.append(unknown)
        wrong_job = receipt()
        wrong_job["jobs"][0]["result"] = "unknown"
        candidates.append(wrong_job)
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                with self.assertRaises(MODULE.ObservationError):
                    MODULE.validate_drain_receipt(
                        candidate,
                        instance(1),
                        broker_policy(),
                        "example-org/example-repo",
                    )

    @mock.patch.object(MODULE, "compute_api")
    def test_serial_receipt_decodes_exactly_once(self, compute_api):
        encoded = base64.b64encode(json.dumps(receipt()).encode()).decode()
        compute_api.return_value = {
            "contents": "boot output\r\n" + MODULE.RECEIPT_PREFIX + encoded + "\r\n"
        }
        result = MODULE.serial_drain_receipt(
            instance(1), broker_policy(), "example-org/example-repo"
        )
        self.assertEqual(result["status"], "drained")

    @mock.patch.object(MODULE, "compute_api")
    def test_serial_receipt_survives_log_interleaving_before_marker(self, compute_api):
        encoded = base64.b64encode(json.dumps(receipt()).encode()).decode()
        compute_api.return_value = {
            "contents": "containerd log prefix "
            + MODULE.RECEIPT_PREFIX
            + encoded
            + "\n"
        }

        result = MODULE.serial_drain_receipt(
            instance(1), broker_policy(), "example-org/example-repo"
        )

        self.assertEqual(result["status"], "drained")

    @mock.patch.object(MODULE, "compute_api")
    def test_conflicting_serial_receipts_fail_closed(self, compute_api):
        first = base64.b64encode(json.dumps(receipt()).encode()).decode()
        changed = receipt()
        changed["drained_at"] = "2026-09-16T22:57:00Z"
        second = base64.b64encode(json.dumps(changed).encode()).decode()
        compute_api.return_value = {
            "contents": (
                MODULE.RECEIPT_PREFIX + first + "\n" + MODULE.RECEIPT_PREFIX + second
            )
        }
        with self.assertRaisesRegex(MODULE.ObservationError, "conflicting"):
            MODULE.serial_drain_receipt(
                instance(1), broker_policy(), "example-org/example-repo"
            )

    def test_github_job_must_match_runner_result_and_completion(self):
        client = mock.Mock()
        client.get.return_value = {
            "jobs": [
                {
                    "runner_name": "also-ran-gcp-prod-01-2baa95cf",
                    "runner_id": 761,
                    "run_id": 35159242947,
                    "labels": ["outrunner-gcp-linux-x64"],
                    "status": "completed",
                    "conclusion": "success",
                    "completed_at": "2026-09-16T22:56:58Z",
                }
            ]
        }
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")
        MODULE.verify_receipt_jobs(receipt(), observer, "outrunner-gcp-linux-x64")
        client.get.return_value["jobs"][0]["conclusion"] = "failure"
        with self.assertRaisesRegex(MODULE.ObservationError, "does not match"):
            MODULE.verify_receipt_jobs(receipt(), observer, "outrunner-gcp-linux-x64")

    def test_rerun_does_not_hide_completed_runner_proof(self):
        old_job = {
            "runner_name": "also-ran-gcp-prod-01-2baa95cf", "runner_id": 761,
            "run_id": 35159242947, "labels": ["outrunner-gcp-linux-x64"],
            "status": "completed", "conclusion": "success",
            "completed_at": "2026-09-16T22:56:58Z",
        }
        client = mock.Mock()
        client.get.side_effect = lambda path, query: {"jobs": [old_job] if query.get("filter") == "all" else []}
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")
        MODULE.verify_receipt_jobs(receipt(), observer, "outrunner-gcp-linux-x64")
        self.assertEqual(client.get.call_args.args[1]["filter"], "all")

    def test_copied_successful_execution_across_attempts_is_one_proof(self):
        job = {
            "id": 101, "run_attempt": 1, "name": "Unit Tests",
            "runner_name": "also-ran-gcp-prod-01-2baa95cf", "runner_id": 761,
            "run_id": 35159242947, "labels": ["outrunner-gcp-linux-x64"],
            "status": "completed", "conclusion": "success",
            "started_at": "2026-09-16T22:55:00Z",
            "completed_at": "2026-09-16T22:56:58Z",
        }
        copied = {**job, "id": 102, "run_attempt": 2}
        client = mock.Mock()
        client.get.return_value = {"jobs": [job, copied]}
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")
        MODULE.verify_receipt_jobs(receipt(), observer, "outrunner-gcp-linux-x64")
        # Every substantive disagreement, same-attempt duplication and absent
        # attempt identity must still quarantine the receipt.
        for field, changed in [("runner_id", 762), ("name", "Different job"),
                ("started_at", "2026-09-16T22:55:01Z"),
                ("completed_at", "2026-09-16T22:56:59Z"),
                ("conclusion", "failure"), ("status", "in_progress"),
                ("labels", ["other-pool"]), ("run_attempt", 1),
                ("run_attempt", None), ("id", 101)]:
            with self.subTest(field=field, changed=changed):
                client.get.return_value = {"jobs": [job, {**copied, field: changed}]}
                with self.assertRaises(MODULE.ObservationError):
                    MODULE.verify_receipt_jobs(receipt(), observer, "outrunner-gcp-linux-x64")

    def test_github_job_missing_runner_fails_closed_with_specific_reason(self):
        client = mock.Mock()
        client.get.return_value = {"jobs": []}
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")

        with self.assertRaisesRegex(MODULE.ObservationError, "was not found"):
            MODULE.verify_receipt_jobs(receipt(), observer, "outrunner-gcp-linux-x64")

    def test_github_job_duplicate_runner_fails_closed_with_specific_reason(self):
        matching_job = {
            "runner_name": "also-ran-gcp-prod-01-2baa95cf",
            "runner_id": 761,
            "run_id": 35159242947,
            "labels": ["outrunner-gcp-linux-x64"],
            "status": "completed",
            "conclusion": "success",
            "completed_at": "2026-09-16T22:56:58Z",
        }
        client = mock.Mock()
        client.get.return_value = {"jobs": [matching_job, matching_job.copy()]}
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")

        with self.assertRaisesRegex(MODULE.ObservationError, "multiple matches"):
            MODULE.verify_receipt_jobs(receipt(), observer, "outrunner-gcp-linux-x64")

    def test_v3_canceled_job_accepts_unique_terminal_replacement(self):
        value = receipt_v3()
        client = mock.Mock()

        def get(path, query=None):
            if path.endswith("/jobs"):
                return {
                    "jobs": [
                        {
                            "name": "Migration replay (clean slate)",
                            "runner_name": "also-ran-gcp-prod-02-replacement",
                            "runner_id": 900,
                            "run_id": 35159242947,
                            "labels": ["outrunner-gcp-linux-x64"],
                            "status": "completed",
                            "conclusion": "success",
                            "started_at": "2026-09-18T13:45:06Z",
                            "completed_at": "2026-09-18T13:46:40Z",
                        }
                    ]
                }
            if path.endswith("/actions/runners"):
                return {"runners": []}
            return {
                "id": 35159242947,
                "status": "completed",
                "conclusion": "success",
                "created_at": "2026-09-18T13:34:01Z",
                "updated_at": "2026-09-18T13:50:58Z",
            }

        client.get.side_effect = get
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")

        MODULE.verify_receipt_jobs(value, observer, "outrunner-gcp-linux-x64")

    def test_v3_canceled_job_rejects_registered_original_runner(self):
        value = receipt_v3()
        client = mock.Mock()

        def get(path, query=None):
            if path.endswith("/jobs"):
                return {"jobs": []}
            if path.endswith("/actions/runners"):
                return {
                    "runners": [
                        {
                            "id": value["jobs"][0]["runner_id"],
                            "name": value["jobs"][0]["runner_name"],
                            "status": "offline",
                            "busy": False,
                        }
                    ]
                }
            self.fail(f"unexpected request: {path}")

        client.get.side_effect = get
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")

        with self.assertRaisesRegex(MODULE.ObservationError, "remains registered"):
            MODULE.verify_receipt_jobs(value, observer, "outrunner-gcp-linux-x64")

    def test_v3_canceled_job_rejects_nonterminal_replacement(self):
        value = receipt_v3()
        client = mock.Mock()

        def get(path, query=None):
            if path.endswith("/jobs"):
                return {
                    "jobs": [
                        {
                            "name": "Migration replay (clean slate)",
                            "runner_name": "also-ran-gcp-prod-02-replacement",
                            "runner_id": 900,
                            "run_id": 35159242947,
                            "labels": ["outrunner-gcp-linux-x64"],
                            "status": "in_progress",
                            "conclusion": None,
                            "started_at": "2026-09-18T13:45:06Z",
                            "completed_at": None,
                        }
                    ]
                }
            if path.endswith("/actions/runners"):
                return {"runners": []}
            self.fail(f"unexpected request: {path}")

        client.get.side_effect = get
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")

        with self.assertRaises(MODULE.ObservationError):
            MODULE.verify_receipt_jobs(value, observer, "outrunner-gcp-linux-x64")


class QuotaTests(unittest.TestCase):
    def setUp(self):
        # The C4 local SSD limit is a live Cloud Quotas read; a test that is
        # not about it gets one that never binds, never the network.
        local_ssd = mock.patch.object(MODULE, "local_ssd_quota_gb", return_value=1_000_000)
        local_ssd.start()
        self.addCleanup(local_ssd.stop)

    def test_shared_capacity_is_exact_and_optional(self):
        candidate = broker_policy()
        self.assertIsNone(MODULE.shared_regional_capacity(candidate))
        candidate["market"]["shared_regional_capacity"] = dict(MODULE.SHARED_REGIONAL_CAPACITY)
        self.assertEqual(MODULE.regional_host_cap(candidate, "europe-west9", "heavy"), 12)
        self.assertEqual(MODULE.regional_host_cap(candidate, "europe-west9", "general"), 12)
        candidate["market"]["shared_regional_capacity"]["reserved_vcpus"] = 0
        with self.assertRaises(MODULE.BrokerError):
            MODULE.validate_policy(candidate)

    @mock.patch.object(MODULE, "hyperdisk_usage_gb", return_value=0)
    @mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=640)
    @mock.patch.object(MODULE, "c4_family_usage_vcpus")
    @mock.patch.object(MODULE, "c4_family_quota_vcpus", return_value=100)
    @mock.patch.object(MODULE, "compute_api")
    def test_shared_capacity_accounts_for_both_pools_and_replacement_reserve(
        self, api, _quota, usage, _disk, _disk_usage
    ):
        candidate = broker_policy()
        candidate["market"]["shared_regional_capacity"] = dict(MODULE.SHARED_REGIONAL_CAPACITY)
        def read(path, query=None):
            return {"quotas": [
                {"metric": "CPUS_ALL_REGIONS", "limit": 384, "usage": 0},
                {"metric": "PREEMPTIBLE_CPUS", "limit": 0, "usage": 0},
                {"metric": "INSTANCES", "limit": 24, "usage": 0},
                {"metric": "IN_USE_ADDRESSES", "limit": 24, "usage": 0},
            ]}
        api.side_effect = read
        for pool, used, expected in (("heavy", 0, 11), ("heavy", 16, 9),
                                     ("general", 80, 3), ("general", 88, 1),
                                     ("heavy", 96, 0)):
            usage.return_value = used
            result = MODULE.observe_create_capacity(candidate, pool=pool, region="europe-west9")
            self.assertEqual(result["available_hosts"], expected)
            self.assertEqual(result["family_cpu_reserved_vcpus"], 8)

    @mock.patch.object(MODULE, "observe_create_capacity")
    def test_shared_capacity_fills_cheapest_region_beyond_old_pool_caps(self, capacity):
        candidate = json.loads((CONTROLLER.parent / "gcp-acceptance-policy.json").read_text())
        now = dt.datetime(2026, 9, 30, tzinfo=dt.timezone.utc)
        state = {
            "mode": "dry-run", "mutation_enabled": False, "price_source": "Cloud Billing Catalog",
            "price_observed_at": "2026-09-30T00:00:00Z",
            "regions": [
                {"name": item["name"], "eligible": item["canary_valid"],
                 "supported_zones": item["zones"], "price": {"hourly_usd": price}}
                for item, price in zip(candidate["market"]["regions"], (0.28, 0.067, 0.078, 0.127, 0.136, 0.175, 0.16))
            ],
        }
        for pool, availability, expected in (("heavy", 11, 11), ("general", 23, 12)):
            capacity.side_effect = lambda policy, **kwargs: {"available_hosts": availability, "global_cpu_hosts": 40}
            decision = {"create_slots": list(range(1,13)), "delete_slots": [], "drain_request_slots": []}
            MODULE.constrain_creates_to_regional_capacity(decision, candidate, (), state, pool, now)
            placements = decision["create_placements"]
            self.assertEqual(sum(item["region"] == "northamerica-northeast1" for item in placements), expected)
            self.assertEqual(len(placements), 12)
        montreal = next(item for item in state["regions"] if item["name"] == "northamerica-northeast1")
        montreal.update(eligible=False, rejection_reasons=["quota_exhausted"])
        capacity.side_effect = lambda policy, **kwargs: {"available_hosts": 1, "global_cpu_hosts": 40}
        decision = {"create_slots": [1], "delete_slots": [], "drain_request_slots": []}
        MODULE.constrain_creates_to_regional_capacity(decision, candidate, (), state, "general", now)
        self.assertEqual(decision["create_placements"][0]["region"], "northamerica-northeast1")
        montreal["rejection_reasons"].append("subnet_missing")
        decision = {"create_slots": [1], "delete_slots": [], "drain_request_slots": []}
        MODULE.constrain_creates_to_regional_capacity(decision, candidate, (), state, "general", now)
        self.assertEqual(decision["create_placements"][0]["region"], "europe-west9")

    def test_shared_admission_serializes_quota_read_through_create(self):
        from concurrent.futures import ThreadPoolExecutor
        import argparse
        import threading
        candidate = broker_policy()
        used = 0
        active = 0
        barrier = threading.Barrier(2)
        def capacity(decision, policy, instances, state, pool, now):
            nonlocal active
            active += 1
            self.assertEqual(active, 1)
            vcpus = 8 if pool == "heavy" else 4
            decision["create_slots"] = decision["create_slots"][:max(0, (92-used)//vcpus)]
            decision["create_placements"] = [{"slot": slot} for slot in decision["create_slots"]]
        def create(placements, policy_path, lifecycle, pool):
            nonlocal used, active
            used += len(placements) * (8 if pool == "heavy" else 4)
            active -= 1
            return {"created": placements}
        with tempfile.TemporaryDirectory() as directory:
            def run(pool):
                args = argparse.Namespace(lock=f"{directory}/{pool}-lock", pool=pool,
                    market_state="unused", apply=True, policy="unused", lifecycle_script="unused")
                barrier.wait()
                return MODULE.admit_and_create({"create_slots": list(range(1,13))}, candidate, args)
            with mock.patch.object(MODULE, "observe_instances", return_value=()), \
                 mock.patch.object(MODULE, "load_json", return_value={}), \
                 mock.patch.object(MODULE, "constrain_creates_to_regional_capacity", side_effect=capacity), \
                 mock.patch.object(MODULE, "create_slots", side_effect=create), \
                 ThreadPoolExecutor(max_workers=2) as executor:
                list(executor.map(run, ("heavy", "general")))
            self.assertLessEqual(used, 92)
            self.assertGreaterEqual(used, 88)
            self.assertEqual(active, 0)

    @mock.patch.object(MODULE, "observe_create_capacity")
    def test_regional_capacity_preserves_global_slots_and_defers_exhausted_market(
        self, observe_capacity
    ):
        decision = MODULE.decide(
            production(),
            MODULE.Demand(queued=3, active_owned=0, rate_remaining=2000),
            (),
        )
        observe_capacity.side_effect = (
            lambda _policy, *, pool, region, existing_hosts: {
                "region": region,
                "available_hosts": 0 if region == "europe-west9" else 8,
                "global_cpu_hosts": 20,
            }
        )
        candidate = broker_policy()
        for pool in ("production", "general_production"):
            candidate[pool]["price_ordered_regions"] = [
                "northamerica-northeast1",
                "europe-west9",
                "us-west2",
                "us-south1",
            ]
        candidate["market"]["maximum_price_age_hours"] = 72
        candidate["market"]["general_secondary_region_maximum_hosts"] = 4
        candidate["market"]["regions"].append(
            {
                "name": "us-west2",
                "zones": ["us-west2-c"],
                "canary_valid": True,
                "regional_maximum_hosts": 6,
                "maximum_hourly_usd": 0.5,
                "foundation": {"subnet": "runner-hosts-us-west2"},
            }
        )
        for region in candidate["market"]["regions"]:
            region["regional_maximum_hosts"] = region.get("regional_maximum_hosts", 10)
            region["maximum_hourly_usd"] = 0.5
        market_state = {
            "mode": "dry-run",
            "mutation_enabled": False,
            "price_source": "Cloud Billing Catalog",
            "price_observed_at": "2026-09-21T00:00:00Z",
            "regions": [
                {
                    "name": name,
                    "eligible": True,
                    "supported_zones": zones,
                    "price": {"hourly_usd": price},
                }
                for name, zones, price in (
                    ("northamerica-northeast1", ["northamerica-northeast1-b"], 0.07),
                    ("europe-west9", ["europe-west9-a"], 0.08),
                    ("us-west2", ["us-west2-c"], 0.13),
                    ("us-south1", ["us-south1-a", "us-south1-b"], 0.15),
                )
            ],
        }

        MODULE.constrain_creates_to_regional_capacity(
            decision,
            candidate,
            (),
            market_state,
            now=dt.datetime(2026, 9, 21, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(decision["create_slots"], [1, 2, 3])
        self.assertEqual(
            decision["create_placements"],
            [
                {
                    "slot": slot,
                    "zone": "northamerica-northeast1-b",
                    "region": "northamerica-northeast1",
                    "hourly_usd": 0.07,
                }
                for slot in (1, 2, 3)
            ],
        )

    @mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=100000)
    @mock.patch.object(MODULE, "c4_family_quota_vcpus", return_value=10000)
    @mock.patch.object(MODULE, "compute_aggregated_list")
    @mock.patch.object(MODULE, "compute_api")
    def test_admission_reads_project_wide_inputs_once_for_every_region(
        self, api, listing, _family_quota, _disk_quota
    ):
        calls = []
        lock = threading.Lock()

        def reply(path, query=None):
            kind = "region" if "/regions/" in path else "project"
            with lock:
                calls.append(kind)
            if kind == "project":
                return {"quotas": [{"metric": "CPUS_ALL_REGIONS", "limit": 1000, "usage": 0}]}
            if kind == "region":
                return {"quotas": [
                    {"metric": "PREEMPTIBLE_CPUS", "limit": 0, "usage": 0},
                    {"metric": "INSTANCES", "limit": 100, "usage": 0},
                    {"metric": "IN_USE_ADDRESSES", "limit": 100, "usage": 0},
                    {"metric": "SSD_TOTAL_GB", "limit": 100000, "usage": 0},
                ]}
            raise AssertionError(path)

        def listed(project, resource, filter_expression=None):
            with lock:
                calls.append(resource)
            return []

        api.side_effect = reply
        listing.side_effect = listed
        regions = (
            ("northamerica-northeast1", "northamerica-northeast1-b", 0.07),
            ("europe-west9", "europe-west9-a", 0.08),
            ("us-west2", "us-west2-c", 0.13),
        )
        candidate = broker_policy()
        candidate["production"]["price_ordered_regions"] = [name for name, _, _ in regions]
        candidate["market"]["maximum_price_age_hours"] = 72
        known = {item["name"] for item in candidate["market"]["regions"]}
        for name, zone, _ in regions:
            if name not in known:
                candidate["market"]["regions"].append({
                    "name": name, "zones": [zone], "canary_valid": True,
                    "foundation": {"subnet": f"runner-hosts-{name}"},
                })
        for region in candidate["market"]["regions"]:
            region["regional_maximum_hosts"] = 10
            region["maximum_hourly_usd"] = 0.5
        market_state = {
            "mode": "dry-run", "mutation_enabled": False,
            "price_source": "Cloud Billing Catalog",
            "price_observed_at": "2026-09-21T00:00:00Z",
            "regions": [
                {"name": name, "eligible": True, "supported_zones": [zone],
                 "price": {"hourly_usd": price}}
                for name, zone, price in regions
            ],
        }
        decision = MODULE.decide(
            production(), MODULE.Demand(queued=3, active_owned=0, rate_remaining=2000), ()
        )

        MODULE.constrain_creates_to_regional_capacity(
            decision, candidate, (), market_state,
            now=dt.datetime(2026, 9, 21, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(sorted(decision["gcp_create_capacity"]), sorted(name for name, _, _ in regions))
        self.assertEqual(calls.count("project"), 1)
        self.assertEqual(calls.count("instances"), 1)
        self.assertEqual(calls.count("disks"), 1)
        self.assertEqual(calls.count("region"), len(regions))
        # Outside an admission nothing is cached.
        MODULE.observe_create_capacity(candidate, region="europe-west9")
        self.assertEqual(calls.count("project"), 2)

    def test_demand_above_quota_fills_available_slots_and_defers_the_rest(self):
        decision = MODULE.decide(
            production(),
            MODULE.Demand(queued=35, active_owned=0, rate_remaining=2000),
            (),
        )

        MODULE.constrain_creates_to_capacity(decision, 5)

        self.assertEqual(decision["action"], "create")
        self.assertEqual(decision["create_slots"], [1, 2, 3, 4, 5])
        self.assertEqual(decision["deferred_create_slots"], list(range(6, 29)))
        self.assertTrue(decision["capacity_limited"])
        self.assertIsNone(decision["suppressed_reason"])

    def test_zero_capacity_still_suppresses_all_creates(self):
        decision = MODULE.decide(
            production(),
            MODULE.Demand(queued=3, active_owned=0, rate_remaining=2000),
            (),
        )

        MODULE.constrain_creates_to_capacity(decision, 0)

        self.assertEqual(decision["action"], "none")
        self.assertEqual(decision["create_slots"], [])
        self.assertEqual(decision["deferred_create_slots"], [1, 2, 3])
        self.assertEqual(decision["suppressed_reason"], "insufficient_gcp_quota")

    @mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=2000)
    @mock.patch.object(MODULE, "c4_family_quota_vcpus", return_value=320)
    @mock.patch.object(MODULE, "compute_aggregated_list")
    @mock.patch.object(MODULE, "compute_api")
    def test_capacity_is_minimum_of_required_live_quotas(
        self, api, listing, _family_quota, _disk_quota
    ):
        policy = broker_policy(automatic_delete=False)
        policy["target"] = {"vcpus_per_host": 8}
        serve_compute(api, listing, *[
            {"quotas": [{"metric": "CPUS_ALL_REGIONS", "limit": 320, "usage": 16}]},
            {
                "quotas": [
                    {"metric": "PREEMPTIBLE_CPUS", "limit": 0, "usage": 0},
                    {"metric": "INSTANCES", "limit": 24, "usage": 1},
                    {"metric": "IN_USE_ADDRESSES", "limit": 24, "usage": 1},
                ]
            },
            [],
            [
                {
                    "zone": "zones/northamerica-northeast1-b",
                    "type": "zones/northamerica-northeast1-b/diskTypes/hyperdisk-balanced",
                    "sizeGb": "32",
                }
            ],
        ])
        result = MODULE.observe_create_capacity(
            policy, region="northamerica-northeast1", existing_hosts=1
        )
        self.assertEqual(result["region"], "northamerica-northeast1")
        self.assertEqual(result["available_hosts"], 23)
        self.assertEqual(result["disk_hosts"], 61)

    @mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=2000)
    @mock.patch.object(MODULE, "c4_family_quota_vcpus", return_value=320)
    @mock.patch.object(MODULE, "compute_aggregated_list")
    @mock.patch.object(MODULE, "compute_api")
    def test_external_address_quota_caps_direct_egress_capacity(
        self, api, listing, _family_quota, _disk_quota
    ):
        candidate = broker_policy(automatic_delete=False)
        candidate["target"] = {"vcpus_per_host": 8}
        serve_compute(api, listing, *[
            {"quotas": [{"metric": "CPUS_ALL_REGIONS", "limit": 320, "usage": 0}]},
            {
                "quotas": [
                    {"metric": "PREEMPTIBLE_CPUS", "limit": 0, "usage": 0},
                    {"metric": "INSTANCES", "limit": 24, "usage": 0},
                    {"metric": "IN_USE_ADDRESSES", "limit": 8, "usage": 2},
                ]
            },
            [],
            [],
        ])

        result = MODULE.observe_create_capacity(
            candidate, region="northamerica-northeast1", existing_hosts=2
        )

        self.assertEqual(result["address_hosts"], 6)
        self.assertEqual(result["available_hosts"], 6)

    @mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=2000)
    @mock.patch.object(MODULE, "c4_family_quota_vcpus", return_value=320)
    @mock.patch.object(MODULE, "compute_aggregated_list")
    @mock.patch.object(MODULE, "compute_api")
    def test_nat_mode_does_not_consume_address_quota(
        self, api, listing, _family_quota, _disk_quota
    ):
        candidate = broker_policy(automatic_delete=False)
        candidate["target"] = {"vcpus_per_host": 8}
        candidate["network"]["egress"]["mode"] = "nat"
        serve_compute(api, listing, *[
            {"quotas": [{"metric": "CPUS_ALL_REGIONS", "limit": 320, "usage": 0}]},
            {
                "quotas": [
                    {"metric": "PREEMPTIBLE_CPUS", "limit": 0, "usage": 0},
                    {"metric": "INSTANCES", "limit": 24, "usage": 0},
                    {"metric": "IN_USE_ADDRESSES", "limit": 0, "usage": 0},
                ]
            },
            [],
            [],
        ])

        result = MODULE.observe_create_capacity(
            candidate, region="northamerica-northeast1", existing_hosts=0
        )

        self.assertNotIn("address_hosts", result)
        self.assertEqual(result["available_hosts"], 24)

    def test_unknown_egress_mode_is_refused(self):
        candidate = broker_policy()
        candidate["network"]["egress"]["mode"] = "hybrid"
        with self.assertRaisesRegex(MODULE.BrokerError, "egress boundary"):
            MODULE.egress_mode(candidate)

    @mock.patch.object(MODULE, "local_ssd_quota_gb", return_value=6000)
    @mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=2000)
    @mock.patch.object(MODULE, "c4_family_quota_vcpus", return_value=320)
    @mock.patch.object(MODULE, "compute_aggregated_list")
    @mock.patch.object(MODULE, "compute_api")
    def test_local_ssd_quota_caps_c4_capacity_across_both_pools(
        self, api, listing, _family_quota, _disk_quota, _ssd_quota
    ):
        def host(machine_type, zone="northamerica-northeast1-b", status="RUNNING"):
            return {
                "status": status,
                "zone": f"zones/{zone}",
                "machineType": f"machineTypes/{machine_type}",
                "disks": [
                    {"type": "PERSISTENT", "diskSizeGb": "32"},
                    {"type": "SCRATCH", "diskSizeGb": "375"},
                ],
            }

        instances = (
            [host("c4-standard-8-lssd") for _ in range(9)]
            + [host("c4-standard-4-lssd") for _ in range(5)]
            # Another region's and a stopped host's SSD do not count here.
            + [host("c4-standard-8-lssd", zone="europe-west9-a")]
            + [host("c4-standard-8-lssd", status="TERMINATED")]
        )
        candidate = broker_policy()
        candidate["target"] = {"vcpus_per_host": 8}
        serve_compute(api, listing, *[
            {"quotas": [{"metric": "CPUS_ALL_REGIONS", "limit": 320, "usage": 0}]},
            {
                "quotas": [
                    {"metric": "PREEMPTIBLE_CPUS", "limit": 0, "usage": 0},
                    {"metric": "INSTANCES", "limit": 24, "usage": 0},
                    {"metric": "IN_USE_ADDRESSES", "limit": 24, "usage": 0},
                ]
            },
            instances,
            [],
        ])

        result = MODULE.observe_create_capacity(
            candidate, region="northamerica-northeast1", existing_hosts=9
        )

        self.assertEqual(result["local_ssd_usage_gb"], 14 * 375)
        self.assertEqual(result["local_ssd_quota_gb"], 6000)
        self.assertEqual(result["local_ssd_hosts"], 2)
        self.assertEqual(result["available_hosts"], 2)

    @mock.patch.object(MODULE, "local_ssd_quota_gb", return_value=-1)
    @mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=2000)
    @mock.patch.object(MODULE, "c4_family_quota_vcpus", return_value=320)
    @mock.patch.object(MODULE, "compute_aggregated_list")
    @mock.patch.object(MODULE, "compute_api")
    def test_unlimited_local_ssd_quota_does_not_cap(
        self, api, listing, _family_quota, _disk_quota, _ssd_quota
    ):
        candidate = broker_policy()
        candidate["target"] = {"vcpus_per_host": 8}
        serve_compute(api, listing, *[
            {"quotas": [{"metric": "CPUS_ALL_REGIONS", "limit": 320, "usage": 0}]},
            {
                "quotas": [
                    {"metric": "PREEMPTIBLE_CPUS", "limit": 0, "usage": 0},
                    {"metric": "INSTANCES", "limit": 24, "usage": 0},
                    {"metric": "IN_USE_ADDRESSES", "limit": 24, "usage": 0},
                ]
            },
            [],
            [],
        ])

        result = MODULE.observe_create_capacity(candidate, region="northamerica-northeast1")

        self.assertNotIn("local_ssd_hosts", result)
        self.assertEqual(result["available_hosts"], 24)

    def test_unknown_local_ssd_shape_fails_closed(self):
        with self.assertRaisesRegex(MODULE.ObservationError, "no known local SSD size"):
            MODULE.local_ssd_gb_per_host("c4-standard-16-lssd")

    @mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=500)
    @mock.patch.object(MODULE, "c4_family_quota_vcpus", return_value=320)
    @mock.patch.object(MODULE, "compute_aggregated_list")
    @mock.patch.object(MODULE, "compute_api")
    def test_hyperdisk_quota_caps_c4_capacity(self, api, listing, _family_quota, _disk_quota):
        candidate = broker_policy()
        candidate["target"] = {"vcpus_per_host": 8}
        serve_compute(api, listing, *[
            {"quotas": [{"metric": "CPUS_ALL_REGIONS", "limit": 320, "usage": 0}]},
            {
                "quotas": [
                    {"metric": "PREEMPTIBLE_CPUS", "limit": 0, "usage": 0},
                    {"metric": "INSTANCES", "limit": 24, "usage": 0},
                    {"metric": "IN_USE_ADDRESSES", "limit": 24, "usage": 0},
                ]
            },
            [],
            [],
        ])

        result = MODULE.observe_create_capacity(
            candidate, region="northamerica-northeast1"
        )

        self.assertEqual(result["available_hosts"], 15)
        self.assertEqual(result["disk_hosts"], 15)

    @mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=500)
    @mock.patch.object(MODULE, "c4_family_quota_vcpus", return_value=320)
    @mock.patch.object(MODULE, "compute_aggregated_list")
    @mock.patch.object(MODULE, "compute_api")
    def test_hyperdisk_capacity_uses_actual_mixed_disk_sizes(
        self, api, listing, _family_quota, _disk_quota
    ):
        candidate = broker_policy()
        candidate["target"] = {"vcpus_per_host": 8}
        serve_compute(api, listing, *[
            {"quotas": [{"metric": "CPUS_ALL_REGIONS", "limit": 320, "usage": 0}]},
            {
                "quotas": [
                    {"metric": "PREEMPTIBLE_CPUS", "limit": 0, "usage": 0},
                    {"metric": "INSTANCES", "limit": 24, "usage": 2},
                    {"metric": "IN_USE_ADDRESSES", "limit": 24, "usage": 2},
                ]
            },
            [],
            [
                {
                    "zone": "zones/northamerica-northeast1-b",
                    "type": "zones/northamerica-northeast1-b/diskTypes/hyperdisk-balanced",
                    "sizeGb": "100",
                },
                {
                    "zone": "zones/northamerica-northeast1-b",
                    "type": "zones/northamerica-northeast1-b/diskTypes/hyperdisk-balanced",
                    "sizeGb": "32",
                },
                {
                    "zone": "zones/us-east1-b",
                    "type": "zones/us-east1-b/diskTypes/hyperdisk-balanced",
                    "sizeGb": "400",
                },
            ],
        ])

        result = MODULE.observe_create_capacity(
            candidate, region="northamerica-northeast1", existing_hosts=2
        )

        self.assertEqual(result["hyperdisk_usage_gb"], 132)
        self.assertEqual(result["disk_hosts"], 11)
        self.assertEqual(result["available_hosts"], 11)

    @mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=2000)
    @mock.patch.object(MODULE, "c4_family_quota_vcpus", return_value=320)
    @mock.patch.object(MODULE, "compute_aggregated_list")
    @mock.patch.object(MODULE, "compute_api")
    def test_missing_quota_fails_closed(self, api, listing, _family_quota, _disk_quota):
        policy = broker_policy(automatic_delete=False)
        policy["target"] = {"vcpus_per_host": 8}
        serve_compute(api, listing, *[
            {"quotas": [{"metric": "CPUS_ALL_REGIONS", "limit": 192, "usage": 0}]},
            {"quotas": []},
            [],
            [],
        ])
        with self.assertRaisesRegex(MODULE.ObservationError, "required GCP quota"):
            MODULE.observe_create_capacity(policy, region="northamerica-northeast1")

    @mock.patch.object(MODULE, "compute_aggregated_list")
    def test_c4_usage_counts_heavy_and_general_hosts(self, listing):
        listing.return_value = [
            {
                "status": "RUNNING",
                "zone": "zones/northamerica-northeast1-b",
                "machineType": "machineTypes/c4-standard-8-lssd",
            },
            {
                "status": "STAGING",
                "zone": "zones/northamerica-northeast1-b",
                "machineType": "machineTypes/c4-standard-4-lssd",
            },
            {
                "status": "RUNNING",
                "zone": "zones/europe-west9-a",
                "machineType": "machineTypes/c4-standard-8-lssd",
            },
        ]
        self.assertEqual(
            MODULE.c4_family_usage_vcpus(broker_policy(), "northamerica-northeast1"),
            12,
        )


class MutationTests(unittest.TestCase):
    @mock.patch.object(MODULE, "gcloud")
    def test_drain_request_is_bound_to_exact_instance_generation(self, gcloud):
        gcloud.return_value = {"updated": True}
        result = MODULE.request_drain_slots((instance(2),), [2], broker_policy())
        self.assertEqual(result["drain_requested"][0]["instance_id"], "1002")
        arguments = gcloud.call_args.args[0]
        self.assertIn("also-ran-runner-gcp-prod-02", arguments)
        self.assertIn("--zone=us-central1-a", arguments)
        self.assertIn("--metadata=runner-drain-request=1002", arguments)

    @mock.patch.object(MODULE, "gcloud")
    def test_drain_result_keeps_identity_not_the_instance_resource(self, gcloud):
        # add-metadata answers with the whole instance, Shielded VM UEFI
        # certificate databases included. Echoing it made ~500 KB ticks that
        # journald split into non-JSON 48 KiB fragments (2026-10-02).
        gcloud.return_value = [
            {
                "name": "also-ran-runner-gcp-prod-02",
                "id": "1002",
                "status": "RUNNING",
                "disks": [
                    {
                        "shieldedInstanceInitialState": {
                            "dbxs": [{"content": "A" * 30000, "fileType": "BIN"}]
                        }
                    }
                ],
                "metadata": {"items": [{"key": "startup-script", "value": "x" * 9000}]},
            }
        ]
        policy = broker_policy()
        result = MODULE.request_drain_slots((instance(2),), [2], policy)

        self.assertEqual(
            result["drain_requested"][0]["result"],
            [{"name": "also-ran-runner-gcp-prod-02", "id": "1002", "status": "RUNNING"}],
        )
        self.assertLess(len(json.dumps(result)), 1000)

    def test_operation_summary_handles_every_payload_shape(self):
        self.assertEqual(MODULE.operation_summary({}), {})
        self.assertEqual(MODULE.operation_summary({"updated": True}), {})
        self.assertEqual(MODULE.operation_summary([]), [])
        self.assertEqual(
            MODULE.operation_summary({"name": "op-1", "status": "DONE", "error": {}}),
            {"name": "op-1", "status": "DONE"},
        )

    @mock.patch.object(MODULE.subprocess, "run")
    def test_gcloud_failure_message_is_bounded(self, run):
        run.return_value = mock.Mock(returncode=1, stdout="", stderr="E" * 50000)
        with self.assertRaises(MODULE.ObservationError) as raised:
            MODULE.gcloud(["compute", "instances", "list"])
        message = str(raised.exception)
        self.assertLess(len(message), MODULE.MAX_ECHOED_TEXT + 200)
        self.assertIn("48000 more chars truncated", message)

    def test_bounded_text_leaves_short_text_alone(self):
        self.assertEqual(MODULE.bounded_text("  short  "), "short")
        self.assertEqual(MODULE.bounded_text("abcdef", 3), "abc... [3 more chars truncated]")

    @mock.patch.object(MODULE, "gcloud")
    def test_drain_batch_stops_when_newer_demand_arrives(self, gcloud):
        gcloud.return_value = {"updated": True}
        continue_guard = mock.Mock(side_effect=[True, False])
        slots = list(range(1, MODULE.DRAIN_REQUEST_WORKERS + 3))

        result = MODULE.request_drain_slots(
            tuple(instance(slot) for slot in slots),
            slots,
            broker_policy(),
            continue_guard,
        )

        first_wave = slots[: MODULE.DRAIN_REQUEST_WORKERS]
        self.assertEqual(
            [item["slot"] for item in result["drain_requested"]], first_wave
        )
        self.assertEqual(
            result["drain_deferred_slots"], slots[MODULE.DRAIN_REQUEST_WORKERS :]
        )
        self.assertEqual(gcloud.call_count, len(first_wave))
        self.assertEqual(continue_guard.call_count, 2)

    @mock.patch.object(MODULE, "gcloud")
    def test_drain_wave_requests_run_concurrently(self, gcloud):
        started = threading.Barrier(MODULE.DRAIN_REQUEST_WORKERS, timeout=5)

        def add_metadata(arguments):
            # Every request of a wave must be in flight at once to pass.
            started.wait()
            return {"name": arguments[3]}

        gcloud.side_effect = add_metadata
        slots = list(range(1, MODULE.DRAIN_REQUEST_WORKERS + 1))

        result = MODULE.request_drain_slots(
            tuple(instance(slot) for slot in slots), slots, broker_policy()
        )

        self.assertEqual([item["slot"] for item in result["drain_requested"]], slots)

    @mock.patch.object(MODULE, "gcloud")
    def test_drain_wave_reports_failure_after_the_wave_lands(self, gcloud):
        def add_metadata(arguments):
            if arguments[3].endswith("-02"):
                raise MODULE.ObservationError("boom")
            return {"name": arguments[3]}

        gcloud.side_effect = add_metadata
        with self.assertRaisesRegex(MODULE.ObservationError, "boom"):
            MODULE.request_drain_slots(
                (instance(1), instance(2), instance(3)), [1, 2, 3], broker_policy()
            )
        self.assertEqual(gcloud.call_count, 3)

    @mock.patch.object(MODULE.subprocess, "run")
    def test_create_invokes_guarded_lifecycle_for_exact_slots(self, run):
        run.return_value = mock.Mock(
            returncode=0,
            stdout='[{"slot": 2, "ok": true}, {"slot": 4, "ok": true}]',
            stderr="",
        )
        MODULE.create_slots(
            [
                {"slot": 2, "zone": "europe-west9-a"},
                {"slot": 4, "zone": "northamerica-northeast1-b"},
            ],
            "/policy.json",
            "/lifecycle.py",
        )
        command = run.call_args.args[0]
        self.assertIn("create-production", command)
        self.assertEqual(command[command.index("--slots") + 1], "2,4")
        self.assertEqual(
            command[command.index("--slot-zones") + 1],
            "2=europe-west9-a,4=northamerica-northeast1-b",
        )
        self.assertIn("--apply", command)

    @mock.patch.object(MODULE.subprocess, "run")
    def test_general_create_passes_explicit_pool_to_lifecycle(self, run):
        run.return_value = mock.Mock(
            returncode=0,
            stdout='[{"slot": 1, "ok": true}]',
            stderr="",
        )
        MODULE.create_slots(
            [{"slot": 1, "zone": "northamerica-northeast1-b"}],
            "/policy.json",
            "/lifecycle.py",
            "general",
        )
        command = run.call_args.args[0]
        self.assertEqual(command[command.index("--pool") + 1], "general")

    @mock.patch.object(MODULE.subprocess, "run")
    def test_partial_create_failure_fails_closed(self, run):
        run.return_value = mock.Mock(
            returncode=0,
            stdout='[{"slot": 2, "ok": false, "error": "capacity"}]',
            stderr="",
        )
        with self.assertRaisesRegex(MODULE.BrokerError, "creates failed"):
            MODULE.create_slots(
                [{"slot": 2, "zone": "europe-west9-a"}],
                "/policy.json",
                "/lifecycle.py",
            )

    @mock.patch.object(MODULE.subprocess, "run")
    def test_create_failure_names_each_slot_and_gcp_reason(self, run):
        refusal = (
            "gcloud command failed (gcloud compute instances create x): "
            "ERROR: (gcloud.compute.instances.create) Could not fetch resource:\n"
            " - Quota 'LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY' exceeded.  Limit: 6000.0 "
            "in region northamerica-northeast1."
        )
        run.return_value = mock.Mock(
            returncode=0,
            stdout=json.dumps([
                {"slot": 2, "ok": True},
                {"slot": 3, "ok": False, "error": refusal},
                {"slot": 5, "ok": False, "error": "ZONE_RESOURCE_POOL_EXHAUSTED"},
            ]),
            stderr="",
        )
        with self.assertRaises(MODULE.BrokerError) as raised:
            MODULE.create_slots(
                [{"slot": slot, "zone": "northamerica-northeast1-b"} for slot in (2, 3, 5)],
                "/policy.json",
                "/lifecycle.py",
            )
        message = str(raised.exception)
        self.assertTrue(message.startswith("2 of 3 production creates failed (created slots 2): "))
        self.assertIn(
            "slot 3: (gcloud.compute.instances.create) Could not fetch resource: - Quota "
            "'LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY' exceeded. Limit: 6000.0",
            message,
        )
        self.assertIn("slot 5: ZONE_RESOURCE_POOL_EXHAUSTED", message)
        self.assertNotIn("\n", message)

    @mock.patch.object(MODULE.subprocess, "run")
    def test_delete_passes_exact_receipt_bound_instance_ids(self, run):
        run.return_value = mock.Mock(
            returncode=0,
            stdout=(
                '[{"slot": 2, "ok": true, "result": {"instance_id": "1002"}},'
                '{"slot": 4, "ok": true, "result": {"instance_id": "1004"}}]'
            ),
            stderr="",
        )
        instances = (instance(2), instance(4))
        MODULE.delete_slots(instances, [2, 4], "/policy.json", "/lifecycle.py")
        command = run.call_args.args[0]
        self.assertEqual(command[command.index("--slots") + 1], "2,4")
        self.assertEqual(
            command[command.index("--expected-instance-ids") + 1],
            "2=1002,4=1004",
        )
        self.assertEqual(
            command[command.index("--slot-zones") + 1],
            "2=us-central1-a,4=us-central1-a",
        )
        self.assertIn("--apply", command)

    @mock.patch.object(MODULE.subprocess, "run")
    def test_delete_result_identity_mismatch_fails_closed(self, run):
        run.return_value = mock.Mock(
            returncode=0,
            stdout='[{"slot": 2, "ok": true, "result": {"instance_id": "9999"}}]',
            stderr="",
        )
        with self.assertRaisesRegex(MODULE.BrokerError, "did not match"):
            MODULE.delete_slots((instance(2),), [2], "/policy.json", "/lifecycle.py")



def drain_requested(slot, status="RUNNING"):
    """A host carrying our own identity-bound drain request."""
    return instance(
        slot,
        metadata={"runner-drain-request": str(1000 + slot)},
        status=status,
    )


class DrainStallTests(unittest.TestCase):
    """A host with a broken receipt needs a separate admission-closed proof.

    Polling cannot replace Outrunner's atomic drain contract. The fallback
    ledger is available only after the guest publishes an identity-bound
    acknowledgement that admission is already closed.
    """

    def test_runner_names_place_owned_jobs_on_slots(self):
        prefix = "also-ran-gcp-prod-"
        self.assertEqual(
            MODULE.slot_from_runner_name("also-ran-gcp-prod-01-1b7632cd", prefix), 1
        )
        self.assertEqual(
            MODULE.slot_from_runner_name("also-ran-gcp-prod-12-b1160b7a", prefix), 12
        )
        for unparseable in (
            "also-ran-gcp-prod-1b7632cd",
            "also-ran-gcp-prod-01",
            "also-ran-gcp-general-01-1b7632cd",
            "",
        ):
            with self.subTest(runner=unparseable):
                self.assertIsNone(MODULE.slot_from_runner_name(unparseable, prefix))

    def test_drain_requested_host_without_ack_is_not_reapable(self):
        stalled = MODULE.stalled_drain_slots(
            (drain_requested(1), instance(2)),
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            {},
            {},
        )
        # Polling cannot prove that slot 1 consumed the request and closed
        # admission, so silence alone never starts an automatic-delete clock.
        self.assertEqual(stalled, set())

    def test_a_host_running_a_job_is_never_stalled(self):
        # The drain request is in flight and the host is finishing its work.
        stalled = MODULE.stalled_drain_slots(
            (drain_requested(1),),
            MODULE.Demand(
                queued=0,
                active_owned=1,
                rate_remaining=1000,
                active_slots=frozenset({1}),
            ),
            {},
            {},
            admission_closed={1},
        )
        self.assertEqual(stalled, set())

    def test_an_unplaceable_owned_job_reaps_nothing(self):
        # One owned job could not be attributed to a slot, so "no job on this
        # slot" is not evidence of anything. Reap nothing rather than guess.
        stalled = MODULE.stalled_drain_slots(
            (drain_requested(1), drain_requested(2)),
            MODULE.Demand(
                queued=0,
                active_owned=1,
                rate_remaining=1000,
                active_slots=frozenset(),
                unattributed_active=1,
            ),
            {},
            {},
            admission_closed={1, 2},
        )
        self.assertEqual(stalled, set())

    def test_a_host_whose_receipt_was_read_is_not_stalled(self):
        demand = MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000)
        self.assertEqual(
            MODULE.stalled_drain_slots(
                (drain_requested(1),), demand, {1: receipt_v3()}, {}
            ),
            set(),
        )
        self.assertEqual(
            MODULE.stalled_drain_slots(
                (drain_requested(1),),
                demand,
                {},
                {1: "GitHub cross-check failed"},
                idle_confirmed={1},
            ),
            set(),
        )

    def test_a_drain_requested_malformed_receipt_is_stalled(self):
        demand = MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000)
        self.assertEqual(
            MODULE.stalled_drain_slots(
                (drain_requested(1),),
                demand,
                {},
                {1: "GCP serial drain receipt is malformed"},
                idle_confirmed=set(),
                admission_closed={1},
            ),
            {1},
        )

    def test_malformed_receipt_is_drained_then_reaped_after_idle_grace(self):
        config = production()
        config["automatic_delete"] = True
        idle = MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000)
        reason = {1: "GCP serial drain receipt is malformed"}
        first_at = dt.datetime(2026, 9, 20, 1, 0, tzinfo=dt.timezone.utc)

        first = MODULE.decide(
            config, idle, (instance(1),), quarantined=reason, now=first_at
        )
        self.assertEqual(first["drain_request_slots"], [1])
        self.assertEqual(first["delete_slots"], [])

        host = (drain_requested(1),)
        stalled = MODULE.stalled_drain_slots(
            host,
            idle,
            {},
            reason,
            idle_confirmed=set(),
            admission_closed={1},
        )
        ledger = MODULE.track_drain_stalls({}, host, stalled, first_at)
        reaped_at = first_at + dt.timedelta(minutes=46)
        decision = MODULE.decide(
            config,
            idle,
            host,
            quarantined=reason,
            now=reaped_at,
            drain_stall_ledger=ledger,
        )

        self.assertEqual(decision["expired_drain_stall_slots"], [1])
        self.assertEqual(decision["delete_slots"], [1])

    def test_departing_hosts_are_left_to_their_own_paths(self):
        demand = MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000)
        for status in ("STOPPING", "TERMINATED"):
            with self.subTest(status=status):
                self.assertEqual(
                    MODULE.stalled_drain_slots(
                        (drain_requested(1, status=status),),
                        demand,
                        {},
                        {},
                        admission_closed={1},
                    ),
                    set(),
                )

    def test_stall_clock_restarts_when_a_slot_is_recycled(self):
        # Slot 1 previously held instance 999. Carrying that age onto the live
        # VM would delete a host that has been up for seconds.
        now = dt.datetime(2026, 9, 18, 7, 0, tzinfo=dt.timezone.utc)
        prior = {
            "drain_stall_since": {
                "1": {"instance_id": "999", "since": "2026-09-18T06:00:00Z"}
            }
        }
        ledger = MODULE.track_drain_stalls(prior, (drain_requested(1),), {1}, now)
        self.assertEqual(ledger["1"]["instance_id"], "1001")
        self.assertEqual(ledger["1"]["since"], "2026-09-18T07:00:00Z")
        self.assertEqual(MODULE.expired_drain_stall_slots(ledger, 45, now), [])

    def test_stall_clock_carries_across_ticks_for_the_same_host(self):
        now = dt.datetime(2026, 9, 18, 7, 0, tzinfo=dt.timezone.utc)
        prior = {
            "observed_at": "2026-09-18T06:59:00Z",
            "drain_stall_since": {
                "1": {"instance_id": "1001", "since": "2026-09-18T06:00:00Z"}
            },
        }
        ledger = MODULE.track_drain_stalls(prior, (drain_requested(1),), {1}, now)
        self.assertEqual(ledger["1"]["since"], "2026-09-18T06:00:00Z")
        self.assertEqual(MODULE.expired_drain_stall_slots(ledger, 45, now), [1])

    def test_slow_maintenance_cycles_preserve_orphan_grace(self):
        now = dt.datetime(2026, 9, 18, 7, 0, tzinfo=dt.timezone.utc)
        prior = {
            "observed_at": "2026-09-18T06:55:00Z",
            "drain_stall_since": {
                "1": {"instance_id": "1001", "since": "2026-09-18T06:00:00Z"}
            },
        }
        ledger = MODULE.track_drain_stalls(prior, (instance(1),), {1}, now)
        self.assertEqual(MODULE.expired_drain_stall_slots(ledger, 45, now), [1])
        self.assertEqual(MODULE.stalled_drain_slots(
            (instance(1),), MODULE.Demand(queued=2, active_owned=0, rate_remaining=4000),
            {}, {}, admission_closed={1}), {1})

    def test_controller_observation_outage_restarts_stall_clock(self):
        now = dt.datetime(2026, 9, 18, 7, 0, tzinfo=dt.timezone.utc)
        prior = {
            "observed_at": "2026-09-18T06:00:00Z",
            "drain_stall_since": {
                "1": {"instance_id": "1001", "since": "2026-09-18T05:00:00Z"}
            },
        }

        ledger = MODULE.track_drain_stalls(prior, (drain_requested(1),), {1}, now)

        self.assertEqual(ledger["1"]["since"], "2026-09-18T07:00:00Z")
        self.assertEqual(MODULE.expired_drain_stall_slots(ledger, 45, now), [])

    def test_queued_work_does_not_block_a_closed_host(self):
        queued = MODULE.Demand(queued=1, active_owned=0, rate_remaining=1000)
        self.assertEqual(
            MODULE.stalled_drain_slots(
                (drain_requested(1),),
                queued,
                {},
                {1: "malformed"},
                admission_closed={1},
            ),
            {1},
        )

    def test_a_slot_that_stops_stalling_loses_its_clock(self):
        # The receipt finally landed, so the next recurrence starts over
        # rather than inheriting an hour of age.
        now = dt.datetime(2026, 9, 18, 7, 0, tzinfo=dt.timezone.utc)
        prior = {
            "drain_stall_since": {
                "1": {"instance_id": "1001", "since": "2026-09-18T06:00:00Z"}
            }
        }
        self.assertEqual(
            MODULE.track_drain_stalls(prior, (drain_requested(1),), set(), now),
            {},
        )

    def test_expired_stall_is_deleted_so_the_fleet_can_reach_zero(self):
        now = dt.datetime(2026, 9, 18, 7, 0, tzinfo=dt.timezone.utc)
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (drain_requested(1),),
            now=now,
            drain_stall_ledger={
                "1": {"instance_id": "1001", "since": "2026-09-18T06:10:00Z"}
            },
        )

        self.assertEqual(result["expired_drain_stall_slots"], [1])
        self.assertEqual(result["delete_slots"], [1])
        self.assertEqual(result["action"], "delete")

    def test_stall_inside_the_grace_window_is_left_alone(self):
        now = dt.datetime(2026, 9, 18, 6, 50, tzinfo=dt.timezone.utc)
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (drain_requested(1),),
            now=now,
            drain_stall_ledger={
                "1": {"instance_id": "1001", "since": "2026-09-18T06:10:00Z"}
            },
        )

        self.assertEqual(result["expired_drain_stall_slots"], [])
        self.assertEqual(result["delete_slots"], [])

    def test_stall_expiry_is_off_when_automatic_delete_is_off(self):
        now = dt.datetime(2026, 9, 18, 7, 0, tzinfo=dt.timezone.utc)
        config = production()
        config["automatic_delete"] = False
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000),
            (drain_requested(1),),
            now=now,
            drain_stall_ledger={
                "1": {"instance_id": "1001", "since": "2026-09-18T06:10:00Z"}
            },
        )

        self.assertEqual(result["expired_drain_stall_slots"], [])
        self.assertEqual(result["delete_slots"], [])

    def test_low_github_rate_limit_suppresses_a_stall_delete(self):
        # The stall clock is only meaningful next to a GitHub reading; when
        # the tick could not take one, nothing is deleted on its strength.
        now = dt.datetime(2026, 9, 18, 7, 0, tzinfo=dt.timezone.utc)
        config = production()
        config["automatic_delete"] = True
        result = MODULE.decide(
            config,
            MODULE.Demand(queued=0, active_owned=0, rate_remaining=10),
            (drain_requested(1),),
            now=now,
            drain_stall_ledger={
                "1": {"instance_id": "1001", "since": "2026-09-18T06:10:00Z"}
            },
        )

        self.assertEqual(result["delete_slots"], [])
        self.assertEqual(result["suppressed_reason"], "github_rate_limit_low")

    def test_a_tick_that_read_nothing_reports_no_stalls(self):
        # Every host looks silent to a tick that never opened a serial
        # console. Reporting stalls here would credit unwatched time.
        idle = MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000)
        self.assertEqual(
            MODULE.stalled_drain_slots(
                (drain_requested(1),),
                idle,
                {},
                {},
                receipts_observed=False,
                admission_closed={1},
            ),
            set(),
        )

    def test_an_observation_gap_restarts_the_stall_clock(self):
        # A blackout (low GitHub rate limit, or automatic_delete switched off)
        # must not bank toward expiry: the host was unwatched, not idle.
        config = production()
        config["automatic_delete"] = True
        host = (drain_requested(1),)
        idle = MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000)
        prior = {
            "drain_stall_since": {
                "1": {"instance_id": "1001", "since": "2026-09-18T06:00:00Z"}
            }
        }

        blind = MODULE.track_drain_stalls(
            prior,
            host,
            MODULE.stalled_drain_slots(
                host,
                idle,
                {},
                {},
                receipts_observed=False,
                admission_closed={1},
            ),
            dt.datetime(2026, 9, 18, 6, 30, tzinfo=dt.timezone.utc),
        )
        self.assertEqual(blind, {})

        # Observation resumes three hours after the original `since`. The
        # clock starts from now, so nothing is deleted on the first tick back.
        resumed_at = dt.datetime(2026, 9, 18, 9, 0, tzinfo=dt.timezone.utc)
        resumed = MODULE.track_drain_stalls(
            {"drain_stall_since": blind},
            host,
            MODULE.stalled_drain_slots(host, idle, {}, {}, admission_closed={1}),
            resumed_at,
        )
        self.assertEqual(resumed["1"]["since"], "2026-09-18T09:00:00Z")
        result = MODULE.decide(
            config, idle, host, now=resumed_at, drain_stall_ledger=resumed
        )
        self.assertEqual(result["delete_slots"], [])

    def test_a_host_busy_when_observation_resumes_is_never_deleted(self):
        # The safety floor underneath the clock: expiry can only ever act on
        # a slot that is in the freshly rebuilt stalled set, so a host holding
        # a job loses its ledger entry no matter how old its clock was.
        config = production()
        config["automatic_delete"] = True
        host = (drain_requested(1),)
        busy = MODULE.Demand(
            queued=0,
            active_owned=1,
            rate_remaining=1000,
            active_slots=frozenset({1}),
        )
        prior = {
            "drain_stall_since": {
                "1": {"instance_id": "1001", "since": "2026-09-18T06:00:00Z"}
            }
        }
        now = dt.datetime(2026, 9, 18, 9, 0, tzinfo=dt.timezone.utc)
        ledger = MODULE.track_drain_stalls(
            prior,
            host,
            MODULE.stalled_drain_slots(host, busy, {}, {}, admission_closed={1}),
            now,
        )

        self.assertEqual(ledger, {})
        result = MODULE.decide(config, busy, host, now=now, drain_stall_ledger=ledger)
        self.assertEqual(result["delete_slots"], [])

    def test_observe_places_in_progress_jobs_on_their_slots(self):
        # Attribution is what makes a stall safe to reap. If it silently
        # returned nothing, every busy host would read as idle.
        label = "outrunner-gcp-linux-x64"
        client = mock.Mock()
        client.last_headers = {"x-ratelimit-remaining": "4000"}

        def get(path, query):
            if path.endswith("/actions/runs"):
                if query.get("status") == "in_progress" and query.get("page") == 1:
                    return {"workflow_runs": [{"id": 7}]}
                return {"workflow_runs": []}
            return (
                {
                    "jobs": [
                        {
                            "labels": [label],
                            "status": "in_progress",
                            "runner_name": "also-ran-gcp-prod-09-1b7632cd",
                        },
                        {
                            "labels": [label],
                            "status": "in_progress",
                            "runner_name": "also-ran-gcp-prod-oddball",
                        },
                        {
                            "labels": [label],
                            "status": "in_progress",
                            "runner_name": "blacksmith-4vcpu-1",
                        },
                        {"labels": [label], "status": "queued", "runner_name": ""},
                    ]
                }
                if query.get("page") == 1
                else {"jobs": []}
            )

        client.get.side_effect = get
        demand = MODULE.GitHubDemandObserver(client, "example-org/example-repo").observe(
            label,
            "also-ran-gcp-prod-",
            dt.datetime(2026, 9, 18, 7, 0, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(demand.queued, 1)
        # The Blacksmith runner is not ours, so it is not owned demand at all.
        self.assertEqual(demand.active_owned, 2)
        self.assertEqual(demand.active_slots, frozenset({9}))
        self.assertEqual(demand.unattributed_active, 1)

    def test_registered_runner_slots_fail_closed_on_unplaceable_name(self):
        client = mock.Mock()
        client.get.return_value = {
            "runners": [
                {"name": "also-ran-gcp-prod-04-deadbeef"},
                {"name": "also-ran-gcp-prod-oddball"},
                {"name": "blacksmith-4vcpu-1"},
            ]
        }
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")

        slots, unattributed = observer.registered_slots("also-ran-gcp-prod-")

        self.assertEqual(slots, frozenset({4}))
        self.assertEqual(unattributed, 1)

    def test_fallback_delete_allows_queue_but_requires_no_registered_runner(self):
        observer = mock.Mock()
        observer.client.last_headers = {"x-ratelimit-remaining": "4000"}
        observer.observe.return_value = MODULE.Demand(
            queued=0, active_owned=0, rate_remaining=4000
        )
        observer.runner_slots.return_value = (frozenset(), frozenset(), 0)
        decision = {"observed_at": "2026-09-18T07:00:00Z"}

        self.assertIsNone(
            MODULE.fallback_delete_block_reason(observer, production(), 1, decision)
        )

        observer.observe.return_value = MODULE.Demand(
            queued=1, active_owned=0, rate_remaining=4000
        )
        self.assertIsNone(
            MODULE.fallback_delete_block_reason(observer, production(), 1, decision)
        )

        observer.observe.return_value = MODULE.Demand(
            queued=0, active_owned=0, rate_remaining=4000
        )
        observer.runner_slots.return_value = (frozenset({1}), frozenset(), 0)
        self.assertEqual(
            MODULE.fallback_delete_block_reason(observer, production(), 1, decision),
            "slot_runner_still_registered",
        )

    def test_fallback_delete_only_blocks_for_activity_on_the_target_slot(self):
        observer = mock.Mock()
        observer.client.last_headers = {"x-ratelimit-remaining": "4000"}
        observer.runner_slots.return_value = (frozenset(), frozenset(), 0)
        decision = {"observed_at": "2026-09-18T07:00:00Z"}

        observer.observe.return_value = MODULE.Demand(
            queued=0,
            active_owned=1,
            rate_remaining=4000,
            active_slots=frozenset({2}),
        )
        self.assertIsNone(
            MODULE.fallback_delete_block_reason(observer, production(), 1, decision)
        )

        observer.observe.return_value = MODULE.Demand(
            queued=0,
            active_owned=1,
            rate_remaining=4000,
            active_slots=frozenset({1}),
        )
        self.assertEqual(
            MODULE.fallback_delete_block_reason(observer, production(), 1, decision),
            "current_github_demand",
        )

    def test_newer_fast_queue_does_not_block_closed_host_deletion(self):
        observer = mock.Mock()
        observer.client.last_headers = {"x-ratelimit-remaining": "4000"}
        observer.observe.return_value = MODULE.Demand(
            queued=0, active_owned=0, rate_remaining=4000
        )
        observer.runner_slots.return_value = (frozenset(), frozenset(), 0)
        decision = {"observed_at": "2026-09-18T07:00:00Z"}
        peer = {
            "observed_at": "2026-09-18T07:00:01Z",
            "queued_jobs": 1,
            "active_owned_jobs": 0,
        }

        self.assertIsNone(
            MODULE.fallback_delete_block_reason(observer, production(), 1, decision, peer)
        )

    def test_newer_fast_activity_on_another_slot_does_not_block_fallback(self):
        observer = mock.Mock()
        observer.client.last_headers = {"x-ratelimit-remaining": "4000"}
        observer.observe.return_value = MODULE.Demand(
            queued=0, active_owned=0, rate_remaining=4000
        )
        observer.runner_slots.return_value = (frozenset(), frozenset(), 0)
        decision = {"observed_at": "2026-09-18T07:00:00Z"}
        peer = {
            "observed_at": "2026-09-18T07:00:01Z",
            "queued_jobs": 0,
            "active_owned_jobs": 1,
            "active_slots": [2],
            "unattributed_active_jobs": 0,
        }

        self.assertIsNone(
            MODULE.fallback_delete_block_reason(
                observer, production(), 1, decision, peer
            )
        )

        peer["active_slots"] = [1]
        self.assertEqual(
            MODULE.fallback_delete_block_reason(
                observer, production(), 1, decision, peer
            ),
            "newer_scale_up_demand",
        )

    def test_blocked_fallback_revokes_accumulated_idle_proof(self):
        decision = {
            "drain_stall_since": {
                "1": {"instance_id": "1001", "since": "2026-09-18T06:00:00Z"},
                "2": {"instance_id": "1002", "since": "2026-09-18T06:00:00Z"},
            }
        }

        MODULE.defer_fallback_delete(decision, 1, "current_github_demand")

        self.assertEqual(
            decision["deferred_delete_slots"], {"1": "current_github_demand"}
        )
        self.assertNotIn("1", decision["drain_stall_since"])
        self.assertIn("2", decision["drain_stall_since"])

    def test_the_incident_replayed_end_to_end(self):
        # The full receipt is unusable, but the guest independently confirmed
        # that admission closed and its JIT runner deregistered.
        config = production()
        config["automatic_delete"] = True
        host = (drain_requested(1),)
        idle = MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000)
        state = {}

        def tick(now):
            nonlocal state
            stalled = MODULE.stalled_drain_slots(
                host, idle, {}, {}, admission_closed={1}
            )
            ledger = MODULE.track_drain_stalls(state, host, stalled, now)
            decision = MODULE.decide(
                config, idle, host, now=now, drain_stall_ledger=ledger
            )
            decision["observed_at"] = now.isoformat().replace("+00:00", "Z")
            state = decision
            return decision

        first = tick(dt.datetime(2026, 9, 18, 21, 41, tzinfo=dt.timezone.utc))
        self.assertEqual(first["stalled_drain_slots"], [1])
        self.assertEqual(first["delete_slots"], [])

        # Continuous one-minute observations preserve the original clock.
        for minute in range(1, 45):
            held = tick(
                dt.datetime(2026, 9, 18, 21, 41, tzinfo=dt.timezone.utc)
                + dt.timedelta(minutes=minute)
            )
        self.assertEqual(held["delete_slots"], [])
        self.assertEqual(
            held["drain_stall_since"]["1"]["since"], "2026-09-18T21:41:00Z"
        )

        reaped = tick(dt.datetime(2026, 9, 18, 22, 26, tzinfo=dt.timezone.utc))
        self.assertEqual(reaped["expired_drain_stall_slots"], [1])
        self.assertEqual(reaped["delete_slots"], [1])

        # Without the ledger this is the behaviour that stranded the fleet:
        # no receipt, so no drained slot, no quarantine, and nothing to expire.
        never = MODULE.decide(
            config,
            idle,
            host,
            now=dt.datetime(2026, 9, 20, 12, 0, tzinfo=dt.timezone.utc),
        )
        self.assertEqual(never["delete_slots"], [])
        self.assertEqual(never["serving_hosts"], 0)



class DrainRequestCeilingTests(unittest.TestCase):
    """A guest that ignores its drain request entirely must still go away.

    general-08 (2026-10-01/02) took the drain request, never published a
    receipt or an admission-closed acknowledgement, stayed registered and idle
    on GitHub, and billed for ~39 hours because no other path could reap it.
    """

    idle = MODULE.Demand(queued=0, active_owned=0, rate_remaining=1000)

    def test_an_idle_drain_requested_host_starts_the_clock(self):
        self.assertEqual(
            MODULE.unanswered_drain_request_slots(
                (drain_requested(1), instance(2)), self.idle, {}
            ),
            {1},
        )

    def test_hosts_that_answered_or_are_busy_do_not_age(self):
        busy = MODULE.Demand(
            queued=0, active_owned=1, rate_remaining=1000, active_slots=frozenset({1})
        )
        unattributed = MODULE.Demand(
            queued=0, active_owned=1, rate_remaining=1000, unattributed_active=1
        )
        host = (drain_requested(1),)
        self.assertEqual(MODULE.unanswered_drain_request_slots(host, busy, {}), set())
        self.assertEqual(
            MODULE.unanswered_drain_request_slots(host, unattributed, {}), set()
        )
        self.assertEqual(
            MODULE.unanswered_drain_request_slots(host, self.idle, {1: {}}), set()
        )
        self.assertEqual(
            MODULE.unanswered_drain_request_slots(
                host, self.idle, {}, receipts_observed=False
            ),
            set(),
        )
        # A quarantined host keeps its own delete path; the ceiling never
        # moves it onto the relaxed gate.
        self.assertEqual(
            MODULE.unanswered_drain_request_slots(
                host, self.idle, {}, quarantined={1: "receipt_contradicts_github"}
            ),
            set(),
        )
        for status in ("STOPPING", "TERMINATED"):
            self.assertEqual(
                MODULE.unanswered_drain_request_slots(
                    (drain_requested(1, status=status),), self.idle, {}
                ),
                set(),
            )

    def test_ceiling_expiry_deletes_only_after_the_ceiling(self):
        config = production()
        config["automatic_delete"] = True
        ledger = {"1": {"instance_id": "1001", "since": "2026-10-02T06:00:00Z"}}
        inside = MODULE.decide(
            config,
            self.idle,
            (drain_requested(1),),
            now=dt.datetime(2026, 10, 2, 6, 59, tzinfo=dt.timezone.utc),
            drain_request_ledger=ledger,
        )
        self.assertEqual(inside["expired_drain_request_slots"], [])
        self.assertEqual(inside["delete_slots"], [])
        self.assertEqual(inside["unanswered_drain_request_slots"], [1])

        past = MODULE.decide(
            config,
            self.idle,
            (drain_requested(1),),
            now=dt.datetime(2026, 10, 2, 7, 0, tzinfo=dt.timezone.utc),
            drain_request_ledger=ledger,
        )
        self.assertEqual(past["expired_drain_request_slots"], [1])
        self.assertEqual(past["delete_slots"], [1])
        self.assertEqual(past["drain_request_since"], ledger)

        config["automatic_delete"] = False
        off = MODULE.decide(
            config,
            self.idle,
            (drain_requested(1),),
            now=dt.datetime(2026, 10, 2, 7, 0, tzinfo=dt.timezone.utc),
            drain_request_ledger=ledger,
        )
        self.assertEqual(off["expired_drain_request_slots"], [])
        self.assertEqual(off["delete_slots"], [])

    def test_the_general_08_incident_replayed(self):
        config = production()
        config["automatic_delete"] = True
        host = (drain_requested(8),)
        state = {}
        start = dt.datetime(2026, 10, 1, 23, 31, tzinfo=dt.timezone.utc)

        def tick(now, demand=self.idle):
            nonlocal state
            ledger = MODULE.track_drain_stalls(
                state,
                host,
                MODULE.unanswered_drain_request_slots(host, demand, {}),
                now,
                key="drain_request_since",
            )
            decision = MODULE.decide(
                config, demand, host, now=now, drain_request_ledger=ledger
            )
            decision["observed_at"] = now.isoformat().replace("+00:00", "Z")
            state = decision
            return decision

        for minute in range(60):
            held = tick(start + dt.timedelta(minutes=minute))
            self.assertEqual(held["delete_slots"], [])
        self.assertEqual(
            held["drain_request_since"]["8"]["since"], "2026-10-01T23:31:00Z"
        )
        reaped = tick(start + dt.timedelta(minutes=60))
        self.assertEqual(reaped["expired_drain_request_slots"], [8])
        self.assertEqual(reaped["delete_slots"], [8])

        # A job in between restarts the clock rather than counting toward it.
        state = {}
        tick(start)
        busy = MODULE.Demand(
            queued=0, active_owned=1, rate_remaining=1000, active_slots=frozenset({8})
        )
        tick(start + dt.timedelta(minutes=30), busy)
        resumed = tick(start + dt.timedelta(minutes=61))
        self.assertEqual(resumed["delete_slots"], [])

    def test_guard_tolerates_the_registration_but_not_a_busy_runner(self):
        observer = mock.Mock()
        observer.client.last_headers = {"x-ratelimit-remaining": "4000"}
        observer.observe.return_value = self.idle
        observer.runner_slots.return_value = (frozenset({1}), frozenset(), 0)
        decision = {"observed_at": "2026-10-02T07:00:00Z"}

        self.assertEqual(
            MODULE.fallback_delete_block_reason(observer, production(), 1, decision),
            "slot_runner_still_registered",
        )
        self.assertIsNone(
            MODULE.fallback_delete_block_reason(
                observer, production(), 1, decision, unanswered_drain=True
            )
        )
        observer.runner_slots.return_value = (frozenset({1}), frozenset({1}), 0)
        self.assertEqual(
            MODULE.fallback_delete_block_reason(
                observer, production(), 1, decision, unanswered_drain=True
            ),
            "slot_runner_busy",
        )
        observer.runner_slots.return_value = (frozenset({1}), frozenset(), 1)
        self.assertEqual(
            MODULE.fallback_delete_block_reason(
                observer, production(), 1, decision, unanswered_drain=True
            ),
            "registered_runner_attribution_incomplete",
        )
        busy = MODULE.Demand(
            queued=0, active_owned=1, rate_remaining=4000, active_slots=frozenset({1})
        )
        observer.observe.return_value = busy
        self.assertEqual(
            MODULE.fallback_delete_block_reason(
                observer, production(), 1, decision, unanswered_drain=True
            ),
            "current_github_demand",
        )

    def test_runner_slots_reads_registration_and_busy_in_one_pass(self):
        client = mock.Mock()
        client.get.return_value = {
            "runners": [
                {"name": "also-ran-gcp-prod-01-aaaa", "busy": True},
                {"name": "also-ran-gcp-prod-02-bbbb", "busy": False},
                {"name": "also-ran-gcp-prod-03-cccc"},
                {"name": "blacksmith-4vcpu-1", "busy": True},
            ]
        }
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")
        registered, busy, unattributed = observer.runner_slots("also-ran-gcp-prod-")
        self.assertEqual(registered, frozenset({1, 2, 3}))
        # A runner without a busy flag is treated as busy: fail closed.
        self.assertEqual(busy, frozenset({1, 3}))
        self.assertEqual(unattributed, 0)
        # One pass over the runner list, so both sets describe the same read.
        self.assertEqual(client.get.call_count, 1)

    def test_runner_slots_ignore_offline_idle_registrations(self):
        client = mock.Mock()
        client.get.return_value = {
            "runners": [
                # A spare left by a host that exited without a receipt.
                {"name": "also-ran-gcp-prod-01-aaaa", "status": "offline", "busy": False},
                # Offline but marked busy: GitHub still thinks it holds a job.
                {"name": "also-ran-gcp-prod-02-bbbb", "status": "offline", "busy": True},
                # Offline with no busy flag: unknown, so fail closed.
                {"name": "also-ran-gcp-prod-03-cccc", "status": "offline"},
                {"name": "also-ran-gcp-prod-04-dddd", "status": "online", "busy": False},
                # No status at all: fail closed.
                {"name": "also-ran-gcp-prod-05-eeee", "busy": False},
            ]
        }
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")
        registered, busy, unattributed = observer.runner_slots("also-ran-gcp-prod-")
        self.assertEqual(registered, frozenset({2, 3, 4, 5}))
        self.assertEqual(busy, frozenset({2, 3}))
        self.assertEqual(unattributed, 0)

    def test_a_blocked_ceiling_delete_restarts_its_clock(self):
        decision = {
            "drain_request_since": {
                "1": {"instance_id": "1001", "since": "2026-10-02T06:00:00Z"}
            }
        }
        MODULE.defer_fallback_delete(decision, 1, "slot_runner_busy")
        self.assertEqual(decision["drain_request_since"], {})


class UnansweredDrainEvidenceTests(unittest.TestCase):
    """The serial tail is the only guest record that survives a ceiling delete."""

    def test_tail_is_bounded_printable_and_newest_first_kept(self):
        contents = "\r\n".join(
            [f"line {n}" for n in range(500)]
            + ["\x1b[0mcolour\x00 reset", "x" * 5000, "", "last"]
        )
        tail = MODULE.serial_evidence_tail(contents)
        lines = tail.split("\n")
        self.assertEqual(lines[-1], "last")
        self.assertIn("[0mcolour reset", lines)
        self.assertEqual(lines[-2], "x" * MODULE.EVIDENCE_LINE_CHARS)
        self.assertLessEqual(len(lines), MODULE.EVIDENCE_TAIL_LINES)
        self.assertLessEqual(len(tail), MODULE.EVIDENCE_TAIL_CHARS)
        self.assertNotIn("line 0", lines)
        self.assertTrue(all(line.isprintable() for line in lines))

    def test_capture_records_the_tail(self):
        with mock.patch.object(
            MODULE, "serial_console_contents", return_value="boot\nouter: polling\n"
        ):
            evidence = MODULE.capture_unanswered_drain_evidence(
                drain_requested(8), broker_policy()
            )
        self.assertEqual(evidence["instance_id"], "1008")
        self.assertEqual(evidence["instance_name"], "also-ran-runner-gcp-prod-08")
        self.assertEqual(evidence["serial_tail"], "boot\nouter: polling")
        self.assertNotIn("error", evidence)

    def test_capture_failure_is_recorded_not_raised(self):
        with mock.patch.object(
            MODULE,
            "serial_console_contents",
            side_effect=MODULE.ObservationError("serial unavailable"),
        ):
            evidence = MODULE.capture_unanswered_drain_evidence(
                drain_requested(8), broker_policy()
            )
        self.assertEqual(evidence["error"], "serial unavailable")
        self.assertNotIn("serial_tail", evidence)

    def run_ceiling_tick(self, serial, order=None):
        order = [] if order is None else order
        read_serial = mock.Mock(side_effect=serial)
        candidate = json.loads(
            (CONTROLLER.parent / "gcp-acceptance-policy.json").read_text()
        )
        candidate["production"]["automatic_delete"] = True
        now = dt.datetime.now(dt.timezone.utc)
        stamp = lambda moment: moment.isoformat().replace("+00:00", "Z")
        host = drain_requested(1)
        prior = {
            "observed_at": stamp(now - dt.timedelta(minutes=1)),
            "drain_request_since": {
                "1": {
                    "instance_id": host.instance_id,
                    "since": stamp(now - dt.timedelta(hours=2)),
                }
            },
        }
        deleted = []
        with tempfile.TemporaryDirectory() as root:
            policy_path = pathlib.Path(root) / "policy.json"
            market_path = pathlib.Path(root) / "market.json"
            state_path = pathlib.Path(root) / "state.json"
            policy_path.write_text(json.dumps(candidate))
            market_path.write_text("{}")
            state_path.write_text(json.dumps(prior))
            with (
                mock.patch.object(MODULE, "read_secret", return_value="test-only"),
                mock.patch.object(
                    MODULE.GitHubDemandObserver,
                    "observe",
                    return_value=MODULE.Demand(
                        queued=0, active_owned=0, rate_remaining=4000
                    ),
                ),
                mock.patch.object(MODULE, "observe_instances", return_value=(host,)),
                mock.patch.object(
                    MODULE,
                    "observe_drained_instances",
                    return_value=({}, {}, set(), set()),
                ),
                mock.patch.object(
                    MODULE,
                    "fallback_delete_block_reason",
                    side_effect=lambda *_a, **_k: order.append("guard"),
                ),
                mock.patch.object(
                    MODULE,
                    "serial_console_contents",
                    side_effect=lambda *a, **k: order.append("capture")
                    or read_serial(*a, **k),
                ),
                mock.patch.object(
                    MODULE,
                    "delete_slots",
                    side_effect=lambda _i, slots, *_a: order.append("delete")
                    or deleted.extend(slots)
                    or {"deleted": [{"slot": slot} for slot in slots]},
                ),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                MODULE.main([
                    "--policy", str(policy_path), "--market-state", str(market_path),
                    "--state", str(state_path), "--lock", root + "/lock",
                    "--github-token-file", "unused", "--once", "--apply",
                    "--maintenance-only",
                ])
            state = json.loads(state_path.read_text())
        return state, deleted

    def test_ceiling_delete_keeps_the_guests_last_words(self):
        state, deleted = self.run_ceiling_tick(["outrunner: listening\n"])
        self.assertEqual(deleted, [1])
        self.assertEqual(state["expired_drain_request_slots"], [1])
        evidence = state["unanswered_drain_evidence"]["1"]
        self.assertEqual(evidence["serial_tail"], "outrunner: listening")
        self.assertIn("unanswered_drain_evidence", state["phases"])

    def test_the_guard_is_the_last_read_before_the_delete(self):
        order = []
        self.run_ceiling_tick(["outrunner: listening\n"], order)
        self.assertEqual(order, ["capture", "guard", "delete"])

    def test_a_failed_capture_never_blocks_the_delete(self):
        state, deleted = self.run_ceiling_tick(MODULE.ObservationError("gone"))
        self.assertEqual(deleted, [1])
        self.assertEqual(state["unanswered_drain_evidence"]["1"]["error"], "gone")


class MultiRepositoryTests(unittest.TestCase):
    """One pool serving several repositories (independent repositories)."""

    LABEL = "outrunner-gcp-general-linux-x64"
    PREFIX = "also-ran-gcp-general-"
    NOW = dt.datetime(2026, 9, 25, tzinfo=dt.timezone.utc)

    def client(self, runs, jobs, runners=None):
        client = mock.Mock()
        client.last_headers = {"x-ratelimit-remaining": "4000"}
        seen = []

        def get(path, query=None):
            seen.append(path)
            if path.endswith("/actions/runners"):
                return {"runners": (runners or {}).get(path, [])}
            if path.endswith("/actions/runs"):
                repository = path.removeprefix("/repos/").removesuffix(
                    "/actions/runs"
                )
                if query.get("status") == "in_progress" and query.get("page") == 1:
                    return {"workflow_runs": runs.get(repository, [])}
                return {"workflow_runs": []}
            return {"jobs": jobs.get(path, [])}

        client.get.side_effect = get
        return client, seen

    def test_demand_sums_every_served_repository_and_keeps_the_split(self):
        client, _ = self.client(
            {"o/also-ran": [{"id": 1}], "o/another-repo": [{"id": 1}]},
            {
                "/repos/o/also-ran/actions/runs/1/jobs": [
                    {"labels": [self.LABEL], "status": "queued"},
                    {
                        "labels": [self.LABEL],
                        "status": "in_progress",
                        "runner_name": self.PREFIX + "03-abc123",
                    },
                ],
                # The same run id in another repository is a different run.
                "/repos/o/another-repo/actions/runs/1/jobs": [
                    {"labels": [self.LABEL], "status": "queued"},
                    {"labels": [self.LABEL], "status": "queued"},
                    {"labels": ["ubuntu-latest"], "status": "queued"},
                ],
            },
        )
        observer = MODULE.GitHubDemandObserver(client, ("o/also-ran", "o/another-repo"))

        demand = observer.observe(self.LABEL, self.PREFIX, self.NOW)

        self.assertEqual(demand.queued, 3)
        self.assertEqual(demand.active_owned, 1)
        self.assertEqual(demand.active_slots, frozenset({3}))
        self.assertEqual(
            demand.by_repository,
            {
                "o/also-ran": {"queued": 1, "active_owned": 1},
                "o/another-repo": {"queued": 2, "active_owned": 0},
            },
        )

    def test_single_repository_string_still_works(self):
        client, seen = self.client({}, {})
        observer = MODULE.GitHubDemandObserver(client, "o/also-ran")
        observer.observe(self.LABEL, self.PREFIX, self.NOW)
        self.assertEqual(observer.repositories, ("o/also-ran",))
        self.assertTrue(all(path.startswith("/repos/o/also-ran/") for path in seen))

    def test_active_run_cap_is_per_repository(self):
        many = [{"id": index} for index in range(1, 4)]
        client, _ = self.client({"o/a": many, "o/b": many}, {})
        MODULE.GitHubDemandObserver(
            client, ("o/a", "o/b"), max_active_runs=3
        ).observe(self.LABEL, self.PREFIX, self.NOW)

        client, _ = self.client({"o/a": many + [{"id": 9}]}, {})
        with self.assertRaisesRegex(MODULE.ObservationError, "for o/a"):
            MODULE.GitHubDemandObserver(
                client, ("o/a", "o/b"), max_active_runs=3
            ).observe(self.LABEL, self.PREFIX, self.NOW)

    def test_organization_scope_reads_org_runners_too(self):
        # An org-registered runner never appears in a repository's runner list.
        # Reading only that list would call a live slot unregistered and let
        # the fallback delete reap it, so organization scope reads the org.
        client, seen = self.client(
            {},
            {},
            runners={
                "/orgs/o/actions/runners": [{"name": self.PREFIX + "05-live01"}],
            },
        )
        observer = MODULE.GitHubDemandObserver(
            client, ("o/also-ran", "o/another-repo"), runner_scope="organization"
        )

        slots, unattributed = observer.registered_slots(self.PREFIX)

        self.assertEqual(slots, frozenset({5}))
        self.assertEqual(unattributed, 0)
        self.assertIn("/orgs/o/actions/runners", seen)

    def test_organization_scope_still_sees_legacy_repository_runners(self):
        # Mid-migration a host from the older image still holds a
        # repository-scoped runner. It must keep its slot registered.
        client, seen = self.client(
            {},
            {},
            runners={
                "/orgs/o/actions/runners": [{"name": self.PREFIX + "05-new001"}],
                "/repos/o/also-ran/actions/runners": [
                    {"name": self.PREFIX + "07-old001"}
                ],
            },
        )
        observer = MODULE.GitHubDemandObserver(
            client, ("o/also-ran", "o/another-repo"), runner_scope="organization"
        )

        slots, _ = observer.registered_slots(self.PREFIX)

        self.assertEqual(slots, frozenset({5, 7}))
        self.assertEqual(
            seen,
            [
                "/orgs/o/actions/runners",
                "/repos/o/also-ran/actions/runners",
                "/repos/o/another-repo/actions/runners",
            ],
        )

    def test_repository_scope_unions_every_repository_runner_list(self):
        client, seen = self.client(
            {},
            {},
            runners={
                "/repos/o/a/actions/runners": [{"name": self.PREFIX + "01-aa"}],
                "/repos/o/b/actions/runners": [{"name": self.PREFIX + "02-bb"}],
            },
        )
        observer = MODULE.GitHubDemandObserver(client, ("o/a", "o/b"))

        slots, _ = observer.registered_slots(self.PREFIX)

        self.assertEqual(slots, frozenset({1, 2}))
        self.assertEqual(
            seen, ["/repos/o/a/actions/runners", "/repos/o/b/actions/runners"]
        )

    def test_canceled_receipt_checks_org_runners_under_organization_scope(self):
        value = receipt_v3()
        client, seen = self.client(
            {},
            {},
            runners={"/orgs/example-org/actions/runners": [{"id": 761}]},
        )
        observer = MODULE.GitHubDemandObserver(
            client,
            ("example-org/example-repo", "example-org/another-repo"),
            runner_scope="organization",
        )

        with self.assertRaisesRegex(MODULE.ObservationError, "remains registered"):
            MODULE.verify_receipt_jobs(value, observer, "outrunner-gcp-linux-x64")
        self.assertIn("/orgs/example-org/actions/runners", seen)

    def test_organization_scope_refuses_repositories_of_two_owners(self):
        with self.assertRaisesRegex(MODULE.BrokerError, "single owner"):
            MODULE.GitHubDemandObserver(
                mock.Mock(), ("o/a", "p/b"), runner_scope="organization"
            )
        with self.assertRaisesRegex(MODULE.BrokerError, "runner scope"):
            MODULE.GitHubDemandObserver(mock.Mock(), "o/a", runner_scope="enterprise")

    def test_receipt_job_from_any_served_repository_is_accepted(self):
        value = receipt_v3()
        value["jobs"][0]["repository"] = "example-org/another-repo"
        value["jobs"][0]["job_workflow_ref"] = (
            "example-org/another-repo/.github/workflows/test.yml@refs/pull/9/merge"
        )
        served = ("example-org/example-repo", "example-org/another-repo")

        result = MODULE.validate_drain_receipt(
            value, instance(1), broker_policy(), served
        )

        self.assertEqual(result["jobs"][0]["repository"], "example-org/another-repo")
        with self.assertRaisesRegex(MODULE.ObservationError, "job identity"):
            MODULE.validate_drain_receipt(
                value, instance(1), broker_policy(), "example-org/example-repo"
            )

    def test_receipt_workflow_ref_must_name_the_jobs_own_repository(self):
        value = receipt_v3()
        value["jobs"][0]["repository"] = "example-org/another-repo"
        # The ref still points at also-ranApp: a job cannot borrow another
        # served repository's workflow identity.
        with self.assertRaisesRegex(MODULE.ObservationError, "v3 job identity"):
            MODULE.validate_drain_receipt(
                value,
                instance(1),
                broker_policy(),
                ("example-org/example-repo", "example-org/another-repo"),
            )

    def test_receipt_jobs_are_verified_in_their_own_repository(self):
        value = receipt()
        value["jobs"][0]["repository"] = "example-org/another-repo"
        client = mock.Mock()
        client.get.return_value = {
            "jobs": [
                {
                    "runner_name": "also-ran-gcp-prod-01-2baa95cf",
                    "runner_id": 761,
                    "run_id": 35159242947,
                    "labels": ["outrunner-gcp-linux-x64"],
                    "status": "completed",
                    "conclusion": "success",
                    "completed_at": "2026-09-16T22:56:58Z",
                }
            ]
        }
        observer = MODULE.GitHubDemandObserver(
            client, ("example-org/example-repo", "example-org/another-repo")
        )

        MODULE.verify_receipt_jobs(value, observer, "outrunner-gcp-linux-x64")

        self.assertEqual(
            client.get.call_args.args[0],
            "/repos/example-org/another-repo/actions/runs/35159242947/jobs",
        )
        narrow = MODULE.GitHubDemandObserver(client, "example-org/example-repo")
        with self.assertRaisesRegex(MODULE.ObservationError, "not served"):
            MODULE.verify_receipt_jobs(value, narrow, "outrunner-gcp-linux-x64")

    def test_shipped_policy_serves_also_ran_and_another_repo_at_organization_scope(self):
        # This rollout keeps also-ran first for attribution of older state and
        # admits another repository only with the organization-registration image.
        policy = json.loads(
            CONTROLLER.with_name("gcp-acceptance-policy.json").read_text()
        )
        repositories = MODULE.configured_repositories(policy)
        self.assertEqual(
            repositories, ("example-org/example-repo", "example-org/another-repo")
        )
        self.assertEqual(
            MODULE.configured_runner_scope(policy, repositories), "organization"
        )



class RetiredRepositoryTests(unittest.TestCase):
    """A repository or organization leaves by retirement, never by deletion."""

    LABEL = "outrunner-gcp-general-linux-x64"
    PREFIX = "also-ran-gcp-general-"
    NOW = dt.datetime(2026, 9, 25, tzinfo=dt.timezone.utc)

    def test_retired_running_job_holds_its_host_but_its_queue_is_not_demand(self):
        client = mock.Mock()
        client.last_headers = {"x-ratelimit-remaining": "4000"}

        def get(path, query=None):
            if path.endswith("/actions/runners"):
                return {
                    "runners": [{"name": self.PREFIX + "04-old001"}]
                    if path == "/repos/o/also-ran/actions/runners"
                    else []
                }
            if path.endswith("/actions/runs"):
                if (
                    path == "/repos/o/also-ran/actions/runs"
                    and query.get("page") == 1
                    and query.get("status") == "in_progress"
                ):
                    return {"workflow_runs": [{"id": 1}]}
                return {"workflow_runs": []}
            return {
                "jobs": [
                    {"labels": [self.LABEL], "status": "queued"},
                    {
                        "labels": [self.LABEL],
                        "status": "in_progress",
                        "runner_name": self.PREFIX + "04-old001",
                    },
                ]
            }

        client.get.side_effect = get
        observer = MODULE.GitHubDemandObserver(
            client, "o/another-repo", retired_repositories=("o/also-ran",)
        )

        demand = observer.observe(self.LABEL, self.PREFIX, self.NOW)
        slots, _ = observer.registered_slots(self.PREFIX)

        self.assertEqual(demand.queued, 0)
        self.assertEqual(demand.active_owned, 1)
        self.assertEqual(demand.active_slots, frozenset({4}))
        self.assertEqual(slots, frozenset({4}))

    def test_receipt_from_a_retired_repository_is_still_verifiable(self):
        value = receipt()
        client = mock.Mock()
        client.get.return_value = {
            "jobs": [
                {
                    "runner_name": "also-ran-gcp-prod-01-2baa95cf",
                    "runner_id": 761,
                    "run_id": 35159242947,
                    "labels": ["outrunner-gcp-linux-x64"],
                    "status": "completed",
                    "conclusion": "success",
                    "completed_at": "2026-09-16T22:56:58Z",
                }
            ]
        }
        observer = MODULE.GitHubDemandObserver(
            client,
            "example-org/another-repo",
            retired_repositories=("example-org/example-repo",),
        )

        MODULE.verify_receipt_jobs(value, observer, "outrunner-gcp-linux-x64")
        self.assertEqual(
            client.get.call_args.args[0],
            "/repos/example-org/example-repo/actions/runs/35159242947/jobs",
        )

    def test_retired_organization_keeps_its_own_runner_list(self):
        # Moving from org o to repositories under org p must still read o.
        observer = MODULE.GitHubDemandObserver(
            mock.Mock(), ("p/a",), retired_organizations=("o",)
        )
        self.assertEqual(
            observer.runner_list_paths(),
            ("/orgs/o/actions/runners", "/repos/p/a/actions/runners"),
        )
        current = MODULE.GitHubDemandObserver(
            mock.Mock(),
            ("o/a",),
            runner_scope="organization",
            retired_organizations=("o",),
        )
        self.assertEqual(current.runner_organizations, ("o",))

    def test_tripwire_names_what_the_policy_stopped_watching(self):
        prior = {
            "github_repositories": ["o/also-ran", "o/another-repo"],
            "github_retired_repositories": ["o/old"],
            "github_runner_organizations": ["o"],
        }
        observer = MODULE.GitHubDemandObserver(mock.Mock(), ("o/another-repo",))

        self.assertEqual(
            MODULE.dropped_without_retirement(prior, observer, (instance(1),)),
            ["o/also-ran", "o/old", "organization o"],
        )
        self.assertEqual(MODULE.dropped_without_retirement(prior, observer, ()), [])
        retired = MODULE.GitHubDemandObserver(
            mock.Mock(),
            ("o/another-repo",),
            retired_repositories=("o/also-ran", "o/old"),
            retired_organizations=("o",),
        )
        self.assertEqual(
            MODULE.dropped_without_retirement(prior, retired, (instance(1),)), []
        )
        self.assertEqual(
            MODULE.dropped_without_retirement({}, observer, (instance(1),)), []
        )

    def test_decision_records_what_the_next_tick_is_checked_against(self):
        observer = MODULE.GitHubDemandObserver(
            mock.Mock(),
            ("o/another-repo",),
            runner_scope="organization",
            retired_repositories=("o/also-ran",),
        )
        decision = {}
        MODULE.record_repositories(decision, observer)
        self.assertEqual(
            decision,
            {
                "github_repositories": ["o/another-repo"],
                "github_runner_scope": "organization",
                "github_retired_repositories": ["o/also-ran"],
                "github_runner_organizations": ["o"],
            },
        )


HEAVY_LABEL = "outrunner-gcp-linux-x64"
GENERAL_LABEL = "outrunner-gcp-general-linux-x64"


def fake_actions_client():
    """Two active runs mixing heavy and general jobs; counts every request."""
    client = mock.Mock(last_headers={"x-ratelimit-remaining": "4000"})

    def get(path, query):
        if path.endswith("/actions/runs"):
            runs = {"queued": [{"id": 1}], "in_progress": [{"id": 2}]}
            return {"workflow_runs": runs[query["status"]] if query["page"] == 1 else []}
        if query["page"] != 1:
            return {"jobs": []}
        if path.endswith("/runs/1/jobs"):
            return {"jobs": [
                {"labels": [HEAVY_LABEL], "status": "queued", "runner_name": None},
                {"labels": [GENERAL_LABEL], "status": "queued", "runner_name": None},
            ]}
        return {"jobs": [
            {"labels": [HEAVY_LABEL], "status": "in_progress",
             "runner_name": "also-ran-gcp-prod-07-1b7632cd"},
            {"labels": [GENERAL_LABEL], "status": "in_progress",
             "runner_name": "also-ran-gcp-general-03-aa11bb22"},
            {"labels": [GENERAL_LABEL], "status": "completed",
             "runner_name": "also-ran-gcp-general-04-aa11bb22"},
        ]}

    client.get.side_effect = get
    return client


class DemandSnapshotTests(unittest.TestCase):
    NOW = dt.datetime(2026, 10, 2, 1, 0, tzinfo=dt.timezone.utc)

    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.path = self.root.name + "/github-demand-snapshot.json"

    def observer(self, client=None):
        return MODULE.GitHubDemandObserver(
            client or fake_actions_client(), "example-org/example-repo"
        )

    def test_snapshot_demand_equals_live_demand_for_each_pool(self):
        for label, prefix in (
            (HEAVY_LABEL, "also-ran-gcp-prod-"),
            (GENERAL_LABEL, "also-ran-gcp-general-"),
        ):
            live = self.observer().observe(label, prefix, self.NOW)
            store = MODULE.DemandSnapshotStore(self.path + label)
            shared = self.observer().observe(
                label, prefix, self.NOW, store, MODULE.DEMAND_SNAPSHOT_MAX_AGE
            )
            for field in ("queued", "active_owned", "rate_remaining",
                          "active_slots", "unattributed_active", "by_repository"):
                self.assertEqual(getattr(shared, field), getattr(live, field), field)
            self.assertIsNone(live.snapshot_age_ms)
            self.assertIsNone(shared.snapshot_age_ms)

    def test_one_fetch_serves_both_pools(self):
        client = fake_actions_client()
        store = MODULE.DemandSnapshotStore(self.path)
        heavy = self.observer(client).observe(
            HEAVY_LABEL, "also-ran-gcp-prod-", self.NOW, store,
            MODULE.DEMAND_SNAPSHOT_MAX_AGE,
        )
        requests = client.get.call_count
        general = self.observer(client).observe(
            GENERAL_LABEL, "also-ran-gcp-general-", self.NOW, store,
            MODULE.DEMAND_SNAPSHOT_MAX_AGE,
        )

        self.assertEqual(client.get.call_count, requests)
        self.assertEqual((heavy.queued, heavy.active_slots), (1, frozenset({7})))
        self.assertEqual((general.queued, general.active_slots), (1, frozenset({3})))
        self.assertIsNotNone(general.snapshot_age_ms)
        self.assertEqual(general.observed_at, heavy.observed_at)

    def test_stale_snapshot_is_refetched(self):
        client = fake_actions_client()
        store = MODULE.DemandSnapshotStore(self.path)
        observer = self.observer(client)
        observer.observe(HEAVY_LABEL, "also-ran-gcp-prod-", self.NOW, store,
                         MODULE.DEMAND_SNAPSHOT_MAX_AGE)
        requests = client.get.call_count
        payload = json.loads(pathlib.Path(self.path).read_text())
        payload["observed_at"] = (
            dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=20)
        ).isoformat().replace("+00:00", "Z")
        pathlib.Path(self.path).write_text(json.dumps(payload))

        reused = observer.observe(HEAVY_LABEL, "also-ran-gcp-prod-", self.NOW, store,
                                  MODULE.DEMAND_SNAPSHOT_MAX_AGE)
        self.assertEqual(client.get.call_count, requests)
        self.assertGreaterEqual(reused.snapshot_age_ms, 20000)

        refreshed = observer.observe(HEAVY_LABEL, "also-ran-gcp-prod-", self.NOW, store,
                                     MODULE.DEMAND_REFRESH_MAX_AGE)
        self.assertEqual(client.get.call_count, 2 * requests)
        self.assertIsNone(refreshed.snapshot_age_ms)

    def test_unusable_snapshot_is_refetched_not_fatal(self):
        future = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5))
        cases = {
            "malformed": "{not json",
            "other_repositories": None,
            "future": future.isoformat().replace("+00:00", "Z"),
            "bad_job": None,
        }
        for case, value in cases.items():
            pathlib.Path(self.path).unlink(missing_ok=True)
            client = fake_actions_client()
            store = MODULE.DemandSnapshotStore(self.path)
            observer = self.observer(client)
            observer.observe(HEAVY_LABEL, "also-ran-gcp-prod-", self.NOW, store,
                             MODULE.DEMAND_SNAPSHOT_MAX_AGE)
            requests = client.get.call_count
            if case == "malformed":
                pathlib.Path(self.path).write_text(value)
            else:
                payload = json.loads(pathlib.Path(self.path).read_text())
                if case == "other_repositories":
                    payload["repositories"] = ["example-org/another-repo"]
                elif case == "future":
                    payload["observed_at"] = value
                else:
                    payload["repositories_jobs"]["example-org/example-repo"][0]["status"] = 1
                pathlib.Path(self.path).write_text(json.dumps(payload))
            demand = observer.observe(HEAVY_LABEL, "also-ran-gcp-prod-", self.NOW, store,
                                      MODULE.DEMAND_SNAPSHOT_MAX_AGE)
            self.assertEqual(client.get.call_count, 2 * requests, case)
            self.assertIsNone(demand.snapshot_age_ms, case)

    def test_concurrent_readers_share_one_fetch(self):
        client = fake_actions_client()
        fetches = []
        real = MODULE.GitHubDemandObserver.active_jobs

        def slow_fetch(observer, now):
            fetches.append(now)
            time.sleep(0.1)
            return real(observer, now)

        results = []
        def read(label, prefix):
            results.append(self.observer(client).observe(
                label, prefix, self.NOW, MODULE.DemandSnapshotStore(self.path),
                MODULE.DEMAND_SNAPSHOT_MAX_AGE,
            ))

        with mock.patch.object(MODULE.GitHubDemandObserver, "active_jobs", slow_fetch):
            threads = [
                threading.Thread(target=read, args=(HEAVY_LABEL, "also-ran-gcp-prod-")),
                threading.Thread(target=read, args=(GENERAL_LABEL, "also-ran-gcp-general-")),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.assertEqual(len(fetches), 1)
        self.assertEqual(len(results), 2)

    def test_demand_fetch_uses_priority_slot_while_other_reads_hold_the_rest(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(
            os.environ,
            ALSO_RAN_GITHUB_BUDGET=root + "/budget.json",
            ALSO_RAN_GITHUB_CONCURRENCY="2",
        ):
            release = threading.Event()
            holding = threading.Event()
            holder = MODULE.JsonClient("token", "https://api.github.com")

            def hold(path, query):
                holding.set()
                release.wait(5)
                return {}

            holder._get = hold
            blocker = threading.Thread(target=holder.get, args=("/receipt",))
            blocker.start()
            self.assertTrue(holding.wait(5))
            try:
                fake = fake_actions_client()
                client = MODULE.CountingJsonClient("token", "https://api.github.com")
                client._get = lambda path, query: fake.get(path, query)
                active = self.observer(client).active_jobs(self.NOW)
            finally:
                release.set()
                blocker.join(5)
            jobs = active["repositories"]["example-org/example-repo"]
            # Run order is preserved even though job listings are read concurrently.
            self.assertEqual([job["status"] for job in jobs],
                             ["queued", "queued", "in_progress", "in_progress", "completed"])
            self.assertEqual(client.requests, 4)

    def test_fallback_delete_gate_reads_github_live(self):
        observer = mock.Mock()
        observer.observe.return_value = MODULE.Demand(
            queued=0, active_owned=0, rate_remaining=4000
        )
        observer.runner_slots.return_value = (frozenset(), frozenset(), 0)
        observer.client.last_headers = {"x-ratelimit-remaining": "4000"}
        MODULE.fallback_delete_block_reason(
            observer, production(), 3, {"observed_at": "2026-10-02T01:00:00Z"}
        )
        args = observer.observe.call_args
        self.assertEqual(len(args.args), 3)
        self.assertNotIn("snapshot", args.kwargs)

    def test_maintenance_stamps_data_time_and_refresh_bound(self):
        candidate = json.loads((CONTROLLER.parent / "gcp-acceptance-policy.json").read_text())
        data_at = dt.datetime(2026, 10, 2, 0, 59, 50, tzinfo=dt.timezone.utc)
        calls = []

        def observe(self_, label, prefix, now, snapshot=None, max_age=None, newer_than=None):
            calls.append((snapshot, max_age, newer_than))
            return MODULE.Demand(queued=0, active_owned=0, rate_remaining=4000,
                                 observed_at=data_at, snapshot_age_ms=7000)

        with tempfile.TemporaryDirectory() as root:
            policy_path = pathlib.Path(root) / "policy.json"
            market_path = pathlib.Path(root) / "market.json"
            state_path = pathlib.Path(root) / "state.json"
            policy_path.write_text(json.dumps(candidate))
            market_path.write_text("{}")
            with (mock.patch.object(MODULE, "read_secret", return_value="test-only"),
                  mock.patch.object(MODULE.GitHubDemandObserver, "observe", observe),
                  mock.patch.object(MODULE, "observe_instances", return_value=()),
                  contextlib.redirect_stdout(io.StringIO())):
                self.assertEqual(MODULE.main([
                    "--policy", str(policy_path), "--market-state", str(market_path),
                    "--state", str(state_path), "--lock", root + "/lock",
                    "--github-token-file", "unused",
                    "--demand-snapshot", root + "/github-demand-snapshot.json",
                    "--once", "--dry-run", "--maintenance-only"]), 0)
            state = json.loads(state_path.read_text())

        self.assertEqual([max_age for _, max_age, _ in calls],
                         [MODULE.DEMAND_SNAPSHOT_MAX_AGE, MODULE.DEMAND_REFRESH_MAX_AGE])
        self.assertTrue(all(isinstance(store, MODULE.DemandSnapshotStore) for store, _, _ in calls))
        # The refresh names the opening read's data; it may reuse it only while
        # it is within DEMAND_REFRESH_MAX_AGE.
        self.assertEqual([newer for _, _, newer in calls], [None, data_at])
        self.assertEqual(state["observed_at"], "2026-10-02T00:59:50Z")
        self.assertEqual(state["initial_demand"]["observed_at"], "2026-10-02T00:59:50Z")
        self.assertEqual(state["demand_snapshot_age_ms"], 7000)
        self.assertEqual(state["initial_demand"]["snapshot_age_ms"], 7000)


class DemandSnapshotFailureTests(unittest.TestCase):
    NOW = dt.datetime(2026, 10, 2, 1, 0, tzinfo=dt.timezone.utc)

    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.path = self.root.name + "/github-demand-snapshot.json"
        self.client = fake_actions_client()
        self.observer = MODULE.GitHubDemandObserver(self.client, "example-org/example-repo")

    def observe(self, store):
        with contextlib.redirect_stderr(io.StringIO()) as stderr:
            demand = self.observer.observe(
                HEAVY_LABEL, "also-ran-gcp-prod-", self.NOW, store,
                MODULE.DEMAND_SNAPSHOT_MAX_AGE,
            )
        return demand, stderr.getvalue()

    def test_unusable_lock_reads_live(self):
        store = MODULE.DemandSnapshotStore(self.root.name + "/missing-dir/snapshot.json")
        demand, warning = self.observe(store)
        self.assertEqual((demand.queued, demand.active_slots), (1, frozenset({7})))
        self.assertIsNone(demand.snapshot_age_ms)
        self.assertIn("demand snapshot lock failed", warning)

    def test_failed_write_keeps_fetched_demand(self):
        store = MODULE.DemandSnapshotStore(self.path)
        with mock.patch.object(MODULE, "atomic_write_json", side_effect=PermissionError("denied")):
            demand, warning = self.observe(store)
        self.assertEqual((demand.queued, demand.active_slots), (1, frozenset({7})))
        self.assertIn("demand snapshot write failed", warning)

    def test_failed_fetch_propagates_and_releases_lock(self):
        store = MODULE.DemandSnapshotStore(self.path)
        with mock.patch.object(
            MODULE.GitHubDemandObserver, "active_jobs",
            side_effect=MODULE.ObservationError("GitHub reads paused by shared backoff"),
        ):
            with self.assertRaises(MODULE.ObservationError):
                self.observe(store)
        self.assertFalse(pathlib.Path(self.path).exists())
        with open(self.path + ".lock", "a", encoding="utf-8") as handle:
            MODULE.fcntl.flock(handle, MODULE.fcntl.LOCK_EX | MODULE.fcntl.LOCK_NB)
        demand, _ = self.observe(store)
        self.assertEqual(demand.queued, 1)


class DemandSnapshotWaiterTests(unittest.TestCase):
    NOW = dt.datetime(2026, 10, 2, 2, 0, tzinfo=dt.timezone.utc)

    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.path = self.root.name + "/github-demand-snapshot.json"
        self.client = fake_actions_client()
        self.observer = MODULE.GitHubDemandObserver(self.client, "example-org/example-repo")
        self.store = MODULE.DemandSnapshotStore(self.path)

    def seed(self, observed_ago, completed_ago):
        self.observer.observe(HEAVY_LABEL, "also-ran-gcp-prod-", self.NOW, self.store,
                              MODULE.DEMAND_SNAPSHOT_MAX_AGE)
        now = dt.datetime.now(dt.timezone.utc)
        payload = json.loads(pathlib.Path(self.path).read_text())
        stamp = lambda ago: (now - dt.timedelta(seconds=ago)).isoformat().replace("+00:00", "Z")
        payload["observed_at"] = stamp(observed_ago)
        payload["completed_at"] = None if completed_ago is None else stamp(completed_ago)
        pathlib.Path(self.path).write_text(json.dumps(payload))
        return self.client.get.call_count

    def read(self, requested_ago, newer_than=None, max_age=MODULE.DEMAND_SNAPSHOT_MAX_AGE):
        requested = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=requested_ago)
        real_now = dt.datetime.now
        calls = iter([requested])

        class Clock(dt.datetime):
            @classmethod
            def now(cls, tz=None):
                # The first clock read is the request time; later ones are real.
                return next(calls, None) or real_now(tz)

        with mock.patch.object(MODULE.dt, "datetime", Clock):
            return self.observer.observe(HEAVY_LABEL, "also-ran-gcp-prod-", self.NOW,
                                         self.store, max_age, newer_than)

    def test_waiter_reuses_fetch_that_completed_while_it_waited(self):
        # Fetch started 90s ago, finished 5s ago; this reader asked 60s ago.
        requests = self.seed(observed_ago=90, completed_ago=5)
        demand = self.read(requested_ago=60)
        self.assertEqual(self.client.get.call_count, requests)
        self.assertGreaterEqual(demand.snapshot_age_ms, 90000)
        self.assertGreaterEqual(demand.snapshot_wait_ms, 59000)

    def test_old_fetch_completed_before_the_request_is_refetched(self):
        requests = self.seed(observed_ago=90, completed_ago=70)
        demand = self.read(requested_ago=60)
        self.assertEqual(self.client.get.call_count, 2 * requests)
        self.assertIsNone(demand.snapshot_age_ms)

    def test_fetch_started_too_long_before_the_request_is_refetched(self):
        lead = MODULE.DEMAND_SNAPSHOT_MAX_FETCH_LEAD.total_seconds()
        requests = self.seed(observed_ago=lead + 70, completed_ago=5)
        self.read(requested_ago=60)
        self.assertEqual(self.client.get.call_count, 2 * requests)

    def test_refresh_reuses_opening_data_while_it_is_fresh(self):
        requests = self.seed(observed_ago=2, completed_ago=1)
        opening = self.read(requested_ago=0)
        refreshed = self.read(requested_ago=0, newer_than=opening.observed_at,
                              max_age=MODULE.DEMAND_REFRESH_MAX_AGE)
        self.assertEqual(self.client.get.call_count, requests)
        self.assertEqual(refreshed.observed_at, opening.observed_at)

    def test_refresh_refetches_opening_data_older_than_refresh_age(self):
        refresh_age = MODULE.DEMAND_REFRESH_MAX_AGE.total_seconds()
        requests = self.seed(observed_ago=refresh_age + 5, completed_ago=1)
        opening = self.read(requested_ago=0)
        self.assertEqual(self.client.get.call_count, requests)
        refreshed = self.read(requested_ago=0, newer_than=opening.observed_at,
                              max_age=MODULE.DEMAND_REFRESH_MAX_AGE)
        self.assertEqual(self.client.get.call_count, 2 * requests)
        self.assertGreater(refreshed.observed_at, opening.observed_at)

    def test_refresh_never_takes_its_own_stale_data_as_completed_while_waiting(self):
        # The opening fetch completed after the refresh asked, but its data is
        # what the refresh replaces and is past the refresh age.
        refresh_age = MODULE.DEMAND_REFRESH_MAX_AGE.total_seconds()
        requests = self.seed(observed_ago=refresh_age + 30, completed_ago=1)
        payload = json.loads(pathlib.Path(self.path).read_text())
        opening_at = MODULE.parse_utc_timestamp(payload["observed_at"], "observed_at")
        self.read(requested_ago=10, newer_than=opening_at,
                  max_age=MODULE.DEMAND_REFRESH_MAX_AGE)
        self.assertEqual(self.client.get.call_count, 2 * requests)

    def test_inconsistent_completion_time_is_refetched(self):
        requests = self.seed(observed_ago=5, completed_ago=10)  # completed before start
        self.read(requested_ago=0)
        self.assertEqual(self.client.get.call_count, 2 * requests)

    def test_legacy_snapshot_without_completion_still_reused_by_age(self):
        requests = self.seed(observed_ago=5, completed_ago=None)
        demand = self.read(requested_ago=0)
        self.assertEqual(self.client.get.call_count, requests)
        self.assertIsNotNone(demand.snapshot_age_ms)

class LowestRateRemainingTests(unittest.TestCase):
    def test_lowest_count_in_the_latest_window_wins(self):
        self.assertEqual(MODULE.lowest_rate_remaining([
            {"x-ratelimit-remaining": "751", "x-ratelimit-reset": "100"},
            {"x-ratelimit-remaining": "749", "x-ratelimit-reset": "100"},
            {"x-ratelimit-remaining": "750", "x-ratelimit-reset": "100"},
        ]), 749)

    def test_a_reset_window_replaces_the_old_count(self):
        self.assertEqual(MODULE.lowest_rate_remaining([
            {"x-ratelimit-remaining": "3", "x-ratelimit-reset": "100"},
            {"x-ratelimit-remaining": "4999", "x-ratelimit-reset": "200"},
        ]), 4999)

    def test_snapshot_counts_the_job_reads_from_worker_threads(self):
        client = MODULE.CountingJsonClient("token", "https://api.github.com")

        def github(path, query):
            jobs = path.endswith("/jobs")
            client.last_headers = {"x-ratelimit-remaining": "749" if jobs else "751",
                                   "x-ratelimit-reset": "100"}
            if jobs:
                return {"jobs": [{"labels": [HEAVY_LABEL], "status": "queued"}]
                        if query["page"] == 1 else []}
            runs = {"queued": [{"id": 1}, {"id": 2}], "in_progress": []}
            return {"workflow_runs": runs[query["status"]] if query["page"] == 1 else []}

        client._get = github
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")
        active = observer.active_jobs(dt.datetime.now(dt.timezone.utc))
        self.assertEqual(active["rate_remaining"], 749)

        # An exhausted allowance is zero, not unknown.
        def exhausted(path, query):
            result = github(path, query)
            if path.endswith("/jobs"):
                client.last_headers["x-ratelimit-remaining"] = "0"
            return result

        client._get = exhausted
        observer = MODULE.GitHubDemandObserver(client, "example-org/example-repo")
        self.assertEqual(observer.active_jobs(dt.datetime.now(dt.timezone.utc))["rate_remaining"], 0)

    def test_no_headers_is_unknown(self):
        self.assertIsNone(MODULE.lowest_rate_remaining([{}, {"etag": "x"}]))


class Reply(io.BytesIO):
    """A urlopen response carrying a JSON body."""

    def __init__(self, payload):
        super().__init__(json.dumps(payload).encode())


def http_error(code, body=b"{}"):
    import urllib.error

    return urllib.error.HTTPError("https://example.invalid", code, "error", {}, io.BytesIO(body))


class GcpRestTests(unittest.TestCase):
    """The broker's provider reads go straight to REST, not through gcloud."""

    def setUp(self):
        MODULE._gcp_token = None
        self.addCleanup(setattr, MODULE, "_gcp_token", None)

    def token_process(self, *tokens):
        return mock.patch.object(
            MODULE.subprocess,
            "run",
            side_effect=[
                mock.Mock(returncode=0, stdout=f"{token}\n") for token in tokens
            ],
        )

    def test_token_is_reused_inside_its_window_and_renewed_after(self):
        clock = [0.0]
        with self.token_process("first", "second") as run, \
                mock.patch.object(MODULE.time, "monotonic", side_effect=lambda: clock[0]):
            self.assertEqual(MODULE.gcp_access_token(), "first")
            clock[0] = MODULE.GCP_TOKEN_REUSE_SECONDS - 1
            self.assertEqual(MODULE.gcp_access_token(), "first")
            clock[0] = MODULE.GCP_TOKEN_REUSE_SECONDS
            self.assertEqual(MODULE.gcp_access_token(), "second")
        self.assertEqual(run.call_count, 2)
        self.assertEqual(run.call_args.args[0], ["gcloud", "auth", "print-access-token"])

    def test_missing_token_is_an_observation_error(self):
        with mock.patch.object(
            MODULE.subprocess, "run", return_value=mock.Mock(returncode=1, stdout="")
        ):
            with self.assertRaisesRegex(MODULE.ObservationError, "access token"):
                MODULE.gcp_access_token()

    def test_request_carries_the_bearer_token_and_query(self):
        with self.token_process("secret"), \
                mock.patch("urllib.request.urlopen", return_value=Reply({"ok": True})) as urlopen:
            self.assertEqual(
                MODULE.compute_api("projects/p/regions/r", {"filter": "a = \"b\""}),
                {"ok": True},
            )
        request = urlopen.call_args.args[0]
        self.assertEqual(
            request.full_url,
            "https://compute.googleapis.com/compute/v1/projects/p/regions/r"
            "?filter=a+%3D+%22b%22",
        )
        self.assertEqual(request.get_header("Authorization"), "Bearer secret")

    def test_rejected_token_is_renewed_once(self):
        with self.token_process("stale", "fresh"), \
                mock.patch("urllib.request.urlopen", side_effect=[http_error(401), Reply({"ok": 1})]) as urlopen:
            self.assertEqual(MODULE.compute_api("projects/p"), {"ok": 1})
        self.assertEqual(
            [call.args[0].get_header("Authorization") for call in urlopen.call_args_list],
            ["Bearer stale", "Bearer fresh"],
        )

    def test_provider_errors_fail_closed_as_observation_errors(self):
        for failure in (
            http_error(403, b'{"error": "quota"}'),
            http_error(401),
            TimeoutError("slow"),
            OSError("offline"),
        ):
            MODULE._gcp_token = None
            with self.subTest(failure=failure), self.token_process("a", "b"), \
                    mock.patch("urllib.request.urlopen", side_effect=[failure, failure]):
                with self.assertRaisesRegex(MODULE.ObservationError, "/compute/v1/projects/p"):
                    MODULE.compute_api("projects/p")

    def test_aggregated_list_flattens_every_scope_and_page(self):
        pages = [
            {
                "items": {
                    "zones/a": {"instances": [{"name": "one"}]},
                    "zones/b": {"warning": {"code": "NO_RESULTS_ON_PAGE"}},
                },
                "nextPageToken": "next",
            },
            {"items": {"zones/c": {"instances": [{"name": "two"}]}}},
        ]
        requests = []

        def api(path, query=None):
            requests.append((path, dict(query or {})))
            return pages[len(requests) - 1]

        with mock.patch.object(MODULE, "compute_api", side_effect=api):
            listed = MODULE.compute_aggregated_list("p", "instances", 'labels.x = "y"')

        self.assertEqual([item["name"] for item in listed], ["one", "two"])
        self.assertEqual(
            requests,
            [
                ("projects/p/aggregated/instances", {"filter": 'labels.x = "y"'}),
                (
                    "projects/p/aggregated/instances",
                    {"filter": 'labels.x = "y"', "pageToken": "next"},
                ),
            ],
        )

    @mock.patch.object(MODULE, "compute_api")
    def test_an_unreachable_zone_fails_the_inventory_instead_of_hiding_hosts(self, api):
        for page in (
            {"items": {"zones/a": {"warning": {"code": "UNREACHABLE"}}}},
            {"items": {}, "unreachables": ["zones/a"]},
        ):
            api.side_effect = [page]
            with self.subTest(page=page), self.assertRaises(MODULE.ObservationError):
                MODULE.compute_aggregated_list("p", "instances")

    @mock.patch.object(MODULE, "compute_aggregated_list", return_value=[])
    def test_instance_inventory_filters_on_the_pool_labels_server_side(self, listing):
        candidate = broker_policy()

        MODULE.observe_instances(candidate)

        listing.assert_called_once_with(
            "also-ran-ci-runners",
            "instances",
            'labels.managed-by = "runner-autoscaler" AND labels.fleet = "also-ran-ci" '
            f'AND labels.lifecycle = "{candidate["production"]["labels"]["lifecycle"]}" '
            'AND labels.provider = "gcp"',
        )


class CloudQuotaDefaultTests(unittest.TestCase):
    """A limit nobody raised is a default entry applied to a list of regions."""

    def setUp(self):
        MODULE.forget_quota_limits()
        self.addCleanup(MODULE.forget_quota_limits)

    @mock.patch.object(MODULE, "gcp_api")
    def test_default_entry_applies_to_its_listed_regions(self, api):
        api.return_value = {"dimensionsInfos": [
            {"dimensions": {"vm_family": "C3"}, "details": {"value": "3000"},
             "applicableLocations": ["northamerica-northeast1"]},
            {"dimensions": {"vm_family": "C4"}, "details": {"value": "6000"},
             "applicableLocations": ["europe-west9", "northamerica-northeast1"]},
            {"details": {}, "applicableLocations": ["northamerica-northeast1"]},
        ]}

        self.assertEqual(
            MODULE.local_ssd_quota_gb(broker_policy(), "northamerica-northeast1"), 6000
        )
        self.assertIn(
            "quotaInfos/LOCAL-SSD-TOTAL-GB-PER-VM-FAMILY-per-project-region",
            api.call_args.args[0],
        )

    @mock.patch.object(MODULE, "gcp_api")
    def test_a_regional_override_outranks_the_default(self, api):
        api.return_value = {"dimensionsInfos": [
            {"dimensions": {"vm_family": "C4"}, "details": {"value": "6000"},
             "applicableLocations": ["northamerica-northeast1"]},
            {"dimensions": {"region": "northamerica-northeast1", "vm_family": "C4"},
             "details": {"value": "12000"},
             "applicableLocations": ["northamerica-northeast1"]},
        ]}

        self.assertEqual(
            MODULE.local_ssd_quota_gb(broker_policy(), "northamerica-northeast1"), 12000
        )

    @mock.patch.object(MODULE, "gcp_api")
    def test_a_region_outside_every_entry_is_missing(self, api):
        api.return_value = {"dimensionsInfos": [
            {"dimensions": {"vm_family": "C4"}, "details": {"value": "6000"},
             "applicableLocations": ["europe-west9"]},
        ]}

        with self.assertRaisesRegex(MODULE.ObservationError, "is missing"):
            MODULE.local_ssd_quota_gb(broker_policy(), "northamerica-northeast1")


class QuotaLimitReuseTests(unittest.TestCase):
    """Cloud Quotas limits are reused off the hot path; usage never is."""

    def setUp(self):
        MODULE.forget_quota_limits()
        self.addCleanup(MODULE.forget_quota_limits)
        # The C4 local SSD limit is a live Cloud Quotas read; a test that is
        # not about it gets one that never binds, never the network.
        local_ssd = mock.patch.object(MODULE, "local_ssd_quota_gb", return_value=1_000_000)
        local_ssd.start()
        self.addCleanup(local_ssd.stop)

    def quota_info(self, value):
        return {"dimensionsInfos": [
            {"dimensions": {"region": "europe-west9", "vm_family": "C4"}, "details": {"value": str(value)}},
            {"dimensions": {"region": "us-west2", "vm_family": "C4"}, "details": {"value": "48"}},
        ]}

    @mock.patch.object(MODULE, "gcp_api")
    def test_limit_is_read_once_and_reused_inside_the_window(self, api):
        api.return_value = self.quota_info(96)
        policy = broker_policy()
        with mock.patch.object(MODULE.time, "monotonic", return_value=100.0):
            self.assertEqual(MODULE.c4_family_quota_vcpus(policy, "europe-west9"), 96)
            api.return_value = self.quota_info(8)
            self.assertEqual(MODULE.c4_family_quota_vcpus(policy, "europe-west9"), 96)
        self.assertEqual(api.call_count, 1)
        self.assertIn("quotaInfos/CPUS-PER-VM-FAMILY-per-project-region", api.call_args.args[0])

    @mock.patch.object(MODULE, "gcp_api")
    def test_each_region_and_quota_has_its_own_entry(self, api):
        api.return_value = self.quota_info(96)
        policy = broker_policy()
        self.assertEqual(MODULE.c4_family_quota_vcpus(policy, "europe-west9"), 96)
        self.assertEqual(MODULE.c4_family_quota_vcpus(policy, "us-west2"), 48)
        api.return_value = {"dimensionsInfos": [
            {"dimensions": {"region": "europe-west9"}, "details": {"value": "640"}},
        ]}
        self.assertEqual(MODULE.hyperdisk_quota_gb(policy, "europe-west9"), 640)
        self.assertEqual(api.call_count, 3)

    @mock.patch.object(MODULE, "gcp_api")
    def test_limit_is_read_again_after_the_window(self, api):
        api.return_value = self.quota_info(96)
        policy = broker_policy()
        with mock.patch.object(MODULE.time, "monotonic", return_value=100.0):
            MODULE.c4_family_quota_vcpus(policy, "europe-west9")
        api.return_value = self.quota_info(8)
        with mock.patch.object(
            MODULE.time, "monotonic", return_value=100.0 + MODULE.QUOTA_LIMIT_REUSE_SECONDS
        ):
            self.assertEqual(MODULE.c4_family_quota_vcpus(policy, "europe-west9"), 8)
        self.assertEqual(api.call_count, 2)

    @mock.patch.object(MODULE, "gcp_api")
    def test_a_failed_read_is_not_remembered(self, api):
        policy = broker_policy()
        api.side_effect = [MODULE.ObservationError("offline"), self.quota_info(96)]
        with self.assertRaisesRegex(MODULE.ObservationError, "could not read GCP quota"):
            MODULE.c4_family_quota_vcpus(policy, "europe-west9")
        self.assertEqual(MODULE.c4_family_quota_vcpus(policy, "europe-west9"), 96)

    @mock.patch.object(MODULE, "hyperdisk_usage_gb", return_value=0)
    @mock.patch.object(MODULE, "hyperdisk_quota_gb", return_value=640)
    @mock.patch.object(MODULE, "c4_family_usage_vcpus")
    @mock.patch.object(MODULE, "read_cloud_quota_value", return_value=100)
    @mock.patch.object(MODULE, "compute_api")
    def test_usage_is_live_while_the_limit_is_reused(
        self, api, read_limit, usage, _disk, _disk_usage
    ):
        api.return_value = {"quotas": [
            {"metric": "CPUS_ALL_REGIONS", "limit": 384, "usage": 0},
            {"metric": "PREEMPTIBLE_CPUS", "limit": 0, "usage": 0},
            {"metric": "INSTANCES", "limit": 24, "usage": 0},
            {"metric": "IN_USE_ADDRESSES", "limit": 24, "usage": 0},
        ]}
        policy = broker_policy()
        hosts = []
        for used in (0, 40, 80):
            usage.return_value = used
            hosts.append(
                MODULE.observe_create_capacity(policy, region="europe-west9")["family_cpu_hosts"]
            )
        self.assertEqual(hosts, [12, 7, 2])
        self.assertEqual(read_limit.call_count, 1)

    def test_a_refused_create_forgets_every_reused_limit(self):
        import argparse

        policy = broker_policy()
        MODULE._quota_limits[("p", "q", ())] = (1, float("inf"))
        args = argparse.Namespace(
            lock=f"{tempfile.mkdtemp()}/lock", pool="heavy", market_state="unused",
            apply=True, policy="unused", lifecycle_script="unused",
        )

        def admit(decision, *_args):
            decision["create_placements"] = [{"slot": 1, "zone": "europe-west9-a"}]

        with mock.patch.object(MODULE, "observe_instances", return_value=()), \
                mock.patch.object(MODULE, "load_json", return_value={}), \
                mock.patch.object(MODULE, "constrain_creates_to_regional_capacity", side_effect=admit), \
                mock.patch.object(
                    MODULE, "create_slots",
                    side_effect=MODULE.BrokerError("production create failed: QUOTA_EXCEEDED"),
                ):
            with self.assertRaisesRegex(MODULE.BrokerError, "QUOTA_EXCEEDED"):
                MODULE.admit_and_create({"create_slots": [1]}, policy, args)
        self.assertEqual(MODULE._quota_limits, {})

    def test_a_successful_create_keeps_the_reused_limits(self):
        import argparse

        policy = broker_policy()
        MODULE._quota_limits[("p", "q", ())] = (1, float("inf"))
        args = argparse.Namespace(
            lock=f"{tempfile.mkdtemp()}/lock", pool="heavy", market_state="unused",
            apply=True, policy="unused", lifecycle_script="unused",
        )

        def admit(decision, *_args):
            decision["create_placements"] = [{"slot": 1, "zone": "europe-west9-a"}]

        with mock.patch.object(MODULE, "observe_instances", return_value=()), \
                mock.patch.object(MODULE, "load_json", return_value={}), \
                mock.patch.object(MODULE, "constrain_creates_to_regional_capacity", side_effect=admit), \
                mock.patch.object(MODULE, "create_slots", return_value={"created": [{"ok": True}]}):
            self.assertEqual(
                MODULE.admit_and_create({"create_slots": [1]}, policy, args),
                {"created": [{"ok": True}]},
            )
        self.assertEqual(list(MODULE._quota_limits), [("p", "q", ())])


if __name__ == "__main__":
    unittest.main()
