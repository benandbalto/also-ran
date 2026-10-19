import contextlib
import importlib.util
import io
import json
import pathlib
import tempfile
import re
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[3] / "scripts" / "gcp-runner-lifecycle.py"
SPEC = importlib.util.spec_from_file_location("gcp_runner_lifecycle", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def policy():
    source_commit = "a" * 40
    return {
        "policy_version": 1,
        "project_id": "also-ran-ci-runners",
        "project_number": "123456789012",
        "image_source_commit": source_commit,
        "base_image": "projects/ubuntu-os-cloud/global/images/ubuntu-2404-test",
        "golden_image": "also-ran-runner-ubuntu2404-" + source_commit[:12],
        "machine_type": "n2-standard-8",
        "network": {
            "name": "also-ran-ci-runners",
            "subnet": "runner-hosts-us-central1",
            "subnet_region": "us-central1",
            "ssh_source_cidrs": ["192.0.2.1/32"],
            "egress": {
                "mode": "direct",
                "network_tier": "PREMIUM",
                "public_ingress_allowlist": ["192.0.2.1/32", "35.235.240.0/20"],
            },
        },
        "identities": {
            "builder": "also-ran-runner-builder@also-ran-ci-runners.iam.gserviceaccount.com",
            "runtime": "also-ran-runner-runtime@also-ran-ci-runners.iam.gserviceaccount.com",
        },
        "builder": {
            "name": "also-ran-runner-image-builder-gcp",
            "zone": "us-central1-c",
            "machine_type": "e2-standard-4",
            "boot_disk_gb": 32,
            "labels": {
                "managed-by": "also-ran-image-builder",
                "fleet": "also-ran-ci",
                "lifecycle": "image-build",
                "provider": "gcp",
            },
        },
        "acceptance": {
            "name": "also-ran-runner-acceptance-gcp-drain-01",
            "zone": "us-central1-a",
            "machine_type": "c4-standard-8-lssd",
            "boot_disk_gb": 32,
            "boot_disk_type": "hyperdisk-balanced",
            "boot_disk_provisioned_iops": 3000,
            "boot_disk_provisioned_throughput": 140,
            "use_local_ssd": True,
            "provisioning_model": "SPOT",
            "maximum_hosts": 1,
            "outrunner_max_runners": 1,
            "automatic_delete": False,
            "scale_set": "also-ran-gcp-drain-01",
            "diagnostic_label": "also-ran-gcp-drain-01",
            "include_shared_label": False,
            "labels": {
                "managed-by": "runner-autoscaler",
                "fleet": "also-ran-ci",
                "lifecycle": "acceptance",
                "autoscale-slot": "gcp-drain-01",
                "provider": "gcp",
                "hold": "true",
            },
        },
        "campaign": {
            "name_prefix": "also-ran-runner-campaign-gcp",
            "zone_cycle": ["us-central1-a", "us-central1-b"],
            "maximum_hosts": 20,
            "boot_disk_gb": 100,
            "scale_set_prefix": "also-ran-gcp-campaign",
            "diagnostic_label_prefix": "also-ran-gcp-campaign",
            "include_shared_label": False,
            "labels": {
                "managed-by": "runner-autoscaler",
                "fleet": "also-ran-ci",
                "lifecycle": "campaign",
                "provider": "gcp",
                "hold": "true",
            },
        },
        "production": {
            "name_prefix": "also-ran-runner-gcp-prod",
            "zone_cycle": [
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
            ],
            "maximum_hosts": 28,
            "minimum_hosts": 0,
            "retention_mode": "demand_only",
            "max_jobs_per_host": 5,
            "vcpus_per_host": 8,
            "autoscale_slot_prefix": "gcp-prod",
            "boot_disk_gb": 32,
            "boot_disk_type": "hyperdisk-balanced",
            "boot_disk_provisioned_iops": 3000,
            "boot_disk_provisioned_throughput": 140,
            "machine_type": "c4-standard-8-lssd",
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
            "scale_set_prefix": "also-ran-gcp-prod",
            "workflow_label": "outrunner-gcp-linux-x64",
            "include_shared_label": False,
            "regional_placement_enabled": True,
            "create_enabled": True,
            "automatic_delete": False,
            "labels": {
                "managed-by": "runner-autoscaler",
                "fleet": "also-ran-ci",
                "lifecycle": "burst",
                "provider": "gcp",
                "hold": "true",
            },
        },
        "general_production": {
            "name_prefix": "also-ran-runner-gcp-general",
            "zone_cycle": [
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
            ],
            "maximum_hosts": 12,
            "minimum_hosts": 0,
            "retention_mode": "demand_only",
            "max_jobs_per_host": 10,
            "vcpus_per_host": 4,
            "autoscale_slot_prefix": "gcp-general",
            "boot_disk_gb": 32,
            "boot_disk_type": "hyperdisk-balanced",
            "boot_disk_provisioned_iops": 3000,
            "boot_disk_provisioned_throughput": 140,
            "machine_type": "c4-standard-4-lssd",
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
            "scale_set_prefix": "also-ran-gcp-general",
            "workflow_label": "outrunner-gcp-general-linux-x64",
            "include_shared_label": False,
            "regional_placement_enabled": True,
            "create_enabled": True,
            "automatic_delete": True,
            "labels": {
                "managed-by": "runner-autoscaler",
                "fleet": "also-ran-ci",
                "lifecycle": "general",
                "provider": "gcp",
                "hold": "true",
            },
        },
        "regional_canaries": {
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
                "fleet": "also-ran-ci",
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
        },
        "market": {
            "regions": [
                {
                    "name": "us-central1",
                    "zones": [
                        "us-central1-a",
                        "us-central1-b",
                        "us-central1-c",
                        "us-central1-f",
                    ],
                    "canary_valid": True,
                    "foundation": {"subnet": "runner-hosts-us-central1"},
                },
                {
                    "name": "northamerica-northeast1",
                    "zones": ["northamerica-northeast1-b"],
                    "canary_valid": True,
                    "foundation": {
                        "subnet": "runner-hosts-northamerica-northeast1"
                    },
                },
                {
                    "name": "europe-west9",
                    "zones": ["europe-west9-a"],
                    "canary_valid": True,
                    "foundation": {"subnet": "runner-hosts-europe-west9"},
                },
                {
                    "name": "us-south1",
                    "zones": ["us-south1-a", "us-south1-b"],
                    "canary_valid": True,
                    "foundation": {"subnet": "runner-hosts-us-south1"},
                },
            ]
        },
        "target": {
            "lifecycle": "broker-created-disposable-spot",
            "provisioning_model": "SPOT",
            "desired_burst_hosts": 20,
            "vcpus_per_host": 8,
            "requested_regional_vcpus": 160,
            "automatic_delete_after_drain": True,
        },
        "spend": {
            "maximum_acceptance_runtime_minutes": 60,
            "maximum_campaign_runtime_minutes": 60,
            "maximum_aggregate_hourly_usd": 5,
        },
    }


def described(config, identity):
    return {
        "id": "123456789",
        "zone": "https://compute.googleapis.com/compute/v1/projects/p/zones/" + config["zone"],
        "labels": {**config["labels"], "source-commit": "a" * 12},
        "serviceAccounts": [{"email": identity}],
        "disks": [
            {
                "boot": True,
                "autoDelete": True,
                "source": "https://compute.googleapis.com/compute/v1/projects/p/zones/z/disks/" + config["name"],
            }
        ],
        "networkInterfaces": [
            {
                "subnetwork": "https://compute.googleapis.com/compute/v1/projects/p/regions/r/subnetworks/"
                + config.get("subnet", "runner-hosts-us-central1"),
                "accessConfigs": [
                    {
                        "type": "ONE_TO_ONE_NAT",
                        "networkTier": "PREMIUM",
                        "natIP": "198.51.100.7",
                    }
                ],
            }
        ],
    }


def firewall_rule(name, sources, **overrides):
    rule = {
        "name": name,
        "network": "https://compute.googleapis.com/compute/v1/projects/p/global/networks/also-ran-ci-runners",
        "direction": "INGRESS",
        "allowed": [{"IPProtocol": "tcp", "ports": ["22"]}],
        "sourceRanges": list(sources),
    }
    rule.update(overrides)
    return rule


class GcpLifecycleTests(unittest.TestCase):
    def test_jakarta_canary_is_isolated_local_ssd(self):
        candidate = json.loads((pathlib.Path(__file__).parents[1] / "gcp-acceptance-policy.json").read_text())
        MODULE.validate_policy(candidate)
        config = MODULE.regional_canary_config(candidate, "asia-southeast2", "asia-southeast2-a")
        self.assertEqual(config["machine_type"], "c4-standard-8-lssd")
        self.assertTrue(config["use_local_ssd"])
        self.assertEqual(config["labels"]["autoscale-slot"], "gcp-drain-09")
        self.assertFalse(config["include_shared_label"])
        with self.assertRaises(MODULE.LifecycleError):
            MODULE.regional_canary_config(candidate, "asia-southeast2", "asia-southeast2-b")


    def test_policy_accepts_exact_held_spot_slot(self):
        MODULE.validate_policy(policy())

    def test_policy_accepts_reviewed_production_auto_delete_switch(self):
        candidate = policy()
        candidate["production"]["automatic_delete"] = True
        MODULE.validate_policy(candidate)

    def test_policy_rejects_nonbaseline_production_disk_performance(self):
        for field, value in (
            ("boot_disk_provisioned_iops", 3600),
            ("boot_disk_provisioned_throughput", 290),
        ):
            with self.subTest(field=field):
                candidate = policy()
                candidate["production"][field] = value
                with self.assertRaisesRegex(
                    MODULE.LifecycleError, "unexpected GCP production boundary"
                ):
                    MODULE.validate_policy(candidate)

    def test_policy_rejects_highcpu_acceptance(self):
        candidate = policy()
        candidate["acceptance"]["machine_type"] = "n2-highcpu-8"
        with self.assertRaisesRegex(MODULE.LifecycleError, "held GCP Spot"):
            MODULE.validate_policy(candidate)

    def test_acceptance_requires_production_storage_contract(self):
        for field, value in [("use_local_ssd", False), ("boot_disk_type", "pd-balanced"),
                             ("boot_disk_gb", 100), ("boot_disk_provisioned_iops", 1000),
                             ("boot_disk_provisioned_throughput", 500)]:
            with self.subTest(field=field):
                candidate = policy()
                candidate["acceptance"][field] = value
                with self.assertRaisesRegex(MODULE.LifecycleError, "held GCP Spot"):
                    MODULE.validate_policy(candidate)

    def test_policy_rejects_shared_label_on_acceptance(self):
        candidate = policy()
        candidate["acceptance"]["include_shared_label"] = True
        with self.assertRaisesRegex(MODULE.LifecycleError, "held GCP Spot"):
            MODULE.validate_policy(candidate)

    def test_policy_rejects_public_ssh(self):
        candidate = policy()
        candidate["network"]["ssh_source_cidrs"] = ["0.0.0.0/0"]
        with self.assertRaisesRegex(MODULE.LifecycleError, "non-public"):
            MODULE.validate_policy(candidate)

    def test_policy_rejects_unknown_egress_mode(self):
        candidate = policy()
        candidate["network"]["egress"]["mode"] = "hybrid"
        with self.assertRaisesRegex(MODULE.LifecycleError, "egress mode"):
            MODULE.validate_policy(candidate)

    def test_policy_rejects_missing_egress_boundary(self):
        candidate = policy()
        del candidate["network"]["egress"]
        with self.assertRaisesRegex(MODULE.LifecycleError, "egress mode"):
            MODULE.validate_policy(candidate)

    def test_policy_rejects_ingress_allowlist_wider_than_ssh_sources(self):
        candidate = policy()
        candidate["network"]["egress"]["public_ingress_allowlist"].append("203.0.113.0/24")
        with self.assertRaisesRegex(MODULE.LifecycleError, "public ingress allowlist"):
            MODULE.validate_policy(candidate)

    def test_nat_mode_still_creates_without_an_address(self):
        candidate = policy()
        candidate["network"]["egress"]["mode"] = "nat"
        MODULE.validate_policy(candidate)
        arguments = MODULE.egress_create_arguments(candidate, spot=True)
        self.assertIn("--no-address", arguments)
        self.assertIn("--network-tier=PREMIUM", arguments)

    def test_builder_keeps_its_address_in_either_mode(self):
        candidate = policy()
        for mode in ("nat", "direct"):
            with self.subTest(mode=mode):
                candidate["network"]["egress"]["mode"] = mode
                arguments = MODULE.egress_create_arguments(candidate, spot=False)
                self.assertNotIn("--no-address", arguments)

    @mock.patch.object(MODULE, "gcloud")
    def test_ingress_gate_accepts_reviewed_ssh_and_iap_sources(self, gcloud):
        candidate = policy()
        gcloud.return_value = {
            "firewalls": [
                firewall_rule("ssh-admin", ["192.0.2.1/32"]),
                firewall_rule("ssh-iap", ["35.235.240.0/20"]),
                firewall_rule("internal", ["10.42.0.0/24"]),
            ]
        }

        result = MODULE.assert_closed_public_ingress(candidate)

        self.assertEqual(result["ingress_rules_checked"], 3)
        self.assertEqual(result["mode"], "direct")

    @mock.patch.object(MODULE, "gcloud")
    def test_ingress_gate_refuses_a_public_allow_rule(self, gcloud):
        candidate = policy()
        gcloud.return_value = {
            "firewalls": [
                firewall_rule("ssh-admin", ["192.0.2.1/32"]),
                firewall_rule("wide-open", ["0.0.0.0/0"]),
            ]
        }

        with self.assertRaisesRegex(MODULE.LifecycleError, "wide-open"):
            MODULE.assert_closed_public_ingress(candidate)

    @mock.patch.object(MODULE, "gcloud")
    def test_ingress_gate_treats_an_absent_source_as_the_whole_internet(self, gcloud):
        candidate = policy()
        rule = firewall_rule("implicit", [])
        del rule["sourceRanges"]
        gcloud.return_value = {"firewalls": [rule]}

        with self.assertRaisesRegex(MODULE.LifecycleError, "implicit"):
            MODULE.assert_closed_public_ingress(candidate)

    @mock.patch.object(MODULE, "gcloud")
    def test_ingress_gate_ignores_denies_disabled_and_egress_rules(self, gcloud):
        candidate = policy()
        denied = firewall_rule("deny-all", ["0.0.0.0/0"])
        del denied["allowed"]
        denied["denied"] = [{"IPProtocol": "all"}]
        gcloud.return_value = {
            "firewalls": [
                denied,
                firewall_rule("retired", ["0.0.0.0/0"], disabled=True),
                firewall_rule("egress-rule", ["0.0.0.0/0"], direction="EGRESS"),
            ]
        }

        result = MODULE.assert_closed_public_ingress(candidate)

        self.assertEqual(result["ingress_rules_checked"], 3)

    @mock.patch.object(MODULE, "gcloud")
    def test_ingress_gate_refuses_an_unreadable_attached_firewall_policy(self, gcloud):
        candidate = policy()
        gcloud.return_value = {
            "firewalls": [firewall_rule("ssh-admin", ["192.0.2.1/32"])],
            "firewallPolicys": [{"name": "org-policy", "rules": []}],
        }

        with self.assertRaisesRegex(MODULE.LifecycleError, "org-policy"):
            MODULE.assert_closed_public_ingress(candidate)

    @mock.patch.object(MODULE, "gcloud")
    def test_ingress_gate_refuses_an_open_ipv6_source(self, gcloud):
        candidate = policy()
        gcloud.return_value = {"firewalls": [firewall_rule("v6-open", ["::/0"])]}

        with self.assertRaisesRegex(MODULE.LifecycleError, "v6-open"):
            MODULE.assert_closed_public_ingress(candidate)

    @mock.patch.object(MODULE, "create_instance", return_value={"ok": True})
    @mock.patch.object(MODULE, "approved_image", return_value={"selfLink": "i"})
    @mock.patch.object(MODULE, "assert_closed_public_ingress")
    def test_every_create_command_passes_through_the_ingress_gate(
        self, gate, _image, _create
    ):
        with tempfile.NamedTemporaryFile("w", suffix=".json") as handle:
            json.dump(policy(), handle)
            handle.flush()
            with contextlib.redirect_stdout(io.StringIO()):
                MODULE.main(["--policy", handle.name, "create-acceptance", "--apply"])

        gate.assert_called_once()

    @mock.patch.object(MODULE, "delete_owned_instance", return_value={"ok": True})
    @mock.patch.object(MODULE, "assert_closed_public_ingress")
    def test_delete_does_not_need_the_ingress_gate(self, gate, _delete):
        with tempfile.NamedTemporaryFile("w", suffix=".json") as handle:
            json.dump(policy(), handle)
            handle.flush()

            with contextlib.redirect_stdout(io.StringIO()):
                MODULE.main(["--policy", handle.name, "delete-acceptance", "--apply"])

        gate.assert_not_called()

    @mock.patch.object(MODULE, "gcloud")
    def test_production_create_takes_a_direct_premium_address(self, gcloud):
        candidate = policy()
        config = MODULE.production_config(candidate, 1)
        gcloud.side_effect = [None, [described(config, candidate["identities"]["runtime"])]]

        created = MODULE.create_instance(
            candidate, config, "projects/p/global/images/i", None, spot=True
        )

        arguments = gcloud.call_args_list[1].args[0]
        self.assertNotIn("--no-address", arguments)
        self.assertIn("--network-tier=PREMIUM", arguments)
        self.assertTrue(any("startup-script=" in arg for arg in arguments))
        self.assertEqual(created["public_ip"], "198.51.100.7")

    @mock.patch.object(MODULE, "gcloud")
    def test_builder_create_does_not_install_runner_recovery(self, gcloud):
        candidate = policy()
        config = candidate["builder"]
        gcloud.side_effect = [None, [described(config, candidate["identities"]["builder"])]]
        MODULE.create_instance(candidate, config, "projects/p/global/images/i", None, spot=False)
        arguments = gcloud.call_args_list[1].args[0]
        self.assertFalse(any("startup-script=" in arg for arg in arguments))

    @mock.patch.object(MODULE, "gcloud")
    def test_create_refuses_an_instance_that_lost_its_direct_address(self, gcloud):
        candidate = policy()
        config = MODULE.production_config(candidate, 1)
        stripped = described(config, candidate["identities"]["runtime"])
        del stripped["networkInterfaces"][0]["accessConfigs"]
        gcloud.side_effect = [None, [stripped]]

        with self.assertRaisesRegex(MODULE.LifecycleError, "direct egress address"):
            MODULE.create_instance(
                candidate, config, "projects/p/global/images/i", None, spot=True
            )

    @mock.patch.object(MODULE, "gcloud")
    def test_nat_mode_refuses_an_instance_that_gained_a_public_address(self, gcloud):
        candidate = policy()
        candidate["network"]["egress"]["mode"] = "nat"
        config = MODULE.production_config(candidate, 1)
        gcloud.return_value = described(config, candidate["identities"]["runtime"])

        with self.assertRaisesRegex(MODULE.LifecycleError, "public address outside policy"):
            MODULE.owned_instance(candidate, config)

    def test_label_args_adds_approved_source_commit(self):
        result = MODULE.label_args({"fleet": "also-ran-ci"}, "a" * 40)
        self.assertEqual(len(result), 1)
        self.assertIn("source-commit=" + "a" * 12, result[0])

    @mock.patch.object(MODULE, "ssh_metadata_file")
    @mock.patch.object(MODULE, "gcloud")
    def test_acceptance_create_requests_spot_shielded_and_metadata(self, gcloud, ssh_file):
        candidate = policy()
        ssh_file.return_value.__enter__.return_value.name = "/tmp/key"
        gcloud.side_effect = [
            None,
            [
                {
                    "id": "1",
                    "networkInterfaces": [
                        {"accessConfigs": [{"type": "ONE_TO_ONE_NAT", "networkTier": "PREMIUM"}]}
                    ],
                }
            ],
        ]
        MODULE.create_instance(
            candidate,
            candidate["acceptance"],
            "projects/p/global/images/i",
            "/tmp/key.pub",
            spot=True,
        )
        arguments = gcloud.call_args_list[1].args[0]
        self.assertIn("--provisioning-model=SPOT", arguments)
        self.assertIn("--instance-termination-action=DELETE", arguments)
        self.assertIn("--maintenance-policy=TERMINATE", arguments)
        self.assertNotIn("--no-address", arguments)
        self.assertIn("--network-tier=PREMIUM", arguments)
        self.assertIn("--boot-disk-type=hyperdisk-balanced", arguments)
        self.assertIn("--machine-type=c4-standard-8-lssd", arguments)
        self.assertIn("--boot-disk-provisioned-iops=3000", arguments)
        self.assertIn("--boot-disk-provisioned-throughput=140", arguments)
        self.assertIn("--shielded-secure-boot", arguments)
        self.assertTrue(any("runner-scale-set=also-ran-gcp-drain-01" in item for item in arguments))
        self.assertTrue(any("runner-slot=gcp-drain-01" in item for item in arguments))
        self.assertTrue(any("runner-image-source-commit=" + "a" * 12 in item for item in arguments))
        self.assertTrue(any("runner-include-shared-label=disabled" in item for item in arguments))
        self.assertTrue(any("runner-use-local-ssd=enabled" in item for item in arguments))
        self.assertTrue(any("runner-max-jobs=1" in item for item in arguments))
        self.assertTrue(
            any("enable-guest-attributes=TRUE" in item for item in arguments),
            "a self-powered-off host is only observable through guest attributes",
        )

    @mock.patch.object(MODULE, "gcloud")
    def test_production_create_uses_c4_compatible_hyperdisk(self, gcloud):
        candidate = policy()
        config = MODULE.production_config(candidate, 1)
        gcloud.side_effect = [None, [described(config, candidate["identities"]["runtime"])]]

        MODULE.create_instance(
            candidate,
            config,
            "projects/p/global/images/i",
            None,
            spot=True,
        )

        arguments = gcloud.call_args_list[1].args[0]
        self.assertIn("--machine-type=c4-standard-8-lssd", arguments)
        self.assertIn("--boot-disk-type=hyperdisk-balanced", arguments)
        self.assertIn("--boot-disk-provisioned-iops=3000", arguments)
        self.assertIn("--boot-disk-provisioned-throughput=140", arguments)
        self.assertTrue(any("runner-use-local-ssd=enabled" in item for item in arguments))
        self.assertTrue(any("runner-max-jobs=5" in item for item in arguments))

    @mock.patch.object(MODULE, "gcloud")
    def test_general_production_uses_four_vcpu_local_ssd_shape(self, gcloud):
        candidate = policy()
        config = MODULE.production_config(candidate, 1, pool="general")
        gcloud.side_effect = [
            None,
            [described(config, candidate["identities"]["runtime"])],
        ]

        MODULE.create_instance(
            candidate,
            config,
            "projects/p/global/images/i",
            None,
            spot=True,
        )

        arguments = gcloud.call_args_list[1].args[0]
        self.assertEqual(config["name"], "also-ran-runner-gcp-general-01")
        self.assertEqual(config["scale_set"], "also-ran-gcp-general-01")
        self.assertEqual(config["labels"]["autoscale-slot"], "gcp-general-01")
        self.assertIn("--machine-type=c4-standard-4-lssd", arguments)
        self.assertTrue(
            any(
                "runner-label=outrunner-gcp-general-linux-x64" in item
                for item in arguments
            )
        )
        self.assertTrue(any("runner-max-jobs=10" in item for item in arguments))

    def test_campaign_slots_are_unique_and_cycle_zones(self):
        candidate = policy()
        first = MODULE.campaign_config(candidate, 1)
        third = MODULE.campaign_config(candidate, 3)
        self.assertEqual(first["zone"], "us-central1-a")
        self.assertEqual(third["zone"], "us-central1-a")
        self.assertNotEqual(first["scale_set"], third["scale_set"])
        self.assertFalse(first["include_shared_label"])

    def test_production_slots_have_stable_identity_and_shared_gcp_label(self):
        candidate = policy()
        first = MODULE.production_config(candidate, 1)
        twentieth = MODULE.production_config(candidate, 20)
        twenty_eighth = MODULE.production_config(candidate, 28)
        self.assertEqual(first["name"], "also-ran-runner-gcp-prod-01")
        self.assertEqual(first["scale_set"], "also-ran-gcp-prod-01")
        self.assertEqual(first["machine_type"], "c4-standard-8-lssd")
        self.assertTrue(first["use_local_ssd"])
        self.assertEqual(first["boot_disk_type"], "hyperdisk-balanced")
        self.assertEqual(first["boot_disk_provisioned_iops"], 3000)
        self.assertEqual(first["boot_disk_provisioned_throughput"], 140)
        self.assertEqual(first["diagnostic_label"], "outrunner-gcp-linux-x64")
        self.assertEqual(first["max_jobs_per_host"], 5)
        self.assertEqual(first["zone"], "northamerica-northeast1-b")
        self.assertEqual(first["subnet"], "runner-hosts-northamerica-northeast1")
        self.assertEqual(twentieth["zone"], "europe-west9-a")
        self.assertEqual(twentieth["labels"]["autoscale-slot"], "gcp-prod-20")
        self.assertEqual(twenty_eighth["zone"], "us-south1-b")
        self.assertEqual(
            twenty_eighth["labels"]["autoscale-slot"], "gcp-prod-28"
        )

    @mock.patch.object(MODULE, "gcloud")
    def test_regional_canary_uses_exact_local_ssd_shape_and_subnet(self, gcloud):
        candidate = policy()
        config = MODULE.regional_canary_config(
            candidate, "europe-west9", "europe-west9-a"
        )
        gcloud.side_effect = [
            None,
            [
                {
                    "id": "1",
                    "networkInterfaces": [
                        {"accessConfigs": [{"type": "ONE_TO_ONE_NAT", "networkTier": "PREMIUM"}]}
                    ],
                }
            ],
        ]

        MODULE.create_instance(
            candidate,
            config,
            "projects/p/global/images/i",
            None,
            spot=True,
        )

        arguments = gcloud.call_args_list[1].args[0]
        self.assertEqual(config["name"], "also-ran-runner-gcp-canary-paris-a")
        self.assertEqual(config["diagnostic_label"], "also-ran-gcp-canary-paris-a")
        self.assertEqual(config["labels"]["autoscale-slot"], "gcp-drain-02")
        self.assertIn("--subnet=runner-hosts-europe-west9", arguments)
        self.assertIn("--machine-type=c4-standard-8-lssd", arguments)
        self.assertIn("--boot-disk-size=32GB", arguments)
        self.assertIn("--boot-disk-type=hyperdisk-balanced", arguments)
        self.assertIn("--boot-disk-provisioned-iops=3000", arguments)
        self.assertIn("--boot-disk-provisioned-throughput=140", arguments)
        self.assertNotIn("--no-address", arguments)
        self.assertTrue(any("runner-use-local-ssd=enabled" in item for item in arguments))

    def test_regional_canary_rejects_unreviewed_zone(self):
        with self.assertRaisesRegex(MODULE.LifecycleError, "outside policy"):
            MODULE.regional_canary_config(
                policy(), "europe-west9", "europe-west9-b"
            )

    def test_regional_canary_slots_are_unique_and_guest_compatible(self):
        candidate = policy()
        configs = [
            MODULE.regional_canary_config(candidate, region, zone)
            for region, item in candidate["regional_canaries"]["regions"].items()
            for zone in item["zones"]
        ]
        slots = [item["labels"]["autoscale-slot"] for item in configs]

        self.assertEqual(len(slots), len(set(slots)))
        self.assertTrue(
            all(re.fullmatch(r"gcp-(drain|prod)-[0-9]{2}", slot) for slot in slots)
        )

    @mock.patch.object(MODULE, "create_instance")
    @mock.patch.object(MODULE, "gcloud")
    def test_regional_canary_concurrency_is_globally_bounded(
        self, gcloud, create_instance
    ):
        candidate = policy()
        configs = [
            MODULE.regional_canary_config(candidate, "europe-west9", "europe-west9-a"),
            MODULE.regional_canary_config(candidate, "us-south1", "us-south1-a"),
            MODULE.regional_canary_config(
                candidate, "northamerica-northeast1", "northamerica-northeast1-b"
            ),
        ]
        gcloud.return_value = [
            {
                "name": config["name"],
                "labels": {
                    **config["labels"],
                    "source-commit": "a" * 12,
                },
            }
            for config in configs
        ]

        with self.assertRaisesRegex(MODULE.LifecycleError, "concurrency limit"):
            MODULE.create_regional_canary(
                candidate, configs[0], "projects/p/global/images/i"
            )
        create_instance.assert_not_called()

    def test_parse_slots_rejects_duplicates_and_out_of_range(self):
        self.assertEqual(MODULE.parse_slots("3,1,2", 20), [1, 2, 3])
        for value in ("", "1,1", "0", "21", "x"):
            with self.subTest(value=value):
                with self.assertRaises(MODULE.LifecycleError):
                    MODULE.parse_slots(value, 20)

    def test_expected_instance_ids_exactly_cover_delete_slots(self):
        self.assertEqual(
            MODULE.parse_expected_instance_ids("1=123,3=789", [1, 3]),
            {1: "123", 3: "789"},
        )
        for value in ("", "1:123", "1=x", "1=123,1=456", "1=123"):
            with self.subTest(value=value):
                with self.assertRaises(MODULE.LifecycleError):
                    MODULE.parse_expected_instance_ids(value, [1, 2])

    def test_slot_zones_exactly_cover_slots(self):
        self.assertEqual(
            MODULE.parse_slot_zones("1=europe-west9-a,3=us-south1-b", [1, 3]),
            {1: "europe-west9-a", 3: "us-south1-b"},
        )
        for value in ("", "1:europe-west9-a", "1=x", "1=europe-west9-a"):
            with self.subTest(value=value):
                with self.assertRaises(MODULE.LifecycleError):
                    MODULE.parse_slot_zones(value, [1, 2])

    @mock.patch.object(MODULE, "gcloud")
    def test_delete_verifies_numeric_identity_twice(self, gcloud):
        candidate = policy()
        vm = described(candidate["acceptance"], candidate["identities"]["runtime"])
        gcloud.side_effect = [vm, vm, {}]
        result = MODULE.delete_owned_instance(candidate, candidate["acceptance"])
        self.assertEqual(result["instance_id"], "123456789")
        self.assertIn("delete_requested_at", result)
        self.assertIn("delete_completed_at", result)
        self.assertEqual(gcloud.call_args_list[-1].args[0][2], "delete")

    @mock.patch.object(MODULE, "gcloud")
    def test_delete_accepts_production_local_scratch_disk(self, gcloud):
        candidate = policy()
        config = MODULE.production_config(candidate, 1)
        vm = described(config, candidate["identities"]["runtime"])
        vm["disks"].append(
            {
                "boot": False,
                "autoDelete": True,
                "deviceName": "local-ssd-0",
                "interface": "NVME",
                "type": "SCRATCH",
            }
        )
        gcloud.side_effect = [vm, vm, {}]

        result = MODULE.delete_owned_instance(candidate, config)

        self.assertEqual(result["instance_id"], "123456789")
        self.assertEqual(gcloud.call_args_list[-1].args[0][2], "delete")

    @mock.patch.object(MODULE, "gcloud")
    def test_delete_refuses_unexpected_non_boot_disk(self, gcloud):
        candidate = policy()
        config = MODULE.production_config(candidate, 1)
        vm = described(config, candidate["identities"]["runtime"])
        vm["disks"].append(
            {
                "boot": False,
                "autoDelete": True,
                "deviceName": "persistent-disk-1",
                "interface": "NVME",
                "type": "PERSISTENT",
            }
        )
        gcloud.return_value = vm

        with self.assertRaisesRegex(MODULE.LifecycleError, "unexpected non-boot disk"):
            MODULE.delete_owned_instance(candidate, config)
        self.assertEqual(gcloud.call_count, 1)

    @mock.patch.object(MODULE, "gcloud")
    def test_delete_reaps_a_host_left_behind_by_the_previous_egress_mode(self, gcloud):
        # The direct-egress switch finds a running fleet that was created with
        # --no-address. Asserting addressing on the delete path would make every
        # one of those hosts unreapable, wedging the broker on the first tick
        # after the cutover and pinning the fleet on the NAT meter it is leaving.
        candidate = policy()
        config = MODULE.production_config(candidate, 1)
        vm = described(config, candidate["identities"]["runtime"])
        del vm["networkInterfaces"][0]["accessConfigs"]
        gcloud.side_effect = [vm, vm, {}]

        result = MODULE.delete_owned_instance(candidate, config)

        self.assertEqual(result["instance_id"], "123456789")
        self.assertEqual(gcloud.call_args_list[-1].args[0][2], "delete")

    @mock.patch.object(MODULE, "gcloud")
    def test_delete_reaps_a_direct_host_after_a_rollback_to_nat(self, gcloud):
        # The same trap in the other direction: rolling back to nat must be able
        # to reap the hosts that still carry a direct-mode address.
        candidate = policy()
        candidate["network"]["egress"]["mode"] = "nat"
        config = MODULE.production_config(candidate, 1)
        vm = described(config, candidate["identities"]["runtime"])
        gcloud.side_effect = [vm, vm, {}]

        result = MODULE.delete_owned_instance(candidate, config)

        self.assertEqual(result["instance_id"], "123456789")
        self.assertEqual(gcloud.call_args_list[-1].args[0][2], "delete")

    @mock.patch.object(MODULE, "gcloud")
    def test_delete_still_refuses_a_foreign_subnet_without_an_address(self, gcloud):
        # Relaxing addressing on delete must relax nothing else.
        candidate = policy()
        config = MODULE.production_config(candidate, 1)
        vm = described(config, candidate["identities"]["runtime"])
        del vm["networkInterfaces"][0]["accessConfigs"]
        vm["networkInterfaces"][0]["subnetwork"] = "projects/p/regions/r/subnetworks/other"
        gcloud.return_value = vm

        with self.assertRaisesRegex(MODULE.LifecycleError, "network does not match"):
            MODULE.delete_owned_instance(candidate, config)

    @mock.patch.object(MODULE, "gcloud")
    def test_steady_state_ownership_still_asserts_addressing(self, gcloud):
        # owned_instance outside the delete path keeps the check that catches a
        # host which silently fell back onto the NAT meter.
        candidate = policy()
        config = MODULE.production_config(candidate, 1)
        vm = described(config, candidate["identities"]["runtime"])
        del vm["networkInterfaces"][0]["accessConfigs"]
        gcloud.return_value = vm

        with self.assertRaisesRegex(MODULE.LifecycleError, "direct egress address"):
            MODULE.owned_instance(candidate, config)

    @mock.patch.object(MODULE, "gcloud")
    def test_delete_refuses_unowned_instance(self, gcloud):
        candidate = policy()
        vm = described(candidate["acceptance"], candidate["identities"]["runtime"])
        vm["labels"]["hold"] = "false"
        gcloud.return_value = vm
        with self.assertRaisesRegex(MODULE.LifecycleError, "ownership"):
            MODULE.delete_owned_instance(candidate, candidate["acceptance"])

    @mock.patch.object(MODULE, "gcloud")
    def test_production_legacy_image_remains_owned_during_rollout(self, gcloud):
        candidate = policy()
        config = MODULE.production_config(candidate, 3)
        vm = described(config, candidate["identities"]["runtime"])
        vm["labels"]["source-commit"] = "c37c1ddeebbb"
        gcloud.return_value = vm

        self.assertEqual(MODULE.owned_instance(candidate, config)["id"], "123456789")

    @mock.patch.object(MODULE, "gcloud")
    def test_delete_refuses_instance_that_does_not_match_receipt(self, gcloud):
        candidate = policy()
        vm = described(candidate["acceptance"], candidate["identities"]["runtime"])
        gcloud.return_value = vm
        with self.assertRaisesRegex(MODULE.LifecycleError, "drain receipt"):
            MODULE.delete_owned_instance(
                candidate,
                candidate["acceptance"],
                expected_instance_id="987654321",
            )
        self.assertEqual(gcloud.call_count, 1)


if __name__ == "__main__":
    unittest.main()
