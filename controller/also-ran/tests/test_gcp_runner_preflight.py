import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).parents[3] / "scripts" / "gcp-runner-preflight.py"
SPEC = importlib.util.spec_from_file_location("gcp_runner_preflight", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class GcpPreflightTests(unittest.TestCase):
    def test_repository_policy_preserves_acceptance_and_target_boundaries(self):
        policy_path = SCRIPT.parents[1] / "controller" / "also-ran" / "gcp-acceptance-policy.json"
        loaded = MODULE.load_policy(str(policy_path))
        self.assertEqual(loaded["target"]["desired_burst_hosts"], 20)
        self.assertEqual(loaded["target"]["vcpus_per_host"], 8)
        self.assertEqual(loaded["production"]["machine_type"], "c4-standard-8-lssd")
        self.assertEqual(loaded["production"]["boot_disk_gb"], 32)
        self.assertTrue(loaded["production"]["use_local_ssd"])

    def test_policy_rejects_automatic_acceptance_deletion(self):
        policy_path = SCRIPT.parents[1] / "controller" / "also-ran" / "gcp-acceptance-policy.json"
        policy = MODULE.load_policy(str(policy_path))
        policy["acceptance"]["automatic_delete"] = True
        with self.assertRaisesRegex(MODULE.PreflightError, "safety rails"):
            MODULE.validate_policy(policy)

    def test_region_from_zone(self):
        self.assertEqual(MODULE.region_from_zone("us-central1-a"), "us-central1")

    def test_region_from_zone_rejects_region(self):
        with self.assertRaisesRegex(MODULE.PreflightError, "invalid GCP zone"):
            MODULE.region_from_zone("us-central1")

    def test_relevant_quotas_are_reduced_without_losing_capacity_inputs(self):
        result = MODULE.relevant_quotas(
            [
                {"metric": "PREEMPTIBLE_CPUS", "usage": 8, "limit": 64},
                {"metric": "CPUS", "usage": 0, "limit": 24},
                {"metric": "DISKS_TOTAL_GB", "usage": 1, "limit": 1000},
            ]
        )
        self.assertEqual(
            [quota["metric"] for quota in result],
            ["PREEMPTIBLE_CPUS", "CPUS", "DISKS_TOTAL_GB"],
        )

    def test_quota_capacity_rounds_down_to_whole_hosts(self):
        quotas = [{"metric": "PREEMPTIBLE_CPUS", "usage": 8, "limit": 55}]
        self.assertEqual(
            MODULE.quota_capacity(
                quotas, metric="PREEMPTIBLE_CPUS", units_per_host=8
            ),
            5,
        )

    def test_missing_quota_is_unknown_not_zero(self):
        self.assertIsNone(
            MODULE.quota_capacity([], metric="PREEMPTIBLE_CPUS", units_per_host=8)
        )

    def test_spot_uses_standard_n2_quota_without_dedicated_pool(self):
        quotas = [
            {"metric": "PREEMPTIBLE_CPUS", "usage": 0, "limit": 0},
            {"metric": "N2_CPUS", "usage": 0, "limit": 200},
        ]
        self.assertEqual(
            MODULE.spot_quota_capacity(quotas, vcpus_per_host=8),
            {
                "hosts": 25,
                "quota_pool": "N2_CPUS",
                "note": "Dedicated preemptible quota is not granted; Spot uses standard quota.",
            },
        )


if __name__ == "__main__":
    unittest.main()
