import importlib.util
import pathlib
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[3] / "scripts" / "gcp-region-foundation.py"
SPEC = importlib.util.spec_from_file_location("gcp_region_foundation", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def foundation(region="europe-west9", cidr="10.42.2.0/24"):
    return {
        "network": "also-ran-ci-runners",
        "subnet": f"runner-hosts-{region}",
        "subnet_cidr": cidr,
        "router": f"also-ran-ci-runners-{region}",
        "nat": f"also-ran-ci-runners-{region}",
    }


class FoundationTests(unittest.TestCase):
    def test_overlap_with_another_subnet_fails_closed(self):
        with self.assertRaisesRegex(Exception, "overlaps existing"):
            MODULE.assert_no_cidr_conflict(
                foundation(),
                [{"name": "other", "ipCidrRange": "10.42.2.128/25"}],
            )

    def test_existing_expected_subnet_must_have_exact_cidr(self):
        with self.assertRaisesRegex(Exception, "CIDR mismatch"):
            MODULE.assert_no_cidr_conflict(
                foundation(),
                [
                    {
                        "name": "runner-hosts-europe-west9",
                        "ipCidrRange": "10.42.20.0/24",
                    }
                ],
            )

    @mock.patch.object(MODULE, "observe_foundation")
    def test_plan_is_read_only_and_lists_only_missing_resources(self, observe):
        observe.return_value = {"subnet": {}, "router": None, "nat": None}

        plan = MODULE.plan_region("europe-west9", foundation(), [])

        self.assertEqual(plan["actions"], ["create_router", "create_nat"])
        self.assertFalse(plan["ready"])

    @mock.patch.object(MODULE, "gcloud")
    def test_apply_uses_private_subnet_and_logged_auto_nat(self, gcloud):
        MODULE.apply_region(
            "europe-west9",
            foundation(),
            ["create_subnet", "create_router", "create_nat"],
        )

        subnet_args = gcloud.call_args_list[0].args[0]
        nat_args = gcloud.call_args_list[2].args[0]
        self.assertIn("--enable-private-ip-google-access", subnet_args)
        self.assertIn("--stack-type=IPV4_ONLY", subnet_args)
        self.assertIn("--auto-allocate-nat-external-ips", nat_args)
        self.assertIn("--nat-all-subnet-ip-ranges", nat_args)
        self.assertIn("--enable-logging", nat_args)
        self.assertIn("--log-filter=ERRORS_ONLY", nat_args)

    def test_non_wave_region_is_rejected(self):
        with self.assertRaisesRegex(Exception, "not approved"):
            MODULE.region_policy({"market": {"regions": []}}, "europe-west8")

    def test_us_west2_uses_its_reviewed_foundation(self):
        policy = {"market": {"regions": [{"name": "us-west2", "foundation": foundation("us-west2", "10.42.4.0/24")} ]}}

        self.assertEqual(
            MODULE.region_policy(policy, "us-west2")["subnet"],
            "runner-hosts-us-west2",
        )


if __name__ == "__main__":
    unittest.main()
