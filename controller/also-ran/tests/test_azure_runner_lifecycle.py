import importlib.util
import pathlib
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[3] / "legacy" / "azure-runner-lifecycle.py"
SPEC = importlib.util.spec_from_file_location("azure_runner_lifecycle", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def policy():
    return {
        "policy_version": 1,
        "resource_group": "also-ran-ci-runners",
        "location": "eastus",
        "image_source_commit": "a" * 40,
        "rollout": {
            "phase": 1,
            "acceptance_region": "eastus",
            "acceptance_size": "Standard_E2as_v5",
            "maximum_hosts": 1,
            "outrunner_max_runners": 1,
            "automatic_delete": False,
        },
        "target": {
            "lifecycle": "broker-created-disposable-spot",
            "size": "Standard_F8as_v6",
            "candidate_regions": ["eastus", "westus2"],
            "automatic_delete_after_drain": True,
        },
        "network": {
            "vnet": "test-vnet",
            "subnet": "test-subnet",
            "ssh_source_cidrs": ["192.0.2.1/32"],
        },
        "builder": {
            "name": "also-ran-runner-image-builder-azure",
            "nic_name": "also-ran-runner-image-builder-azureVMNic",
            "public_ip_name": "also-ran-runner-image-builder-azurePublicIP",
            "tags": {
                "managed-by": "also-ran-image-builder",
                "fleet": "also-ran-ci",
                "lifecycle": "image-build",
                "provider": "azure",
            },
        },
        "burst": {
            "name": "also-ran-runner-burst-azure-01",
            "nic_name": "also-ran-runner-burst-azure-01VMNic",
            "public_ip_name": "also-ran-runner-burst-azure-01PublicIP",
            "size": "Standard_E2as_v5",
            "os_disk_size_gb": 256,
            "tags": {
                "managed-by": "runner-autoscaler",
                "fleet": "also-ran-ci",
                "lifecycle": "burst",
                "autoscale-slot": "burst-01",
                "provider": "azure",
                "hold": "true",
            },
        },
        "paid_acceptance": {
            "name": "also-ran-runner-paid-azure-01",
            "nic_name": "also-ran-runner-paid-azure-01VMNic",
            "public_ip_name": "also-ran-runner-paid-azure-01PublicIP",
            "location": "westus2",
            "size": "Standard_F8s_v2",
            "os_disk_size_gb": 256,
            "network": {
                "vnet": "test-paid-vnet",
                "subnet": "test-subnet",
                "nsg": "test-paid-nsg",
                "address_prefix": "10.44.0.0/24",
                "subnet_prefix": "10.44.0.0/26",
                "ssh_source_cidrs": ["192.0.2.1/32"],
            },
            "tags": {
                "managed-by": "runner-autoscaler",
                "fleet": "also-ran-ci",
                "lifecycle": "acceptance",
                "autoscale-slot": "paid-acceptance-01",
                "provider": "azure",
                "billing": "standard",
                "hold": "true",
            },
        },
    }


class AzureLifecycleTests(unittest.TestCase):
    def test_policy_accepts_exact_held_slot(self):
        MODULE.validate_policy(policy())

    def test_policy_rejects_public_ssh(self):
        candidate = policy()
        candidate["network"]["ssh_source_cidrs"] = ["0.0.0.0/0"]
        with self.assertRaisesRegex(MODULE.LifecycleError, "non-public"):
            MODULE.validate_policy(candidate)

    def test_policy_rejects_unheld_slot(self):
        candidate = policy()
        candidate["burst"]["tags"]["hold"] = "false"
        with self.assertRaisesRegex(MODULE.LifecycleError, "held Azure slot"):
            MODULE.validate_policy(candidate)

    def test_tag_args_adds_short_source_commit(self):
        arguments = MODULE.tag_args({"fleet": "also-ran-ci"}, "a" * 40)
        self.assertEqual(arguments[0], "--tags")
        self.assertIn("fleet=also-ran-ci", arguments)
        self.assertIn("source-commit=" + "a" * 12, arguments)

    def test_commit_maps_to_stable_numeric_gallery_version(self):
        self.assertEqual(MODULE.image_version("1b12e6ad9d3bb1f3"), "1.1774310.11377979")

    def test_policy_keeps_acceptance_shape_separate_from_target(self):
        candidate = policy()
        candidate["target"]["size"] = candidate["burst"]["size"]
        with self.assertRaisesRegex(MODULE.LifecycleError, "target"):
            MODULE.validate_policy(candidate)

    def test_policy_rejects_paid_acceptance_outside_candidate_regions(self):
        candidate = policy()
        candidate["paid_acceptance"]["location"] = "southcentralus"
        with self.assertRaisesRegex(MODULE.LifecycleError, "paid acceptance"):
            MODULE.validate_policy(candidate)

    @mock.patch.object(MODULE, "az")
    def test_orphan_cleanup_requires_unattached_owned_public_ip(self, az):
        az.return_value = {
            "ipConfiguration": None,
            "tags": policy()["builder"]["tags"],
        }
        self.assertTrue(MODULE.delete_owned_public_ip(policy(), policy()["builder"]))
        self.assertEqual(az.call_count, 2)
        self.assertEqual(az.call_args.args[0][:3], ["network", "public-ip", "delete"])

    @mock.patch.object(MODULE, "az")
    def test_orphan_cleanup_refuses_attached_public_ip(self, az):
        az.return_value = {
            "ipConfiguration": {"id": "/some/nic/ipConfigurations/default"},
            "tags": policy()["builder"]["tags"],
        }
        with self.assertRaisesRegex(MODULE.LifecycleError, "attached"):
            MODULE.delete_owned_public_ip(policy(), policy()["builder"])
        self.assertEqual(az.call_count, 1)

    @mock.patch.object(MODULE, "az")
    def test_failed_create_cleanup_deletes_owned_nic_then_public_ip(self, az):
        candidate = policy()
        public_ip_id = "/subscriptions/example/resourceGroups/also-ran-ci-runners/providers/Microsoft.Network/publicIPAddresses/also-ran-runner-burst-azure-01PublicIP"
        az.side_effect = [
            None,
            {
                "tags": candidate["burst"]["tags"],
                "ipConfigurations": [{"publicIPAddress": {"id": public_ip_id}}],
            },
            {
                "id": public_ip_id,
                "ipConfiguration": {"id": "/expected/nic/ipConfigurations/default"},
                "tags": candidate["burst"]["tags"],
            },
            {},
            {
                "id": public_ip_id,
                "ipConfiguration": None,
                "tags": candidate["burst"]["tags"],
            },
            {},
        ]
        result = MODULE.cleanup_failed_create(candidate, candidate["burst"])
        self.assertEqual(result, {"nic_deleted": True, "public_ip_deleted": True})
        self.assertEqual(az.call_args_list[3].args[0][:3], ["network", "nic", "delete"])
        self.assertEqual(
            az.call_args_list[5].args[0][:3], ["network", "public-ip", "delete"]
        )

    @mock.patch.object(MODULE, "az")
    def test_create_requests_trusted_launch_and_disposable_resources(self, az):
        candidate = policy()
        az.side_effect = [
            None,
            {"id": "/vm/id", "publicIpAddress": "192.0.2.10"},
        ]
        MODULE.create_vm(
            candidate,
            candidate["burst"],
            "/gallery/image/id",
            "/tmp/test.pub",
            "a" * 40,
            spot=True,
        )
        arguments = az.call_args_list[1].args[0]
        self.assertIn("TrustedLaunch", arguments)
        self.assertIn("--enable-secure-boot", arguments)
        self.assertIn("--enable-vtpm", arguments)
        self.assertIn("--nic-delete-option", arguments)
        self.assertIn("--os-disk-delete-option", arguments)
        self.assertIn("--eviction-policy", arguments)
        self.assertIn("--max-price", arguments)

    @mock.patch.object(MODULE, "az")
    def test_paid_acceptance_uses_its_region_network_and_standard_priority(self, az):
        candidate = policy()
        az.side_effect = [None, {"id": "/vm/id", "publicIpAddress": "192.0.2.11"}]
        MODULE.create_vm(
            candidate,
            candidate["paid_acceptance"],
            "/gallery/image/id",
            "/tmp/test.pub",
            "a" * 40,
            spot=False,
        )
        arguments = az.call_args_list[1].args[0]
        self.assertEqual(arguments[arguments.index("--location") + 1], "westus2")
        self.assertEqual(
            arguments[arguments.index("--vnet-name") + 1], "test-paid-vnet"
        )
        self.assertNotIn("--priority", arguments)

    @mock.patch.object(MODULE, "az")
    def test_regional_foundation_keeps_resource_group_home_location(self, az):
        candidate = policy()
        az.side_effect = [{}, {}, {}, {}, {"id": "/subnet/id"}]
        MODULE.ensure_foundation(
            candidate,
            "a" * 40,
            location="westus2",
            network=candidate["paid_acceptance"]["network"],
        )
        group_arguments = az.call_args_list[0].args[0]
        nsg_arguments = az.call_args_list[1].args[0]
        self.assertEqual(
            group_arguments[group_arguments.index("--location") + 1], "eastus"
        )
        self.assertEqual(
            nsg_arguments[nsg_arguments.index("--location") + 1], "westus2"
        )


if __name__ == "__main__":
    unittest.main()
