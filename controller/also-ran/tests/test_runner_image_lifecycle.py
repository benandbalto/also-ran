import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).parents[3] / "legacy" / "runner-image-lifecycle.py"
SPEC = importlib.util.spec_from_file_location("runner_image_lifecycle", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def policy():
    return {
        "policy_version": 1,
        "builder": {
            "name": "also-ran-runner-image-builder",
            "labels": {
                "managed-by": "also-ran-image-builder",
                "fleet": "also-ran-ci",
                "lifecycle": "image-build",
            },
        },
        "burst": {
            "name": "also-ran-runner-burst-01",
            "labels": {
                "managed-by": "runner-autoscaler",
                "fleet": "also-ran-ci",
                "lifecycle": "burst",
                "autoscale-slot": "burst-01",
                "hold": "true",
            },
        },
        "protected_server_ids": [10],
        "ssh_key_ids": [20],
        "firewall_id": 30,
    }


class FakeClient:
    def __init__(self, servers=None):
        self.servers = servers or []
        self.posts = []

    def get(self, path):
        if path.startswith("/servers?"):
            return {"servers": self.servers}
        server_id = int(path.rsplit("/", 1)[1])
        return {
            "server": next(
                server for server in self.servers if server["id"] == server_id
            )
        }

    def post(self, path, payload):
        self.posts.append((path, payload))
        return {
            "server": {
                "id": 40,
                "name": payload["name"],
                "status": "initializing",
                "public_net": {"ipv4": {"ip": "192.0.2.40"}},
                "labels": payload["labels"],
            }
        }


class LifecycleTests(unittest.TestCase):
    def test_policy_requires_exact_held_burst_labels(self):
        candidate = policy()
        candidate["burst"]["labels"]["hold"] = "false"
        with self.assertRaisesRegex(MODULE.LifecycleError, "held RFC slot"):
            MODULE.validate_policy(candidate)

    def test_protected_server_cannot_be_owned(self):
        client = FakeClient(
            [
                {
                    "id": 10,
                    "name": "also-ran-runner-image-builder",
                    "labels": policy()["builder"]["labels"],
                }
            ]
        )
        with self.assertRaisesRegex(MODULE.LifecycleError, "protected server"):
            MODULE.get_owned_server(client, 10, policy()["builder"], {10})

    def test_duplicate_stable_name_blocks_create(self):
        duplicate = {
            "id": 41,
            "name": "also-ran-runner-image-builder",
            "status": "running",
            "public_net": {"ipv4": {"ip": "192.0.2.41"}},
            "labels": {},
        }
        with self.assertRaisesRegex(MODULE.LifecycleError, "already exists"):
            MODULE.create_server(
                FakeClient([duplicate]),
                policy()["builder"],
                policy(),
                "ubuntu-26.04",
                "a" * 40,
            )

    def test_create_payload_is_bounded_by_policy(self):
        client = FakeClient()
        config = {
            **policy()["builder"],
            "server_type": "cpx32",
            "location": "nbg1",
        }
        result = MODULE.create_server(
            client, config, policy(), "ubuntu-26.04", "a" * 40
        )
        path, payload = client.posts[0]
        self.assertEqual(path, "/servers")
        self.assertEqual(payload["firewalls"], [{"firewall": 30}])
        self.assertEqual(payload["ssh_keys"], [20])
        self.assertFalse(payload["public_net"]["enable_ipv6"])
        self.assertEqual(payload["labels"]["source-commit"], "a" * 12)
        self.assertEqual(result["name"], "also-ran-runner-image-builder")


if __name__ == "__main__":
    unittest.main()
