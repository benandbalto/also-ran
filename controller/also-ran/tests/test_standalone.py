"""Exercise the public bootstrap path against independent deployment identities."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from deployment_identity import valid_legacy_images

ROOT = Path(__file__).resolve().parents[3]


class StandaloneTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.output = Path(temporary.name) / "deployment"

    def configure(self, *extra):
        return subprocess.run([
            sys.executable, str(ROOT / "scripts/configure.py"),
            "--project-id", "customer-ci-project", "--project-number", "987654321098",
            "--resource-prefix", "acme", "--fleet", "acme-ci",
            "--github-repository", "example-org/app",
            "--image-source-commit", "a" * 40,
            "--ssh-source-cidr", "192.0.2.1/32",
            "--output-dir", str(self.output), *extra,
        ], capture_output=True, text=True)

    def validate(self, *extra):
        return subprocess.run([
            sys.executable, str(ROOT / "scripts/validate-config.py"),
            "--deployment", str(self.output / "deployment.json"),
            "--policy", str(self.output / "gcp-policy.json"), *extra,
        ], capture_output=True, text=True)

    def read_policy(self):
        return json.loads((self.output / "gcp-policy.json").read_text())

    def write_policy(self, policy):
        (self.output / "gcp-policy.json").write_text(json.dumps(policy))

    def approve_fixture_regions(self):
        policy = self.read_policy()
        for region in policy["market"]["regions"]:
            region["canary_valid"] = True
        self.write_policy(policy)

    def test_second_project_validates_without_source_edits_and_starts_disabled(self):
        result = self.configure()
        self.assertEqual(result.returncode, 0, result.stderr)
        result = self.validate("--bootstrap")
        self.assertEqual(result.returncode, 0, result.stderr)
        policy = self.read_policy()
        self.assertFalse(any(r["canary_valid"] for r in policy["market"]["regions"]))
        self.assertFalse(policy["production"]["automatic_delete"])
        self.assertFalse(policy["general_production"]["automatic_delete"])
        for pool in ("production", "general_production"):
            self.assertEqual(policy[pool]["retention_mode"], "demand_only")
            self.assertNotIn("warm_pool", policy[pool])
        self.assertFalse(json.loads((self.output / "image-vars.json").read_text())["telemetry_enabled"])
        self.assertEqual((self.output.stat().st_mode & 0o777), 0o700)
        for path in self.output.iterdir():
            self.assertEqual((path.stat().st_mode & 0o777), 0o600)
        variables = json.loads((self.output / "controller-vars.json").read_text())
        self.assertFalse(variables["also_ran_gcp_broker_enabled"])
        self.assertFalse(variables["also_ran_gcp_broker_apply"])
        self.assertFalse(variables["also_ran_gcp_general_broker_apply"])
        self.assertNotEqual(self.validate().returncode, 0)
        self.approve_fixture_regions()
        result = self.validate()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_cost_and_telemetry_defaults_require_explicit_opt_in(self):
        result = self.configure("--warm-pool", "--enable-telemetry")
        self.assertEqual(result.returncode, 0, result.stderr)
        policy = self.read_policy()
        for pool in ("production", "general_production"):
            self.assertEqual(policy[pool]["retention_mode"], "peak_warm_pool")
            self.assertGreater(policy[pool]["warm_pool"]["minimum_hosts"], 0)
            self.assertFalse(policy[pool]["automatic_delete"])
        self.assertTrue(json.loads((self.output / "image-vars.json").read_text())["telemetry_enabled"])
        self.assertEqual(self.validate("--bootstrap").returncode, 0)

    def test_policy_cannot_redirect_trusted_project(self):
        self.assertEqual(self.configure().returncode, 0)
        self.approve_fixture_regions()
        policy = self.read_policy()
        policy["project_id"] = "some-other-project"
        self.write_policy(policy)
        result = self.validate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("project boundary", result.stderr)

    def test_malformed_identity_cannot_reach_policy_validation(self):
        self.assertEqual(self.configure().returncode, 0)
        identity = json.loads((self.output / "deployment.json").read_text())
        identity["fleet"] = 'unsafe" OR true'
        (self.output / "deployment.json").write_text(json.dumps(identity))
        result = self.validate("--bootstrap")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid deployment identity", result.stderr)

    def test_production_cli_rejects_unapproved_regions_before_provider_access(self):
        self.assertEqual(self.configure().returncode, 0)
        result = subprocess.run([
            sys.executable, str(ROOT / "scripts/gcp-runner-lifecycle.py"),
            "--policy", str(self.output / "gcp-policy.json"),
            "create-production", "--slots", "1",
            "--slot-zones", "1=europe-west9-a", "--apply",
        ], env={**os.environ, "ALSO_RAN_DEPLOYMENT": str(self.output / "deployment.json"), "PATH": "/nonexistent"}, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("production region is not validated", result.stderr)
        self.assertNotIn("No such file", result.stderr)

    def test_public_ssh_is_rejected_before_writing_any_files(self):
        result = self.configure("--ssh-source-cidr", "0.0.0.0/0")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())

    def test_existing_operator_configuration_is_never_overwritten(self):
        self.assertEqual(self.configure().returncode, 0)
        marker = self.output / "deployment.json"
        before = marker.read_bytes()
        self.assertNotEqual(self.configure().returncode, 0)
        self.assertEqual(marker.read_bytes(), before)

    def test_independent_identity_reaches_guest_recovery_script(self):
        self.assertEqual(self.configure().returncode, 0)
        code = """
import base64, importlib.util, pathlib, subprocess, sys
path = pathlib.Path(sys.argv[1])
spec = importlib.util.spec_from_file_location('lifecycle', path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
with module.reaper_startup_file() as output:
    script = pathlib.Path(output.name).read_text()
    encoded = script.split("printf '%s' '", 1)[1].split("'", 1)[0]
    reaper = base64.b64decode(encoded).decode()
    assert 'customer-ci-project' in reaper
    assert 'also-ran-ci-runners' not in reaper
    assert '{{' not in reaper
    subprocess.run(['bash', '-n'], input=reaper, text=True, check=True)
"""
        result = subprocess.run([
            sys.executable, "-c", code, str(ROOT / "scripts/gcp-runner-lifecycle.py"),
        ], env={**os.environ, "ALSO_RAN_DEPLOYMENT": str(self.output / "deployment.json")}, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_legacy_image_allowlist_requires_unique_full_commits(self):
        self.assertTrue(valid_legacy_images({"legacy_image_source_commits": []}))
        self.assertTrue(valid_legacy_images({"legacy_image_source_commits": ["a" * 40]}))
        for values in (None, ["a" * 12], ["a" * 40] * 2, [False], "a" * 40):
            with self.subTest(values=values):
                self.assertFalse(valid_legacy_images({"legacy_image_source_commits": values}))
