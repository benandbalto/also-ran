"""Source entrypoints must never select a synthetic deployment implicitly."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
ENTRYPOINTS = (
    "scripts/gcp-runner-lifecycle.py",
    "scripts/gcp-region-foundation.py",
    "scripts/gcp-runner-preflight.py",
    "controller/also-ran/gcp_broker.py",
    "controller/also-ran/gcp_market_observer.py",
    "controller/also-ran/gcp_reconciler.py",
    "controller/also-ran/gcp_run_telemetry.py",
)


class ExplicitConfigurationTests(unittest.TestCase):
    def run_cli(self, path, *args, deployment=None):
        env = dict(os.environ)
        env.pop("ALSO_RAN_DEPLOYMENT", None)
        if deployment is not None:
            env["ALSO_RAN_DEPLOYMENT"] = str(deployment)
        return subprocess.run([sys.executable, str(ROOT / path), *args],
                              env=env, capture_output=True, text=True)

    def test_source_commands_refuse_implicit_fixture_before_provider_access(self):
        for path in ENTRYPOINTS:
            with self.subTest(path=path):
                result = self.run_cli(path)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("set ALSO_RAN_DEPLOYMENT", result.stderr)
                self.assertNotIn("Traceback", result.stderr)

    def test_help_does_not_require_a_deployment(self):
        for path in ENTRYPOINTS:
            with self.subTest(path=path):
                result = self.run_cli(path, "--help")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("usage:", result.stdout)

    def test_missing_and_malformed_identity_report_friendly_error(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "deployment.json"
            for contents in (None, "not JSON", "[]"):
                if contents is not None:
                    config.write_text(contents)
                result = self.run_cli(ENTRYPOINTS[0], deployment=config)
                self.assertEqual(result.returncode, 1)
                self.assertIn("configuration error:", result.stderr)
                self.assertNotIn("Traceback", result.stderr)

    def test_operator_commands_require_policy_argument(self):
        fixture = ROOT / "controller/also-ran/deployment.json"
        for path, args in (
            (ENTRYPOINTS[0], ["status"]),
            (ENTRYPOINTS[1], ["--region", "europe-west9"]),
            (ENTRYPOINTS[2], ["--project", "example", "--zone", "europe-west9-a"]),
        ):
            with self.subTest(path=path):
                result = self.run_cli(path, *args, deployment=fixture)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn("--policy", result.stderr)

    def test_validator_reports_missing_policy_without_traceback(self):
        result = self.run_cli("scripts/validate-config.py", "--deployment",
                              str(ROOT / "controller/also-ran/deployment.json"),
                              "--policy", "/nonexistent-also-ran-policy.json")
        self.assertEqual(result.returncode, 1)
        self.assertIn("configuration error:", result.stderr)
        self.assertNotIn("Traceback", result.stderr)
