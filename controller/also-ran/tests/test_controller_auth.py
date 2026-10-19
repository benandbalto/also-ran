"""Credential selection fails closed before installing provider credentials."""

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[3]
ROLE = ROOT / "ansible/roles/also_ran"
SPEC = importlib.util.spec_from_file_location("credential_validation", ROLE / "files/validate_gcp_credentials.py")
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)


class CredentialValidationTests(unittest.TestCase):
    def federation(self):
        return {
            "type": "external_account", "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1/token",
            "credential_source": {"file": "/run/identity/token"},
        }

    def key(self):
        return {"type": "service_account", "client_email": "controller@example.iam.gserviceaccount.com",
                "private_key": "test-value-only", "token_uri": "https://oauth2.googleapis.com/token"}

    def test_key_and_federation_are_not_interchangeable(self):
        VALIDATOR.validate("service_account_key", self.key())
        VALIDATOR.validate("workload_identity_federation", self.federation())
        for mode, config in [("service_account_key", self.federation()), ("workload_identity_federation", self.key())]:
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                VALIDATOR.validate(mode, config)

    def test_federation_rejects_unsafe_or_ambiguous_sources(self):
        for source in [{"file": "relative/token"}, {"url": "http://issuer/token"},
                       {"executable": {"command": "echo unsafe"}}, {},
                       {"file": "/run/token", "url": "https://issuer/token"}]:
            config = self.federation()
            config["credential_source"] = source
            with self.subTest(source=source), self.assertRaises(ValueError):
                VALIDATOR.validate("workload_identity_federation", config)
        config["credential_source"] = {"url": "https://issuer.example/token"}
        VALIDATOR.validate("workload_identity_federation", config)

    def test_cli_failure_does_not_disclose_credentials(self):
        result = subprocess.run([sys.executable, str(ROLE / "files/validate_gcp_credentials.py"), "workload_identity_federation"],
                                input=json.dumps(self.key()), capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("test-value-only", result.stderr)
        self.assertNotIn("controller@example", result.stderr)

    def test_malformed_or_user_credentials_rejected(self):
        for config in [None, [], {}, {"type": "authorized_user"}, {"type": "service_account"}]:
            with self.subTest(config=config), self.assertRaises(ValueError):
                VALIDATOR.validate("service_account_key", config)


try:
    from jinja2 import Environment, FileSystemLoader
except ImportError:
    Environment = None


@unittest.skipIf(Environment is None, "install requirements-dev.txt for systemd template rendering")
class RenderedAuthenticationTests(unittest.TestCase):
    def test_every_provider_service_uses_the_selected_authentication(self):
        environment = Environment(loader=FileSystemLoader(ROLE / "templates"))
        environment.filters["bool"] = bool
        environment.filters["ternary"] = lambda value, yes, no: yes if value else no
        services = sorted((ROLE / "templates").glob("also-ran-gcp*.service.j2"))
        self.assertEqual(len(services), 5)
        for mode in ["attached_service_account", "workload_identity_federation", "service_account_key"]:
            for service in services:
                rendered = environment.get_template(service.name).render(
                    also_ran_gcp_auth_mode=mode, also_ran_github_app_enabled=False,
                    also_ran_github_token_path="/etc/also-ran/github-token",
                    also_ran_gcp_broker_apply=False, also_ran_gcp_general_broker_apply=False)
                with self.subTest(mode=mode, service=service.name):
                    self.assertIn("Environment=CLOUDSDK_CONFIG=%t/also-ran-gcloud-%N", rendered)
                    self.assertIn("CLOUDSDK_AUTH_ACCESS_TOKEN", rendered)
                    if mode == "attached_service_account":
                        self.assertNotIn("LoadCredential=gcp-controller-key", rendered)
                        self.assertIn("UnsetEnvironment=CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", rendered)
                    else:
                        self.assertIn("LoadCredential=gcp-controller-key:/etc/also-ran/gcp-controller-key.json", rendered)
                        self.assertIn("Environment=CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=%d/gcp-controller-key", rendered)
                        self.assertNotIn("UnsetEnvironment=CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", rendered)
