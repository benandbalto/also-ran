"""Regression guards for the isolated GCP Docker runner image."""

import json
from pathlib import Path
import unittest


REPOSITORY = Path(__file__).resolve().parents[3]
DOCKERFILE = REPOSITORY / "ansible/roles/outrunner/files/Dockerfile.also-ran"
BOOTSTRAP = (
    REPOSITORY
    / "ansible/roles/gcp_runner_bootstrap/templates/gcp-runner-bootstrap.sh.j2"
)
LOCAL_SSD = REPOSITORY / "ansible/roles/gcp_runner_bootstrap/files/gcp-runner-local-ssd"
OUTRUNNER_TASKS = REPOSITORY / "ansible/roles/outrunner/tasks/main.yml"
CANARY_VARS = REPOSITORY / "examples/image-vars.json"


class GcpRunnerImageTest(unittest.TestCase):
    def test_gcp_job_template_has_no_host_privilege_mounts(self):
        bootstrap = BOOTSTRAP.read_text()

        self.assertNotIn("/var/run/docker.sock", bootstrap)
        self.assertNotIn("host_network:", bootstrap)
        self.assertIn('image: "{{ outrunner_runner_image }}"', bootstrap)

    def test_gcp_hosts_keep_one_slot_with_one_pre_registered_spare(self):
        bootstrap = BOOTSTRAP.read_text(encoding="utf-8")
        # One job at a time per VM, with a registered runner waiting for it.
        # idle_runners needs Outrunner 1.2.1-arboreal.12 or later.
        self.assertIn("    max_runners: 1\n", bootstrap)
        self.assertIn("    idle_runners: 1\n", bootstrap)

    def test_image_keeps_pinned_node_pnpm_and_writable_toolcache(self):
        dockerfile = DOCKERFILE.read_text()

        self.assertIn("ARG NODE_VERSION=24.20.0", dockerfile)
        self.assertIn("ARG PNPM_VERSION=10.33.0", dockerfile)
        self.assertIn("ENV RUNNER_TOOL_CACHE=/opt/hostedtoolcache", dockerfile)
        self.assertIn("chown -R runner:runner /opt/hostedtoolcache", dockerfile)
        self.assertNotIn("docker-compose-linux-x86_64", dockerfile)

    def test_host_service_can_create_but_jobs_cannot_control_containers(self):
        tasks = OUTRUNNER_TASKS.read_text()

        self.assertIn("Allow Outrunner to provision isolated job containers", tasks)
        self.assertIn("groups: docker", tasks)
        self.assertNotIn("DOCKER_GID", tasks)

    def test_image_bakes_pinned_and_verified_gh_az_and_bicep(self):
        dockerfile = DOCKERFILE.read_text()

        self.assertIn("ARG GH_VERSION=2.102.0", dockerfile)
        self.assertIn("ARG AZURE_CLI_VERSION=2.90.0", dockerfile)
        self.assertIn("ARG BICEP_VERSION=0.47.16", dockerfile)
        for name in ("GH_SHA256", "AZURE_CLI_SHA256", "BICEP_SHA256"):
            self.assertRegex(dockerfile, rf"ARG {name}=[0-9a-f]{{64}}\n")
            self.assertIn(f'echo "${{{name}}}  /', dockerfile)
        # Pinned .deb, not a mutable apt source an in-job upgrade could move.
        self.assertIn("azure-cli_${AZURE_CLI_VERSION}-1~noble_amd64.deb", dockerfile)
        self.assertNotIn("/etc/apt/sources.list.d", dockerfile)
        self.assertNotIn("aka.ms/InstallAzureCLI", dockerfile)

    def test_e2e_tools_lock_matches_image_and_rebuilds_on_change(self):
        tool_dir = DOCKERFILE.parent / "e2e-tools"
        manifest = json.loads((tool_dir / "package.json").read_text())
        lock = json.loads((tool_dir / "package-lock.json").read_text())
        dockerfile = DOCKERFILE.read_text()
        self.assertEqual(manifest["dependencies"], lock["packages"][""]["dependencies"])
        for package, arg in (
            ("azure-functions-core-tools", "FUNCTIONS_CORE_TOOLS_VERSION"),
            ("azurite", "AZURITE_VERSION"),
            ("playwright", "PLAYWRIGHT_VERSION"),
        ):
            version = manifest["dependencies"][package]
            self.assertIn(f"ARG {arg}={version}\n", dockerfile)
            self.assertEqual(lock["packages"][f"node_modules/{package}"]["version"], version)
        for name, package in lock["packages"].items():
            if name:
                self.assertTrue(package["resolved"].startswith("https://registry.npmjs.org/"))
                self.assertRegex(package["integrity"], r"^sha(?:1|512)-[A-Za-z0-9+/]+=*$")
        self.assertIn("npm ci --prefix /opt/also-ran-e2e --ignore-scripts", dockerfile)
        for arg in ("FUNCTIONS_CORE_TOOLS_SHA256", "FUNCTIONS_BUNDLE_SHA256"):
            self.assertRegex(dockerfile, rf"ARG {arg}=[0-9a-f]{{64}}\n")
            self.assertIn(f'echo "${{{arg}}}  /tmp/', dockerfile)
        self.assertIn("src: e2e-tools/", OUTRUNNER_TASKS.read_text())
        self.assertIn("outrunner_e2e_tools.changed", OUTRUNNER_TASKS.read_text())

    def test_jobs_never_update_or_download_cli_tools(self):
        dockerfile = DOCKERFILE.read_text()

        for setting in (
            "GH_NO_UPDATE_NOTIFIER=1",
            "GH_NO_EXTENSION_UPDATE_NOTIFIER=1",
            "GH_TELEMETRY=false",
            "AZURE_CORE_COLLECT_TELEMETRY=false",
            "AZURE_BICEP_USE_BINARY_FROM_PATH=true",
            "AZURE_BICEP_CHECK_VERSION=false",
        ):
            self.assertIn(setting, dockerfile)

    def test_image_smoke_runs_cli_tools_as_runner_user(self):
        dockerfile = DOCKERFILE.read_text()
        smoke = dockerfile[dockerfile.rindex("USER runner") :]

        for command in (
            "gh --version",
            "az version",
            "bicep --version",
            "az bicep version",
        ):
            self.assertIn(command, smoke)

    def test_local_ssd_mkfs_skips_discard_but_keeps_the_journal(self):
        local_ssd = LOCAL_SSD.read_text()
        mkfs = [line for line in local_ssd.splitlines() if line.lstrip().startswith("mkfs.ext4 ")]

        self.assertEqual(len(mkfs), 1)
        # A fresh local SSD is already zeroed; the whole-device discard costs ~5 s
        # and zeroing the journal another ~3 s.
        self.assertIn("-E nodiscard,assume_storage_prezeroed=1", mkfs[0])
        # Dropping the journal saves nothing measurable and would need fsck after a reboot.
        self.assertNotIn("-O", mkfs[0])
        self.assertNotIn("journal", mkfs[0])

    def test_docker_store_copy_writes_back_early(self):
        local_ssd = LOCAL_SSD.read_text()
        writeback = local_ssd.index("sysctl --quiet --write vm.dirty_background_bytes=67108864")

        # Before either way of populating the store, after the SSD is formatted.
        self.assertLess(writeback, local_ssd.index('lazy_lowers "${reason_file}"'))
        self.assertLess(local_ssd.index('mkfs.ext4 -F'), writeback)

    def test_image_tag_names_the_cli_tools(self):
        self.assertIn(
            "pgclient-make-dind-gh-az-bicep-func4.6.0-pw1.63.0-azurite3.37.0-bundle4.39.1-r13",
            CANARY_VARS.read_text(),
        )


class OptionalGuestTelemetryTest(unittest.TestCase):
    def test_bootstrap_only_fetches_axiom_and_starts_exporters_when_enabled(self):
        template = BOOTSTRAP.read_text()
        sections = template.split('{% if telemetry_enabled | bool %}')
        self.assertEqual(len(sections), 3)
        base = sections[0]
        for section in sections[1:]:
            optional, base_part = section.split('{% endif %}', 1)
            base += base_part
        self.assertNotIn('secret_value "{{ gcp_runner_bootstrap_axiom_secret }}"', base)
        self.assertNotIn('systemctl enable --now otelcol-contrib.service', base)
        self.assertNotIn('/etc/otelcol-contrib/config.yaml', base)
        self.assertIn('systemctl enable --now outrunner.service', base)
        self.assertIn('systemctl enable --now also-ran-runner-readiness.service', base)
        self.assertIn('systemctl enable --now gcp-runner-reaper.timer', base)
        self.assertIn('drain_receipt:', base)

    def test_disabled_bootstrap_stops_present_exporters_and_removes_credential(self):
        import subprocess
        import tempfile

        template = BOOTSTRAP.read_text()
        disabled = template.split('{% else %}', 1)[1].split('{% endif %}', 1)[0]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            credential = root / 'credential'
            credential.write_text('old-secret')
            calls = root / 'calls'
            script = ('set -euo pipefail\n'
                      'systemctl() {\n'
                      '  if [[ "$1" == cat ]]; then\n'
                      '    [[ "$2" != also-ran-runner-telemetry-sample.service ]]\n'
                      '  else printf "%s\\n" "$*" >> "$CALLS"; fi\n'
                      '}\n' + disabled.replace('/etc/otelcol-contrib/otelcol-contrib.conf', str(credential)))
            result = subprocess.run(['bash', '-c', script], env={'CALLS': str(calls)},
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(credential.exists())
            self.assertEqual(calls.read_text().splitlines(), [
                'disable --now otelcol-contrib.service',
                'disable --now also-ran-runner-telemetry-sample.timer',
            ])

    def test_builder_scope_and_telemetry_default_are_safe(self):
        playbook = (REPOSITORY / 'ansible/playbooks/runner-image-builder.yml').read_text()
        self.assertIn('hosts: image_builders', playbook)
        self.assertNotIn('hosts: all', playbook)
        defaults = (REPOSITORY / 'ansible/roles/telemetry/defaults/main.yml').read_text()
        self.assertIn('telemetry_enabled: false', defaults)
        bootstrap_defaults = (REPOSITORY / 'ansible/roles/gcp_runner_bootstrap/defaults/main.yml').read_text()
        self.assertIn('gcp_runner_bootstrap_shared_label: outrunner-gcp-linux-x64', bootstrap_defaults)


if __name__ == "__main__":
    unittest.main()
