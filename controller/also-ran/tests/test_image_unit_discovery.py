"""Exercise optional image-unit discovery using Ansible and a fake systemctl."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

try:
    import yaml
except ImportError:
    yaml = None

ROOT = Path(__file__).resolve().parents[3]
ANSIBLE = shutil.which("ansible-playbook") or str(ROOT / ".venv/bin/ansible-playbook")


@unittest.skipUnless(yaml is not None and Path(ANSIBLE).is_file(), "install requirements-dev.txt for Ansible behavior checks")
class ImageUnitDiscoveryTests(unittest.TestCase):
    def exercise_role(self, role, inspect_name, stop_name, missing):
        tasks = yaml.safe_load((ROOT / "ansible/roles" / role / "tasks/main.yml").read_text())
        selected = [task for task in tasks if task["name"] in (inspect_name, stop_name)]
        self.assertEqual(len(selected), 2)
        expected_units = set(selected[0]["loop"]) - {missing}
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            log = directory / "stopped.jsonl"
            fake_systemctl = directory / "systemctl"
            fake_systemctl.write_text(
                "#!/usr/bin/env python3\nimport json,sys\n"
                + "if sys.argv[1] == 'show':\n"
                + f" print('not-found' if sys.argv[-1] == {missing!r} else 'loaded')\n"
                + "elif sys.argv[1] == 'stop':\n"
                + f" with open({str(log)!r}, 'a') as stream: stream.write(json.dumps(sys.argv[-1]) + '\\n')\n"
                + "else: sys.exit(2)\n")
            fake_systemctl.chmod(0o755)
            selected[0]["ansible.builtin.command"]["argv"][0] = str(fake_systemctl)
            module_args = selected[1].pop("ansible.builtin.systemd_service")
            self.assertIs(module_args["enabled"], False)
            self.assertEqual(module_args["state"], "stopped")
            selected[1]["ansible.builtin.command"] = {
                "argv": [str(fake_systemctl), "stop", module_args["name"]]}
            playbook = directory / "test.yml"
            playbook.write_text(yaml.safe_dump([{
                "hosts": "localhost", "gather_facts": False,
                "vars": {"telemetry_enabled": False}, "tasks": selected}]))
            result = subprocess.run([ANSIBLE, "-i", "localhost,", "-c", "local", str(playbook)],
                                    capture_output=True, text=True, env={**os.environ, "ANSIBLE_NOCOLOR": "1"})
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            stopped = {json.loads(line) for line in log.read_text().splitlines()}
            self.assertEqual(stopped, expected_units)
            self.assertTrue(any(unit.endswith(".timer") for unit in stopped))

    def test_disabling_telemetry_stops_timer_but_skips_absent_optional_unit(self):
        self.exercise_role("telemetry", "Inspect optional guest telemetry units, including timers",
                           "Stop and disable previously installed guest telemetry", "otelcol-contrib.service")

    def test_finalization_stops_reaper_and_sample_timers_without_optional_collector(self):
        self.exercise_role("runner_image_finalize", "Inspect image service and timer units",
                           "Disable image services before snapshot", "otelcol-contrib.service")
