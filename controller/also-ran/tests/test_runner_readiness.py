import datetime as dt
import importlib.util
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "runner_readiness", ROOT / "ansible/roles/gcp_runner_bootstrap/files/runner-readiness.py"
)
r = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(r)


class ListenerReadinessTests(unittest.TestCase):
    def setUp(self):
        self.identity = {
            "instance_id": "111",
            "boot_id": "current-boot",
            "pool": "general",
            "runner_scale_set": "gcp-general-01",
            "image_source_commit": "abcdef123456",
            "boot_started_at": "2026-10-09T00:00:00Z",
        }
        self.now = r.timestamp("2026-10-09T00:02:00Z")
        self.container = "a" * 64
        self.marker = (
            "2026-10-09T00:00:45.125000000Z 2026-10-09 00:00:45Z: Listening for Jobs\n"
        )

    def command(self, *args):
        if args[0] == "ps":
            return (
                f"{self.container} gcp-general-01-spare-1\n"
                + "b" * 64
                + " unrelated-runner\n"
            )
        self.assertEqual(
            args, ("logs", "--timestamps", "--tail", "200", self.container)
        )
        return "DO-NOT-EXPORT-SECRET\n" + self.marker

    def test_only_listener_session_marker_establishes_readiness(self):
        event = r.observe(self.identity, "current-boot", self.now, self.command)
        self.assertEqual(event["event_id"], "ready:111:current-boot")
        self.assertEqual(event["boot_to_runner_online_seconds"], 45.125)
        self.assertEqual(event["runner_name"], "gcp-general-01-spare-1")
        self.assertNotIn("DO-NOT-EXPORT-SECRET", str(event))
        self.assertEqual(
            r.observe(self.identity, "current-boot", self.now, self.command), event
        )

    def test_container_or_process_start_and_failed_sessions_are_not_readiness(self):
        logs = "Container started\nRunner.Listener\nConnected to GitHub\nListening for Jobs\n"
        self.assertIsNone(
            r.listener_online_at(
                logs, r.timestamp(self.identity["boot_started_at"]), self.now
            )
        )

    def test_stale_boot_or_foreign_generation_cannot_produce_event(self):
        self.assertIsNone(
            r.observe(self.identity, "other-boot", self.now, self.command)
        )
        self.assertIsNone(
            r.observe(
                {**self.identity, "instance_id": ""},
                "current-boot",
                self.now,
                self.command,
            )
        )

    def test_clock_errors_and_markers_from_other_boots_stay_unknown(self):
        boot = r.timestamp(self.identity["boot_started_at"])
        for logs in (
            self.marker.replace("2026-10-09T", "2026-10-08T"),
            self.marker.replace("00:00:45.125", "00:02:45.125"),
            self.marker.replace("00:00:45Z:", "00:00:40Z:"),
        ):
            self.assertIsNone(r.listener_online_at(logs, boot, self.now))

    def test_container_removal_race_is_missing_evidence(self):
        def command(*args):
            if args[0] == "ps":
                return f"{self.container} gcp-general-01-spare-1\n"
            raise subprocess.CalledProcessError(1, ["docker"])

        self.assertIsNone(r.observe(self.identity, "current-boot", self.now, command))

    def test_service_persists_receipt_and_restart_never_replaces_original_time(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory)
            current_boot = (
                pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip()
            )
            now = dt.datetime.now(dt.timezone.utc)
            online = now - dt.timedelta(seconds=30)
            identity = {
                **self.identity,
                "boot_id": current_boot,
                "boot_started_at": (now - dt.timedelta(seconds=60)).isoformat(),
            }
            (state / "identity.json").write_text(json.dumps(identity))
            marker = (
                online.isoformat()
                + " "
                + online.strftime("%Y-%m-%d %H:%M:%SZ")
                + ": Listening for Jobs"
            )
            fake = state / "docker"
            fake.write_text(
                f'#!/bin/sh\nif [ "$1" = ps ]; then\n  echo "{self.container} gcp-general-01-spare-1"\nelse\n  echo "{marker}"\n  echo "DO-NOT-EXPORT-SECRET" >&2\nfi\n'
            )
            fake.chmod(0o755)
            command = [sys.executable, str(SPEC.origin), "--state-dir", directory]
            env = {**os.environ, "PATH": directory + ":" + os.environ["PATH"]}
            first = subprocess.run(
                command, env=env, capture_output=True, text=True, timeout=10, check=True
            )
            self.assertNotIn("DO-NOT-EXPORT-SECRET", first.stdout + first.stderr)
            receipt = (state / "readiness-metric.json").read_text()
            self.assertEqual(json.loads(receipt), json.loads(first.stdout))
            second = subprocess.run(
                command, env=env, capture_output=True, text=True, timeout=10, check=True
            )
            self.assertEqual(second.stdout, "")
            self.assertEqual((state / "readiness-metric.json").read_text(), receipt)


if __name__ == "__main__":
    unittest.main()
