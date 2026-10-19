"""Exercise admission gating without starting a real daemon on the dev host."""
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest

WRAPPER = Path(__file__).resolve().parents[3] / "ansible/roles/outrunner/files/runner-with-docker"


class RunnerDockerStartupTest(unittest.TestCase):
    def run_wrapper(self, ready):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, script in {
                "sudo": "#!/bin/sh\nexec /bin/sleep 30\n",
                "docker": "#!/bin/sh\nexit " + ("0" if ready else "1") + "\n",
                "sleep": "#!/bin/sh\nexit 0\n",
                "run.sh": '#!/bin/sh\nprintf "admitted:%s:%s\\n" "$1" "$2"\n',
            }.items():
                path = root / name
                path.write_text(script)
                path.chmod(0o755)
            env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}", TMPDIR=directory)
            process = subprocess.Popen(
                ["bash", str(WRAPPER), "--jitconfig", "opaque-test-value"],
                cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, start_new_session=True,
            )
            try:
                stdout, stderr = process.communicate(timeout=10)
                return process.returncode, stdout, stderr
            finally:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def test_ready_daemon_admits_runner_and_preserves_jit_arguments(self):
        status, stdout, _ = self.run_wrapper(True)
        self.assertEqual(status, 0)
        self.assertEqual(stdout.strip(), "admitted:--jitconfig:opaque-test-value")

    def test_unready_daemon_fails_without_admitting_runner(self):
        status, stdout, stderr = self.run_wrapper(False)
        self.assertNotEqual(status, 0)
        self.assertNotIn("admitted:", stdout)
        self.assertIn("failed to become ready", stderr)
