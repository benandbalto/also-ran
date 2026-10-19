"""The on-host publisher must never be stricter than the controller.

`gcp-publish-drain-receipt` is the only thing that puts a drain receipt where
the broker can see it. If it rejects a receipt the broker would have accepted,
nothing reaches serial port 1, and a slot with no receipt at all is neither
drained nor quarantined nor idle-confirmed -- so it is invisible to every reap
path and its host bills until someone deletes it by hand.

That is not hypothetical. v3 receipts are allowed to carry `reason: "external"`
(the broker admits them; `test_external_zero_job_receipt_is_valid` covers the
v2 spelling), but the publisher only admitted `reason` on its v2 branch, so
every externally drained host stranded. The publisher has no test coverage of
its own, which is why that shipped.
"""

import base64
import importlib.util
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest

CONTROLLER_DIR = pathlib.Path(__file__).parents[1]
if str(CONTROLLER_DIR) not in sys.path:
    sys.path.insert(0, str(CONTROLLER_DIR))

import test_gcp_broker as broker_tests


MODULE = broker_tests.MODULE
PUBLISHER = (
    pathlib.Path(__file__).parents[3]
    / "ansible"
    / "roles"
    / "gcp_runner_bootstrap"
    / "templates"
    / "gcp-publish-drain-receipt.sh.j2"
)


def publisher_filter():
    """Pull the jq program the publisher actually runs out of the script.

    Matched by content rather than by variable name so this keeps working
    whether the program is inlined into the `jq` call or hoisted into a
    variable -- the point is to test the predicate that ships, not its
    spelling.
    """
    # Comment prose carries apostrophes, which would throw off quote pairing.
    source = "\n".join(
        line
        for line in PUBLISHER.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )
    candidates = [
        literal
        for literal in re.findall(r"'([^']*)'", source, re.DOTALL)
        if ".identity.provider" in literal and ".completed_jobs" in literal
    ]
    assert len(candidates) == 1, (
        f"expected exactly one jq receipt predicate, found {len(candidates)}"
    )
    return candidates[0]


def publisher_accepts(receipt, jq_filter):
    completed = subprocess.run(
        ["jq", "--exit-status", jq_filter],
        input=json.dumps(receipt),
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode in (0, 1), completed.stderr
    return completed.returncode == 0


def broker_accepts(receipt, slot=1, instance_id="1001"):
    try:
        MODULE.validate_drain_receipt(
            receipt,
            broker_tests.instance(slot),
            broker_tests.broker_policy(),
            "example-org/example-repo",
            broker_tests.production(),
        )
    except MODULE.ObservationError:
        return False
    return True


def external(receipt, completed_jobs=None, version=None):
    """An externally drained spelling of `receipt`.

    v1 may not carry `reason` at all, so an external v1 drain is spelled v2;
    v3 carries `reason` alongside the v1 field set.
    """
    value = dict(receipt)
    value["reason"] = "external"
    if version is not None:
        value["version"] = version
    if completed_jobs is not None:
        value["completed_jobs"] = completed_jobs
        value["jobs"] = value["jobs"][:completed_jobs]
    return value


@unittest.skipIf(shutil.which("jq") is None, "jq is required to run the publisher filter")
class DrainReceiptPublisherTests(unittest.TestCase):
    def setUp(self):
        self.filter = publisher_filter()

    def test_v3_external_receipt_publishes(self):
        # The exact shape that stranded also-ran-runner-gcp-prod-{01,04,05,06,07,08}
        # and then prod-03: v3, one completed job, drained on an external request.
        receipt = external(broker_tests.receipt_v3())
        self.assertTrue(broker_accepts(receipt))
        self.assertTrue(
            publisher_accepts(receipt, self.filter),
            "a v3 external drain receipt must reach the serial console",
        )

    def test_v4_synchronized_receipts_publish(self):
        for reason in ("external", "idle", "max_jobs"):
            candidate = broker_tests.receipt_v3()
            candidate.update(version=4, reason=reason)
            if reason != "max_jobs":
                candidate.update(completed_jobs=0, jobs=[])
            with self.subTest(reason=reason):
                self.assertTrue(broker_accepts(candidate))
                self.assertTrue(publisher_accepts(candidate, self.filter))
        candidate["max_jobs"] = 2
        self.assertFalse(broker_accepts(candidate))
        self.assertFalse(publisher_accepts(candidate, self.filter))

    def test_publisher_is_never_stricter_than_the_broker(self):
        candidates = {
            "v1 full host": broker_tests.receipt(),
            "v2 external": external(
                broker_tests.receipt(), completed_jobs=1, version=2
            ),
            "v2 external zero jobs": external(
                broker_tests.receipt(), completed_jobs=0, version=2
            ),
            "v3 full host": broker_tests.receipt_v3(),
            "v3 external": external(broker_tests.receipt_v3()),
        }
        for name, receipt in candidates.items():
            with self.subTest(receipt=name):
                self.assertTrue(
                    broker_accepts(receipt),
                    f"{name} is not a broker-valid receipt, so it proves nothing",
                )
                self.assertTrue(
                    publisher_accepts(receipt, self.filter),
                    f"broker accepts {name} but the publisher would suppress it",
                )

    def test_v2_marks_version_two_as_the_only_zero_job_receipt(self):
        v3_zero = external(broker_tests.receipt_v3(), completed_jobs=0)
        self.assertFalse(broker_accepts(v3_zero))
        self.assertFalse(publisher_accepts(v3_zero, self.filter))

    def test_malformed_receipts_are_still_suppressed(self):
        partial = dict(broker_tests.receipt_v3())
        partial["max_jobs"] = 4
        unknown_reason = external(broker_tests.receipt_v3())
        unknown_reason["reason"] = "operator"
        extra_key = external(broker_tests.receipt_v3())
        extra_key["surprise"] = True
        bad_identity = external(broker_tests.receipt_v3())
        bad_identity["identity"] = dict(bad_identity["identity"], provider="azure")
        for name, receipt in {
            "v3 partial host without reason": partial,
            "reason other than external": unknown_reason,
            "unexpected top-level key": extra_key,
            "non-gcp identity": bad_identity,
        }.items():
            with self.subTest(receipt=name):
                self.assertFalse(broker_accepts(receipt))
                self.assertFalse(publisher_accepts(receipt, self.filter))

    def test_diagnostic_marker_cannot_be_read_back_as_a_receipt(self):
        source = PUBLISHER.read_text(encoding="utf-8")
        diagnostic = re.search(
            r'^readonly serial_diagnostic_prefix="(?P<value>[^"]+)"$',
            source,
            re.MULTILINE,
        )
        self.assertIsNotNone(diagnostic)
        # serial_drain_receipt() decodes whatever follows RECEIPT_PREFIX, so a
        # diagnostic line carrying that marker would quarantine the slot rather
        # than explain the suppression.
        self.assertNotIn(MODULE.RECEIPT_PREFIX.strip(), diagnostic.group("value"))
        self.assertNotIn(
            MODULE.ADMISSION_CLOSED_PREFIX.strip(), diagnostic.group("value")
        )


if __name__ == "__main__":
    sys.exit(unittest.main())


@unittest.skipIf(
    shutil.which("jq") is None or shutil.which("bash") is None,
    "bash and jq are required to run the publisher",
)
class PublisherScriptTests(unittest.TestCase):
    """Run the shipped script, not a paraphrase of it.

    The predicate tests above cover which receipts are valid. These cover the
    control flow around it -- which is where the original failure actually
    lived: `jq` exiting non-zero under `set -euo pipefail` before reaching any
    `systemd-cat`, so the suppression left no trace anywhere.
    """

    def run_publisher(self, receipt, **environ):
        workspace = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, workspace, ignore_errors=True)
        serial = workspace / "ttyS0"
        serial.touch()
        receipt_path = workspace / "drain-receipt.json"
        if receipt is not None:
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

        stub = workspace / "bin"
        stub.mkdir()
        (stub / "systemd-cat").write_text(
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                cat >&2
                """
            ),
            encoding="utf-8",
        )
        (stub / "systemd-cat").chmod(0o755)
        # Record every guest-attribute write as `<url> <body>` instead of
        # reaching the metadata server; GUEST_ATTRIBUTES_FAIL makes it refuse.
        self.last_guest_attributes = workspace / "guest-attributes"
        (stub / "curl").write_text(
            textwrap.dedent(
                f"""\
                #!/usr/bin/env bash
                url="${{@: -1}}"
                body=""
                while (($#)); do
                  if [[ "$1" == --data-binary ]]; then body="$2"; shift; fi
                  shift
                done
                if [[ -n "${{GUEST_ATTRIBUTES_FAIL:-}}" ]]; then
                  echo "curl: (7) Failed to connect" >&2
                  exit 7
                fi
                printf '%s %s\\n' "$url" "$body" >>{self.last_guest_attributes}
                """
            ),
            encoding="utf-8",
        )
        (stub / "curl").chmod(0o755)

        script = PUBLISHER.read_text(encoding="utf-8")
        script = script.replace("/var/lib/outrunner/drain-receipt.json", str(receipt_path))
        admission_closed_path = workspace / "admission-closed.json"
        script = script.replace(
            "/var/lib/outrunner/admission-closed.json",
            str(admission_closed_path),
        )
        self.last_admission_closed_path = admission_closed_path
        script = script.replace("/dev/ttyS0", str(serial))
        runnable = workspace / "publish"
        runnable.write_text(script, encoding="utf-8")
        runnable.chmod(0o755)

        completed = subprocess.run(
            ["bash", str(runnable)],
            capture_output=True,
            text=True,
            env={
                "PATH": f"{stub}{os.pathsep}{os.environ['PATH']}",
                "SERVICE_RESULT": "success",
                "EXIT_CODE": "exited",
                "EXIT_STATUS": "0",
                **environ,
            },
        )
        return completed, serial.read_text(encoding="utf-8")

    def test_a_valid_receipt_reaches_the_serial_console_intact(self):
        receipt = external(broker_tests.receipt_v3())
        completed, serial = self.run_publisher(receipt)

        self.assertEqual(completed.returncode, 0, completed.stderr)
        marker = MODULE.RECEIPT_PREFIX
        self.assertIn(marker, serial)
        encoded = serial[serial.index(marker) + len(marker):].strip()
        self.assertEqual(json.loads(base64.b64decode(encoded).decode()), receipt)
        self.assertIn(MODULE.ADMISSION_CLOSED_PREFIX, serial)
        self.assertTrue(self.last_admission_closed_path.exists())

    def guest_attributes(self):
        if not self.last_guest_attributes.exists():
            return {}
        written = {}
        for line in self.last_guest_attributes.read_text(encoding="utf-8").splitlines():
            url, body = line.split(" ", 1)
            written[url.rsplit("/guest-attributes/", 1)[1]] = body
        return written

    def test_a_valid_receipt_also_reaches_guest_attributes(self):
        # Serial output is unreadable once the VM is TERMINATED; guest
        # attributes are not (shutdown regression).
        receipt = external(broker_tests.receipt_v3())
        completed, serial = self.run_publisher(receipt)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        written = self.guest_attributes()
        self.assertEqual(set(written), {"also-ran/admission-closed", "also-ran/receipt"})
        self.assertEqual(
            json.loads(base64.b64decode(written["also-ran/receipt"])), receipt
        )
        acknowledgement = json.loads(
            base64.b64decode(written["also-ran/admission-closed"])
        )
        self.assertEqual(acknowledgement["status"], "admission_closed")
        # Both channels carry byte-identical payloads.
        self.assertIn(
            f"{MODULE.RECEIPT_PREFIX}{written['also-ran/receipt']}", serial
        )

    def test_a_guest_attribute_failure_never_blocks_the_serial_receipt(self):
        receipt = external(broker_tests.receipt_v3())
        completed, serial = self.run_publisher(receipt, GUEST_ATTRIBUTES_FAIL="1")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn(MODULE.RECEIPT_PREFIX, serial)
        self.assertIn(MODULE.ADMISSION_CLOSED_PREFIX, serial)
        self.assertIn("guest attribute also-ran/receipt not written", completed.stderr)

    def test_a_suppressed_receipt_writes_no_guest_attribute_receipt(self):
        rejected = external(broker_tests.receipt_v3())
        rejected["identity"]["provider"] = "azure"
        completed, _ = self.run_publisher(rejected)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(self.guest_attributes(), {})

    def test_malformed_full_receipt_still_publishes_admission_closed_ack(self):
        rejected = external(broker_tests.receipt_v3())
        rejected["reason"] = "operator"
        completed, serial = self.run_publisher(rejected)

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn(MODULE.ADMISSION_CLOSED_PREFIX, serial)
        self.assertNotIn(MODULE.RECEIPT_PREFIX, serial)
        self.assertTrue(self.last_admission_closed_path.exists())

        line = next(
            item
            for item in serial.splitlines()
            if item.startswith(MODULE.ADMISSION_CLOSED_PREFIX)
        )
        acknowledgement = json.loads(
            base64.b64decode(line[len(MODULE.ADMISSION_CLOSED_PREFIX):]).decode()
        )
        self.assertEqual(acknowledgement["status"], "admission_closed")
        self.assertEqual(
            acknowledgement["identity"]["instance_id"],
            rejected["identity"]["instance_id"],
        )

    def test_suppression_is_never_silent(self):
        # The whole reason this went unnoticed: the failure path produced no
        # journal line and no serial line, and the unit still looked healthy.
        rejected = external(broker_tests.receipt_v3())
        rejected["reason"] = "operator"
        completed, serial = self.run_publisher(rejected)

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("receipt suppressed", completed.stderr)
        self.assertIn("ALSO_RAN_DRAIN_SUPPRESSED_V1", serial)
        self.assertNotIn(MODULE.RECEIPT_PREFIX, serial)

    def test_a_receipt_field_cannot_smuggle_the_receipt_marker(self):
        # The broker finds RECEIPT_PREFIX by unanchored substring search over
        # the whole serial history, so a diagnostic echoing it would be read
        # as a garbled receipt and quarantine the slot.
        hostile = external(broker_tests.receipt_v3())
        hostile["reason"] = MODULE.RECEIPT_PREFIX + "Zm9v"
        completed, serial = self.run_publisher(hostile)

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("ALSO_RAN_DRAIN_SUPPRESSED_V1", serial)
        self.assertNotIn(MODULE.RECEIPT_PREFIX, serial)

    def test_invalid_identity_never_publishes_admission_closed(self):
        rejected = external(broker_tests.receipt_v3())
        rejected["identity"]["provider"] = "azure"

        completed, serial = self.run_publisher(rejected)

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("cannot prove identity-bound admission closure", completed.stderr)
        self.assertNotIn(MODULE.ADMISSION_CLOSED_PREFIX, serial)
        self.assertNotIn(MODULE.RECEIPT_PREFIX, serial)
        self.assertFalse(self.last_admission_closed_path.exists())

    def test_an_absent_receipt_and_a_failed_exit_are_both_reported(self):
        completed, serial = self.run_publisher(None)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("is absent", completed.stderr)
        self.assertIn("ALSO_RAN_DRAIN_SUPPRESSED_V1", serial)

        completed, serial = self.run_publisher(
            external(broker_tests.receipt_v3()),
            SERVICE_RESULT="exit-code",
            EXIT_STATUS="1",
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("unsuccessful Outrunner exit", completed.stderr)
        self.assertNotIn(MODULE.RECEIPT_PREFIX, serial)
        self.assertNotIn(MODULE.ADMISSION_CLOSED_PREFIX, serial)
        self.assertFalse(self.last_admission_closed_path.exists())

    def test_persistent_marker_prevents_outrunner_restart_after_reboot(self):
        publisher = PUBLISHER.read_text(encoding="utf-8")
        override = (
            PUBLISHER.parents[1]
            / "templates"
            / "outrunner-drain-receipt.conf.j2"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "ConditionPathExists=!/var/lib/outrunner/admission-closed.json",
            override,
        )
        self.assertIn(
            "ExecStartPre=/usr/bin/test ! -e "
            "/var/lib/outrunner/admission-closed.json",
            override,
        )
        moved = publisher.index(
            'mv "${admission_closed_tmp}" "${admission_closed_path}"'
        )
        durable = publisher.index(
            'sync --file-system "${admission_closed_path}"'
        )
        published = publisher.index(
            "printf '%s %s\\n' \"${admission_closed_prefix}\" "
            '"${admission_closed_base64}"'
        )
        self.assertLess(moved, durable)
        self.assertLess(durable, published)
        completed, _ = self.run_publisher(external(broker_tests.receipt_v3()))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertTrue(self.last_admission_closed_path.exists())
