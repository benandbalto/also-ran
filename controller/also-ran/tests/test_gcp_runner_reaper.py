"""Execute the guest recovery path with fake host services and metadata."""
import base64
import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock

import test_gcp_broker as broker
import test_gcp_runner_lifecycle as lifecycle

ROOT = pathlib.Path(__file__).parents[3]
REAPER = ROOT / 'ansible/roles/gcp_runner_bootstrap/templates/gcp-runner-reaper.sh.j2'


class ReaperTests(unittest.TestCase):
    def run_reaper(self, **settings):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = pathlib.Path(temp.name)
        binaries = root / 'bin'
        binaries.mkdir()
        state = root / 'outrunner'
        state.mkdir()
        if 'RECEIPT' in settings:
            (state / 'drain-receipt.json').write_text(settings.pop('RECEIPT'))
        serial = root / 'serial'
        serial.touch()
        uptime = root / 'uptime'
        uptime.write_text(str(settings.pop('UPTIME', 1000)) + '.0 1000.0\n')
        boot = root / 'boot'
        boot.write_text('12345678-1234-1234-1234-123456789abc\n')
        complete = root / 'complete'
        if settings.pop('COMPLETE', False):
            complete.touch()
        calls = root / 'calls'
        attributes = root / 'attributes'
        metadata = {
            'instance/id': '1001', 'instance/name': 'also-ran-runner-gcp-prod-01',
            'instance/zone': 'projects/123456789012/zones/us-central1-a',
            'project/project-id': 'also-ran-ci-runners',
            'instance/attributes/runner-scale-set': 'also-ran-gcp-prod-01',
            'instance/attributes/runner-slot': 'gcp-prod-01',
            'instance/attributes/runner-image-source-commit': 'a' * 12,
        }
        if settings.pop('REQUEST', True):
            metadata['instance/attributes/runner-drain-request'] = settings.pop('REQUEST_ID', '1001')
        payload = json.dumps(metadata)
        (binaries / 'curl').write_text(f'''#!/usr/bin/env python3
import json,sys
args=sys.argv[1:];url=args[-1]
if '--request' in args:
 open({str(attributes)!r},'a').write(args[args.index('--data')+1]+'\\n')
else:
 values=json.loads({payload!r});key=url.split('/computeMetadata/v1/')[1]
 if key not in values: sys.exit(22)
 print(values[key],end='')
''')
        (binaries / 'systemctl').write_text('''#!/usr/bin/env bash
printf '%s\\n' "$*" >>"$CALLS"
case "$1" in
 is-active) [[ "${RUNNER_STATE:-inactive}" == active ]] ;;
 is-failed) [[ "${BOOTSTRAP_STATE:-failed}" == failed ]] ;;
 show)
   case "$3" in
    ActiveState) if [[ "$5" == gcp-runner-bootstrap.service ]]; then echo "${BOOTSTRAP_STATE:-failed}"; else echo "${RUNNER_STATE:-inactive}"; fi ;;
    SubState) echo "${BOOTSTRAP_SUBSTATE:-failed}" ;;
    MainPID) echo "${MAIN_PID:-0}" ;;
    ExecMainStartTimestampMonotonic) echo "${STARTED:-0}" ;;
   esac ;;
 kill) exit 0 ;;
 *) exit 99 ;;
esac
''')
        (binaries / 'docker').write_text('''#!/usr/bin/env bash
[[ "${DOCKER_HANG:-0}" == 0 ]] || sleep 2
[[ "${DOCKER_FAIL:-0}" == 0 ]] || exit 1
printf '%s' "${CONTAINERS:-}"
''')
        (binaries / 'pgrep').write_text('''#!/usr/bin/env bash
if [[ "$*" == *containerd-shim* ]]; then [[ "${SHIM:-0}" == 1 ]]; else [[ "${WORKER:-0}" == 1 ]]; fi
''')
        (binaries / 'systemd-cat').write_text('#!/usr/bin/env bash\ncat >/dev/null\n')
        for path in binaries.iterdir():
            path.chmod(0o755)
        source = REAPER.read_text().replace('{{ gcp_runner_bootstrap_project_id | quote }}', 'also-ran-ci-runners').replace('/var/lib/outrunner', str(state)).replace(
            '/var/lib/gcp-runner-bootstrap/complete', str(complete)).replace(
            '/proc/uptime', str(uptime)).replace('/proc/sys/kernel/random/boot_id', str(boot)).replace(
            '/dev/ttyS0', str(serial))
        if settings.get('DOCKER_HANG'):
            source = source.replace('timeout --kill-after=5s 30s', 'timeout --kill-after=0.1s 0.1s')
        script = root / 'reaper'
        script.write_text(source)
        env = dict(os.environ, PATH=str(binaries)+':'+os.environ['PATH'], CALLS=str(calls),
                   **{k: str(v) for k,v in settings.items()})
        result = subprocess.run(['bash', str(script)], capture_output=True, text=True, env=env)
        self.last_calls = calls.read_text() if calls.exists() else ''
        self.last_marker = state / 'admission-closed.json'
        self.last_attributes = attributes.read_text() if attributes.exists() else ''
        self.last_serial = serial.read_text()
        self.last_script = script
        self.last_env = env
        return result

    def test_active_runner_only_receives_graceful_signal(self):
        result = self.run_reaper(RUNNER_STATE='active', STARTED=5, CONTAINERS='runner')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('--signal=SIGUSR1', self.last_calls)
        self.assertFalse(self.last_marker.exists())
        self.assertEqual(self.last_serial, '')
        self.assertNotIn('stop ', self.last_calls)

    def test_failed_boot_can_close_admission_without_docker_or_request(self):
        result = self.run_reaper(REQUEST=False, DOCKER_FAIL=1)
        self.assertEqual(result.returncode, 0, result.stderr)
        ack = json.loads(self.last_marker.read_text())
        broker.MODULE.validate_admission_closed(ack, broker.instance(1), broker.broker_policy(), broker.production())
        self.assertEqual(json.loads(base64.b64decode(self.last_attributes.strip())), ack)
        # Repeated ticks reuse exactly the same timestamp and identity.
        again = subprocess.run(['bash', str(self.last_script)], capture_output=True, text=True, env=self.last_env)
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertEqual(json.loads(self.last_marker.read_text()), ack)

    def test_early_boot_failure_gets_provisioning_grace(self):
        result = self.run_reaper(REQUEST=False, UPTIME=899)
        self.assertEqual(result.returncode, 0)
        self.assertFalse(self.last_marker.exists())
        self.assertEqual(self.last_serial, '')

    def test_bootstrap_still_running_cannot_publish_proof(self):
        result = self.run_reaper(BOOTSTRAP_STATE='activating', BOOTSTRAP_SUBSTATE='start')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.last_marker.exists())

    def test_existing_work_or_uncertain_runtime_cannot_publish_proof(self):
        for settings in [dict(CONTAINERS='job'), dict(WORKER=1), dict(DOCKER_FAIL=1, SHIM=1),
                         dict(DOCKER_FAIL=1, STARTED=42), dict(MAIN_PID=123), dict(RUNNER_STATE='deactivating')]:
            with self.subTest(settings=settings):
                result = self.run_reaper(**settings)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.last_serial, '')
                self.assertEqual(self.last_attributes, '')

    def test_stopped_runtime_can_close_admission_if_docker_confirms_no_work(self):
        result = self.run_reaper(COMPLETE=True, BOOTSTRAP_STATE='active', BOOTSTRAP_SUBSTATE='exited', STARTED=42)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.last_attributes)

    def test_malformed_receipt_does_not_strand_a_stopped_host(self):
        result = self.run_reaper(RECEIPT='interrupted json')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.last_attributes)

    def test_stopped_runtime_is_recovered_without_a_controller_request(self):
        result = self.run_reaper(REQUEST=False, COMPLETE=True, BOOTSTRAP_STATE='active',
                                 BOOTSTRAP_SUBSTATE='exited', STARTED=42)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.last_attributes)

    def test_healthy_active_host_without_request_is_untouched(self):
        result = self.run_reaper(REQUEST=False, COMPLETE=True, BOOTSTRAP_STATE='active',
                                 BOOTSTRAP_SUBSTATE='exited', RUNNER_STATE='active', STARTED=42)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('kill ', self.last_calls)
        self.assertFalse(self.last_marker.exists())
        self.assertEqual(self.last_serial, '')

    def test_hung_docker_is_bounded_and_only_never_started_runners_recover(self):
        result = self.run_reaper(DOCKER_HANG=1)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.last_attributes)
        result = self.run_reaper(DOCKER_HANG=1, STARTED=42)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.last_attributes, '')

    def test_wrong_generation_request_is_rejected(self):
        result = self.run_reaper(REQUEST_ID='999')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.last_marker.exists())


class RecoveryTransportTests(unittest.TestCase):
    @mock.patch.object(broker.MODULE, 'compute_api')
    def test_guest_ack_recovers_missing_or_interrupted_serial(self, compute_api):
        ack = broker.admission_closed()
        items = [{'namespace':'also-ran', 'key':'admission-closed',
                  'value':base64.b64encode(json.dumps(ack).encode()).decode()}]
        compute_api.return_value = {'queryValue': {'items': items}}
        for contents in ['', 'ALSO_RAN_ADMISSION_CLOSED_V1 broken']:
            self.assertEqual(broker.MODULE.serial_admission_closed(contents, broker.instance(1),
                              broker.broker_policy(), broker.production()), ack)
        items[0]['value'] = base64.b64encode(json.dumps(broker.admission_closed(instance_id='999')).encode()).decode()
        with self.assertRaises(broker.MODULE.ObservationError):
            broker.MODULE.serial_admission_closed('', broker.instance(1), broker.broker_policy(), broker.production())

    @mock.patch.object(broker.MODULE, 'compute_api')
    def test_serial_failure_recovers_guest_ack_with_one_transport_read(self, compute_api):
        ack = broker.admission_closed()
        compute_api.side_effect = [broker.MODULE.ObservationError('serial unavailable'),
            {'queryValue': {'items': [{'namespace':'also-ran', 'key':'admission-closed',
              'value':base64.b64encode(json.dumps(ack).encode()).decode()}]}}]
        observer = mock.Mock(known_repositories=('example-org/example-repo',))
        drained, quarantined, idle, closed = broker.MODULE.observe_drained_instances(
            (broker.instance(1),), broker.broker_policy(), broker.production(), observer)
        self.assertEqual(closed, {1})
        self.assertEqual(drained, {})
        self.assertEqual(idle, set())
        self.assertIn(1, quarantined)
        self.assertEqual(compute_api.call_count, 2)  # one serial attempt, one guest read

    @mock.patch.object(broker.MODULE, 'compute_api')
    def test_healthy_host_uses_one_guest_read_for_both_parsers(self, compute_api):
        compute_api.side_effect = [{'contents':''}, {'queryValue': {}}]
        observer = mock.Mock(known_repositories=('example-org/example-repo',))
        self.assertEqual(broker.MODULE.observe_drained_instances(
            (broker.instance(1),), broker.broker_policy(), broker.production(), observer),
            ({}, {}, set(), set()))
        self.assertEqual(compute_api.call_count, 2)

    @mock.patch.object(broker.MODULE, 'compute_api')
    def test_booting_host_without_a_serial_console_is_not_quarantined(self, compute_api):
        # A VM still being created has no serial console yet; GCP refuses the
        # read. It cannot hold a receipt, so it keeps serving instead of being
        # quarantined and drained before it boots.
        not_ready = broker.MODULE.ObservationError(
            "GCP API .../serialPort failed: HTTP 400 {\"reason\": \"resourceNotReady\", "
            "\"message\": \"The resource 'projects/p/zones/z/instances/x' is not ready\"}")
        compute_api.side_effect = [not_ready, not_ready]
        observer = mock.Mock(known_repositories=('example-org/example-repo',))
        self.assertEqual(broker.MODULE.observe_drained_instances(
            (broker.instance(1),), broker.broker_policy(), broker.production(), observer),
            ({}, {}, set(), set()))

    @mock.patch.object(broker.MODULE, 'compute_api')
    def test_other_serial_failures_still_quarantine(self, compute_api):
        compute_api.side_effect = [broker.MODULE.ObservationError('HTTP 500 backend error'),
                                   broker.MODULE.ObservationError('HTTP 404')]
        observer = mock.Mock(known_repositories=('example-org/example-repo',))
        _, quarantined, _, _ = broker.MODULE.observe_drained_instances(
            (broker.instance(1),), broker.broker_policy(), broker.production(), observer)
        self.assertEqual(quarantined, {1: 'HTTP 500 backend error'})

    def test_new_vm_startup_installs_recovery_independently_of_bootstrap(self):
        with lifecycle.MODULE.reaper_startup_file() as handle:
            source = pathlib.Path(handle.name).read_text()
            self.assertIn('ConditionPathExists=\n', source)
            self.assertIn('TimeoutStartSec=15min', source)
            self.assertIn('TimeoutStartSec=5min', source)
            self.assertIn('TimeoutStartSec=5min', REAPER.with_name('gcp-runner-reaper.service.j2').read_text())
            self.assertIn('systemctl enable --now gcp-runner-reaper.timer', source)
            # Already-baked images poll every minute; the drop-in moves them to
            # the 15s cadence the image template now ships, and re-arms it.
            self.assertIn('OnUnitActiveSec=\nOnBootSec=1min\nOnUnitActiveSec=15s\n', source)
            # The reset clears OnBootSec too; the drop-in must restate it.
            drop_in = source.split('cadence.conf <<', 1)[1].split('\nEOF\n', 1)[0]
            reset = drop_in.index('OnUnitActiveSec=\n')
            self.assertGreater(drop_in.index('OnBootSec=1min'), reset)
            self.assertIn('systemctl restart gcp-runner-reaper.timer', source)
            self.assertIn('OnUnitActiveSec=15s', REAPER.with_name('gcp-runner-reaper.timer.j2').read_text())
            result = subprocess.run(['bash', '-n', handle.name], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
