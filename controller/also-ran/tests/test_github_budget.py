import importlib.util
import json
import os
import pathlib
import tempfile
import threading
import time
import unittest
import urllib.error
from unittest import mock

from also_ran import JsonClient, ObservationError


class BudgetTests(unittest.TestCase):
    def test_low_allowance_blocks_other_process_client_without_another_request(self):
        with tempfile.TemporaryDirectory() as root:
            path = str(pathlib.Path(root) / 'budget.json')
            with mock.patch.dict(os.environ, ALSO_RAN_GITHUB_BUDGET=path):
                client = JsonClient('never-persist-this', 'https://api.github.com')
                def response(*args):
                    client.last_headers = {'x-ratelimit-remaining': '749', 'x-ratelimit-reset': '9999999999'}
                    return {'ok': True}
                with mock.patch.object(client, '_get', side_effect=response):
                    self.assertEqual(client.get('/first'), {'ok': True})
                peer = JsonClient('never-persist-this', 'https://api.github.com')
                with mock.patch.object(peer, '_get') as request:
                    with self.assertRaisesRegex(ObservationError, 'paused'):
                        peer.get('/second')
                    request.assert_not_called()
                self.assertNotIn('never-persist-this', pathlib.Path(path).read_text())

    def test_malformed_state_fails_closed_without_spending_quota(self):
        for state in [[], {"blocked_until": None}, {"next_request_at": "NaN"}]:
            with self.subTest(state=state), tempfile.TemporaryDirectory() as root:
                path = str(pathlib.Path(root) / 'budget.json')
                pathlib.Path(path).write_text(json.dumps(state))
                with mock.patch.dict(os.environ, ALSO_RAN_GITHUB_BUDGET=path):
                    client = JsonClient('token', 'https://api.github.com')
                    with mock.patch.object(client, '_get') as request:
                        with self.assertRaisesRegex(ObservationError, 'malformed'):
                            client.get('/blocked')
                        request.assert_not_called()

    def test_network_failure_backs_off_other_clients(self):
        with tempfile.TemporaryDirectory() as root:
            path = str(pathlib.Path(root) / 'budget.json')
            with mock.patch.dict(os.environ, ALSO_RAN_GITHUB_BUDGET=path):
                client = JsonClient('token', 'https://api.github.com')
                with mock.patch('urllib.request.urlopen', side_effect=urllib.error.URLError('DNS failed')) as request:
                    with self.assertRaisesRegex(ObservationError, 'DNS failed'):
                        client.get('/first')
                    with self.assertRaisesRegex(ObservationError, 'paused'):
                        JsonClient('token', 'https://api.github.com').get('/second')
                    request.assert_called_once()

    def test_reset_resumes_requests(self):
        with tempfile.TemporaryDirectory() as root:
            path = str(pathlib.Path(root) / 'budget.json')
            pathlib.Path(path).write_text(json.dumps({'blocked_until': 1}))
            with mock.patch.dict(os.environ, ALSO_RAN_GITHUB_BUDGET=path):
                client = JsonClient('token', 'https://api.github.com')
                with mock.patch.object(client, '_get', return_value={}) as request:
                    client.get('/resumed')
                    request.assert_called_once()

    def test_priority_read_passes_normal_reads_holding_every_other_slot(self):
        with tempfile.TemporaryDirectory() as root:
            path = str(pathlib.Path(root) / 'budget.json')
            with mock.patch.dict(os.environ, ALSO_RAN_GITHUB_BUDGET=path,
                                 ALSO_RAN_GITHUB_CONCURRENCY='2'):
                release = threading.Event()
                started = threading.Event()
                order = []
                def slow(path, query):
                    order.append(path)
                    if path == '/receipt':
                        started.set()
                        release.wait(5)
                    return {}
                holder = JsonClient('token', 'https://api.github.com')
                waiter = JsonClient('token', 'https://api.github.com')
                demand = JsonClient('token', 'https://api.github.com')
                for client in (holder, waiter, demand):
                    client._get = slow
                first = threading.Thread(target=holder.get, args=('/receipt',))
                first.start()
                self.assertTrue(started.wait(5))
                second = threading.Thread(target=waiter.get, args=('/telemetry',))
                second.start()
                time.sleep(0.2)
                with demand.prioritized():
                    demand.get('/demand')
                self.assertEqual(order, ['/receipt', '/demand'])
                release.set()
                first.join(5)
                second.join(5)
                self.assertEqual(order, ['/receipt', '/demand', '/telemetry'])

    def test_requests_are_spaced_only_when_allowance_is_low(self):
        for remaining, spaced in (('4000', False), ('1500', True)):
            with self.subTest(remaining=remaining), tempfile.TemporaryDirectory() as root:
                path = str(pathlib.Path(root) / 'budget.json')
                pathlib.Path(path).write_text(json.dumps({
                    'blocked_until': 0, 'next_request_at': time.time() + 1,
                    'remaining': remaining}))
                with mock.patch.dict(os.environ, ALSO_RAN_GITHUB_BUDGET=path):
                    client = JsonClient('token', 'https://api.github.com')
                    def response(*args):
                        client.last_headers = {'x-ratelimit-remaining': remaining}
                        return {}
                    with mock.patch.object(client, '_get', side_effect=response), \
                            mock.patch('time.sleep') as sleep:
                        client.get('/first')
                    self.assertEqual(sleep.called, spaced)

    def test_concurrent_success_keeps_another_reads_backoff(self):
        with tempfile.TemporaryDirectory() as root:
            path = str(pathlib.Path(root) / 'budget.json')
            with mock.patch.dict(os.environ, ALSO_RAN_GITHUB_BUDGET=path):
                client = JsonClient('token', 'https://api.github.com')
                def response(*args):
                    # Another process opened a backoff while this read was in flight.
                    pathlib.Path(path).write_text(json.dumps({
                        'blocked_until': time.time() + 60, 'next_request_at': 0}))
                    client.last_headers = {'x-ratelimit-remaining': '4000'}
                    return {}
                with mock.patch.object(client, '_get', side_effect=response):
                    client.get('/first')
                with self.assertRaisesRegex(ObservationError, 'paused'):
                    JsonClient('token', 'https://api.github.com').get('/second')

    def test_failed_pool_does_not_skip_other_pool(self):
        source = pathlib.Path(__file__).parents[1] / 'gcp_pools.py'
        spec = importlib.util.spec_from_file_location('gcp_pools', source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        def result(command, **kwargs):
            return mock.Mock(returncode=1 if command[command.index('--pool')+1] == 'heavy' else 0)
        with mock.patch.object(module.subprocess, 'run', side_effect=result) as run:
            self.assertEqual(module.main(['--heavy-mode', 'apply', '--general-mode', 'dry-run', '--once', '--scale-up-only']), 1)
            self.assertEqual(run.call_count, 2)
            locks = [call.args[0][call.args[0].index('--lock')+1] for call in run.call_args_list]
            self.assertEqual(len(set(locks)), 2)
            snapshots = {call.args[0][call.args[0].index('--demand-snapshot')+1] for call in run.call_args_list}
            self.assertEqual(snapshots, {'/var/lib/also-ran/github-demand-snapshot.json'})
