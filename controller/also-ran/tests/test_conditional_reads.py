import http.client
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

from also_ran import JsonClient, ObservationError


class Response(io.BytesIO):
    status = 200

    def __init__(self, payload, etag='"v1"'):
        super().__init__(json.dumps(payload).encode())
        self.headers = {'etag': etag, 'x-ratelimit-remaining': '4000'}


def unchanged():
    return urllib.error.HTTPError('https://api.github.com/jobs', 304, 'unchanged',
                                  {'x-ratelimit-remaining': '4000'}, io.BytesIO())


class ConditionalTests(unittest.TestCase):
    def test_revalidate_across_clients_and_replace_changed_representation(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(
                os.environ, ALSO_RAN_GITHUB_BUDGET=root + '/budget.json'), mock.patch('time.sleep'):
            replies = [Response({'state': 'queued'}), unchanged(),
                       Response({'state': 'completed'}, '"v2"'), unchanged()]
            with mock.patch('urllib.request.urlopen', side_effect=replies) as request:
                values = [JsonClient('private-token', 'https://api.github.com').get('/jobs')
                          for _ in range(4)]
            self.assertEqual(values, [{'state': 'queued'}, {'state': 'queued'},
                                      {'state': 'completed'}, {'state': 'completed'}])
            self.assertIsNone(request.call_args_list[0].args[0].get_header('If-none-match'))
            self.assertEqual(request.call_args_list[1].args[0].get_header('If-none-match'), '"v1"')
            self.assertEqual(request.call_args_list[3].args[0].get_header('If-none-match'), '"v2"')
            for file in pathlib.Path(root).rglob('*'):
                if file.is_file():
                    self.assertNotIn('private-token', file.read_text())

    def test_entry_pruned_before_the_locked_reread_sends_no_validator(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(
                os.environ, ALSO_RAN_GITHUB_BUDGET=root + '/budget.json'), mock.patch('time.sleep'):
            client = JsonClient('token', 'https://api.github.com')
            with mock.patch('urllib.request.urlopen', return_value=Response({'state': 'queued'})):
                client.get('/jobs')
            real = client._conditional_cache
            calls = []

            def pruned_on_reread(url):
                calls.append(url)
                path, cached = real(url)
                return (path, cached) if len(calls) == 1 else (path, None)

            with mock.patch.object(client, '_conditional_cache', side_effect=pruned_on_reread), \
                    mock.patch('urllib.request.urlopen',
                               return_value=Response({'state': 'completed'}, '"v2"')) as request:
                self.assertEqual(client.get('/jobs'), {'state': 'completed'})
            self.assertEqual(len(calls), 2)
            self.assertIsNone(request.call_args.args[0].get_header('If-none-match'))

    def test_network_failure_never_returns_previously_cached_proof(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(
                os.environ, ALSO_RAN_GITHUB_BUDGET=root + '/budget.json'), mock.patch('time.sleep'):
            client = JsonClient('token', 'https://api.github.com')
            with mock.patch('urllib.request.urlopen', return_value=Response({'state': 'idle'})):
                client.get('/runners')
            with mock.patch('urllib.request.urlopen', side_effect=urllib.error.URLError('offline')):
                with self.assertRaises(ObservationError):
                    client.get('/runners')

    def test_truncated_body_is_retried_once(self):
        class Truncated(Response):
            def read(self, *args):
                raise http.client.IncompleteRead(b'{"part', 40)
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(
                os.environ, ALSO_RAN_GITHUB_BUDGET=root + '/budget.json'), mock.patch('time.sleep'):
            replies = [Truncated({}), Response({'state': 'idle'})]
            with mock.patch('urllib.request.urlopen', side_effect=replies) as request:
                value = JsonClient('token', 'https://api.github.com').get('/runners')
            self.assertEqual(value, {'state': 'idle'})
            self.assertEqual(request.call_count, 2)
            budget = json.loads(pathlib.Path(root, 'budget.json').read_text())
            self.assertEqual(budget['blocked_until'], 0.0)

    def test_repeated_truncation_is_an_observation_error_with_backoff(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(
                os.environ, ALSO_RAN_GITHUB_BUDGET=root + '/budget.json'), mock.patch('time.sleep'):
            failures = [ConnectionResetError('reset'), http.client.IncompleteRead(b'', 10)]
            with mock.patch('urllib.request.urlopen', side_effect=failures) as request:
                with self.assertRaises(ObservationError):
                    JsonClient('token', 'https://api.github.com').get('/runners')
            self.assertEqual(request.call_count, 2)
            budget = json.loads(pathlib.Path(root, 'budget.json').read_text())
            self.assertGreater(budget['blocked_until'], 0)

    def test_timeout_is_not_retried(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(
                os.environ, ALSO_RAN_GITHUB_BUDGET=root + '/budget.json'), mock.patch('time.sleep'):
            with mock.patch('urllib.request.urlopen', side_effect=TimeoutError('slow')) as request:
                with self.assertRaises(ObservationError):
                    JsonClient('token', 'https://api.github.com').get('/runners')
            self.assertEqual(request.call_count, 1)

    def test_credentials_and_queries_have_distinct_cache_entries(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(
                os.environ, ALSO_RAN_GITHUB_BUDGET=root + '/budget.json'), mock.patch('time.sleep'):
            with mock.patch('urllib.request.urlopen', side_effect=lambda *a, **k: Response({})) as request:
                JsonClient('one', 'https://api.github.com').get('/jobs', {'filter': 'latest'})
                JsonClient('two', 'https://api.github.com').get('/jobs', {'filter': 'latest'})
                JsonClient('one', 'https://api.github.com').get('/jobs', {'filter': 'all'})
            self.assertTrue(all(call.args[0].get_header('If-none-match') is None
                                for call in request.call_args_list))

    def test_four_processes_revalidate_one_shared_representation(self):
        statuses = []
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                status = 304 if self.headers.get('If-None-Match') == '"same"' else 200
                statuses.append(status)
                self.send_response(status)
                self.send_header('ETag', '"same"')
                self.send_header('X-RateLimit-Remaining', '4000')
                self.end_headers()
                if status == 200:
                    self.wfile.write(b'{"safe":true}')
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as root:
                source = pathlib.Path(__file__).parents[1]
                code = '''
import sys, urllib.request
from also_ran import JsonClient
original = urllib.request.urlopen
def local(request, **kwargs):
    request.full_url = sys.argv[1] + '/jobs'
    return original(request, **kwargs)
urllib.request.urlopen = local
assert JsonClient('test-only-token', 'https://api.github.com').get('/jobs') == {'safe': True}
'''
                env = {**os.environ, 'PYTHONPATH': str(source),
                       'ALSO_RAN_GITHUB_BUDGET': root + '/budget.json'}
                children = [subprocess.Popen([sys.executable, '-c', code,
                            f'http://127.0.0.1:{server.server_port}'], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(4)]
                try:
                    for child in children:
                        out, err = child.communicate(timeout=10)
                        self.assertEqual(child.returncode, 0, (out, err))
                finally:
                    for child in children:
                        if child.poll() is None:
                            child.kill()
                            child.wait()
                self.assertEqual(statuses.count(200), 1)
                self.assertEqual(statuses.count(304), 3)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
