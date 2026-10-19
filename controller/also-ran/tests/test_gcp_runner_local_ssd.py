"""Execute the local SSD relocation with fake host services and a fake Docker store."""
import json
import os
import pathlib
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).parents[3]
SCRIPT = ROOT / 'ansible/roles/gcp_runner_bootstrap/files/gcp-runner-local-ssd'
JOB_IMAGE = 'also-ran-actions-runner:test'
LAYERS = ['a' * 64, 'b' * 64]

# Records mounts as "target<TAB>source<TAB>options" lines, so findmnt can answer.
MOUNT = r'''#!/usr/bin/env python3
import os, sys
args = sys.argv[1:]
table = os.environ['MOUNTS']
log = open(os.environ['CALLS'], 'a'); log.write('mount ' + ' '.join(args) + '\n'); log.close()
rows = [line.rstrip('\n').split('\t') for line in open(table)] if os.path.exists(table) else []
if '--bind' in args:
    if os.environ.get('BIND_FAIL') == '1' and '/overlay2/' in args[-1]:
        sys.exit(32)
    source, target = args[-2], args[-1]
    on_ssd = source.startswith(os.environ['SSD'] + '/')
    rows.append([target, os.environ['DEVICE'] + '[/sysbox]' if on_ssd else source, 'rw'])
elif '-o' in args and args[args.index('-o') + 1].startswith('remount'):
    target = args[-1]
    ro = os.environ.get('REMOUNT_IGNORED') != '1'
    rows = [[t, s, 'ro' if t == target and ro else o] for t, s, o in rows]
else:
    rows.append([args[-1], os.environ['DEVICE'], 'rw,discard'])
open(table, 'w').write(''.join('\t'.join(row) + '\n' for row in rows))
'''

UMOUNT = r'''#!/usr/bin/env python3
import os, sys
target = sys.argv[-1]
open(os.environ['CALLS'], 'a').write('umount ' + target + '\n')
if os.environ.get('UMOUNT_FAIL') == '1':
    sys.exit(32)
table = os.environ['MOUNTS']
rows = [line for line in open(table) if line.split('\t')[0] != target]
open(table, 'w').write(''.join(rows))
'''

FINDMNT = r'''#!/usr/bin/env python3
import os, sys
args = sys.argv[1:]
rows = [line.rstrip('\n').split('\t') for line in open(os.environ['MOUNTS'])] \
    if os.path.exists(os.environ['MOUNTS']) else []
output = args[args.index('--output') + 1]
if '--list' in args:
    print('\n'.join(['/'] + [t for t, _, _ in rows]))
elif '--mountpoint' in args:
    target = args[args.index('--mountpoint') + 1]
    match = [o for t, _, o in rows if t == target]
    if not match:
        sys.exit(1)
    print(match[-1])
else:
    target = args[args.index('--target') + 1]
    best = max((r for r in rows if target == r[0] or target.startswith(r[0] + '/')),
               key=lambda r: len(r[0]), default=None)
    print(best[1] if best else '/dev/sda1')
'''

SYSTEMCTL = r'''#!/usr/bin/env bash
printf 'systemctl %s\n' "$*" >>"$CALLS"
if [[ "$1" == start && "$2" == docker.service ]]; then
  count=$(( $(cat "$STARTS" 2>/dev/null || echo 0) + 1 )); echo "$count" >"$STARTS"
  [[ "${DOCKER_START_FAIL:-0}" != "$count" ]] || exit 1
fi
exit 0
'''

DOCKER = r'''#!/usr/bin/env bash
printf 'docker %s\n' "$*" >>"$CALLS"
case "$1" in
  info)
    if [[ "$3" == *DockerRootDir* ]]; then
      root="$(jq -r '."data-root"' "$DAEMON_JSON")"
      [[ "${DOCKER_ROOT_WRONG:-0}" == 0 ]] || root=/var/lib/docker
      printf '%s\n' "$root"
    else
      printf '%s\n' "${DOCKER_DRIVER:-overlay2}"
    fi ;;
  run) exit "${SMOKE_STATUS:-0}" ;;
  *) exit 99 ;;
esac
'''

# Real install(1) without the ownership flags an unprivileged test cannot honour.
INSTALL = r'''#!/usr/bin/env bash
args=()
while (( $# )); do
  case "$1" in -o|-g) shift 2 ;; *) args+=("$1"); shift ;; esac
done
exec /usr/bin/install "${args[@]}"
'''


class LocalSsdTests(unittest.TestCase):
    def run_script(self, daemon=None, layout=None, **settings):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = pathlib.Path(temp.name)
        binaries = root / 'bin'
        binaries.mkdir()
        device = root / 'nvme-ssd'
        device.touch()
        ssd = root / 'titanium'
        source = root / 'var-lib-docker'
        sysbox = root / 'var-lib-sysbox'
        sysbox.mkdir()
        (sysbox / 'state').write_text('sysbox')
        daemon_json = root / 'daemon.json'
        daemon_json.write_text(json.dumps(daemon if daemon is not None else {
            'features': {'containerd-snapshotter': False},
            'storage-driver': 'overlay2',
            'default-runtime': 'sysbox-runc',
        }))

        # A minimal overlay2 store: two image layers, the l/ short links, layerdb.
        (source / 'overlay2' / 'l').mkdir(parents=True)
        (source / 'containers').mkdir()
        for index, layer in enumerate(LAYERS):
            layer_dir = source / 'overlay2' / layer
            (layer_dir / 'diff' / 'usr' / 'bin').mkdir(parents=True)
            (layer_dir / 'diff' / 'usr' / 'bin' / f'tool{index}').write_text('binary')
            (layer_dir / 'link').write_text(f'SHORT{index}')
            (source / 'overlay2' / 'l' / f'SHORT{index}').symlink_to(f'../{layer}/diff')
            chain = source / 'image' / 'overlay2' / 'layerdb' / 'sha256' / (str(index) * 64)
            chain.mkdir(parents=True)
            (chain / 'cache-id').write_text(layer)
        (source / 'image' / 'overlay2' / 'repositories.json').write_text('{}')
        if layout:
            layout(source)

        calls = root / 'calls'
        mounts = root / 'mounts'
        for name, body in {
            'mount': MOUNT, 'umount': UMOUNT, 'findmnt': FINDMNT, 'systemctl': SYSTEMCTL,
            'docker': DOCKER, 'install': INSTALL,
            'blkid': '#!/usr/bin/env bash\nexit 2\n',
            'mkfs.ext4': '#!/usr/bin/env bash\nprintf "mkfs.ext4 %s\\n" "$*" >>"$CALLS"\n',
            'mountpoint': '#!/usr/bin/env bash\n'
                          '[[ -f "$MOUNTS" ]] && awk -F "\\t" -v t="${@: -1}" '
                          '\'$1 == t { found = 1 } END { exit !found }\' "$MOUNTS"\n',
            'sysctl': '#!/usr/bin/env bash\nprintf "sysctl %s\\n" "$*" >>"$CALLS"\n',
            'timeout': '#!/usr/bin/env bash\nshift 2\nexec "$@"\n',
        }.items():
            path = binaries / name
            path.write_text(body)
            path.chmod(0o755)
        script_source = SCRIPT.read_text()
        for real, fake in {
            '/dev/disk/by-id/google-local-nvme-ssd-0': device,
            '/mnt/disks/titanium': ssd,
            '/var/lib/docker': source,
            '/var/lib/sysbox': sysbox,
            '/etc/docker/daemon.json': daemon_json,
        }.items():
            self.assertIn(real, script_source)
            script_source = script_source.replace(real, str(fake))
        # The fake device is a regular file, not a block device.
        script_source = script_source.replace('-b "${local_ssd_device}"', '-e "${local_ssd_device}"')
        script = root / 'gcp-runner-local-ssd'
        script.write_text(script_source)
        env = dict(os.environ, PATH=f'{binaries}:{os.environ["PATH"]}', CALLS=str(calls),
                   MOUNTS=str(mounts), DEVICE=str(device), SSD=str(ssd), DAEMON_JSON=str(daemon_json),
                   STARTS=str(root / 'starts'), **{k: str(v) for k, v in settings.items()})
        result = subprocess.run(['bash', str(script), JOB_IMAGE], capture_output=True,
                                text=True, env=env, timeout=60)
        self.source = source
        self.docker_root = ssd / 'docker'
        self.marker = ssd / '.also-ran-ci-local-ssd'
        self.calls = calls.read_text() if calls.exists() else ''
        self.mounts = [line.split('\t') for line in mounts.read_text().splitlines()] \
            if mounts.exists() else []
        self.daemon = json.loads(daemon_json.read_text())
        return result

    def layer_binds(self):
        prefix = f'{self.docker_root}/overlay2/'
        return {target: (src, opts) for target, src, opts in self.mounts if target.startswith(prefix)}

    def assert_full_copy(self, result):
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.marker.read_text(), 'docker_store=full-copy\n')
        self.assertEqual(self.layer_binds(), {})
        for index, layer in enumerate(LAYERS):
            copied = self.docker_root / 'overlay2' / layer / 'diff' / 'usr' / 'bin' / f'tool{index}'
            self.assertEqual(copied.read_text(), 'binary')
        self.assertIn('docker_store=full-copy seconds=', result.stdout)

    def test_lazy_lowers_copies_metadata_and_binds_each_layer_read_only(self):
        result = self.run_script()

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertRegex(result.stdout, r'docker_store=lazy-lowers layers=2 seconds=[0-9.]+')
        self.assertEqual(self.marker.read_text(), 'docker_store=lazy-lowers\n')
        self.assertEqual(self.daemon['data-root'], str(self.docker_root))
        binds = self.layer_binds()
        self.assertEqual(binds, {
            f'{self.docker_root}/overlay2/{layer}/diff': (f'{self.source}/overlay2/{layer}/diff', 'ro')
            for layer in LAYERS
        })
        for index, layer in enumerate(LAYERS):
            # Layer content is not copied; the mount point is an empty directory.
            self.assertEqual(list((self.docker_root / 'overlay2' / layer / 'diff').iterdir()), [])
            self.assertEqual((self.docker_root / 'overlay2' / layer / 'link').read_text(), f'SHORT{index}')
            self.assertEqual(
                os.readlink(self.docker_root / 'overlay2' / 'l' / f'SHORT{index}'), f'../{layer}/diff')
        self.assertTrue((self.docker_root / 'image/overlay2/repositories.json').exists())
        self.assertIn(f'docker run --rm --pull=never --network=none --entrypoint /bin/true {JOB_IMAGE}',
                      self.calls)
        self.assertNotIn('umount', self.calls)
        # The fast-format and early write-back settings apply to both paths.
        self.assertIn('-E nodiscard,assume_storage_prezeroed=1', self.calls)
        self.assertIn('sysctl --quiet --write vm.dirty_background_bytes=67108864', self.calls)
        self.assertLess(self.calls.index('mount --bind'), self.calls.index('start docker.service'))

    def test_failed_smoke_run_unmounts_layers_and_falls_back_to_full_copy(self):
        result = self.run_script(SMOKE_STATUS=125)

        self.assert_full_copy(result)
        self.assertIn('abandoned reason=job image smoke run failed', result.stdout)
        for layer in LAYERS:
            self.assertIn(f'umount {self.docker_root}/overlay2/{layer}/diff', self.calls)
        # The boot-disk store survives the discarded attempt.
        self.assertEqual((self.source / 'overlay2' / LAYERS[0] / 'diff/usr/bin/tool0').read_text(), 'binary')
        self.assertEqual(self.calls.count('start docker.service'), 2)

    def test_bind_mount_failure_falls_back(self):
        result = self.run_script(BIND_FAIL=1)

        self.assert_full_copy(result)
        self.assertIn('abandoned reason=mount --bind', result.stdout)
        self.assertEqual(self.calls.count('start docker.service'), 1)

    def test_bind_that_stays_writable_falls_back(self):
        result = self.run_script(REMOUNT_IGNORED=1)

        self.assert_full_copy(result)
        self.assertIn('is not mounted read-only', result.stdout)

    def test_unhealthy_docker_after_restart_falls_back(self):
        for settings, reason in [
            ({'DOCKER_START_FAIL': 1}, 'Docker or Sysbox did not start'),
            ({'DOCKER_DRIVER': 'vfs'}, 'Docker is not using overlay2'),
        ]:
            with self.subTest(reason=reason):
                result = self.run_script(**settings)
                self.assert_full_copy(result)
                self.assertIn(f'abandoned reason={reason}', result.stdout)

    def test_unexpected_store_layout_falls_back_before_mounting(self):
        def extra_layer(source):
            (source / 'overlay2' / ('c' * 64) / 'diff').mkdir(parents=True)

        def container(source):
            (source / 'containers' / 'abc').mkdir()

        def missing_diff(source):
            import shutil
            shutil.rmtree(source / 'overlay2' / LAYERS[1] / 'diff')
            (source / 'overlay2' / 'l' / 'SHORT1').unlink()

        def no_layerdb(source):
            import shutil
            shutil.rmtree(source / 'image')

        cases = [
            ({}, None, 'storage-driver is \'\', not overlay2'),
            ({'storage-driver': 'overlay2'}, None, 'containerd-snapshotter is not disabled'),
            (None, extra_layer, 'is not an image layer'),
            (None, container, 'baked store holds containers'),
            (None, missing_diff, 'has no diff directory'),
            (None, no_layerdb, 'unexpected Docker store layout'),
        ]
        for daemon, layout, reason in cases:
            with self.subTest(reason=reason):
                result = self.run_script(daemon=daemon, layout=layout)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.marker.read_text(), 'docker_store=full-copy\n')
                self.assertIn(reason, result.stdout)
                self.assertNotIn('mount --bind ' + str(self.source), self.calls)
                self.assertEqual(self.calls.count('start docker.service'), 1)

    def test_fallback_refuses_to_delete_while_a_layer_mount_remains(self):
        result = self.run_script(SMOKE_STATUS=1, UMOUNT_FAIL=1)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn('mounts remain under', result.stderr)
        self.assertFalse(self.marker.exists())
        self.assertEqual((self.source / 'overlay2' / LAYERS[0] / 'diff/usr/bin/tool0').read_text(), 'binary')

    def test_full_copy_still_fails_the_boot_when_its_own_checks_fail(self):
        result = self.run_script(SMOKE_STATUS=1, DOCKER_ROOT_WRONG=1)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Docker data root did not move to Titanium SSD', result.stderr)
        self.assertFalse(self.marker.exists())

    def test_sysbox_state_moves_to_the_ssd_before_docker_starts(self):
        result = self.run_script()

        self.assertEqual(result.returncode, 0, result.stderr)
        sysbox_mount = [m for m in self.mounts if m[0].endswith('var-lib-sysbox')]
        self.assertEqual(len(sysbox_mount), 1)
        self.assertLess(self.calls.index('var-lib-sysbox'), self.calls.index('start sysbox.service'))


class BootstrapWiringTests(unittest.TestCase):
    def test_bootstrap_delegates_local_ssd_with_the_job_image(self):
        bootstrap = (ROOT / 'ansible/roles/gcp_runner_bootstrap/templates/gcp-runner-bootstrap.sh.j2').read_text()
        tasks = (ROOT / 'ansible/roles/gcp_runner_bootstrap/tasks/main.yml').read_text()

        self.assertIn('/usr/local/sbin/gcp-runner-local-ssd "{{ outrunner_runner_image }}"', bootstrap)
        self.assertNotIn('cp -a /var/lib/docker', bootstrap)
        self.assertIn('src: gcp-runner-local-ssd', tasks)
        self.assertIn('dest: /usr/local/sbin/gcp-runner-local-ssd', tasks)


if __name__ == '__main__':
    unittest.main()
