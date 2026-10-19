#!/usr/bin/env python3
"""Run both pools even when one fails; retain an overall failed service result."""
from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--heavy-mode', choices=('apply', 'dry-run'), required=True)
    parser.add_argument('--general-mode', choices=('apply', 'dry-run'), required=True)
    parser.add_argument('--state-directory', default='/var/lib/also-ran')
    parser.add_argument('--lock-directory', default='/run/also-ran')
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--scale-up-only', action='store_true')
    mode.add_argument('--maintenance-only', action='store_true')
    args, common = parser.parse_known_args(argv)
    scale_up = args.scale_up_only
    common.append('--scale-up-only' if scale_up else '--maintenance-only')
    broker = pathlib.Path(__file__).with_name('gcp_broker.py')

    def run(pool):
        stem = 'gcp' if pool == 'heavy' else 'gcp-general'
        suffix = '-scale-up' if scale_up else ''
        command = [sys.executable, str(broker), *common, '--pool', pool,
                   '--state', f'{args.state_directory}/{stem}{suffix}-state.json',
                   '--lock', f'{args.lock_directory}/{stem}{suffix}-lock',
                   # One GitHub demand fetch shared by both pools and both loops.
                   '--demand-snapshot', f'{args.state_directory}/github-demand-snapshot.json',
                   '--' + getattr(args, pool + '_mode')]
        if not scale_up:
            command.extend(['--scale-up-state', f'{args.state_directory}/{stem}-scale-up-state.json'])
        return subprocess.run(command, check=False).returncode

    # Separate pool locks prevent starvation while the shared GitHub budget
    # still bounds aggregate API use. Both results are awaited and reported.
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(run, ('heavy', 'general')))
    return 1 if any(results) else 0


if __name__ == '__main__':
    sys.exit(main())
