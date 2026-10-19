# Runner images

The image-builder playbook targets only the `image_builders` inventory group.
Keep disposable builders in that group and controllers in `also_ran_controllers`.
Review the target list before running a build:

```sh
.venv/bin/ansible-playbook --list-hosts -i deployment.local/hosts.ini ansible/playbooks/runner-image-builder.yml
```

The builder playbook installs packages, builds a Docker image, removes runtime
credentials and host identity, and prepares the VM for a snapshot. Run it only
on a disposable builder. A successfully built image still needs the live
acceptance procedure in [deployment.md](deployment.md).

## Optional guest telemetry

Guest Axiom telemetry is off by default (`telemetry_enabled: false`). Disabled
images do not download or install the OpenTelemetry collector, fetch an Axiom
secret, or start collector/sampler services. Rebuilding an existing builder with
telemetry disabled stops any previous exporter and removes its ingest credential.
Runner bootstrap independently preserves local readiness evidence, drain receipts,
and journal logs; these do not require an Axiom account.

To opt in, set `telemetry_enabled: true` in your generated `image-vars.json`
(or use `configure.py --enable-telemetry`). Build a new image and accept it before
activation. The builder installs the pinned collector without credentials; guests
fetch `gcp_runner_bootstrap_axiom_secret` (default `axiom-api-token`) at first boot.
Give the **guest** service account access to that secret and provision an Axiom
ingest token with write access to the configured metrics and logs datasets. Never
put its value in inventory, image variables, the image, or the repository. Guest
telemetry sends host metrics and journal events to Axiom; review the collector
configuration before enabling it. Changing this setting requires rebuilding the
image; it is not a controller runtime toggle.

## Customize the job toolchain

The supplied job image is opinionated: Node.js, pnpm, PostgreSQL tools, Docker,
GitHub CLI, Azure CLI/Bicep, Functions tools, Playwright, and Azurite. Those tools
are historical workload choices rather than controller requirements. The project
does not currently ship a separate minimal job image.

To change the toolchain:

1. Edit `ansible/roles/outrunner/files/Dockerfile.also-ran`. Keep a pinned runner
   base digest and pinned, verified downloads. Remove unused tools and their smoke
   commands together; update `e2e-tools/package.json` and its lockfile together if
   changing the bundled Node tools.
2. Preserve the `runner` user, writable runner/tool-cache directories, and the
   `/usr/local/bin/runner-with-docker` entrypoint. The host uses Sysbox for isolated
   Docker inside job containers. Do not add the host Docker socket, host networking,
   or privileged mounts to make a tool work.
3. Change `outrunner_runner_image` in your image variables to a new immutable tag.
   Record the full source commit in `runner_image_finalize_source_commit`. The
   bootstrap and local-SSD preparation must refer to the same job image tag.
4. Update the offline image regression tests to describe your intended toolchain,
   build on a disposable VM, and run real representative jobs plus the regional
   acceptance checks before approving that image in policy.

`outrunner_runner_image` names the locally built Docker job image, while the
controller policy names the GCP golden VM image that contains it. Changing only
the tag does not substitute a remote job image: the Outrunner role always builds
its checked-in Dockerfile into that tag.
