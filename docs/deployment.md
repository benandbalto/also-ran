# Deploying also-ran

This is an operator-run bootstrap, not a one-command cloud installer. The public
configuration path has offline regression coverage; a fresh-project cloud
acceptance is still needed. Cloud mutations in the examples require explicit
`--apply` or an Ansible run without `--check`.

## Prerequisites

Use a dedicated GCP runner project with billing enabled, Compute Engine, Secret
Manager, Cloud Billing Catalog, and Cloud Quotas API access. The controller
needs Linux/systemd, Python 3.12, OpenSSL, and the Google Cloud CLI installed in
the system service's PATH. The reference runner image is Ubuntu 24.04 x86-64.

Generate configuration using the README, and review all four generated files:

| File | Purpose |
| --- | --- |
| `deployment.json` | Trusted project identity, resource prefix, and fleet |
| `gcp-policy.json` | Images, IAM identities, network, pools, regions, budgets |
| `image-vars.json` | Pinned image-build dependencies and bootstrap identity |
| `controller-vars.json` | Source paths, credential paths, and activation switches |

The policy under `controller/also-ran/gcp-acceptance-policy.json` is a synthetic
regression fixture, not a ready-to-use deployment. Generated image names encode
the requested source commit; generating a name does not build that image.

## Cloud and GitHub identities

Provision the four service accounts listed in `policy.identities` with separate
responsibilities. The controller needs observation and narrowly scoped runner
instance creation, metadata mutation, and deletion, plus permission to attach
the runtime account. The builder account is for image construction. The runtime
account needs access only to its named secrets and guest operations; jobs must
not receive controller credentials. The observer identity is read-only.

The lifecycle helper can also create network foundations and build images.
Use a separate operator identity for these commands; the steady-state
controller does not need to administer IAM, networks, or billing. See
[IAM permissions by account](iam.md) for the operation-derived permission lists and scope boundaries. This project does not apply IAM grants for you.
Review and provision them under your organizational policy.

For controller GitHub reads, create and install a GitHub App on the selected
organization/repositories. Grant repository **Actions: read** and
**Administration: read**, and organization **Self-hosted runners: read**.
Set its app ID, installation ID, and external private-key path in
`controller-vars.json`. The systemd token minter refreshes the installation
token. A separately supplied token file is also supported.

Outrunner needs its own runner-management identity, stored in the GCP Secret
Manager secret named by `gcp_runner_bootstrap_github_secret`. Follow the pinned
[Outrunner documentation](https://github.com/ArborealMgmt/gha-outrunner) for
the credential format and runner-management permissions. It is separate from
the controller's read-only App. Guest telemetry is off by default. If you explicitly enable
`telemetry_enabled` when building the image, bootstrap also reads
`gcp_runner_bootstrap_axiom_secret`; supply an Axiom ingest credential
with access to your chosen datasets. See [runner images](runner-image.md). No secrets belong in Git, image build
variables, instance metadata, or the golden image.

## Workflow trust and runner groups

Organization registration is not a workflow trust policy. Restrict the runner
group to the repositories and workflows that should reach this fleet. Repository
allowlisting in the controller limits observed demand; it does not prevent other
workflows from selecting an organization runner. Do not run untrusted fork pull
requests on privileged self-hosted runners. Review GitHub workflow permissions,
secret access and runner-group settings before making a repository public.
Containers do not provide a security boundary for hostile multitenancy. Keep the
controller and its metadata identity on a separate host from runner jobs.

## Network and regional foundations

Provision the custom VPC named by `policy.network.name`, its initial
`us-central1` subnet, and the exact SSH/IAP firewall rules named by the policy.
Limit SSH to your reviewed source CIDRs and Google's IAP forwarding range.
The generated policy selects private NAT egress. Provision Cloud Router/NAT and
the regional subnets named in `market.regions[].foundation`; CIDRs must not
overlap. Ensure the image builder has the outbound access needed to download
its pinned packages.

`scripts/gcp-region-foundation.py` can inspect and plan the supported additional
regions once the shared VPC and SSH rules exist:

```bash
export ALSO_RAN_DEPLOYMENT="$PWD/deployment.local/deployment.json"
python3 scripts/gcp-region-foundation.py \
  --policy deployment.local/gcp-policy.json \
  --region northamerica-northeast1 --region europe-west9 --region us-south1
```

Adding `--apply` creates the planned regional resources. Read and review that
plan before applying it. The helper does not create the shared VPC or IAM.
The initial private-egress helper also requires your policy explicitly:

```bash
python3 scripts/gcp-runner-lifecycle.py \
  --policy deployment.local/gcp-policy.json foundation-private-egress --apply
```

This mutates the initial private-egress foundation defined by the policy.

Direct public-IP egress is an advanced opt-in. It requires the exact reviewed
ingress allowlist and a live firewall-posture check. Do not select it merely to
bypass missing NAT or subnet setup.

## Build the runner image

Use the same checked-out Git commit that you supplied to `configure.py`.
Install the hash-locked development requirements, then create the builder:

```bash
export ALSO_RAN_DEPLOYMENT="$PWD/deployment.local/deployment.json"
python3 scripts/gcp-runner-lifecycle.py \
  --policy deployment.local/gcp-policy.json \
  --ssh-key-file "$HOME/.ssh/id_ed25519.pub" create-builder --apply
```

Set the builder's reachable SSH address under `[image_builders]` in your own
inventory. The playbook targets only that group; never include controllers or
live runners in it. The helper
registers its initial SSH key for `runner-admin`. Build with the generated
image variables:

```bash
ansible-playbook --check --diff -i your-builder-inventory.ini \
  ansible/playbooks/runner-image-builder.yml -e @deployment.local/image-vars.json
ansible-playbook -i your-builder-inventory.ini \
  ansible/playbooks/runner-image-builder.yml -e @deployment.local/image-vars.json
```

First-install check mode may be unable to inspect packages or users that do not
exist yet. Investigate those failures; a syntax check alone does not prove
that an image built successfully. The image playbook intentionally removes
credentials, cloud-init state, SSH keys, and transient runtime data during its
finalization stage. Keep Outrunner disabled on the builder.

Stop the builder, capture the named image, and delete the owned builder when
capture is verified:

```bash
python3 scripts/gcp-runner-lifecycle.py --policy deployment.local/gcp-policy.json stop-builder --apply
python3 scripts/gcp-runner-lifecycle.py --policy deployment.local/gcp-policy.json capture-image --apply
python3 scripts/gcp-runner-lifecycle.py --policy deployment.local/gcp-policy.json delete-builder --apply
```

## Acceptance and activation

Generated configurations mark every region `canary_valid: false` and disable
the broker. The first release requires successful canaries for
`northamerica-northeast1`, `europe-west9`, and `us-south1` before production
validation succeeds. Inspect regional C4, global CPU, Hyperdisk, and local-SSD
quotas; a valid local policy does not establish quota or availability.

Use `create-acceptance` and the regional-canary subcommands of the lifecycle
helper to create held test hosts from the built image. Each mutation requires
`--apply`; regional-canary deletion also requires its expected numeric instance
ID. Bootstrap commands allow unapproved production regions; production creation
does not. In every production region, verify cold boot, local-SSD Docker storage,
online organization runners, a completed isolated job, admission closure, drain
receipt publication, identity checks, and teardown. Verify interruption/recovery
behavior before relying on Spot capacity for important jobs.

Record that evidence outside the example fixture, then explicitly approve only
the verified regions in your deployment's policy. Run production validation:

```bash
python3 scripts/validate-config.py \
  --deployment deployment.local/deployment.json --policy deployment.local/gcp-policy.json
```

This command is offline. It checks policy structure and approval flags, not the
truth of your acceptance evidence.

On your controller, install system-wide prerequisites first. Choose the
[controller authentication mode](authentication.md). Generated variables default
to the attached service account of a dedicated GCE controller VM. For an external
host, select Workload Identity Federation and supply its trusted configuration;
explicit key mode remains available. Set GitHub App/key or token paths in
`controller-vars.json`; the role copies file credentials into root-owned service
credential sources. Edit your controller inventory and install in dry-run mode:

```bash
ansible-playbook --check --diff -i your-controller-inventory.ini \
  ansible/playbooks/also-ran.yml -e @deployment.local/controller-vars.json
ansible-playbook -i your-controller-inventory.ini \
  ansible/playbooks/also-ran.yml -e @deployment.local/controller-vars.json
```

Review successful market-observer output. To schedule observation of both pools,
set `also_ran_gcp_broker_enabled: true` while keeping both `*_apply` switches
false, and redeploy. After reviewing dry-run decisions and your budgets, enable
the apply switch for one pool at a time. Automatic deletion is a separate
per-pool policy switch; it starts false. Enabling creation without a tested
deletion path can leave billable hosts, so supervise and clean up the canary
before unattended operation. Generated pools use `demand_only`; warm pools require `configure.py --warm-pool`
or an explicit reviewed policy change. Warm-pool schedules and regional
price/capacity ceilings are explicit policy; review them before activation.

## Operation and rollback

```bash
systemctl status also-ran-gcp.timer also-ran-gcp-scale-up.timer
journalctl -u also-ran-gcp.service -u also-ran-gcp-scale-up.service --since '30 minutes ago'
```

Pause both timers and the optional reconciler before rolling back controller
code or changing identity. Stopping a timer does not stop a running oneshot.
Wait for active passes to finish; the deployment role quiesces broker work
before replacing it. Keep the last known-good policy, image, and source commit.
Do not roll back to an image unable to publish the receipt schema expected by
the controller. Retain explicit legacy image identities only while their hosts
are still draining.

Optional invoice reconciliation uses `scripts/runner-fleet-efficiency.py`.
For live reads, set `ALSO_RAN_BILLING_TABLE=project.dataset.table` and
`AXIOM_ORG_ID`, provide read-only billing/Axiom credentials, and follow its
`--help`. Controller and guest events are JSON journal entries; Axiom is not
required for the core controller's journal logging.
