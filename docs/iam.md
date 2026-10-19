# IAM and credential boundaries

These permission sets are derived from the current API calls in
`gcp_broker.py`, `gcp_market_observer.py`, `gcp-runner-lifecycle.py`,
`gcp-runner-preflight.py`, `gcp-region-foundation.py`, and the guest bootstrap.
They are an implementation-specific starting point for custom roles, not a
fresh-project acceptance result. No IAM bindings or live permission probes were
performed while writing this guide. Verify the sets with a canary and your
organization's IAM Conditions, API policies, and Cloud Audit Logs before
activation. IAM policies are provisioned separately; also-ran does not create them.

Use a dedicated runner project. Bind project-wide list/create permissions only
in that project. An IAM condition on an instance name cannot scope a project
list or create operation to the fleet: application ownership guards complement
IAM but do not replace an isolation boundary. Keep the controller identity off
runner guests. Review current
[custom-role support](https://cloud.google.com/iam/docs/custom-roles-permissions-support)
when creating roles; some Compute permissions have testing support levels.

## Read-only observation

Create an `alsoRanObserver` custom role with these permissions on the runner
project. The controller needs this set as well as its mutation set below.

```text
cloudquotas.quotas.get
compute.disks.list
compute.instances.get
compute.instances.list
compute.machineTypes.get
compute.projects.get
compute.regions.get
compute.routers.get
compute.subnetworks.get
```

These cover aggregated instance/disk inventories, machine availability, project
and regional quota usage, router/subnet readiness, and Cloud Quotas quotaInfos
reads. Cloud Quotas confirms
[`cloudquotas.quotas.get`](https://cloud.google.com/iam/docs/roles-permissions/cloudquotas)
for quota reads. The market observer does not request quota changes.
Its foundation check calls `compute networks subnets describe` and
`compute routers describe`: those require `compute.subnetworks.get` and
`compute.routers.get`. It does not describe the VPC or individual firewall
rules; the separate network-foundation operator does those reads.

The public Billing Catalog needs authentication/API access but
[no Billing IAM permissions](https://cloud.google.com/billing/docs/access-control)
for services/SKUs lists. It does not need Billing Account Viewer, billing export
access, or permission to change billing. If a credential uses a quota/consumer
project, grant `serviceusage.services.use` on that project as required by its
credential configuration, and enable the APIs there separately.

The installer currently runs the market observer with the controller credential;
the policy's observer account records a boundary for a separately operated
observer, not an automatically deployed credential split. Use only the read set
when running the observer separately.

## Steady-state controller

Add these permissions to the controller's custom role in the runner project:

```text
compute.disks.create
compute.disks.delete
compute.disks.get
compute.images.get
compute.instances.create
compute.instances.delete
compute.instances.getGuestAttributes
compute.instances.getSerialPortOutput
compute.instances.setLabels
compute.instances.setMetadata
compute.instances.setServiceAccount
compute.instances.setTags
compute.networks.getEffectiveFirewalls
compute.subnetworks.use
compute.zoneOperations.get
```

The create path sets labels, metadata, tags, the runtime account, and a new boot
disk. Deletion explicitly deletes attached disks. Reads establish image labels,
instance identity, serial evidence and guest drain receipts. The direct-egress
option additionally needs `compute.subnetworks.useExternalIp`; omit it for
private NAT production runners. The effective-firewall read checks inherited
policy as well as VPC rules. These permissions follow Google's
[VM creation requirements](https://cloud.google.com/compute/docs/instances/create-start-instance)
and [effective firewall API](https://cloud.google.com/compute/docs/reference/rest/v1/networks/getEffectiveFirewalls).

Separately grant `compute.images.useReadOnly` on the approved golden image
(or its dedicated image project if resource-specific binding is impractical).
Grant `roles/iam.serviceAccountUser` **on the runtime service account itself** to
the controller: its `iam.serviceAccounts.actAs` permits attaching that identity.
This is separate from Token Creator and should not be granted project-wide.
No runtime secret access, service-account key creation, IAM administration,
network creation, or billing administration is needed by the controller.

## Runtime and image-builder guests

Bind `roles/secretmanager.secretAccessor` to the **runtime service account on
each named secret**, not the project: the Outrunner credential secret and,
only when guest telemetry is enabled, the Axiom ingest secret. The relevant
permission is `secretmanager.versions.access`; see Google's
[Secret Manager access control](https://cloud.google.com/secret-manager/docs/access-control).
Grant no Compute mutation role. Drain receipt writes and metadata reads use
the guest metadata server, not a project IAM write permission. The local
reaper stops Outrunner; the controller verifies receipts and deletes the VM.

Runner guests use the metadata-server token for the attached runtime service
account. The VM OAuth scope is `cloud-platform`; IAM still limits that token.
Workflow code can generally reach that identity and its allowed secrets, so
run only trusted workloads and restrict GitHub repository/runner-group access.
An organization registration token must not become a credential for unrelated
projects or repositories.

The **builder service account attached to the image-builder VM needs no GCP
IAM role** for the shipped image playbook. It downloads pinned public packages
and installs files; it does not create the image resource or read runtime
secrets. Those API operations are performed by the separate operator. Do not
reuse the runtime account on the builder or enable Outrunner there. Image
finalization removes transient credentials before capture.

## Bootstrap and image operator

Use a separate human/federated operator identity. For builder, acceptance and
regional-canary lifecycle commands, start with the controller sets above, plus:

```text
compute.instances.stop
compute.images.create
compute.images.setLabels
compute.disks.useReadOnly
compute.globalOperations.get
compute.regionOperations.get
compute.networks.get
compute.firewalls.get
compute.subnetworks.list
```

Grant `iam.serviceAccounts.actAs` on the builder account as well as runtime
account. Image capture uses `compute.images.create` and
[`compute.disks.useReadOnly` on the source disk](https://cloud.google.com/compute/docs/reference/rest/v1/images/insert).
The builder uses an external address, so the operator also needs
`compute.subnetworks.useExternalIp` even when production egress uses NAT.
Preflight additionally lists enabled services with `serviceusage.services.list`
and resolves the requested public image family with
[`compute.images.getFromFamily`](https://cloud.google.com/compute/docs/reference/rest/v1/images/getFromFamily)
in its image project; public Ubuntu image access is normally inherited from its
public IAM. This family lookup permission is distinct from `compute.images.get`,
which the lifecycle helper uses to inspect exact golden images.

The network-foundation helpers additionally require the following on the runner
project, only for operators authorized to apply their reviewed network plan:

```text
compute.subnetworks.create
compute.routers.create
compute.routers.update
compute.networks.updatePolicy
compute.firewalls.create
```

These are additions for subnet creation, router/Cloud NAT creation and the
initial IAP SSH firewall; `compute.networks.updatePolicy` also authorizes
[attaching a router to its VPC](https://cloud.google.com/compute/docs/reference/rest/v1/routers/insert). The helpers read the VPC/firewall/router/subnet boundary before
applying and wait on regional/global operations. Review Google's
[Compute permissions reference](https://cloud.google.com/iam/docs/roles-permissions/compute)
when separating these into an independent network-operator role.

Creation of the project, billing linkage, API enablement, service accounts,
custom roles, IAM bindings, initial VPC/firewall rules, and secret versions is
an **administrator bootstrap** task outside those helper permissions. Grant
only the administrative privileges required for your provisioning system;
Owner/Editor is not a steady-state runtime prerequisite. Enabling APIs requires
`serviceusage.services.enable`. Do not leave bootstrap credentials on the
controller or image.

## SSH, IAP and external credentials

IAP users need `iap.tunnelInstances.accessViaIAP` (normally
`roles/iap.tunnelResourceAccessor`) on the selected VM/project with a condition
limiting TCP port 22, and `compute.instances.get`/`compute.instances.list` for
CLI discovery. IAP does not authorize the guest SSH login: provision the
reviewed `runner-admin` key, or separately configure OS Login and its IAM roles.
The shipped flow uses instance SSH metadata, so an operator changing keys needs
`compute.instances.setMetadata`; it does not require project-wide SSH key
mutation. See [IAP TCP forwarding](https://cloud.google.com/iap/docs/using-tcp-forwarding).

For controller authentication, an attached GCP account or workload federation
avoids long-lived keys. Federated impersonation requires an appropriately
conditioned `roles/iam.workloadIdentityUser` grant on the controller account;
explicit service-account impersonation requires Token Creator on that account.
These grants authorize the caller, not the runner guest. Keep the credential
and gcloud configuration readable only by the service user/root. Select the
trusted deployment file explicitly with `ALSO_RAN_DEPLOYMENT` for source
commands; installed services use `/opt/also-ran/deployment.json`.

GitHub permissions are independent of GCP IAM. The controller's read-only App
needs repository Actions read, repository Administration read, and organization
Self-hosted runners read on the configured repositories/organization. Outrunner
uses a separate runner-management credential; follow the pinned
[Outrunner documentation](https://github.com/ArborealMgmt/gha-outrunner) for its
credential format and write permissions. Optional Axiom credentials should be
ingest-only for the selected datasets. Neither credential belongs in Git,
instance metadata, or the golden image.
