# Controller authentication

The controller invokes the Google Cloud CLI for provider commands and to mint
short-lived bearer tokens for its REST reads. Broker, reconciler, market observer,
telemetry collector, and their lifecycle child processes share the selected mode.
This is separate from the GitHub App/token and from the runner guest's service
account. Authentication does not grant IAM permissions; see [IAM](iam.md).

## Attached service account (default)

Run the controller on a Compute Engine VM with a dedicated service account
attached and the `cloud-platform` access scope. Grant only the permissions needed
by the enabled controller operations. Set:

```yaml
also_ran_gcp_auth_mode: attached_service_account
also_ran_gcp_controller_credential_source: ''
```

No exported GCP key is required. `gcloud` uses the attached identity through the
metadata server. This mode requires a reachable GCE metadata server; it does not
provide authentication on an arbitrary laptop or non-GCP host. Avoid putting the
controller on a runner host: runner jobs must not obtain the controller identity.

## Workload Identity Federation

For a controller outside GCP, configure a trusted workload identity provider and
obtain its `external_account` configuration using Google's documented flow. The
installer supports file- or HTTPS URL-sourced subject tokens, optionally with
service account impersonation. Executable and AWS environment credential sources
are deliberately unsupported by this installer.

```yaml
also_ran_gcp_auth_mode: workload_identity_federation
also_ran_gcp_controller_credential_source: /secure/controller-wif.json
```

The source path is on the Ansible control machine. Ansible validates the selected
credential type without contacting Google, then copies it to a root-owned file
and exposes it to each service through systemd `LoadCredential`. Configuration
shape validation does not verify provider trust, IAM, or token validity.

Provision subject-token refresh separately. For a `credential_source.file`, use
an absolute path on the **controller host** that the `also-ran` user can read,
such as `/run/also-ran-identity/subject-token`. Keep it outside `/home` and `/tmp`,
which the systemd sandbox hides or replaces. Do not put the rotating token inside
the copied JSON or systemd's credential snapshot: those files are refreshed only
when the service restarts. Refresh the host token atomically before expiry.
The URL mode must use HTTPS and must remain reachable from the service sandbox.
Treat federation configuration as trusted input: it chooses token source and
exchange endpoints. Never accept a credential configuration from an untrusted PR.

## Explicit service account key (compatibility)

```yaml
also_ran_gcp_auth_mode: service_account_key
also_ran_gcp_controller_credential_source: /secure/controller-service-account.json
```

This mode requires a JSON `service_account` key. It remains available for hosts
where attached identity and federation are unavailable. Store it outside this
repository, limit access, and rotate it using your normal secret-management
process. User `authorized_user` credentials are rejected.

## Isolation and migration

Every GCP service uses its own runtime `CLOUDSDK_CONFIG` directory. Operator
`gcloud auth login` sessions and home-directory credentials are not used. The
units clear inherited account, impersonation, access-token, and ADC overrides.
File modes set `CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE` to the systemd credential
path; an unreadable or invalid selected credential fails authentication rather
than silently choosing the VM identity. Child commands inherit these settings.
No application-default login or activation command is needed.

For an existing key installation, first provision attached identity or federation
and its permissions independently. Set the explicit mode and run the controller
playbook with your reviewed inventory. The installer quiesces capacity loops
before changing units. Switching to attached mode removes the obsolete
`/etc/also-ran/gcp-controller-key.json` copy. After deployment, verify the selected
identity with a read-only provider call under the same service account, sandbox,
and environment; an interactive shell's successful login is insufficient. Start
with dry-run broker modes, inspect logs and readbacks, and enable mutations only
after live acceptance. Once keyless access is verified, revoke the old key in IAM
and remove remaining operator-side copies. The installer does not revoke keys.

Reference: Google's [gcloud authentication guidance](https://docs.cloud.google.com/sdk/docs/authenticate),
[configuration isolation](https://docs.cloud.google.com/sdk/gcloud/reference/topic/configurations),
and [Workload Identity Federation deployment guidance](https://docs.cloud.google.com/iam/docs/workload-identity-federation-with-other-providers).
