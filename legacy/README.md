# Legacy provider helpers

These Hetzner/Azure policies and operator helpers preserve the original pilot's
regression coverage. They are not installed by the GCP deployment playbooks and
are not supported deployment paths for this release.

Both lifecycle helpers require an explicit `--policy` path. Policies here contain
synthetic identities; do not apply them to real infrastructure. Existing guards
and confirmation switches remain in place. Shared observation primitives stay in
`controller/also-ran/also_ran.py` because the GCP controller imports them.

Run their offline regression tests through `make check` at the repository root.
