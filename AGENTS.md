# Agent instructions

This is the standalone also-ran project. Its source of truth is this repository.

- Run offline tests with `make check`; the controller uses Python's standard library.
- For deployment changes, also run both Ansible syntax checks in the README.
- Keep dry-run/validation separate from cloud mutations and host changes.
- Do not provision, delete, deploy, or rotate credentials without a request for that action.
- Never commit secrets, private inventories, runner tokens, state, or real job payloads.
- Preserve project/instance ownership, complete observations, quota guards, and receipt verification.
- Pin third-party packages, container images, GitHub Actions, and runner binaries.
- Treat example identities and acceptance flags as fixtures, never proof of live acceptance.
- Keep the core controller independent of telemetry vendors and private repositories.
