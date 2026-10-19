# Contributing

Open an issue describing the behavior you want to change, then submit a focused
pull request with the relevant validation. Run `make check`; changes to Ansible
also need `make ansible-check` after installing the hash-locked development requirements.

Preserve fail-closed observation, complete pagination, project/instance
ownership checks, quota admission, and receipt verification. A mocked API result
is not live cloud acceptance evidence. Changes to these boundaries need tests
for rejected or incomplete observations as well as the successful path.

Do not include credentials, private deployment inventories, account-specific
logs, Terraform state, or runner registration material. Provider mutations and
host changes are separate from offline validation. Keep dependency versions,
checksums, container digests, and GitHub Actions references pinned.

Contributions intentionally submitted for inclusion are provided under the
project's Apache-2.0 license, as described in section 5 of LICENSE. Preserve
third-party license and attribution notices.

Follow the [code of conduct](CODE_OF_CONDUCT.md). Maintainers track release
changes in [CHANGELOG.md](CHANGELOG.md); experimental tags use prerelease
versions until fresh-project acceptance and compatibility expectations are
established. Dependency update PRs must preserve immutable pins and regenerate
`requirements-dev.txt` with hashes when Python dependencies change.
