# Developer - 4 - Publish a Release

The release has one manual entrypoint: run `create_release_tag` from GitHub Actions. It does not read or wait for main CI; it creates the tag and directly dispatches `manual-release.yml`. The release workflow builds and validates all artifacts in the release run, then starts GitHub Release, PyPI, and Docker Hub publication jobs in parallel. Local tag pushes and a second manual publish workflow are outside the supported path.

## Scope

| This page covers | This page does not cover | Why |
|---|---|---|
| Creating a release tag through GitHub Actions | Choosing the next version number | Version policy is a maintainer decision |
| Publishing GitHub Release assets, the `fluxon-py` PyPI wheel, and the Quick Start Docker image | Other package indexes or image registries | The current public destinations are GitHub, PyPI, and Docker Hub |
| Deterministic artifact checks, read-only Codex review, and destination approvals | Functional or integration testing | The release path does not run main CI, and Codex output is not a test result |
| The GitHub Pages doc-site entrypoint | Dispatching a release onto remote machines | Remote deployment belongs to `deployment/manual_dispatch_release.py` |

## 1. Prepare the version pull request

The repository does not have one global version file. Before publishing, check these public surfaces:

| Public surface | Main files | Notes |
|---|---|---|
| Python package version | `fluxon_py/__init__.py` | The repo-root `setup.py` reads the version from here |
| Rust crate versions | `fluxon_rs/Cargo.toml`, `fluxon_rs/*/Cargo.toml`, `fluxon_rs/setup.py` | Release crate and wheel versions must stay aligned |
| Closed SDK open-surface requirement | `fluxon_rs/fluxon_commu_contract/src/lib.rs`, `fluxon_release/closed_sdk/manifest.json`, and matching SDK libraries | The SDK requirement must match `FLUXON_COMMU_OPEN_SURFACE_VERSION`; this contract version is independent from the public release version |
| Quick Start image tag | `examples/fluxon_quick_start/build_image.py`, `examples/fluxon_quick_start/README.md` | Docker Hub publishes `hanbaoaaa/fluxon_quick_start:<version>` |
| GitHub Release notes | `fluxon_release/release_notes/v<version>.md` | The release body comes from the tagged revision |
| README release text | `README.md`, `README_CN.md` | Includes the badge and versioned Docker examples |

Search for release-facing uses of the previous version. Do not mechanically rewrite version-specific test fixtures or YAML examples.

```bash
OLD=0.2.1  # replace with the previous release version
rg -n "$OLD" README.md README_CN.md fluxon_py fluxon_rs examples fluxon_release
```

The closed communication SDK reports its own `sdk_version` and `required_open_surface_version`. The runtime compares the latter with `FLUXON_COMMU_OPEN_SURFACE_VERSION`, rather than the Cargo package version. A release-only version bump leaves this contract unchanged; rebuild and verify the SDK libraries before changing the open-surface constant or generated manifest. The release workflow does not run the `fluxon_commu` runtime contract test, so approvers need that evidence from another source.

Run the local metadata and workflow-contract checks:

```bash
python3 fluxon_release/resolve_release_meta.py --git-ref refs/tags/v<version>
python3 -m unittest \
  setup_and_pack.tests.test_resolve_release_meta \
  setup_and_pack.tests.test_release_workflows
```

The release can start as soon as the version pull request is merged. Default-branch CI may run independently, but its status and artifacts are not release or tag gates.

## 2. Configure credentials and environments once

Repository administrators must configure these external controls:

| Control | Required configuration | Purpose |
|---|---|---|
| Repository Actions token | No external credential; `create_release_tag` grants the built-in `GITHUB_TOKEN` contents write permission | Creates the tag and dispatches the release workflow |
| Codex credentials | Put `OPENAI_API_KEY` and `OPENAI_BASE_URL` in environment `OPENAI_API_KEY` | Runs the read-only readiness review |
| GitHub Release approval | Configure required reviewers on environment `github-release` | Gates the only job with `contents: write` |
| PyPI approval | Configure required reviewers on environment `pypi`; bind the PyPI Trusted Publisher to `manual-release.yml` and this environment | Gates OIDC upload of the release-built, package-validated wheel |
| Docker approval | Configure required reviewers plus `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` on environment `docker-image` | Gates the versioned Docker Hub push |

Referencing an environment in YAML does not add reviewers. An administrator must configure its protection rules in GitHub settings.

The tag workflow uses the repository's built-in `GITHUB_TOKEN` to create the ref and emit an internal `repository_dispatch` event containing only the source commit and repository-derived tag. `manual-release.yml` accepts only the event sent by `github-actions[bot]` and proves that the tag points to that commit and that the commit belongs to default-branch history. No external release identity, PAT, release-tag secret, or CI run ID is required.

## 3. Start the release from GitHub Actions

`.github/workflows/create-release-tag.yml` is the only manual release entrypoint.

1. Open GitHub Actions and select `create_release_tag`.
2. Select the default branch as the workflow ref.
3. Run the workflow; it has no release parameters.

The workflow derives the exact `v<version>` tag from the repository's validated version sources and takes the release body from `fluxon_release/release_notes/v<version>.md`. Before creating the tag, it checks only the tag shape, default-branch ref, tag absence, release metadata, and release notes. The built-in token then creates the lightweight tag and dispatches the release workflow without waiting for main CI.

This entrypoint deliberately permits tag creation and publication while main CI is absent, running, or failed. Approvers must not infer that functional, integration, or runtime contract tests passed merely because the release workflow succeeded.

Do not run `git tag` or push a release tag locally. If a release input is wrong, fix the version pull request and run `create_release_tag` for a new tag.

## 4. Prepare and review all destinations

`.github/workflows/manual-release.yml`, whose workflow name is `publish_release`, starts only from the internal `repository_dispatch` sent after tag creation. Its filename preserves the existing PyPI Trusted Publisher identity; the workflow has no `workflow_dispatch` entrypoint.

The workflow performs these phases:

1. `verify-release` verifies tag-to-source identity, default-branch ancestry, version metadata, and release notes without reading CI.
2. `pack-release` builds `fluxon_release.tar.gz`, the Quick Start image archive, and the PyPI wheel candidate in the release run. `prepare-pypi-wheel` checks that wheel's version, compatibility tag, size, checksum, and PyPI metadata, then runs `twine check`.
3. `release-readiness-review` checks release checksums, proves that the GitHub Release wheel and PyPI wheel are identical, and runs `openai/codex-action` with `.github/codex/release-readiness-prompt.md` under a read-only permission profile.
4. After the review succeeds, the three publication jobs become runnable in parallel. Each destination waits on its own protected environment.

Codex can identify inconsistencies and evidence gaps. It must disclose that main CI was skipped and must not describe packaging or metadata validation as functional testing. Its text is not parsed as an automatic authorization.

## 5. Publish three destinations in parallel

| Job | Environment | Published object | Credential boundary |
|---|---|---|---|
| `publish-github-release` | `github-release` | `fluxon_release.tar.gz` and `fluxon_quick_start_<version>_docker_image.tar.gz` | This job alone receives `contents: write` |
| `publish-pypi` | `pypi` | The `fluxon_py-*.whl` built in the release workflow and checked against the package contract | This job alone receives `id-token: write`; no `PYPI_TOKEN` is used |
| `publish-docker-image` | `docker-image` | `hanbaoaaa/fluxon_quick_start:<version>` loaded from the reviewed image archive | Docker Hub credentials exist only in this environment |

The three jobs depend on the same readiness review and do not depend on one another. Approving or retrying one destination does not serialize the others.

The PyPI preparation checks tag identity, default-branch ancestry, distribution and version, the supported `cp38-abi3-manylinux_2_28_x86_64` wheel tag, `Requires-Python >=3.10`, file size, checksum, and `twine check`. Users install it with:

```bash
python3 -m pip install fluxon-py
```

The Docker job loads the exact reviewed archive, verifies its local image identity, retags it with the canonical Docker Hub repository and release version, and pushes only that versioned tag. It does not update `latest`.

## 6. Retry without a second release entrypoint

There is no manual “publish an existing tag” workflow. For transient preparation, Codex, approval, or destination-upload failures, use GitHub Actions' rerun-failed-jobs operation on the existing `publish_release` run. A source or release-content defect requires a new commit, version, and tag; the release path does not wait for or rerun main CI.

Do not move a published tag. PyPI versions and versioned Docker image tags are treated as immutable release outputs. Any artifact or install-behavior change requires a new version and tag.

## 7. Publish the doc site

`.github/workflows/docs-pages.yml` is separate from the three release destinations. It builds `fluxon_release/doc_site/` and deploys GitHub Pages. Verify the matching run when README, install docs, developer docs, or roadmap pages change.

## 8. Rerun conditions

- Fix the version pull request and rerun `create_release_tag` when tag preflight fails; do not bypass it with a local tag.
- Fix source or release defects in a new commit and use a new version; do not move an existing tag to another commit.
- Rerun failed jobs in `publish_release` for transient preparation, Codex, approval, GitHub Release, PyPI, or Docker Hub failures.
- A version that reached PyPI or Docker Hub must not be overwritten with different content.
- Rerun `docs-pages` after README, `fluxon_doc_cn/`, `fluxon_doc_en/`, or navigation changes.
