# Developer - 4 - Publish a Release

This page explains how the current repository publishes a public release. The stable flow is: update public version strings and release-facing text, commit and wait for the commit CI to pass, then create and push a `v<version>` tag. GitHub Actions builds and tests the wheel for that tag, creates the GitHub Release, and publishes the same wheel to PyPI after the complete tag CI succeeds. If the change also touched the README or docs, publish GitHub Pages separately.

## Scope

| This page covers | This page does not cover | Why |
|---|---|---|
| Publishing install artifacts on GitHub Releases | Dispatching a release onto remote machines | Remote deployment belongs to `deployment/manual_dispatch_release.py` |
| Publishing the `fluxon-ai` wheel to PyPI | Other Python package indexes | PyPI is the only public index in the current flow |
| The `publish_release` build entrypoint | The local wheel build internals | Local packaging details live in Developer 1 / 2 |
| The GitHub Pages doc-site publish entrypoint | testbed / testrunner test flows | The test stack has its own workflow |

## 1. Update version strings and release-facing text first

The repository does not have one single global version file. Before publishing, at least check these public surfaces:

| Public surface | Main files | Notes |
|---|---|---|
| Python package version | `fluxon_py/__init__.py` | The repo-root `setup.py` reads the version from here |
| PyPI distribution name | `setup_and_pack/package_contract.py`, `setup.py` | The distribution is `fluxon-ai`; wheel filenames use the `fluxon_ai` prefix |
| Rust crate versions | `fluxon_rs/Cargo.toml`, `fluxon_rs/*/Cargo.toml`, `fluxon_rs/setup.py` | Public crate and wheel versions should stay aligned |
| Quick Start image tag | `examples/fluxon_quick_start/build_image.py`, `examples/fluxon_quick_start/README.md` | The current public image name is `fluxon_quick_start:<version>` |
| Release workflow artifact names | `.github/workflows/manual-release.yml` | The workflow currently emits `fluxon_quick_start_<version>_docker_image.tar.gz` |
| GitHub Release notes | `fluxon_release/release_notes/v<version>.md` | The release body is read directly from this versioned file |
| README release text | `README.md`, `README_CN.md` | Includes the badge, image-tag examples, and developer-doc links |

A repo-wide search helps catch the previous public version string:

```bash
OLD=0.2.1  # replace with the previous release version
rg -n "$OLD" README.md README_CN.md fluxon_py fluxon_rs examples .github/workflows
```

After the version and text updates are ready, push the commit first, then create and push the release tag.

The release notes now use one file per version. The required path pattern is:

```text
fluxon_release/release_notes/v<version>.md
```

For example, `v0.2.1` uses:

```text
fluxon_release/release_notes/v0.2.1.md
```

If that file is missing, `publish_release` fails fast instead of publishing a release with an incomplete body.

## 2. Push a `v<version>` tag to trigger the release and tag CI

The GitHub Release build entrypoint is `.github/workflows/manual-release.yml`, whose workflow name is `publish_release`. A `v*` tag push also starts `.github/workflows/all_test.yml`, where the complete tag CI builds and tests the wheel prepared for PyPI. `publish_release` retains a `workflow_dispatch` entrypoint for GitHub Release rebuilds. The stable path is:

```bash
git push origin <branch>
# wait for the commit CI to pass
git tag v0.2.1
git push origin v0.2.1
```

On the GitHub runner it does these steps:

1. Validate that the tag, `fluxon_py/__init__.py`, `examples/fluxon_quick_start/build_image.py`, `fluxon_rs/setup.py`, and the release Cargo manifests all declare the same version.
2. Validate that `fluxon_release/release_notes/v<version>.md` exists.
3. Install Python 3.10, Docker, and packaging dependencies.
4. Build the manylinux builder image.
5. Generate the CI `pack_fluxonkv_pylib_env.yaml`.
6. Run `python3 setup_and_pack/pack_release.py --release-dir fluxon_release`.
7. Run `python3 examples/fluxon_quick_start/build_image.py --mode existing_release --release-dir fluxon_release`.
8. Upload the workflow artifact.
9. Create or update the matching GitHub Release and upload the release assets automatically.

If the tag is `v0.2.1` but the repository still declares `0.2.0` on its public version surfaces, or if `fluxon_release/release_notes/v0.2.1.md` is missing, the workflow fails fast instead of publishing an incomplete release.

After the workflow finishes, both the GitHub Actions artifact and the GitHub Release assets should contain at least:

- `fluxon_release.tar.gz`
- `fluxon_quick_start_<version>_docker_image.tar.gz`

`fluxon_release.tar.gz` contains the `fluxon_release/` directory with the core wheel, `pylib_src.tar.gz`, `install.py`, `ext_images.tar.gz`, and `fluxon_release.sha256`. For the internal packaging layout, see:

- [Developer - 1 - Package Core Install Artifacts](<./Developer - 1 - Package Core Install Artifacts.md>)
- [Developer - 2 - Package Middleware and Images](<./Developer - 2 - Package Middleware and Images.md>)

## 3. Publish the PyPI wheel after the tag CI succeeds

The PyPI entrypoint is `.github/workflows/publish-pypi.yml`. It does not respond directly to a tag push. It waits for the `ci_2_virt_node` workflow to finish and proceeds only when the source event is `push`, the CI conclusion is `success`, and the source ref is a `v<version>` tag.

The publishing flow consumes the tag CI artifact named `fluxon-ci-release-<commit SHA>` and checks that:

1. The tag still points to the commit that produced the CI run.
2. The commit is in the default-branch history.
3. The artifact contains exactly one `fluxon_ai-*.whl`.
4. The wheel distribution is `fluxon-ai`, and its version matches the tag.
5. The wheel tag is the supported `cp38-abi3-manylinux_2_28_x86_64`, and `Requires-Python` is `>=3.10`.
6. The wheel is below PyPI's default file-size limit and passes `twine check`.

The workflow then uploads that same tested wheel through GitHub OIDC and PyPI Trusted Publishing. It does not use a long-lived `PYPI_TOKEN`. Configure these external settings before the first release:

- PyPI project: `fluxon-ai`
- GitHub owner / repository: `Tele-AI/Fluxon`
- Trusted Publisher workflow: `publish-pypi.yml`
- GitHub environment: `pypi`

After publication, users install the package with:

```bash
python3 -m pip install fluxon-ai
```

The CI gate applies to the exact commit referenced by the tag. Default-branch protection is responsible for requiring checks on changes entering the mainline; the publishing flow does not require every ancestor commit to have never failed an earlier run.

## 4. When to trigger it manually

In the normal path, no manual GitHub Release page work is needed. Once the `v<version>` tag push succeeds, the workflow reads `fluxon_release/release_notes/v<version>.md` and creates or updates the release automatically.

Use the manual `publish_release` trigger only in cases like these:

- The GitHub runner had a transient failure and an existing tag needs a rebuild.
- The workflow failed late and the same tag needs the assets uploaded again.
- You want to validate the release pipeline without minting a new tag.

For the manual trigger, provide an already existing tag such as `v0.2.1`. That tag must still match the repository public version exactly.

Manually triggering `publish_release` only rebuilds the GitHub Release; it cannot bypass the tag CI to upload to PyPI. PyPI versions are immutable, so a successfully published version remains unchanged and subsequent content uses a new version and tag.

If a change only edits the GitHub Release text, update `fluxon_release/release_notes/v<version>.md` and rerun `publish_release` manually so the release body and assets stay in sync.

## 5. Publish the doc site

The doc-site release is a separate pipeline from the binary release. The current doc-site workflow is `.github/workflows/docs-pages.yml`:

- It triggers automatically on pushes to `main` / `master` when README, doc content, or doc-site build code changes.
- It can also be run manually from GitHub Actions.
- It builds `fluxon_release/doc_site/` and deploys that output to GitHub Pages.
- It does not upload wheels, `fluxon_release.tar.gz`, or Docker images.

If the release changes the README, install docs, developer docs, or roadmap, publish `docs-pages` as part of the same release pass.

## 6. When to rerun

- Push the matching release tag again, or rerun `publish_release`, after version-string, README badge, image-tag, or release-artifact-name changes.
- Push the matching release tag again, or rerun `publish_release`, after release-related changes under `setup_and_pack/`, `examples/fluxon_quick_start/`, `fluxon_py/`, or `fluxon_rs/`.
- If a PyPI job fails before upload, rerun the tag CI or the failed `publish_pypi` workflow. A version that reached PyPI cannot be overwritten.
- Rerun `docs-pages` after README, `fluxon_doc_cn/`, `fluxon_doc_en/`, or doc-navigation changes.
- If only the descriptive text on the GitHub Release page changes, no workflow needs to rerun.
