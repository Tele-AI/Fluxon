# Developer - 1 - Package Core Install Artifacts

This page only covers how `setup_and_pack/pack_release.py` gathers the core install artifacts into `fluxon_release/`.

## Artifacts

- `fluxon_release/fluxon-*.whl`
- `fluxon_release/fluxon_pyo3-*.whl`
- `fluxon_release/pylib_src.tar.gz`
- `fluxon_release/install.py`
- `fluxon_release/ext_images.tar.gz`
- `fluxon_release/fluxon_release.sha256`

## Commands

```bash
python3 setup_and_pack/pack_release.py
python3 setup_and_pack/pack_release.py --release-dir ./fluxon_release
```

## Behavior

- The default output directory is `<repo_root>/fluxon_release/`
- The public packaging flow does not require a manual `transport backend` argument
- `pack_release.py` automatically chains into `setup_and_pack/pack_release_ext.py`
- Core artifacts and external runtime artifacts are written into the same release directory

## Repackage When

- `fluxon_py/`
- `fluxon_rs/`
- `setup_and_pack/`
