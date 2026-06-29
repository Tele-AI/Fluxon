# User - 0 - Installation

## Installation

If you use Fluxon directly, you usually deal with two install bundles:

- Fluxon core artifacts: `fluxon-*.whl`, `fluxon_pyo3-*.whl`, `pylib_src.tar.gz`, `install.py`, `fluxon_release.sha256`
- Runtime packages for `etcd / Greptime / TiKV`: `ext_images.tar.gz`

If your Python process only attaches to an existing service plane, you normally only need the Fluxon core package. If you also need to start the KV / MQ / FS service plane yourself, prepare the `etcd / Greptime / TiKV` runtime package as well. See [Architecture and Concepts](<./User - 1 - Architecture and Concepts.md>) for the role model.

### Download from GitHub Releases

<!-- TODO: add the real release URL after the Releases page is public -->

```text
https://github.com/<org>/fluxon/releases
```

#### Install Fluxon Core

```bash
tar xzf fluxon_release.tar.gz
cd fluxon_release
pip install fluxon-*.whl fluxon_pyo3-*.whl
```

#### Unpack `etcd / Greptime / TiKV`

```bash
tar xzf ext_images.tar.gz
```

Expected files:

- `ext_images/etcd/etcd`
- `ext_images/etcd/etcdctl`
- `ext_images/etcd/start.sh`
- `ext_images/greptime/greptime`
- `ext_images/greptime/start.sh`
- `ext_images/tikv/pd-server`
- `ext_images/tikv/tikv-server`
- `ext_images/tikv/start_pd.sh`
- `ext_images/tikv/start_tikv.sh`
- `ext_images/ext_images.sha256`

Fluxon does not replace these external dependencies from inside `fluxon_py.runtime`.

### Build from Source

The main packaging entrypoint is `setup_and_pack/pack_release.py`. It automatically calls `setup_and_pack/pack_release_ext.py` and gathers core wheels, `pylib_src.tar.gz`, `install.py`, `ext_images.tar.gz`, and `fluxon_release.sha256` into `fluxon_release/`.

- `setup_and_pack/pack_release.py`: package Fluxon core artifacts
- `setup_and_pack/pack_release_ext.py`: export `etcd / Greptime / TiKV` runtime objects

Related docs:

- [Developer - 1 - Package Core Install Artifacts](<../dev_doc/Developer - 1 - Package Core Install Artifacts.md>)
- [Developer - 2 - Package Middleware and Images](<../dev_doc/Developer - 2 - Package Middleware and Images.md>)

### Artifact List

#### Fluxon Core

| File | Description |
|---|---|
| `fluxon-*.whl` | Fluxon Python package |
| `fluxon_pyo3-*.whl` | Fluxon Rust bindings (PyO3) |
| `pylib_src.tar.gz` | Python source bundle |
| `install.py` | Release runtime entrypoint |
| `fluxon_release.sha256` | SHA256 checksum file for all release artifacts |

#### `etcd / Greptime / TiKV`

| File | Description |
|---|---|
| `ext_images/etcd/etcd` | `etcd` executable |
| `ext_images/etcd/etcdctl` | `etcd` CLI |
| `ext_images/etcd/start.sh` | `etcd` startup script |
| `ext_images/greptime/greptime` | `greptime` executable |
| `ext_images/greptime/start.sh` | `greptime` startup script |
| `ext_images/tikv/pd-server` | TiKV PD executable |
| `ext_images/tikv/tikv-server` | TiKV executable |
| `ext_images/tikv/start_pd.sh` | TiKV PD startup script |
| `ext_images/tikv/start_tikv.sh` | TiKV startup script |
| `ext_images/ext_images.sha256` | SHA256 checksum file for runtime artifacts |

### Verify the Install

```bash
python3 -c "import fluxon_py; print('ok')"
```

If you also unpacked local runtime artifacts, a quick binary check is:

```bash
test -x ext_images/etcd/etcd
test -x ext_images/greptime/greptime
test -x ext_images/tikv/pd-server
test -x ext_images/tikv/tikv-server
```

### System Requirements

- Linux only
- Python `>= 3.10`
- Prebuilt Python wheels currently use the `manylinux_2_28` ABI baseline
- When building from source, follow `fluxon_rs/rust-toolchain.toml`, currently pinned to `1.93.0`
- Docker is only required for Quick Start or for running `setup_and_pack/pack_release_ext.py`
