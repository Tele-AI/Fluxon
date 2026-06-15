# Developer - 2 - Package Middleware and Images

This page only covers `setup_and_pack/pack_release_ext.py` and `examples/fluxon_quick_start/build_image.py`.

## `ext_images/`

`pack_release_ext.py` exports these runtime objects into `fluxon_release/ext_images/`:

- `etcd/etcd`
- `etcd/etcdctl`
- `etcd/start.sh`
- `greptime/greptime`
- `greptime/start.sh`
- `tikv/pd-server`
- `tikv/tikv-server`
- `tikv/start_pd.sh`
- `tikv/start_tikv.sh`
- `ext_images.sha256`

## Command

```bash
python3 setup_and_pack/pack_release_ext.py --release-dir ./fluxon_release --with-tikv-runtime true
```

## Quick Start Image

`examples/fluxon_quick_start/build_image.py` supports exactly two modes:

- `existing_release`
- `url_download`

### Existing Release

```bash
python3 examples/fluxon_quick_start/build_image.py --mode existing_release
```

### URL Download

```bash
python3 examples/fluxon_quick_start/build_image.py \
  --mode url_download \
  --fluxon-wheel-url <url> \
  --fluxon-pyo3-wheel-url <url> \
  --pylib-src-url <url>
```

## Repackage This Layer When

- `deployment/deployconf.yaml`
- `setup_and_pack/pack_release_ext.py`
- `examples/fluxon_quick_start/`
- Middleware image versions
