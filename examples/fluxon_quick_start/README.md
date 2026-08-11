# Fluxon Quick Start

`fluxon_quick_start` is the one-command bring-up entrypoint for Fluxon.

It solves "bring up one runnable environment quickly and operate it immediately".
It does not replace the formal service-plane, KV, MQ, or FS interface docs.

## User-Facing Objects

- `fluxon_py.quick_start`
  - unified entrypoint included in the installed `fluxon-py` distribution
- `examples/fluxon_quick_start/start.py`
  - compatibility wrapper for source-tree development
- `build_image.py`
  - quick-start image build entrypoint
- `fluxon_quick_start:0.2.2`
  - quick-start Docker image

## Runtime Modes

Quick start supports three runtime modes:

- installed-package mode
  - `pip install fluxon-py`, then call `serve_s3_single_node(...)` without downloading the repository
- image-first mode
  - primary path
  - build the image from release artifacts, then run the container
- repo-run mode
  - development-only path
  - runs the same `fluxon_py.quick_start` module from the checkout
  - requires the current Python environment to already have a working Fluxon runtime installed
  - quick start does not create a venv or install wheels at runtime

## What Quick Start Launches

Quick start launches the minimum runnable environment for each mode:

- `kv`
  - `etcd`
  - `greptime`
  - `fluxonkv master`
  - `owner`
  - KV HTTP re-export
  - interactive KV CLI
- `mq`
  - `etcd`
  - `greptime`
  - `fluxonkv master`
  - `owner`
  - one producer
  - one consumer
  - interactive MQ shell
- `fs`
  - `etcd`
  - `greptime`
  - `fluxonkv master`
  - `owner`
  - `fs_master`
  - `fs_agent`
  - interactive FS shell
  - FS web UI

Quick start is only for fast bring-up and interaction:

- Formal service-plane docs:
  - `fluxon_doc_en/user_doc/User - 2 - Service Plane.md`
- Formal business interface docs:
  - KV, MQ, and FS user docs

## Shared Constraints

- Linux only
- Docker mode defaults to host network: `--network host`
- Ports must be specified explicitly and must not conflict

## Build The Image

```bash
python3 examples/fluxon_quick_start/build_image.py --mode existing_release
```

The image consumes release artifacts only. It installs the unified `fluxon-py` wheel from
`fluxon_release/*.whl` and does not use editable source installs.

Repo-run mode is also supported for development, but it is not self-bootstrapping.
Before running `python3 -m fluxon_py.quick_start`, make sure the current
Python environment can already import both `fluxon_py` and `fluxon_pyo3`.

## KV Quick Start

```bash
docker run --rm -it --network host \
  fluxon_quick_start:0.2.2 \
  --mode kv \
  --etcd-client-port 12379 \
  --master-p2p-port 31000 \
  --panel-port 18080 \
  --greptime-http-port 14000 \
  --kv-http-port 8083
```

Run directly from the repo:

```bash
python3 -m fluxon_py.quick_start \
  --mode kv \
  --etcd-client-port 12379 \
  --master-p2p-port 31000 \
  --panel-port 18080 \
  --greptime-http-port 14000 \
  --kv-http-port 8083
```

Inside the shell:

```text
put demo:hello world
get demo:hello
del demo:hello
```

## MQ Quick Start

```bash
docker run --rm -it --network host \
  fluxon_quick_start:0.2.2 \
  --mode mq \
  --etcd-client-port 37379 \
  --kv-master-port 34200 \
  --greptime-http-port 14000 \
  --panel-port 18080
```

Run directly from the repo:

```bash
python3 -m fluxon_py.quick_start \
  --mode mq \
  --etcd-client-port 37379 \
  --kv-master-port 34200 \
  --greptime-http-port 14000 \
  --panel-port 18080
```

Inside the shell:

```text
put hello
put world
status
exit
```

`status` prints the current producer/consumer binding info.

The background consumer keeps printing received messages.

## FS Quick Start

```bash
docker run --rm -it --network host \
  fluxon_quick_start:0.2.2 \
  --mode fs \
  --etcd-client-port 36379 \
  --kv-master-port 34100 \
  --greptime-http-port 14000 \
  --panel-port 34180
```

Run directly from the repo:

```bash
python3 -m fluxon_py.quick_start \
  --mode fs \
  --share-mem-path /dev/shm/fluxon-fs-quick-start \
  --etcd-client-port 36379 \
  --kv-master-port 34100 \
  --greptime-http-port 14000 \
  --panel-port 34180
```

Inside the shell:

```text
ls
echo "hello fs" > notes.txt
cat notes.txt
ui
```

FS quick start also prints:

- `fs_s3` endpoint
- object UI URL
- bucket UI URL
- bootstrap Basic Auth credentials: `admin / admin`

On a new FS workdir, the first Web UI sign-in requires changing both the username and password.
Until that succeeds, normal Web UI pages redirect to `Change Credentials` and S3 requests return
`AccessDenied`. The result is persisted in `<workdir>/fs_master/access.db`; keep the workdir across
restarts. On Unix, Quick Start creates that database with mode `0600`. Allow about two seconds for
the updated permissions to reach the FS agent before the first S3 request.

### Long-Running FS/S3 Service

After `pip install fluxon-py`, the user script provides the complete KV master and owner configs, then
starts FS S3. The owner shared-memory path is explicit and independent from persistent state:

```python
from fluxon_py.quick_start import serve_s3_single_node

kv_master_config = {
    "etcd_endpoints": ["127.0.0.1:22379"],
    "cluster_name": "fluxon_s3",
    "instance_key": "fluxon_s3_master",
    "port": 25100,
    "log_dir": "/absolute/path/to/state/kv-master/log",
    "monitoring": {
        "prometheus_base_url": "http://127.0.0.1:24000/v1/prometheus",
        "prom_remote_write_url": ["http://127.0.0.1:24000/v1/prometheus/write"],
        "otlp_log_api": {
            "otlp_endpoint": "http://127.0.0.1:24000/v1/otlp/v1/logs",
            "db_name": "public",
            "table_name": "fluxon_logs",
        },
    },
}
kv_owner_config = {
    "instance_key": "fluxon_s3_owner",
    "contribute_to_cluster_pool_size": {"dram": 1073741824, "vram": {}},
    "fluxonkv_spec": {
        "etcd_addresses": ["127.0.0.1:22379"],
        "cluster_name": "fluxon_s3",
        "share_mem_path": "/dev/shm/fluxon-s3",
        "sub_cluster": "default",
        "large_file_paths": ["/absolute/path/to/state/kv-owner/large"],
    },
}
serve_s3_single_node(
    "/absolute/path/to/data",
    "/absolute/path/to/state",
    kv_master_config=kv_master_config,
    kv_owner_config=kv_owner_config,
    export_name="local-data",
)
```

`/dev/shm/fluxon-s3` contains the owner's `mmap.file`, `shared.json`, and peer metadata. It is
rebuildable runtime state; ensure the selected filesystem has enough capacity for the owner pool.
`export_name` is also the S3 bucket name; omit it to use `quick-start-export`.

The equivalent container command overrides the image's default Quick Start command and runs the
same scenario composition directly with `python3 -c`:

```bash
docker run -d --name fluxon-s3 --restart unless-stopped --network host --shm-size 2g \
  --mount type=bind,src=/absolute/path/to/data,dst=/data \
  --mount type=bind,src=/absolute/path/to/state,dst=/state \
  --entrypoint python3 \
  fluxon_quick_start:<version> \
  -c '
from fluxon_py.quick_start import serve_s3_single_node

kv_master_config = {
    "etcd_endpoints": ["127.0.0.1:22379"],
    "cluster_name": "fluxon_s3",
    "instance_key": "fluxon_s3_master",
    "port": 25100,
    "log_dir": "/state/kv-master/log",
    "monitoring": {
        "prometheus_base_url": "http://127.0.0.1:24000/v1/prometheus",
        "prom_remote_write_url": ["http://127.0.0.1:24000/v1/prometheus/write"],
        "otlp_log_api": {
            "otlp_endpoint": "http://127.0.0.1:24000/v1/otlp/v1/logs",
            "db_name": "public",
            "table_name": "fluxon_logs",
        },
    },
}
kv_owner_config = {
    "instance_key": "fluxon_s3_owner",
    "contribute_to_cluster_pool_size": {"dram": 1073741824, "vram": {}},
    "fluxonkv_spec": {
        "etcd_addresses": ["127.0.0.1:22379"],
        "cluster_name": "fluxon_s3",
        "share_mem_path": "/dev/shm/fluxon-s3",
        "sub_cluster": "default",
        "large_file_paths": ["/state/kv-owner/large"],
    },
}
serve_s3_single_node(
    "/data",
    "/state",
    kv_master_config=kv_master_config,
    kv_owner_config=kv_owner_config,
    export_name="local-data",
)
'
```

Use a Python package and image version containing the same feature set. Keep the panel port private
until the bootstrap credentials have been replaced.

## When Not To Use Quick Start

Go back to the formal service-plane and interface paths if you need to:

- control the lifecycle of `etcd`, `greptime`, `master`, `owner`, `fs_master`, or `fs_agent` yourself
- persist config as Python dict or YAML and hand it to a supervisor
- write formal KV, MQ, or FS business code

Document entrypoints:

- Service plane:
  - `fluxon_doc_en/user_doc/User - 2 - Service Plane.md`
- KV and node-to-node RPC:
  - `fluxon_doc_en/user_doc/User - 3 - KV and RPC Interface.md`
- MQ:
  - `fluxon_doc_en/user_doc/User - 4 - MQ Interface.md`
- FS:
  - `fluxon_doc_en/user_doc/User - 5 - FS Interface.md`
