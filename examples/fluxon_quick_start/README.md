# Fluxon Quick Start

`fluxon_quick_start` is the one-command bring-up entrypoint for Fluxon.

It solves "bring up one runnable environment quickly and operate it immediately".
It does not replace the formal service-plane, KV, MQ, or FS interface docs.

## User-Facing Objects

- `start.py`
  - unified quick-start entrypoint
- `build_image.py`
  - quick-start image build entrypoint
- `fluxon_quick_start:0.2.1`
  - quick-start Docker image

## Runtime Modes

Quick start supports two runtime modes:

- image-first mode
  - primary path
  - build the image from release artifacts, then run the container
- repo-run mode
  - development-only path
  - runs `examples/fluxon_quick_start/start.py` from the checkout
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

The image consumes release artifacts only. It installs `fluxon` and `fluxon_pyo3`
from `fluxon_release/*.whl` and does not use editable source installs.

Repo-run mode is also supported for development, but it is not self-bootstrapping.
Before running `python3 examples/fluxon_quick_start/start.py`, make sure the current
Python environment can already import both `fluxon_py` and `fluxon_pyo3`.

## KV Quick Start

```bash
docker run --rm -it --network host \
  fluxon_quick_start:0.2.1 \
  --mode kv \
  --etcd-client-port 12379 \
  --master-p2p-port 31000 \
  --panel-port 18080 \
  --greptime-http-port 14000 \
  --kv-http-port 8083
```

Run directly from the repo:

```bash
python3 examples/fluxon_quick_start/start.py \
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
  fluxon_quick_start:0.2.1 \
  --mode mq \
  --etcd-client-port 37379 \
  --kv-master-port 34200 \
  --greptime-http-port 14000 \
  --panel-port 18080
```

Run directly from the repo:

```bash
python3 examples/fluxon_quick_start/start.py \
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
  fluxon_quick_start:0.2.1 \
  --mode fs \
  --etcd-client-port 36379 \
  --kv-master-port 34100 \
  --greptime-http-port 14000 \
  --panel-port 34180
```

Run directly from the repo:

```bash
python3 examples/fluxon_quick_start/start.py \
  --mode fs \
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
- Basic Auth entry

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
