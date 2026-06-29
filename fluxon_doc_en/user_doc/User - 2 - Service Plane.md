# User - 2 - Service Plane

## Service Plane

To use Fluxon KV you need to understand the fixed service-plane objects that come before your business process. They own control-plane metadata, the shared memory pool, and the startup choreography for KV roles.

From a user point of view the most common objects are:

- External dependencies: `etcd`, `Greptime`, `TiKV`
- Fluxon-native roles: `Master`, `Owner Client`
- Startup entrypoints: raw `etcd / Greptime / TiKV` runtimes, `fluxon_py.runtime`, and your own supervisor or scripts

If you are writing business code, this page answers three questions:

- Which processes must be started first
- How those processes relate to one another
- Which objects may be started by `fluxon_py.runtime` and which may not

For the user-facing API, continue to [User - 3 - KV and RPC Interface](<./User - 3 - KV and RPC Interface.md>).

### Role Model

The service plane can be reduced to five stable objects:

- External dependency: `etcd`
- External dependency: `Greptime`
- External dependency: `TiKV`
- Fluxon-native role: `Master`
- Fluxon-native role: `Owner Client`

Deployment layout:

![](../../pics/deploy_arch_1.png)

Responsibilities:

- `etcd`: control-plane metadata
- `Greptime`: standard observability path
- `TiKV`: persistent task-state storage for extended features such as FS transfer-state persistence
- `Master`: membership, routing, leases, monitoring broadcast, master-side logs
- `Owner Client`: local shared-memory pool and `shared.json`

### Minimum Startup Order

The minimum chain for KV is:

- KV: `Greptime -> etcd -> Fluxon KV Master -> Owner Client -> business process new_store(...)`

If you also need transfer-state-backed features such as directory transfer or pre-scan, add:

- Transfer / Pre-Scan: `TiKV PD -> TiKV -> FS Master transfer_state_store`

Hard boundary:

- `etcd`, `Greptime`, and `TiKV` are external dependencies
- `Master` and `Owner Client` are Fluxon-native roles

If the control plane is missing, `Master` is unavailable. If `Owner Client` is missing, `FluxonKvClientConfig({...}) -> new_store(...)` cannot attach to the shared-memory pool. TiKV is not needed for the minimum KV read/write path, but it is required for features that depend on `transfer_state_store`.

### Start `etcd`, `Greptime`, and `TiKV`

First prepare the runtime package described in [User - 0 - Installation](<./User - 0 - Installation.md>) and confirm these files exist:

- `ext_images/etcd/etcd`
- `ext_images/etcd/etcdctl`
- `ext_images/etcd/start.sh`
- `ext_images/greptime/greptime`
- `ext_images/greptime/start.sh`
- `ext_images/tikv/pd-server`
- `ext_images/tikv/tikv-server`
- `ext_images/tikv/start_pd.sh`
- `ext_images/tikv/start_tikv.sh`

All startup scripts use the same contract:

- `--config/-c`: shell config file
- `--workdir/-w`: local work directory

Example `etcd` config:

```bash
cat > /tmp/etcd.config.sh <<'EOF'
ETCD_ARGS=(
  --data-dir "$WORKDIR/etcd-data"
  --name etcd0
  --advertise-client-urls "http://127.0.0.1:2379"
  --listen-client-urls "http://0.0.0.0:2379"
  --listen-peer-urls "http://0.0.0.0:2380"
  --initial-advertise-peer-urls "http://127.0.0.1:2380"
  --initial-cluster "etcd0=http://127.0.0.1:2380"
  --initial-cluster-token "etcd-cluster"
  --initial-cluster-state "new"
  --auto-compaction-retention=1
)
EOF

bash ./ext_images/etcd/start.sh \
  --config /tmp/etcd.config.sh \
  --workdir /tmp/fluxon_service_plane_demo/etcd
```

Example `greptime` config:

```bash
cat > /tmp/greptime.config.sh <<'EOF'
GREPTIME_ARGS=(
  standalone start
  --data-home "$WORKDIR/greptimedb"
  --http-addr 0.0.0.0:34030
)
EOF

bash ./ext_images/greptime/start.sh \
  --config /tmp/greptime.config.sh \
  --workdir /tmp/fluxon_service_plane_demo/greptime
```

Example TiKV PD config:

```bash
cat > /tmp/pd.config.sh <<'EOF'
PD_ARGS=(
  --name pd0
  --data-dir "$WORKDIR/pd-data"
  --client-urls "http://127.0.0.1:12379"
  --advertise-client-urls "http://127.0.0.1:12379"
  --peer-urls "http://127.0.0.1:12380"
  --advertise-peer-urls "http://127.0.0.1:12380"
  --initial-cluster "pd0=http://127.0.0.1:12380"
  --log-file "$WORKDIR/pd.log"
)
EOF

bash ./ext_images/tikv/start_pd.sh \
  --config /tmp/pd.config.sh \
  --workdir /tmp/fluxon_service_plane_demo/tikv_pd
```

Example TiKV config:

```bash
cat > /tmp/tikv.config.sh <<'EOF'
TIKV_ARGS=(
  --pd-endpoints "127.0.0.1:12379"
  --addr "127.0.0.1:20160"
  --advertise-addr "127.0.0.1:20160"
  --status-addr "127.0.0.1:20180"
  --data-dir "$WORKDIR/tikv-data"
  --log-file "$WORKDIR/tikv.log"
)
EOF

bash ./ext_images/tikv/start_tikv.sh \
  --config /tmp/tikv.config.sh \
  --workdir /tmp/fluxon_service_plane_demo/tikv
```

These external services are not started by `fluxon_py.runtime`.

### `fluxon_py.runtime`

`fluxon_py.runtime` only manages Fluxon-native roles. It does not replace `etcd`, `Greptime`, or `TiKV`.

Common entrypoints:

- `start_kv_master_process(config=...)`
- `start_owner_kvclient_process(config=...)`

Optional wrapper log argument:

- `log_path=...`

This controls the Python wrapper subprocess `stdout/stderr` destination, not the service's own business-log directory.

If you are an installed-wheel user, prefer these Python entrypoints directly and pass Python dicts instead of depending on `examples/` path layout.

See `examples/start_master_owner.py` for the common local pattern:

- Default: start `Master + Owner Client`
- `--without-master`: start only `Owner Client` and attach to an existing `Master`

The same role chain is reused by MQ and FS. MQ-specific behavior belongs to [User - 4 - MQ Interface](<./User - 4 - MQ Interface.md>) and FS-specific behavior belongs to [User - 5 - FS Interface](<./User - 5 - FS Interface.md>).
