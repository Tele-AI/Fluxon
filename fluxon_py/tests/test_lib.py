
from math import log
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from fluxon_py.kvclient import KvClientType
import logging
from logging import Logger
from fluxon_py.logging import init_logger, update_log_level
import multiprocessing
from typing import List
from fluxon_py.kvclient.kvclient_interface import KvClient
from fluxon_py import FluxonKvClientConfig
from fluxon_py import new_store
from fluxon_py import ChanType, ChanRole, chan_new, chan_bind, MPSCChanConsumer, MPMCChanConsumer, MPSCChanProducer, MPMCChanProducer
from typing import Optional, Dict, Union
from fluxon_py import api_ext_chan
from typing import Any, Callable, Iterable
import signal
from typing import Tuple
import subprocess
import time
import itertools
import etcd3
from pathlib import Path

# Global upper bound for integration-style tests.
# Rationale: CI contention and transient transport churn can make the system progress slowly; using a
# 30-minute ceiling reduces flaky "timed out waiting ..." failures while still bounding hangs.
TEST_TIMEOUT_SECONDS = 30 * 60

from setup_and_pack.utils.repo_config_utils import (
    _verify_host_port,
    _verify_url,
    load_test_config_mapping,
    load_test_etcd_address_from_test_config,
    load_test_fluxon_cluster_name_from_test_config,
    load_test_fluxon_share_mem_path_from_test_config,
    load_test_kv_svc_type_from_test_config,
)

# --------------------
# Test config helpers (backed by setup_and_pack/utils/repo_config_utils.py)
# --------------------
def load_test_kv_svc_type(*, config_path: Optional[Path] = None) -> str:
    """Load kv_svc_type from fluxon_py/tests/test_config.yaml without defaults."""
    return load_test_kv_svc_type_from_test_config(config_path=config_path)


def load_test_kv_svc_ip(*, config_path: Optional[Path] = None) -> str:
    """Load test backend host from test_config.yaml."""
    etcd_addr = load_test_etcd_address_from_test_config(config_path=config_path)
    s, _port = _verify_host_port(etcd_addr, field="test_config.yaml.etcd_address")
    if "://" in s or not s:
        raise ValueError("test backend host should be a host or IP without scheme, e.g. 127.0.0.1")
    return s


def load_test_mooncake_metadata_server(*, config_path: Optional[Path] = None) -> str:
    """Load required mooncake metadata URL from test_config.yaml when mooncake tests are used."""
    cfg = load_test_config_mapping(config_path=config_path)
    raw = cfg.get("mooncake_metadata_server")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("test_config.yaml must define non-empty mooncake_metadata_server for mooncake tests")
    return _verify_url(raw.strip(), field="test_config.yaml.mooncake_metadata_server")


def load_test_mooncake_master_server_address(*, config_path: Optional[Path] = None) -> str:
    """Load required mooncake master address from test_config.yaml when mooncake tests are used."""
    cfg = load_test_config_mapping(config_path=config_path)
    raw = cfg.get("mooncake_master_server_address")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(
            "test_config.yaml must define non-empty mooncake_master_server_address for mooncake tests"
        )
    host, port = _verify_host_port(raw.strip(), field="test_config.yaml.mooncake_master_server_address")
    return f"{host}:{port}"


def load_test_fluxon_cluster_name(*, config_path: Optional[Path] = None) -> str:
    """Load required Fluxon cluster name from test_config.yaml."""
    return load_test_fluxon_cluster_name_from_test_config(config_path=config_path)


def load_test_fluxon_share_mem_path(*, config_path: Optional[Path] = None) -> str:
    """Load required Fluxon shared-memory path from test_config.yaml."""
    return load_test_fluxon_share_mem_path_from_test_config(config_path=config_path)


def load_test_chan_config(*, config_path: Optional[Path] = None) -> Dict[str, int]:
    """Return default chan_config for tests.

    Tests should specify their own values when needed; we do not read from config.
    """
    return {"capacity": 10, "ttl_seconds": 90, "weight": 1}

# Resolve ETCD host/port and test configuration via test_config.yaml (single explicit authority)
_ETCD_ADDRESS = load_test_etcd_address_from_test_config()
ETCD_HOST, _ETCD_PORT = _verify_host_port(_ETCD_ADDRESS, field="test_config.yaml.etcd_address")
ETCD_PORT = int(_ETCD_PORT)
KV_SVC_TYPE = load_test_kv_svc_type()
KV_SVC_IP = load_test_kv_svc_ip()
CHAN_CONFIG_TEST = load_test_chan_config()
MOONCAKE_METADATA_SERVER = (
    load_test_mooncake_metadata_server() if KV_SVC_TYPE == KvClientType.MOONCAKE.value else ""
)
MOONCAKE_MASTER_SERVER_ADDRESS = (
    load_test_mooncake_master_server_address() if KV_SVC_TYPE == KvClientType.MOONCAKE.value else ""
)

valid_backend_type = [e.value for e in KvClientType]
if KV_SVC_TYPE not in valid_backend_type:
    raise ValueError(f"Invalid kv_svc type: {KV_SVC_TYPE}, valid types: {valid_backend_type}")

BACKUP_LOGGING_ADD_HANDLER: Any = None
BACKUP_LOGGING_REMOVE_HANDLER: Any = None
BACKUP_LOGGING_BASIC_CONFIG: Any = None
BACKUP_LOGGING_SET_LEVEL: Any = None
BACKUP_LOGGING_ROOT: Any = None

# Global test arg matrix (can be overridden by tests if needed)
# Keys are argument names injected into callbacks; values are iterables of variants.
TEST_ARGMATRIX: Dict[str, Iterable[Any]] = {
    "prefetch": (0,),
}


def run_with_argmatrix(
    callback: Callable[..., Any],
    *,
    matrix: Optional[Dict[str, Iterable[Any]]] = None,
) -> None:
    """Run `callback` across a matrix of argument values.

    - When `matrix` is None, defaults to global `TEST_ARGMATRIX`.
    - Calls `callback(**kwargs)` for each cartesian combination of the matrix.
    - No environment variables are mutated; if subprocesses need values,
      pass them via CLI in the test code.
    """
    # Use provided matrix or fall back to global TEST_ARGMATRIX
    if matrix is None:
        matrix = TEST_ARGMATRIX

    keys = list(matrix.keys())
    lists = [list(matrix[k]) for k in keys]

    for combo in itertools.product(*lists):
        kwargs = {k: v for k, v in zip(keys, combo)}
        logging.info(
            "[test_lib] Running with matrix args: %s",
            ", ".join(f"{k}={kwargs[k]}" for k in keys),
        )
        callback(**kwargs)

def setup_test_environment(logger: Logger, print_config: bool = True):
    global BACKUP_LOGGING_ADD_HANDLER
    global BACKUP_LOGGING_REMOVE_HANDLER
    global BACKUP_LOGGING_BASIC_CONFIG
    global BACKUP_LOGGING_SET_LEVEL
    global BACKUP_LOGGING_ROOT

    # unset proxy
    proxy_env_vars = ["http_proxy", "https_proxy", "no_proxy"]
    for var in proxy_env_vars:
        if var in os.environ:
            del os.environ[var]
        upper_var=var.upper()
        if upper_var in os.environ:
            del os.environ[upper_var]

    # try:
    #     multiprocessing.set_start_method('spawn')
    # except RuntimeError as e:
    #     print(f"Failed to set start method to spawn: {e}, current start method: {multiprocessing.get_start_method()}")

    loglevel_str="DEBUG"
    os.environ["LOG_LEVEL"] = loglevel_str
    os.environ["FLUXON_LOG"] = loglevel_str
    LOGGING_LEVEL= logging.DEBUG
    update_log_level(loglevel_str)

    print("=================================================")
    print(f"LOGGING_LEVEL from test_lib.py: {LOGGING_LEVEL}")
    print("=================================================")
    if BACKUP_LOGGING_ADD_HANDLER is None:
        logging.basicConfig(level=LOGGING_LEVEL,)
        class FlushStreamHandler(logging.StreamHandler):
            def emit(self, record):
                super().emit(record)
                self.flush()  # Flush immediately for every log record

        handler = FlushStreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logging.root.handlers = []  # Clear old handlers
        logging.root.addHandler(handler)

        BACKUP_LOGGING_ADD_HANDLER = logging.root.addHandler
        BACKUP_LOGGING_REMOVE_HANDLER = logging.root.removeHandler
        BACKUP_LOGGING_BASIC_CONFIG = logging.basicConfig
        BACKUP_LOGGING_SET_LEVEL = logging.root.setLevel
        BACKUP_LOGGING_ROOT = logging.root
        def raise_error(*args, **kwargs):
            raise RuntimeError("Logging config is locked by test_lib.py!")
        logging.root.addHandler = raise_error
        logging.root.removeHandler = raise_error
        logging.basicConfig = raise_error
        logging.root.setLevel = raise_error
    else:
        if BACKUP_LOGGING_ROOT != logging.root:
            logging.root = BACKUP_LOGGING_ROOT
        BACKUP_LOGGING_SET_LEVEL(LOGGING_LEVEL)

    if print_config:
        logger.debug(f"=============== TEST CONFIGURATION ==============")
        logger.info(f"KV_SVC_TYPE: {KV_SVC_TYPE}")
        logger.info(f"KV_SVC_IP: {KV_SVC_IP}")
        logger.info(f"CHAN_CONFIG_TEST: {CHAN_CONFIG_TEST}")
        logger.info(f"ETCD_HOST: {ETCD_HOST}")
        logger.info(f"ETCD_PORT: {ETCD_PORT}")
        logger.info(f"=================================================")


def new_shared_stores(
    key_prefix: str,
    count: int,
    backend_type: str = "mooncake",
    ip: str = KV_SVC_IP,
    instance_suffix: str = "",
) -> List[KvClient]:
    """
    Create the requested number of shared store instances.

    Args:
        key_prefix: Prefix for store instance names.
        count: Number of stores to create.
        backend_type: Backend type ('mooncake' or 'fluxon').

    Returns:
        list: A list of store instances. The first is the main instance; the rest are forked shared instances.
    """
    if count <= 0:
        return []

    # Create the main instance and its shared instances.
    stores: List[KvClient] = []
    allowed = [e.value for e in KvClientType]
    if backend_type not in allowed:
        raise ValueError(f"Invalid backend_type {backend_type!r}, must be one of {allowed}")

    for i in range(count):
        instance_name = f"{key_prefix}_main" if i == 0 else f"{key_prefix}_{i}"
        if instance_suffix:
            instance_name = f"{instance_name}__{instance_suffix}"

        base_cfg = {
            "instance_key": instance_name,
            "contribute_to_cluster_pool_size": {
                "dram": 0,
                "vram": {},
            },
        }

        if backend_type == KvClientType.MOONCAKE.value:
            spec = {
                "mooncake_spec": {
                    "local_buffer_size": 16777216 * 10,
                    "metadata_server": MOONCAKE_METADATA_SERVER,
                    "master_server_address": MOONCAKE_MASTER_SERVER_ADDRESS,
                    "etcd_addresses": [f"{ETCD_HOST}:{ETCD_PORT}"],
                }
            }
        else:
            # Strictly require fluxon-specific fields from the shared test/example deployconf.
            cluster_name = load_test_fluxon_cluster_name()
            share_mem = load_test_fluxon_share_mem_path()
            spec = {
                "fluxonkv_spec": {
                    "cluster_name": cluster_name,
                    "share_mem_path": share_mem,
                }
            }

        config = FluxonKvClientConfig({**base_cfg, **spec})
        result = new_store(config)
        if not result.is_ok():
            raise ValueError(f"Failed to create {backend_type} store: {result.unwrap_error()}")
        # consume Result explicitly and append to return list
        stores.append(result.unwrap())

    return stores



def new_test_consumer(
    construct_type: str,
    store: KvClient,
    chan_id: Optional[str],
    chan_config: Dict[str, int],
    new_or_bind_key: Optional[str] = None,
    chan_type: ChanType = ChanType.MPSC
) -> Union[MPSCChanConsumer, MPMCChanConsumer]:
    if chan_id is not None:
        assert isinstance(chan_id, str) and chan_id.isdigit(), "chan_id should be a digit-only string or None"
    logging.debug(f"new_test_consumer with construct_type: {construct_type}, chan_id: {chan_id}, chan_config: {chan_config}, new_or_bind_key: {new_or_bind_key}, chan_type: {chan_type}")
    if construct_type == "cstyle":
        if chan_id is None:
            chan_id=chan_new(store, chan_config, chan_type, ChanRole.CONSUMER).unwrap()
        else:
            chan_bind(store, chan_config, chan_id, chan_type, ChanRole.CONSUMER).unwrap()
        # consumer=api_ext_chan.CHANID_2_NODES[chan_id]
        consumer=api_ext_chan.get_chan_by_id(chan_type, chan_id).unwrap()
        if chan_type == ChanType.MPMC:
            assert isinstance(consumer, MPMCChanConsumer), "chan_new should return a MPMCChanConsumer"
        else:
            assert isinstance(consumer, MPSCChanConsumer), "chan_new should return a MPSCChanConsumer"
        return consumer
    elif construct_type == "constructor":
        return MPSCChanConsumer(store, chan_id, chan_config)
    elif construct_type == "bind":
        assert chan_id is not None, "chan_id should not be None for construct_type = bind"
        chan_bind(store, chan_config, chan_id, chan_type, ChanRole.CONSUMER).unwrap()
        consumer = api_ext_chan.get_chan_by_id(chan_type, chan_id).unwrap()
        if chan_type == ChanType.MPMC:
            assert isinstance(consumer, MPMCChanConsumer), "chan_bind should return a MPMCChanConsumer"
        else:
            assert isinstance(consumer, MPSCChanConsumer), "chan_bind should return a MPSCChanConsumer"
        return consumer
    elif construct_type == "new_or_bind":
        assert new_or_bind_key is not None, "new_or_bind_key should not be None for construct_type = new_or_bind"
        res= api_ext_chan.new_or_bind_with_unique_key(
            store,chan_config,new_or_bind_key, chan_type, ChanRole.CONSUMER).unwrap()
        if chan_type == ChanType.MPMC:
            assert isinstance(res, MPMCChanConsumer), "new_or_bind_with_unique_key should return a MPMCChanConsumer"
        else:
            assert isinstance(res, MPSCChanConsumer), "new_or_bind_with_unique_key should return a MPSCChanConsumer"
        return res
    else:
        raise ValueError(f"Invalid construct type: {construct_type}")
    
def new_test_producer(
    construct_type: str,
    store: KvClient,
    chan_id: Optional[str],
    chan_config: Dict[str, int],
    new_or_bind_key: Optional[str] = None,
    chan_type: ChanType = ChanType.MPSC
) -> Union[MPSCChanProducer, MPMCChanProducer]:
    logging.debug(f"new_test_producer with construct_type: {construct_type}, chan_id: {chan_id}, chan_config: {chan_config}, new_or_bind_key: {new_or_bind_key}, chan_type: {chan_type}")
    if construct_type == "cstyle":
        if chan_id is None:
            chan_id=chan_new(store, chan_config, chan_type, ChanRole.PRODUCER).unwrap()
        else:
            chan_bind(store, chan_config, chan_id, chan_type, ChanRole.PRODUCER).unwrap()
        producer=api_ext_chan.get_chan_by_id(chan_type, chan_id).unwrap()
        if chan_type == ChanType.MPMC:
            assert isinstance(producer, MPMCChanProducer), "chan_new should return a MPMCChanProducer"
        else:
            assert isinstance(producer, MPSCChanProducer), "chan_new should return a MPSCChanProducer"
        return producer
    elif construct_type == "constructor":
        return MPSCChanProducer(store, chan_id, chan_config)
    elif construct_type == "bind":
        assert chan_id is not None, "chan_id should not be None for construct_type = bind"
        chan_bind(store, chan_config, chan_id, chan_type, ChanRole.PRODUCER).unwrap()
        producer = api_ext_chan.get_chan_by_id(chan_type, chan_id).unwrap()
        if chan_type == ChanType.MPMC:
            assert isinstance(producer, MPMCChanProducer), "chan_bind should return a MPMCChanProducer"
        else:
            assert isinstance(producer, MPSCChanProducer), "chan_bind should return a MPSCChanProducer"
        return producer
    elif construct_type == "new_or_bind":
        assert new_or_bind_key is not None, "new_or_bind_key should not be None for construct_type = new_or_bind"
        res= api_ext_chan.new_or_bind_with_unique_key(
            store,chan_config,new_or_bind_key, chan_type, ChanRole.PRODUCER).unwrap()
        if chan_type == ChanType.MPMC:
            assert isinstance(res, MPMCChanProducer), "new_or_bind_with_unique_key should return a MPMCChanProducer"
        else:
            assert isinstance(res, MPSCChanProducer), "new_or_bind_with_unique_key should return a MPSCChanProducer"
        return res
    else:
        raise ValueError(f"Invalid construct type: {construct_type}")
    

def check_chan_key_all_removed(etcd: etcd3.Etcd3Client, chan_type: ChanType):
    prefix=""
    if chan_type == ChanType.MPSC:
        prefix=f"/channels/"
    elif chan_type == ChanType.MPMC:
        prefix=f"/mpmc_channels/"
    else:
        raise ValueError(f"Invalid chan type: {chan_type}")
    res=list(etcd.get_prefix(prefix))
    kvs=list(map(lambda x: (x[1].key, x[0]), res))
    if len(kvs) > 0:
        errmsg=f"chan key should be removed, but {len(kvs)} keys found: {kvs}"
        logging.warning(errmsg)
        raise RuntimeError(errmsg)
    
def manully_unbind_if_cstyle_construct(chan_type: ChanType, chan_id: str, construct_type: str):
    if construct_type == "cstyle":
        api_ext_chan.chan_unbind(chan_type, chan_id).unwrap()


# --------------------
# Cross-test process housekeeping
# --------------------
def pre_kill_existing_test_processes(script_path: str, subcommands: List[str], sleep_after_seconds: int) -> None:
    """Backward-compatible wrapper: dispatch to the script-basename based implementation.

    Note: per current convention, the matching rule is simplified to "same script basename".
    """
    import os as _os
    base = _os.path.basename(script_path)
    pre_kill_existing_test_processes_by_script_name(base, sleep_after_seconds)


def pre_kill_existing_test_processes_by_script_name(script_basename: str, sleep_after_seconds: int) -> None:
    """Kill stale test processes by script file name only, then sleep for a window.

    Constraints and rationale:
    - Match by script basename only (e.g. `test_api_chan_mpsc_base.py`). This is called from the
      test "main entry" to avoid killing unrelated processes.
    - Protect the current process ancestor chain so tmux / shell launch wrappers are not mistaken
      for stale runs of the same script.
    - Signal order: SIGTERM -> bounded wait -> SIGKILL -> bounded wait. If still alive, fail fast.
    - No fallback/defaults. If we cannot enumerate processes via `ps`, treat it as a missing
      prerequisite and fail explicitly.
    - After successful cleanup, sleep `sleep_after_seconds` seconds unconditionally.
    """
    # Read `ps` output; if we cannot, treat it as a missing prerequisite and fail explicitly.
    try:
        out = subprocess.check_output(["ps", "-eo", "pid,ppid,args"], text=True)
    except Exception as err:  # noqa: BLE001
        explanation = (
            "Failed to run `ps -eo pid,ppid,args` to enumerate stale test processes. By convention this is not a "
            "tolerable 'degraded' mode; it is a missing prerequisite. Continuing without knowing the process "
            "state is likely to race with a previous run's leftover producer/consumer using the same "
            "channel/payload/lease, making the test outcome non-deterministic and potentially leaving "
            "cumulative side effects on the cluster (e.g. offsets not released, leases held, flow-control "
            f"thresholds misinterpreted). Therefore we fail explicitly here. Original error: {err}"
        )
        raise RuntimeError(explanation)

    lines = out.splitlines()
    header_skipped = False
    processes: List[Tuple[int, int, str]] = []
    pid_to_ppid: Dict[int, int] = {}
    for line in lines:
        if not header_skipped:
            header_skipped = True
            continue
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) != 3:
            continue
        pid_str, ppid_str, cmd = parts
        if not pid_str.isdigit() or not ppid_str.isdigit() or not cmd:
            continue
        pid = int(pid_str)
        ppid = int(ppid_str)
        processes.append((pid, ppid, cmd))
        pid_to_ppid[pid] = ppid

    current_pid = os.getpid()
    if current_pid not in pid_to_ppid:
        raise RuntimeError(
            "Current process pid was not present in `ps -eo pid,ppid,args` output. By convention we do not "
            "continue because we cannot safely distinguish stale processes from the current launch chain."
        )

    protected_pids: set[int] = set()
    walk_pid = current_pid
    while walk_pid > 0 and walk_pid not in protected_pids:
        protected_pids.add(walk_pid)
        parent_pid = pid_to_ppid.get(walk_pid)
        if parent_pid is None or parent_pid <= 0 or parent_pid == walk_pid:
            break
        walk_pid = parent_pid

    targets: List[int] = []
    for pid, _ppid, cmd in processes:
        if pid in protected_pids:
            continue
        # Match by script basename OR by module tail. A token can be either:
        # - an absolute/relative path ending with `/<name>` (script execution)
        # - exactly `<name>` (script execution)
        # - exactly `<stem>` or ending with `.<stem>` (module execution via `python -m ...`)
        # Use conservative matching to avoid false positives.
        tokens = cmd.split()
        name = script_basename
        stem = name[:-3] if name.endswith(".py") else name
        for t in tokens:
            if t.endswith("/" + name) or t == name:
                targets.append(pid)
                break
            if t == stem or t.endswith("." + stem):
                targets.append(pid)
                break

    if not targets:
        print(f"[test_lib] no stale test processes to kill for {script_basename}", flush=True)
        return

    print(f"[test_lib] found stale test processes: {targets}", flush=True)
    survivors = _signal_and_collect_survivors(targets, signal.SIGTERM, wait_seconds=5.0)
    if survivors:
        print(f"[test_lib] sending SIGKILL to: {survivors}", flush=True)
        survivors = _signal_and_collect_survivors(survivors, signal.SIGKILL, wait_seconds=2.0)
    if survivors:
        try:
            ps_detail = subprocess.check_output(
                ["ps", "-o", "pid,args", "-p", ",".join(str(p) for p in survivors)],
                text=True,
            )
        except Exception:  # noqa: BLE001
            ps_detail = "(failed to collect ps details)"
        raise RuntimeError(
            "Stale test processes are still alive after SIGKILL. By convention this indicates a missing "
            "prerequisite: we must clean them up before starting a new test run. We do not continue, and we "
            "do not introduce any fallback logic or implicit retries. Please inspect and terminate the "
            "following processes, then retry:\n" + ps_detail
        )

    print(f"[test_lib] sleeping {sleep_after_seconds}s after killing stale processes", flush=True)
    time.sleep(float(sleep_after_seconds))


def _signal_and_collect_survivors(pids: List[int], sig: signal.Signals, *, wait_seconds: float) -> List[int]:
    errors: List[Tuple[int, Exception]] = []
    for pid in pids:
        try:
            os.kill(pid, sig)
        except ProcessLookupError as err:
            errors.append((pid, err))
        except PermissionError as err:
            errors.append((pid, err))
    deadline = time.time() + float(wait_seconds)
    survivors: List[int] = []
    for pid in pids:
        if any(pid == e[0] and isinstance(e[1], ProcessLookupError) for e in errors):
            continue
        while True:
            alive = _is_alive(pid)
            if not alive:
                break
            if time.time() >= deadline:
                survivors.append(pid)
                break
            time.sleep(0.1)

    if errors:
        details = ", ".join([f"pid={pid}, err={type(err).__name__}: {err}" for pid, err in errors])
        print(
            "\n".join(
                [
                    "[test_lib][WARNING] Expected exceptions while terminating stale test processes.",
                    "[test_lib][WARNING] Causes:",
                    "[test_lib][WARNING] - ProcessLookupError/ESRCH: process exited before/during signal.",
                    "[test_lib][WARNING] - PermissionError/EPERM: insufficient permission in shared/restricted environments.",
                    "[test_lib][WARNING] Policy: no retries, no privilege escalation, no fallback paths.",
                    "[test_lib][WARNING] Convergent flow: send signal -> bounded wait -> liveness probe; survivors are surfaced.",
                    f"[test_lib][WARNING] Details: {details}",
                ]
            ),
            flush=True,
        )
    return survivors


def _is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return os.path.exists(f"/proc/{pid}")
    return True
