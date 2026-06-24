#!/usr/bin/env python3

import argparse
import signal

from fluxon_py import FluxonKvClientConfig, new_store

RPC_SERVER_INSTANCE_KEY = "demo_rpc_server"
RPC_CLIENT_INSTANCE_KEY = "demo_rpc_client"
CLUSTER_NAME = "demo-kv-cluster"
SHARE_MEM_PATH = "/dev/shm/fluxon_kv_demo"


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal node-to-node RPC example")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="Start one RPC handler process")
    serve_parser.add_argument("--instance-key", default=RPC_SERVER_INSTANCE_KEY, help="RPC handler instance key")

    call_parser = subparsers.add_parser("call", help="Call one RPC handler and print the counter")
    call_parser.add_argument("--instance-key", default=RPC_CLIENT_INSTANCE_KEY, help="RPC caller instance key")
    call_parser.add_argument(
        "--target-instance-key",
        default=RPC_SERVER_INSTANCE_KEY,
        help="Target RPC handler instance key",
    )

    args = parser.parse_args()
    if args.command == "serve":
        run_server(instance_key=args.instance_key)
        return
    if args.command == "call":
        run_client(instance_key=args.instance_key, target_instance_key=args.target_instance_key)
        return
    raise AssertionError("unreachable")


def _build_config(*, instance_key: str) -> FluxonKvClientConfig:
    return FluxonKvClientConfig(
        {
            "instance_key": instance_key,
            "fluxonkv_spec": {
                "cluster_name": CLUSTER_NAME,
                "share_mem_path": SHARE_MEM_PATH,
            },
            "test_spec_config": {
                "disable_observability": True,
            },
        }
    )


def run_server(*, instance_key: str) -> None:
    store = new_store(_build_config(instance_key=instance_key)).unwrap("new_store failed")
    count = 0

    def count_handler(from_node_id: str, payload: dict) -> dict:
        nonlocal count
        count += 1
        print(f"rpc from={from_node_id} payload={payload} count={count}")
        return {
            "count": count,
            "payload": payload["payload"],
        }

    try:
        store.rpc_register("/count", count_handler).unwrap("rpc_register failed")
        print(f"[rpc] handler ready instance_key={instance_key}")
        print("[rpc] waiting for Ctrl-C")
        signal.pause()
    except KeyboardInterrupt:
        print("[rpc] caught Ctrl-C, stopping handler")
        raise SystemExit(130)
    finally:
        store.close().unwrap("close failed")


def run_client(*, instance_key: str, target_instance_key: str) -> None:
    store = new_store(_build_config(instance_key=instance_key)).unwrap("new_store failed")
    try:
        resp = (
            store.rpc_call(target_instance_key, "/count", {"payload": b"hi"})
            .unwrap("rpc_call failed")
            .wait()
            .unwrap("rpc wait failed")
        )
        print(resp["count"])
    finally:
        store.close().unwrap("close failed")


if __name__ == "__main__":
    main()
