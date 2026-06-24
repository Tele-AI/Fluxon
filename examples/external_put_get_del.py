#!/usr/bin/env python3

from fluxon_py import FluxonKvClientConfig, new_store

INSTANCE_KEY = "demo_kv_external"
CLUSTER_NAME = "demo-kv-cluster"
SHARE_MEM_PATH = "/dev/shm/fluxon_kv_demo"


def main() -> None:
    cfg = FluxonKvClientConfig(
        {
            "instance_key": INSTANCE_KEY,
            "fluxonkv_spec": {
                "cluster_name": CLUSTER_NAME,
                "share_mem_path": SHARE_MEM_PATH,
            },
            "test_spec_config": {
                "disable_observability": True,
            },
        }
    )
    store = new_store(cfg).unwrap("new_store failed")

    key = "hello"
    value = b"world"

    try:
        store.put_blocking(key, {"payload": value}).unwrap("put_blocking failed")
        print(f"OK put key={key}")

        mem = store.get_blocking(key).unwrap("get_blocking failed")
        flat = mem.access().unwrap("mem.access failed")
        payload = flat["payload"]
        if not isinstance(payload, (bytes, bytearray)):
            raise RuntimeError(f"payload is not bytes: {type(payload)}")
        print(bytes(payload).decode("utf-8"))

        store.remove(key).unwrap("remove failed")
        print(f"OK del key={key}")

        exists = store.is_exist(key).unwrap("is_exist failed")
        if exists:
            raise RuntimeError(f"expected is_exist({key!r}) to be False after remove")
        print("OK is_exist after remove -> False")
    finally:
        # Release MemHolder-related references before close(); client shutdown waits
        # until all user-visible holders are dropped.
        if "flat" in locals():
            del flat
        if "mem" in locals():
            del mem
        store.close().unwrap("close failed")


if __name__ == "__main__":
    main()
