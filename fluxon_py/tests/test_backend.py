"""Backend tests for Fluxon and Mooncake KV clients."""

import argparse
import ctypes
import os
import sys
import time
import uuid
from typing import Callable, List, Optional, Tuple, Union

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from fluxon_py.api_error import (
    ApiError,
    EtcdError,
    KeyAlreadyExistsError,
    KeyNotFoundError,
    TransportName,
    TransportUser,
)
from fluxon_py.kvclient.fluxon import FluxonKVCacheStore
from fluxon_py.kvclient.kvclient_interface import KvClient, MemHolder, PutOptionalArgs
from fluxon_py.kvclient.nonzerocopy_encode import (
    DLPackBytesView,
    _dlpack_cpu_tensor_info,
    decode_flat_kv_dict,
    encode_flat_kv_dict,
    wrap_flat_dict_dlpack,
)
from fluxon_py.logging import init_logger
from fluxon_py.tests.test_lib import KV_SVC_TYPE, new_shared_stores, setup_test_environment
from fluxon_py.tool import import_fluxon_pyo3_local

logging = init_logger("test_backend")
TEST_INSTANCE_SUFFIX = ""
RPC_CLUSTER_READY_WAIT_SECONDS = 2.5


class _StubEtcdConfigApi:
    """Minimal etcd config provider for transport-identity tests."""

    def get_etcd_config(self) -> List[str]:
        return ["127.0.0.1:2379"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Fluxon backend test runner")
    parser.add_argument("--test-id", help="Run only the named test id")
    parser.add_argument("--instance-suffix", default="", help="Append suffix to test store instance_key names")
    args = parser.parse_args()
    global TEST_INSTANCE_SUFFIX
    TEST_INSTANCE_SUFFIX = args.instance_suffix.strip()

    try:
        total_test = _build_test_plan(args.test_id)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 2

    setup_test_environment(logging, True)
    total_result: List[bool] = []
    green = "\x1b[32;20m"
    red = "\x1b[31;20m"
    reset = "\x1b[0m"

    for index, (test_id, test) in enumerate(total_test):
        logging.info(f"[{index + 1}/{len(total_test)}] Test: {test_id} ({test.__name__}) started")
        result = test()
        if result == 0:
            total_result.append(True)
            logging.info(f"[{index + 1}/{len(total_test)}] Test: {test_id} ({test.__name__}) passed.")
        else:
            total_result.append(False)
            logging.error(f"[{index + 1}/{len(total_test)}] Test: {test_id} ({test.__name__}) failed with err_code: {result}")

    print("")
    print("=" * 30 + " Final Report " + "=" * 30)
    for index, (test_id, test) in enumerate(total_test):
        if total_result[index]:
            print(f"{green} SUCCESS!  Test {index} 		 {test_id} ({test.__name__}) {reset}")
        else:
            print(f"{red} FAILED!  Test {index} 		 {test_id} ({test.__name__}) {reset}")
    print("=" * 30 + "==============" + "=" * 30)
    return 0 if all(total_result) else 1


def _build_test_plan(selected_test_id: Optional[str]) -> List[Tuple[str, Callable[[], int]]]:
    common_tests: List[Tuple[str, Callable[[], int]]] = [
        ("grpc_transport_identity", test_grpc_transport_identity),
        ("dlpack_codec_roundtrip", test_dlpack_codec_roundtrip),
        ("rust_flat_dict_dlpack_decode", test_rust_flat_dict_dlpack_decode),
        ("basic_put_and_get", test_basic_put_and_get),
        ("put_and_get_flat_dict", test_put_and_get_flat_dict),
        ("put_and_get_dlpack_tensor", test_put_and_get_dlpack_tensor),
        ("rpc_roundtrip_dlpack_tensor", test_rpc_roundtrip_dlpack_tensor),
        ("key_not_found", test_key_not_found),
    ]
    if KV_SVC_TYPE.lower() == "fluxon":
        common_tests.append(("put_reject_if_exists", test_put_reject_if_exists))
    if KV_SVC_TYPE.lower() == "mooncake":
        common_tests.append(("mooncake_renew_put_and_get", test_mooncake_renew_put_and_get))
    if selected_test_id is None:
        return common_tests

    for test_id, test in common_tests:
        if test_id == selected_test_id:
            return [(test_id, test)]
    available = ", ".join(test_id for test_id, _ in common_tests)
    raise ValueError(f"unknown --test-id: {selected_test_id}. Available: {available}")


def handle_error(op: str, error: Union[ApiError, Exception, None], store: KvClient):
    """Log the error and close the store explicitly."""
    logging.error(f"{op} failed with error: {error}")
    logging.error("Terminating...")
    try:
        res = store.close()
        if res.is_ok():
            _ = res.unwrap()
            return
        err = res.unwrap_error()
        logging.warning(f"Failed to close the store (api error): {err}")
        return
    except Exception as exc:
        logging.warning(f"Failed to close the store with Exception: {exc}")
        return


def close_store_or_log(op: str, store: KvClient) -> bool:
    """Close the store and keep the close outcome visible in test logs."""
    try:
        result = store.close()
    except Exception as exc:
        logging.error(f"{op}: store.close() raised exception: {exc}")
        return False
    if not result.is_ok():
        logging.error(f"{op}: store.close() failed: {result.unwrap_error()}")
        return False
    _ = result.unwrap()
    return True


def close_stores_or_log(op: str, stores: List[KvClient]) -> bool:
    """Close a store list and keep every close failure visible."""
    ok = True
    for index, store in enumerate(stores):
        if not close_store_or_log(f"{op} store[{index}]", store):
            ok = False
    return ok


def test_grpc_transport_identity() -> int:
    """Etcd grpc failures should expose the transport user to the caller."""
    from fluxon_py import api_ext_chan as api_ext_chan_mod
    from fluxon_py._api_ext_chan import mpmc as mpmc_mod

    fake_api = _StubEtcdConfigApi()

    def _raise_grpc_ctor(*args: object, **kwargs: object) -> object:
        raise RuntimeError("grpc dial exploded")

    original_api_ext_chan_client = api_ext_chan_mod.etcd3.client
    original_mpmc_client = mpmc_mod.etcd3.client
    api_ext_chan_mod.etcd3.client = _raise_grpc_ctor
    mpmc_mod.etcd3.client = _raise_grpc_ctor
    try:
        cases = [
            ("api_ext_chan", api_ext_chan_mod.new_etcd_client(fake_api)),
            ("mpmc", mpmc_mod.new_etcd_client(fake_api)),
        ]
        for case_name, result in cases:
            if result.is_ok():
                logging.error("%s new_etcd_client should fail when grpc ctor raises", case_name)
                return 1
            err = result.unwrap_error()
            if not isinstance(err, EtcdError):
                logging.error("%s expected EtcdError, got %r", case_name, err)
                return 2
            if err.transport != TransportName.GRPC:
                logging.error("%s expected transport grpc, got %r", case_name, err.transport)
                return 3
            if err.transport_user != TransportUser.ETCD:
                logging.error("%s expected transport_user etcd, got %r", case_name, err.transport_user)
                return 4
            rendered = str(err)
            if "transport='grpc'" not in rendered or "transport_user='etcd'" not in rendered:
                logging.error("%s rendered error missing transport identity: %s", case_name, rendered)
                return 5
            fields = err.to_dict().get("fields")
            if not isinstance(fields, dict):
                logging.error("%s error fields missing from to_dict: %r", case_name, err.to_dict())
                return 6
            if fields.get("transport") != "grpc" or fields.get("transport_user") != "etcd":
                logging.error("%s to_dict missing transport identity: %r", case_name, fields)
                return 7
        return 0
    finally:
        api_ext_chan_mod.etcd3.client = original_api_ext_chan_client
        mpmc_mod.etcd3.client = original_mpmc_client


def test_basic_put_and_get() -> int:
    """Test for basic put and get."""
    logging.debug(f"start new {KV_SVC_TYPE} store.")
    store = new_shared_stores(
        key_prefix="test_put",
        count=1,
        backend_type=KV_SVC_TYPE,
        instance_suffix=TEST_INSTANCE_SUFFIX,
    )[0]
    logging.debug(f"new share store success with Backend type: {KV_SVC_TYPE}")

    key = "/test_1"
    value_dict = {"v": b"test_1"}
    result = store.put(key, value_dict)
    if not result.is_ok():
        handle_error("put", result.unwrap_error(), store)
        return 1
    wait_result = result.unwrap().wait()
    if not wait_result.is_ok():
        handle_error("put future", wait_result.unwrap_error(), store)
        return 2
    _ = wait_result.unwrap()
    logging.info("Successfully put one.")

    get_result = store.get(key)
    if not get_result.is_ok():
        handle_error("get", get_result.unwrap_error(), store)
        return 3
    wait_result = get_result.unwrap().wait()
    if not wait_result.is_ok():
        handle_error("get future", wait_result.unwrap_error(), store)
        return 4
    wait_success = wait_result.unwrap()
    if not isinstance(wait_success, MemHolder):
        logging.error(
            "It seems that the type is not correct as expected."
            f"Expected: MemHolder, Get: {type(wait_success)}"
        )
        return 5
    got = wait_success.access().unwrap().get("v")
    if got != value_dict["v"]:
        logging.error(f"What did you get?\n Expected: {value_dict['v']!r}, Get: {got!r}")
        return 6
    logging.info("Successfully get! Test completed.")
    if not close_store_or_log("basic_put_and_get success path", store):
        return 7
    return 0


def test_put_reject_if_exists() -> int:
    """Exercise create-only and overwrite semantics through every native PUT binding."""
    if KV_SVC_TYPE.lower() != "fluxon":
        return 0

    store = new_shared_stores(
        key_prefix="test_put_reject_if_exists",
        count=1,
        backend_type=KV_SVC_TYPE,
        instance_suffix=TEST_INSTANCE_SUFFIX,
    )[0]
    assert isinstance(store, FluxonKVCacheStore)
    key_prefix = f"/test_put_reject_if_exists/{uuid.uuid4().hex}"
    exists_code = -KeyAlreadyExistsError(message="duplicate key").code()

    def submit_put(
        mode: str, key: str, payload: bytes, opts: Optional[PutOptionalArgs]
    ) -> int:
        # Keep the raw buffer alive until each synchronous completion boundary.
        buffer = ctypes.create_string_buffer(payload)
        ptr = ctypes.addressof(buffer)
        if mode == "async":
            result = store.put(key, {"payload": payload}, opts=opts).unwrap().wait()
        elif mode == "blocking":
            result = store.put_blocking(key, {"payload": payload}, opts=opts)
        elif mode == "batch":
            result = store.batch_put_blocking([key], [{"payload": payload}], opts=opts)[0]
        elif mode == "payload_async":
            result = store.put_payload_from_ptr(key, ptr, len(payload), opts=opts).unwrap().wait()
        elif mode == "payload_blocking":
            result = store.put_payload_from_ptr_blocking(key, ptr, len(payload), opts=opts)
        elif mode == "batch_payload":
            return store.batch_put_from([key], [ptr], [len(payload)], opts=opts)[0]
        else:
            raise AssertionError(f"unknown PUT mode: {mode}")
        if result.is_ok():
            result.unwrap()
            return 0
        error = result.unwrap_error()
        assert isinstance(error, KeyAlreadyExistsError), error
        return -error.code()

    failed = False
    try:
        for mode in (
            "async",
            "blocking",
            "batch",
            "payload_async",
            "payload_blocking",
            "batch_payload",
        ):
            key = f"{key_prefix}/{mode}"
            create_only = PutOptionalArgs(reject_if_exists=True)
            assert submit_put(mode, key, b"original", create_only) == 0, mode
            duplicate_code = submit_put(mode, key, b"rejected", create_only)
            # Raw batch reports native KV ApiError codes; Result paths map to Python errors.
            expected_code = -123 if mode == "batch_payload" else exists_code
            assert duplicate_code == expected_code, (mode, duplicate_code, expected_code)
            assert (
                store.get_blocking(key).unwrap().access().unwrap()["payload"] == b"original"
            ), mode
            assert submit_put(mode, key, b"default overwrite", None) == 0, mode
            assert (
                store.get_blocking(key).unwrap().access().unwrap()["payload"]
                == b"default overwrite"
            ), mode
            assert submit_put(
                mode, key, b"explicit overwrite", PutOptionalArgs(reject_if_exists=False)
            ) == 0, mode
            assert (
                store.get_blocking(key).unwrap().access().unwrap()["payload"]
                == b"explicit overwrite"
            ), mode
    except Exception:
        logging.exception("PUT create-only/overwrite contract failed")
        failed = True
    finally:
        if not close_store_or_log("put_reject_if_exists", store):
            failed = True
    return 1 if failed else 0


def test_put_and_get_flat_dict() -> int:
    """Put a flat dict value and verify msgpack roundtrip."""
    logging.debug(f"start new {KV_SVC_TYPE} store.")
    store = new_shared_stores(
        key_prefix="test_put_flat_dict",
        count=1,
        backend_type=KV_SVC_TYPE,
        instance_suffix=TEST_INSTANCE_SUFFIX,
    )[0]

    key = "/test_flat_dict_1"
    value_dict = {
        "i": 123,
        "s": "hello",
        "b": True,
        "payload": b"\x01\x02\x03\x04",
    }

    result = store.put(key, value_dict)
    if not result.is_ok():
        handle_error("put_flat_dict", result.unwrap_error(), store)
        return 1
    wait_result = result.unwrap().wait()
    if not wait_result.is_ok():
        handle_error("put_flat_dict future", wait_result.unwrap_error(), store)
        return 2
    _ = wait_result.unwrap()

    get_result = store.get(key)
    if not get_result.is_ok():
        handle_error("get_flat_dict", get_result.unwrap_error(), store)
        return 3
    wait_result = get_result.unwrap().wait()
    if not wait_result.is_ok():
        handle_error("get_flat_dict future", wait_result.unwrap_error(), store)
        return 4
    holder = wait_result.unwrap()
    if not isinstance(holder, MemHolder):
        logging.error(f"Expected MemHolder, got {type(holder)}")
        return 5
    expected = {
        "i": 123,
        "s": "hello",
        "b": True,
        "payload": b"\x01\x02\x03\x04",
    }
    got = holder.access().unwrap()
    if got != expected:
        logging.error(f"Flat dict mismatch. Expected: {expected}, got: {got}")
        return 6
    if not close_store_or_log("put_and_get_flat_dict success path", store):
        return 7
    return 0


def test_dlpack_codec_roundtrip() -> int:
    payload = b"\x01\x02\x03\x04"
    tensor = DLPackBytesView(payload, dtype_code=1, bits=8, lanes=1, shape=(4,))
    encoded = encode_flat_kv_dict({"t": tensor})
    if not encoded.is_ok():
        logging.error(f"encode dlpack failed: {encoded.unwrap_error()}")
        return 1

    decoded = decode_flat_kv_dict(encoded.unwrap())
    if not decoded.is_ok():
        logging.error(f"decode dlpack failed: {decoded.unwrap_error()}")
        return 2

    wrapped = wrap_flat_dict_dlpack(decoded.unwrap())
    if not wrapped.is_ok():
        logging.error(f"wrap dlpack failed: {wrapped.unwrap_error()}")
        return 3

    d = wrapped.unwrap()
    t = d.get("t")
    if t is None or not hasattr(t, "__dlpack__"):
        logging.error(f"Expected dlpack value for 't', got {type(t)}")
        return 4

    info = _dlpack_cpu_tensor_info(t)  # type: ignore[arg-type]
    if not info.is_ok():
        logging.error(f"dlpack tensor info failed: {info.unwrap_error()}")
        return 5

    ptr, nbytes, capsule, dtype_code, bits, lanes, shape = info.unwrap()
    _ = capsule
    got_bytes = bytes(ctypes.string_at(ptr, nbytes))
    if got_bytes != payload:
        logging.error(f"dlpack payload mismatch: expected {payload!r} got {got_bytes!r}")
        return 6
    if (dtype_code, bits, lanes, shape) != (1, 8, 1, (4,)):
        logging.error(f"dlpack meta mismatch: {(dtype_code, bits, lanes, shape)}")
        return 7
    return 0


def test_rust_flat_dict_dlpack_decode() -> int:
    fluxon_pyo3 = import_fluxon_pyo3_local()
    payload = b"\x01\x02\x03\x04"
    tensor = DLPackBytesView(payload, dtype_code=1, bits=8, lanes=1, shape=(4,))
    encoded = encode_flat_kv_dict({"t": tensor})
    if not encoded.is_ok():
        logging.error(f"encode rust dlpack decode payload failed: {encoded.unwrap_error()}")
        return 1

    decoded = fluxon_pyo3.decode_flat_dict_payload(encoded.unwrap())
    if not decoded.is_ok():
        logging.error(f"rust flat dict decode failed: {decoded.unwrap_error()}")
        return 2

    value = decoded.unwrap()
    t = value.get("t")
    if t is None or not hasattr(t, "__dlpack__"):
        logging.error(f"Expected rust-decoded dlpack value for 't', got {type(t)}")
        return 3

    info = _dlpack_cpu_tensor_info(t)  # type: ignore[arg-type]
    if not info.is_ok():
        logging.error(f"rust-decoded dlpack tensor info failed: {info.unwrap_error()}")
        return 4

    ptr, nbytes, capsule, dtype_code, bits, lanes, shape = info.unwrap()
    _ = capsule
    got_bytes = bytes(ctypes.string_at(ptr, nbytes))
    if got_bytes != payload:
        logging.error(f"rust-decoded dlpack payload mismatch: expected {payload!r} got {got_bytes!r}")
        return 5
    if (dtype_code, bits, lanes, shape) != (1, 8, 1, (4,)):
        logging.error(f"rust-decoded dlpack meta mismatch: {(dtype_code, bits, lanes, shape)}")
        return 6
    return 0


def test_put_and_get_dlpack_tensor() -> int:
    logging.debug(f"start new {KV_SVC_TYPE} store.")
    store = new_shared_stores(
        key_prefix="test_put_dlpack_tensor",
        count=1,
        backend_type=KV_SVC_TYPE,
        instance_suffix=TEST_INSTANCE_SUFFIX,
    )[0]

    key = "/test_dlpack_tensor_1"
    payload = b"\x01\x02\x03\x04"
    value_dict = {
        "t": DLPackBytesView(payload, dtype_code=1, bits=8, lanes=1, shape=(4,)),
    }

    result = store.put(key, value_dict)
    if not result.is_ok():
        handle_error("put_dlpack_tensor", result.unwrap_error(), store)
        return 1
    wait_result = result.unwrap().wait()
    if not wait_result.is_ok():
        handle_error("put_dlpack_tensor future", wait_result.unwrap_error(), store)
        return 2
    _ = wait_result.unwrap()

    get_result = store.get(key)
    if not get_result.is_ok():
        handle_error("get_dlpack_tensor", get_result.unwrap_error(), store)
        return 3
    wait_result = get_result.unwrap().wait()
    if not wait_result.is_ok():
        handle_error("get_dlpack_tensor future", wait_result.unwrap_error(), store)
        return 4
    holder = wait_result.unwrap()
    if not isinstance(holder, MemHolder):
        logging.error(f"Expected MemHolder, got {type(holder)}")
        return 5

    accessed = holder.access()
    if not accessed.is_ok():
        handle_error("access_dlpack_tensor", accessed.unwrap_error(), store)
        return 6
    value = accessed.unwrap()
    tensor = value.get("t")
    if tensor is None or not hasattr(tensor, "__dlpack__"):
        logging.error(f"Expected dlpack value for 't', got {type(tensor)}")
        return 7

    info = _dlpack_cpu_tensor_info(tensor)  # type: ignore[arg-type]
    if not info.is_ok():
        logging.error(f"kv dlpack tensor info failed: {info.unwrap_error()}")
        return 8

    ptr, nbytes, capsule, dtype_code, bits, lanes, shape = info.unwrap()
    _ = capsule
    got_bytes = bytes(ctypes.string_at(ptr, nbytes))
    if got_bytes != payload:
        logging.error(f"kv dlpack payload mismatch: expected {payload!r} got {got_bytes!r}")
        return 9
    if (dtype_code, bits, lanes, shape) != (1, 8, 1, (4,)):
        logging.error(f"kv dlpack meta mismatch: {(dtype_code, bits, lanes, shape)}")
        return 10
    if not close_store_or_log("put_and_get_dlpack_tensor success path", store):
        return 11
    return 0


def test_rpc_roundtrip_dlpack_tensor() -> int:
    stores = new_shared_stores(
        key_prefix="test_rpc_roundtrip_dlpack_tensor",
        count=2,
        backend_type=KV_SVC_TYPE,
        instance_suffix=TEST_INSTANCE_SUFFIX,
    )
    server_store = stores[0]
    client_store = stores[1]
    server_key_result = server_store.instance_key()
    if not server_key_result.is_ok():
        logging.error(f"rpc server instance_key failed: {server_key_result.unwrap_error()}")
        if not close_stores_or_log("rpc_roundtrip_dlpack_tensor instance_key failure", stores):
            return 2
        return 1
    server_instance_key = server_key_result.unwrap()

    payload = b"\x08\x07\x06\x05\x04\x03\x02\x01"

    def _rpc_handler(from_node_id: str, request: dict) -> dict:
        if not isinstance(from_node_id, str):
            raise RuntimeError(f"from_node_id must be str, got {type(from_node_id)}")
        if not isinstance(request, dict):
            raise RuntimeError(f"request must be dict, got {type(request)}")
        return {
            "reply_tensor": DLPackBytesView(payload, dtype_code=1, bits=8, lanes=1, shape=(len(payload),)),
            "echo_seq": request.get("seq"),
            "echo_ok": True,
        }

    register_result = server_store.rpc_register("/dlpack_roundtrip", _rpc_handler)
    if not register_result.is_ok():
        logging.error(f"rpc_register failed: {register_result.unwrap_error()}")
        if not close_stores_or_log("rpc_roundtrip_dlpack_tensor register failure", stores):
            return 4
        return 3
    _ = register_result.unwrap()
    time.sleep(RPC_CLUSTER_READY_WAIT_SECONDS)

    call_result = client_store.rpc_call(
        server_instance_key,
        "/dlpack_roundtrip",
        {"seq": 42},
    )
    if not call_result.is_ok():
        logging.error(f"rpc_call failed: {call_result.unwrap_error()}")
        if not close_stores_or_log("rpc_roundtrip_dlpack_tensor call failure", stores):
            return 6
        return 5

    wait_result = call_result.unwrap().wait()
    if not wait_result.is_ok():
        logging.error(f"rpc wait failed: {wait_result.unwrap_error()}")
        if not close_stores_or_log("rpc_roundtrip_dlpack_tensor wait failure", stores):
            return 8
        return 7

    response = wait_result.unwrap()
    if not isinstance(response, dict):
        logging.error(f"rpc response must be dict, got {type(response)}")
        if not close_stores_or_log("rpc_roundtrip_dlpack_tensor response type failure", stores):
            return 10
        return 9
    if response.get("echo_seq") != 42 or response.get("echo_ok") is not True:
        logging.error(f"rpc response scalar fields mismatch: {response}")
        if not close_stores_or_log("rpc_roundtrip_dlpack_tensor scalar failure", stores):
            return 12
        return 11

    tensor = response.get("reply_tensor")
    if tensor is None or not hasattr(tensor, "__dlpack__"):
        logging.error(f"Expected dlpack tensor response, got {type(tensor)}")
        if not close_stores_or_log("rpc_roundtrip_dlpack_tensor tensor missing", stores):
            return 14
        return 13

    info = _dlpack_cpu_tensor_info(tensor)  # type: ignore[arg-type]
    if not info.is_ok():
        logging.error(f"rpc dlpack tensor info failed: {info.unwrap_error()}")
        if not close_stores_or_log("rpc_roundtrip_dlpack_tensor info failure", stores):
            return 16
        return 15

    ptr, nbytes, capsule, dtype_code, bits, lanes, shape = info.unwrap()
    _ = capsule
    got_bytes = bytes(ctypes.string_at(ptr, nbytes))
    if got_bytes != payload:
        logging.error(f"rpc dlpack payload mismatch: expected {payload!r} got {got_bytes!r}")
        if not close_stores_or_log("rpc_roundtrip_dlpack_tensor payload mismatch", stores):
            return 18
        return 17
    if (dtype_code, bits, lanes, shape) != (1, 8, 1, (len(payload),)):
        logging.error(f"rpc dlpack meta mismatch: {(dtype_code, bits, lanes, shape)}")
        if not close_stores_or_log("rpc_roundtrip_dlpack_tensor meta mismatch", stores):
            return 20
        return 19
    if not close_stores_or_log("rpc_roundtrip_dlpack_tensor success path", stores):
        return 21
    return 0


def test_mooncake_renew_put_and_get() -> int:
    """Mooncake-specific renew behavior after internal client close."""
    if KV_SVC_TYPE.lower() != "mooncake":
        logging.error(f"test_mooncake_renew_put_and_get requires mooncake backend, got {KV_SVC_TYPE}")
        return 1

    from fluxon_py.kvclient.mooncake import MooncakeStore

    logging.debug(f"start new {KV_SVC_TYPE} store.")
    store = new_shared_stores(
        key_prefix="test_renew_put",
        count=1,
        backend_type=KV_SVC_TYPE,
        instance_suffix=TEST_INSTANCE_SUFFIX,
    )[0]
    if not isinstance(store, MooncakeStore):
        logging.error(f"Expected MooncakeStore, got {type(store)}")
        return 1

    logging.info("Closing store...")
    store._store.close()
    time.sleep(2)

    key = "/test_2"
    value_dict = {"v": b"test_2"}
    result = store.put(key, value_dict)
    if not result.is_ok():
        handle_error("put", result.unwrap_error(), store)
        return 2
    wait_result = result.unwrap().wait()
    if not wait_result.is_ok():
        handle_error("put future", wait_result.unwrap_error(), store)
        return 3
    _ = wait_result.unwrap()
    logging.info("Successfully put one.")

    logging.info("Closing store...")
    store._store.close()
    time.sleep(2)

    get_result = store.get(key)
    if not get_result.is_ok():
        handle_error("get", get_result.unwrap_error(), store)
        return 4
    wait_result = get_result.unwrap().wait()
    if not wait_result.is_ok():
        handle_error("get future", wait_result.unwrap_error(), store)
        return 5
    wait_success = wait_result.unwrap()
    if not isinstance(wait_success, MemHolder):
        logging.error(
            "It seems that the type is not correct as expected."
            f"Expected: MemHolder, Get: {type(wait_success)}"
        )
        return 6
    got = wait_success.access().unwrap().get("v")
    if got != value_dict["v"]:
        logging.error(f"What did you get?\n Expected: {value_dict['v']!r}, Get: {got!r}")
        return 7
    logging.info("Successfully get! Test completed.")
    return 0


def test_key_not_found() -> int:
    """Observe the behavior when the key does not exist."""
    store = new_shared_stores(
        key_prefix="test_key_not_found",
        count=1,
        backend_type=KV_SVC_TYPE,
        instance_suffix=TEST_INSTANCE_SUFFIX,
    )[0]
    rc = 0
    result = store.get("/test_3")
    if not result.is_ok():
        logging.error("Future should be returned instead of an immediate error")
        rc = 1
    else:
        future_result = result.unwrap().wait()
        if future_result.is_ok():
            holder = future_result.unwrap()
            logging.error(f"Expected KeyNotFoundError, but got success payload: {holder}")
            rc = 2
        else:
            err = future_result.unwrap_error()
            if not isinstance(err, KeyNotFoundError):
                logging.error(f"Should be key not found error, but got {err}")
                rc = 3

    if not close_store_or_log("key_not_found", store):
        logging.error(f"key_not_found: store.close failed; previous_rc={rc}")
        return 4
    return rc


def test_async_put_and_get():
    """Reserved for future async coverage."""
    stores = new_shared_stores(
        "test_async_put_and_get",
        6,
        KV_SVC_TYPE,
        instance_suffix=TEST_INSTANCE_SUFFIX,
    )
    logging.info(f"successfully new 6 stores with KV_SVC_TYPE: {KV_SVC_TYPE}.")
    for index, store in enumerate(stores):
        if not close_store_or_log(f"test_async_put_and_get store[{index}]", store):
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
