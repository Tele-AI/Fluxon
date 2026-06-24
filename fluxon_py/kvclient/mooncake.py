"""
Mooncake backend implementation for the KV Cache API layer.

This module provides a wrapper around the original MooncakeDistributedStore
to conform to the unified KV Cache API.
"""

from calendar import c
from typing import Union, Optional, Any, Callable, Tuple, List, Dict
from concurrent.futures import ThreadPoolExecutor, Future
import threading
from .kvclient_interface import KvClient
from .backend_fallback_close import unregister_store_from_cleanup
from .kvclient_interface import (
    KvFuture,
    MemHolder,
    PutOptionalArgs,
    DLPacked,
    decode_flat_kv_dict,
)
from .nonzerocopy_encode import wrap_flat_dict_dlpack
from ..api_error import (
    BackendInitFailedError,
    Result,
    ApiError,
    OkNone,
    StoreInitFailedError,
    GeneralError,
    KeyNotFoundError,
    ResourceExhaustedError,
    InvalidArgumentError,
    BackendUnavailableError,
    ValueSizeChangedError,
    exception_to_error,
    try_new_error_from_mooncake,
)
from ..tool import import_fluxon_pyo3_local
from mooncake.store import MooncakeDistributedStore
from ..config import FluxonKvClientConfig
from ..tool import limit_rate
from .kvclient_interface import encode_flat_kv_dict
from ..logging import init_logger, update_log_level
import time
import typing
from threading import Lock
from readerwriterlock import rwlock

logging = init_logger()

MOONCAKE_NO_RENEW_ERROR_CODES = frozenset({-703, -705, -706, -707, -708})

# Preload bundled RDMA/provider DSOs via the fluxon_pyo3 bootstrap before importing
# or constructing Mooncake objects. Some hosts do not expose libibverbs globally,
# so relying on the system loader alone makes Mooncake owner startup node-dependent.
try:
    import_fluxon_pyo3_local()
except Exception:
    # Keep Mooncake import behavior unchanged here; backend construction will surface
    # a concrete error through new_store() if runtime libs are still unavailable.
    pass


class ReadWriteLock:
    def __init__(self) -> None:
        self._lock = rwlock.RWLockFair()

    def read_lock(self):
        return self._lock.gen_rlock()

    def write_lock(self):
        return self._lock.gen_wlock()


class ThreadPoolKvFuture(KvFuture):
    """Thread pool based implementation of KvFuture."""
    
    def __init__(self, future: Future, operation_name: str = "operation"):
        self._future = future
        self._operation_name = operation_name
        self._result: Optional[Result[Any, ApiError]] = None
        self._lock = threading.Lock()
    
    def is_waiting(self) -> bool:
        """Check if the operation is still waiting to complete."""
        return not self._future.done()
    
    def wait(self) -> Result[Any, ApiError]:
        """Block until the operation completes and return the result."""
        with self._lock:
            if self._result is not None:
                return self._result
            try:
                logging.debug(f"=============== Mooncake store wait operation ===============\n"
                    f"operation_name: {self._operation_name}\n"
                    f"==============================================================\n")
                result = self._future.result() # Result[Any, ApiError]
                logging.debug(f"=============== Mooncake store wait operation result ===============\n"
                    f"operation_name: {self._operation_name}\n"
                    f"result: {result}\n"
                    f"==============================================================\n")
                self._result = result
                return result
            except Exception as e:
                error = exception_to_error(e)
                self._result = Result.new_error(error)
                return self._result


def _extract_mooncake_code(err: ApiError) -> Optional[int]:
    details = err.details
    if not isinstance(details, dict):
        return None
    code = details.get("mooncake_code")
    if isinstance(code, int):
        return code
    return None


def _should_return_without_renew(err: ApiError) -> bool:
    if isinstance(err, (KeyNotFoundError, ResourceExhaustedError)):
        return True
    mooncake_code = _extract_mooncake_code(err)
    return mooncake_code in MOONCAKE_NO_RENEW_ERROR_CODES


class MooncakeStore(KvClient):
    """Mooncake implementation of the KV Cache Store interface."""
    def __init__(self, config: "FluxonKvClientConfig"):
        """Initialize the Mooncake store wrapper."""
        self._store = MooncakeDistributedStore()
        self._config = config
        self._initialized = False
        self._instance_key = None
        self._thread_pool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="mooncake-kv")
        self._rwlock = ReadWriteLock()
        self._renew_lock = threading.Lock()
        self._closed = False
        # config = self._config
        device_name = ""
        if config.protocol_type == "rdma":
            device_name = config.protocol_rdma_device_names

        server_name = config.instance_key

        logging.info(
            "=============== Mooncake store setup args ===============\n"
            f"server_name: {server_name}\n"
            f"metadata_server: {config.mooncake_spec_metadata_server}\n"
            f"master_server_address: {config.mooncake_spec_master_server_address}\n"
            f"contribute_to_cluster_pool_size: {config.contribute_to_cluster_pool_size}\n"
            f"local_buffer_size: {config.mooncake_spec_local_buffer_size}\n"
            f"protocol_type: {config.protocol_type}\n"
            f"device_name: {device_name}\n"
            f"==============================================================\n"
        )

        shared = {
            "lock": Lock(),
            "cur_thread": -1,
            "fails": [],
            "ok": False,
        }

        for i in range(1):

            def setup_store() -> None:
                try:
                    retcode = self._store.setup(
                        server_name,
                        config.mooncake_spec_metadata_server,
                        config.contribute_to_cluster_pool_size["dram"],  # Use DRAM only
                        config.mooncake_spec_local_buffer_size,
                        config.protocol_type,
                        device_name,
                        config.mooncake_spec_master_server_address,
                    )
                except Exception as e:  # pragma: no cover - defensive
                    retcode = -1
                    errmsg = f"init_mooncake setup raised exception: {e}"
                    logging.error(errmsg)
                    with shared["lock"]:
                        shared["fails"].append(errmsg)
                    return

                with shared["lock"]:
                    if f"fail_{i}" in shared:
                        return
                    if i > shared["cur_thread"] and not shared["ok"]:
                        shared["cur_thread"] = i
                        if retcode == 0:
                            logging.info("Mooncake store setup successful")
                            shared["ok"] = True
                        else:
                            errmsg = (
                                f"init_mooncake timeout for {i} times, setup ret code={retcode}"
                            )
                            logging.error(errmsg)
                            shared["fails"].append(errmsg)

            t = threading.Thread(target=setup_store, daemon=True)
            t.start()
            logging.debug("start noblock setup")
            t.join(timeout=30)
            logging.debug("noblock setup for 30s, starting check")
            with shared["lock"]:
                if not shared["ok"]:
                    logging.warning(
                        f"Thread {i} is still alive after 30 seconds, continuing to next iteration"
                    )
                    shared[f"fail_{i}"] = True
                else:
                    break

        if not shared["ok"]:
            raise RuntimeError(
                f"init_mooncake timeout for 5 times, fails: {shared['fails']}"
            )

        logging.info("RECEIVED Mooncake store setup successful")
        # Mark store initialized and set a stable instance identity for API callers.
        self._initialized = True
        self._instance_key = config.instance_key
    
    @classmethod
    def new(cls, config: "FluxonKvClientConfig") -> Result[KvClient, ApiError]:
        try:
            return Result.new_ok(cls(config))
        except Exception as e:
            return Result.new_error(exception_to_error(e))


    def close_noblock(self, timeout: float = 30.0) -> None:
        """
        Close the store in a non-blocking way with timeout.
        
        Args:
            timeout: Maximum time to wait for close operation (default: 30 seconds)
        """
        close_result: List[Optional[Exception]] = [None]  # Use list to store result from thread
        close_exception: List[Optional[Exception]] = [None]  # Use list to store exception from thread
        
        def close_store():
            try:
                logging.debug("closing the store...")
                with self._rwlock.write_lock():
                    ret_code = self._store.close()
                close_result[0] = ret_code
                if ret_code == 0:
                    logging.info("The store successfully closed.")
                else:
                    logging.warning(f"The store isn't closed properly.\n\terror code:{ret_code}, error:{try_new_error_from_mooncake(ret_code)}")
            except Exception as e:
                close_exception[0] = e
                logging.error(f"During closing the store, exception {e} occurred!")
        
        close_thread = threading.Thread(target=close_store, daemon=True)
        close_thread.start()
        close_thread.join(timeout=timeout)
        
        if close_thread.is_alive():
            logging.error(f"Store close operation timed out after {timeout} seconds. Proceeding anyway.")
            # Thread is still running, but we proceed anyway
        elif close_exception[0] is not None:
            logging.error(f"Store close operation failed with exception: {close_exception[0]}. Proceeding anyway.")
    
    def renew_store(self) -> Result[MooncakeDistributedStore, ApiError]:
        """
        Renew the store only.
        
        - First, try to close the old store.
        - Then, renew one.
        """
        # Close the old store with timeout
        self.close_noblock(timeout=30.0)

        try:
            MooncakeStore._allow_init = True
            try:
                new_store = MooncakeStore(self._config)
            finally:
                MooncakeStore._allow_init = False
        except Exception as e:
            logging.error(f"Renew store error with {e}")
            return Result.new_error(
                BackendInitFailedError(
                    message=f"renew store error with {e}"
                )
            )

        if not isinstance(new_store, MooncakeStore):  # defensive
            logging.error(
                f"The type of store should be MooncakeStore, But get {type(new_store)}"
            )
            return Result.new_error(
                BackendUnavailableError(
                    message=(
                        "renew store error with KvClientType. "
                        f"Expected: mooncake, Get: {type(new_store)}"
                    )
                )
            )

        return Result.new_ok(new_store._store)
    
    def retry_operation(self, operation: Callable, *args) -> Result[Any, ApiError]:
        """
        Retry operations abstraction.
        
        Args:
            operation(Callable): The retry operation that we want to do.
            *args: Available arguments.
        
        Returns:
            result(Result[Any, ApiError]): The final result of the operation.
        """
        try:
            # Try for the first time. If success: return result. else close the store and renew it.
            result: Result[Any, ApiError] = operation(*args)
            if result.is_ok():
                logging.debug(f"{operation.__name__} with key: {args[0]} success!")
                return result
            err = result.unwrap_error()
            if _should_return_without_renew(err):
                logging.warning(
                    f"[{operation.__name__}] return without renew after first trial: "
                    f"key={args[0]}, error={err}"
                )
                return Result.new_error(err)
            
            logging.warning(
                f"="*15 + f" Mooncake store {operation.__name__} first trial failed " + "="*15 + "\n"
                f"With error: \n\t{err}\n"
                f"With key: {args[0]}\n"
                f"Retry with new_store..."
            )
        except Exception as e:
            logging.warning(f"mooncake store {operation.__name__} failed with exception:\n\t{e}")

        # Renew a store.
        with self._renew_lock:
            try:
                logging.debug(f"Enter critical area. {operation.__name__} for check other threads renew.")
                result: Result[Any, ApiError] = operation(*args)
                if result.is_ok():
                    logging.debug(f"{operation.__name__} with key: {args[0]} success!")
                    return result
                err = result.unwrap_error()
                if _should_return_without_renew(err):
                    logging.warning(
                        f"[{operation.__name__}] return without renew in renew gate: "
                        f"key={args[0]}, error={err}"
                    )
                    return Result.new_error(err)
                logging.warning(
                    f"="*15 + f" Mooncake store {operation.__name__} trial before renew failed " + "="*15 + "\n"
                    f"With error: \n\t{err}\n"
                    f"With key: {args[0]}\n"
                    f"Retry with new_store..."
                )
            except Exception as e:
                logging.warning(f"mooncake store {operation.__name__} failed with exception:\n\t{e}")
                
            try:
                result = self.renew_store()
                if not result.is_ok():
                    logging.warning(f"[{operation.__name__}] Renew Store Failed")
                    return result
                with self._rwlock.write_lock():
                    self._store = result.unwrap()
                logging.debug(f"[{operation.__name__}] renew success!")
            except Exception as e:
                logging.error(f"[{operation.__name__}] Renew mooncake store failed with exception:\n\t{e}")
                return Result.new_error(
                    exception_to_error(e)
                )

        # Try again.
        try:
            result: Result[Any, ApiError] = operation(*args)
            if not result.is_ok():
                logging.warning(f"[{operation.__name__}] After renew a store, operation failed with {result.unwrap_error()}")
                return result
            logging.debug(f"[{operation.__name__}] success after renew!")
            return result
        except Exception as e:
            logging.error(f"[{operation.__name__}] failed with exception: {e}")
            return Result.new_error(
                exception_to_error(e)
            )
        

    def put(
        self,
        key: str,
        value: Dict[str, Union[int, float, bool, str, bytes, DLPacked]],
        opts: Optional[PutOptionalArgs] = None,
    ) -> Result[KvFuture, ApiError]:
        """
        Store a key-value pair with single or multiple data parts (non-blocking).

        Args:
            key: The key to store
            *values: One or more values to store as bytes

        Returns:
            Result[KvFuture, ApiError]
        """
        if opts is not None:
            # Check Mooncake compatibility of provided optional args
            ok, unsupported = opts.support_mooncake()
            if not ok:
                limit_rate(
                    key="mooncake_put_opts_warning",
                    func=logging.warning,
                    msg=f"Mooncake backend put() received unsupported opts fields {unsupported}; opts will be ignored. Note: KV eviction risk.",
                    max_calls=1,
                    period=5,
                )

        if not self._initialized:
            return Result.new_error(
                GeneralError(message="Store not initialized when put(). Call setup() first.")
            )

        encoded = encode_flat_kv_dict(value)
        if not encoded.is_ok():
            return Result.new_error(encoded.unwrap_error())
        values_bytes = (encoded.unwrap(),)

        def put_operation(key: str, values: Tuple[bytes, ...]):
            """
            Put operations explicitly remove old data before writing new data.

            Args:
                key(str): The input key.
                values(Tuple[bytes, ...]): When `len(value) == 1`, use `put`. When `len(value) > 1`, use `put_parts`. Otherwise failed.

            Returns:
                Result(Union[OkNone, None]): The final put result.
            """
            def debug_values():
                values_debug=None
                if isinstance(values,list) or isinstance(values,tuple):
                    try:
                        values_debug=values[0][:100]
                    except:
                        values_debug=f"values[0] type: {type(values[0])}"
                else:
                    raise ValueError(f"values type: {type(values)}, not Tuple or list")
                return values_debug
            
            def try_put(key: str, values: Tuple[bytes, ...]) -> Result[OkNone, ApiError]:
                """Try one force-delete-then-put Mooncake write attempt."""
                try:
                    with self._rwlock.read_lock():
                        remove_retcode = self._store.remove(key, True)

                    logging.debug(
                        f"[put_operation] Force remove retcode: {remove_retcode} for key: {key}"
                    )
                    if remove_retcode != 0:
                        remove_error = try_new_error_from_mooncake(
                            remove_retcode,
                            f"Remove operation failed for key '{key}'",
                            key=key,
                        )
                        if not isinstance(remove_error, KeyNotFoundError):
                            logging.warning(
                                "=============== Mooncake store delete-before-put failed ===============\n"
                                f"key: {key}\n"
                                f"values: {debug_values()}\n"
                                f"error: {remove_error}\n"
                                "==============================================================\n"
                            )
                            return Result.new_error(remove_error)

                    with self._rwlock.read_lock():
                        if len(values) == 1:
                            retcode = self._store.put(key, values[0])
                        else:
                            retcode = self._store.put_parts(key, *values)

                    logging.debug(f"[put_operation] Put retcode: {retcode} for key: {key}")
                    if retcode == 0:
                        logging.debug(f"=============== Mooncake store put operation success ===============\n"
                            f"key: {key}\n"
                            f"values: {debug_values()}\n"
                            f"==============================================================\n")
                        return Result.new_ok(OkNone())
                    else:
                        error = try_new_error_from_mooncake(
                            retcode, f"Put operation failed for key '{key}'", key=key
                        )
                        logging.warning(f"=============== Mooncake store put operation failed ===============\n"
                            f"key: {key}\n"
                            f"values: {debug_values()}\n"
                            f"error: {error}\n"
                            f"==============================================================\n")
                        return Result.new_error(error)
                except Exception as e:
                    logging.warning(f"=============== Mooncake store put operation failed ===============\n"
                        f"key: {key}\n"
                        f"values: {debug_values()}\n"
                        f"error: {e}\n"
                        f"==============================================================\n")
                    return Result.new_error(exception_to_error(e))
            result = self.retry_operation(try_put, key, values)
            if not result.is_ok():
                logging.error(f"[put_operation] failed with error: {result.unwrap_error()}")
            else:
                logging.debug(f"[put_operation] succeeded!")
            return result
    
        try:
            future = self._thread_pool.submit(put_operation, key, values_bytes)
            kv_future = ThreadPoolKvFuture(future, f"put_{key}")
            return Result.new_ok(kv_future)
        except Exception as e:
            return Result.new_error(exception_to_error(e))

    def put_blocking(
        self,
        key: str,
        value: Dict[str, Union[int, float, bool, str, bytes, DLPacked]],
        opts: Optional[PutOptionalArgs] = None,
    ) -> Result[OkNone, ApiError]:
        """Synchronous Mooncake put via the public blocking KV contract."""
        put_result = self.put(key, value, opts=opts)
        if not put_result.is_ok():
            return Result.new_error(put_result.unwrap_error())
        wait_result = put_result.unwrap().wait()
        if not wait_result.is_ok():
            return Result.new_error(wait_result.unwrap_error())
        _ = wait_result.unwrap()
        return Result.new_ok(OkNone())

    def get(
        self,
        key: str,
    ) -> Result[KvFuture, ApiError]:
        """
        Retrieve a value by key (non-blocking).

        Args:
            key: The key to retrieve

        Returns:
            Result[KvFuture, ApiError]
        """
        if not self._initialized:
            return Result.new_error(
                GeneralError(message="Store not initialized when get(). Call setup() first.")
            )

        def get_operation(key: str):
            """
            The Get operation. We will handle by as follows:
            
            - If we successfully get, we return.
            - Otherwise:
                - Encounter with `KeyNotFoundError`. No retry, directly returns.
                - Encounter with other error: 
                    - Renew a new store.
                    - retry get_again.
            
            Args:
                key(str): The value which we want to retrieve.
            
            Returns:
                result(Result[Memholder, ApiError]): The data retrieved.
            """
            def try_get(key: str) -> Result[MemHolder, ApiError]:
                try:
                    # https://github.com/kvcache-ai/Mooncake/blob/e475a369fe45d528135b2b318d7e9464e1846222/docs/source/mooncake-store-api/python-binding.md?plain=1#L682
                    with self._rwlock.read_lock():
                        datasize = self._store.get_size(key)
                        if datasize < 0:
                            logging.warning(f"[get_operation] Get failed with retcode:{datasize}")

                            return Result.new_error(
                                try_new_error_from_mooncake(
                                    datasize, f"Get failed for key '{key}'"))
                        logging.debug("[get_operation] The key exists.")
                        data: Optional[bytes] = self._store.get(key)

                    # mooncake store always return bytes
                    assert data is not None, "Data should have bytes after exists checking!"

                    if len(data)!=datasize:
                        return Result.new_error(
                            ValueSizeChangedError.new(key,  datasize,len(data))
                        )

                    # Create a simple MemHolder wrapper for the data
                    mem_holder = SimpleMemHolder(data)
                    return Result.new_ok(mem_holder)
                except Exception as e:
                    return Result.new_error(exception_to_error(e))
            
            result = self.retry_operation(try_get, key)
            if not result.is_ok():
                logging.error(f"[get_operation] failed with error: {result.unwrap_error()}")
            else:
                logging.debug(f"[get_operation] succeeded!")
            return result
    
        try:
            future = self._thread_pool.submit(get_operation, key)
            kv_future = ThreadPoolKvFuture(future, f"get_{key}")
            return Result.new_ok(kv_future)
        except Exception as e:
            return Result.new_error(exception_to_error(e))

    def get_blocking(self, key: str) -> Result[MemHolder, ApiError]:
        """Synchronous Mooncake get via the public blocking KV contract."""
        get_result = self.get(key)
        if not get_result.is_ok():
            return Result.new_error(get_result.unwrap_error())
        return get_result.unwrap().wait()

    
    def get_size(self, key: str) -> Result[int, ApiError]:
        """
        Get the size of a stored value (non-blocking).

        Args:
            key: The key to check

        Returns:
            Result[KvFuture, ApiError]
        """
        if not self._initialized:
            return Result.new_error(
                GeneralError(message="Store not initialized when get_size(). Call setup() first.")
            )

        try:
            with self._rwlock.read_lock():
                size = self._store.get_size(key)
            if size < 0:
                error = try_new_error_from_mooncake(
                    size, f"Get size failed for key '{key}'", key=key
                )
                raise Exception(str(error))
            return Result.new_ok(size)
        except Exception as e:
            return Result.new_error(exception_to_error(e))


    def is_exist(self, key: str) -> Result[bool, ApiError]:
        """
        Check if a key exists in the store (non-blocking).

        Args:
            key: The key to check

        Returns:
            Result[KvFuture, ApiError]
        """
        if not self._initialized:
            return Result.new_error(
                GeneralError(message="Store not initialized when is_exist(). Call setup() first.")
            )


        try:
            with self._rwlock.read_lock():
                exists = self._store.is_exist(key)
            if exists < 0:
                error = try_new_error_from_mooncake(
                    exists, f"Existence check failed for key '{key}'", key=key
                )
                raise Exception(str(error))
            return Result.new_ok(exists == 1)
        except Exception as e:
            return Result.new_error(exception_to_error(e))

    def remove(self, key: str) -> Result[OkNone, ApiError]:
        """
        Remove a key from the store with force-delete semantics.

        - Treat missing key as success.
        - Return Mooncake semantic errors immediately instead of retrying forever.
        - Retry only for transient failures that are not object-state conflicts.

        Args:
            key: The key to remove

        Returns:
            Result[OkNone, ApiError]
        """
        if not self._initialized:
            return Result.new_error(
                GeneralError(message="Store not initialized when remove(). Call setup() first.")
            )

        i=0
        while True:
            i+=1
            try:
                with self._rwlock.read_lock():
                    retcode = self._store.remove(key, True)
            except Exception as e:
                logging.warning(f"[remove] remove exception: key={key}, error={e}")
                time.sleep(0.3)
                continue

            if retcode == 0:
                return Result.new_ok(OkNone())

            error = try_new_error_from_mooncake(
                retcode, f"Remove operation failed for key '{key}'", key=key
            )
            if isinstance(error, KeyNotFoundError):
                return Result.new_ok(OkNone())
            if _should_return_without_renew(error):
                return Result.new_error(error)

            logging.debug(f"[remove] remove failed: key={key}, error={error}, will retry for time {i}")
            time.sleep(0.3)


    def sync_kv_to_file(
        self,
        key: str,
        target_instance_key: str,
        filepath: str,
        file_offset: int,
        bytes_field_key: str,
        timeout_ms: int = 60_000,
    ) -> Result[KvFuture, ApiError]:
        return Result.new_error(
            InvalidArgumentError(
                message=(
                    "Mooncake backend does not support sync_kv_to_file. "
                    "Please use the Fluxon backend (fluxon_pyo3) for remote file sync."
                )
            )
        )

    def instance_key(self) -> Result[str, ApiError]:
        """
        Get the unique instance key for this store instance.

        Returns:
            Result[str, ApiError]
        """
        if self._instance_key is None:
            return Result.new_error(
                GeneralError(message="Store not initialized when instance_key(). Call setup() first.")
            )

        return Result.new_ok(self._instance_key)

    def close(self) -> Result[OkNone, ApiError]:
        """
        Close and tear down the store.

        Returns:
            Result[Success, ApiError]
        """
        if not self._initialized:
            logging.info("Mooncake store not initialized, nothing to close.")
            unregister_store_from_cleanup(self)
            return Result.new_ok(OkNone())

        if self._closed:
            logging.info("Mooncake store already closed, no need to close again.")
            unregister_store_from_cleanup(self)
            return Result.new_ok(OkNone())
        
        logging.info("Mooncake store closing...")
        self._closed = True
        try:
            # Shutdown thread pool
            self._thread_pool.shutdown(wait=True)

            with self._rwlock.write_lock():
                retcode = self._store.close()
            if retcode == 0:
                self._initialized = False
                self._instance_key = None
                logging.info("Mooncake store closed")
                unregister_store_from_cleanup(self)
                return Result.new_ok(OkNone())
            else:
                error = try_new_error_from_mooncake(retcode, "Close operation failed")
                logging.warning(f"Mooncake store close failed: {error}")
                return Result.new_error(error)
        except Exception as e:
            error = exception_to_error(e)
            logging.warning(f"Mooncake store close failed: {error}")
            return Result.new_error(error)
    
    def is_write_once(self) -> bool:
        """
        Check if the store is write-once (keys cannot be overwritten).

        Returns:
            True if the store is write-once, False if keys can be overwritten
        """
        return False

    def count_prefix(self, prefix: str) -> Result[int, ApiError]:
        """Mooncake backend does not support prefix counting.

        This API is primarily used for the unified Fluxon backend to
        implement MQ-style capacity checks. For Mooncake, return a
        clear error so callers can fall back or disable the feature.
        """
        return Result.new_error(
            GeneralError(
                message=f"count_prefix is not supported on Mooncake backend (prefix={prefix})"
            )
        )

    def config(self) -> FluxonKvClientConfig:
        """
        Get the configuration of the store.
        """
        return self._config


    def get_cluster_name(self) -> str:
        cluster = self._config.fluxonkv_spec_cluster_name
        if cluster is None:
            raise InvalidConfigurationError(message="fluxonkv_spec.cluster_name is required for channel APIs")
        return str(cluster)

    def get_etcd_config(self) -> List[str]:
        endpoints = self._config.get_etcd_config()
        if not endpoints:
            raise InvalidConfigurationError(message="empty etcd endpoints")
        for addr in endpoints:
            if "://" in addr:
                raise InvalidConfigurationError(message=f"etcd endpoint must be raw host:port (no scheme), got: {addr!r}")
        return endpoints

    def third_party_logs_dir(self) -> Result[str, ApiError]:
        return Result.new_error(
            GeneralError(
                message="third_party_logs_dir is only supported by the Fluxon backend"
            )
        )

    def ensure_zero_contribution_for_channel(self) -> None:
        self._config.ensure_zero_contribution_for_channel()


class SimpleMemHolder(MemHolder):
    """Simple implementation of MemHolder for Mooncake backend."""
    
    def __init__(self, data: bytes):
        self._data = data
        self._holder_id = id(self)
    
    def access(self) -> Result[Dict[str, Union[int, float, bool, str, bytes, DLPacked]], ApiError]:
        decoded = decode_flat_kv_dict(self._data)
        if not decoded.is_ok():
            return Result.new_error(decoded.unwrap_error())
        wrapped = wrap_flat_dict_dlpack(decoded.unwrap())
        if not wrapped.is_ok():
            return Result.new_error(wrapped.unwrap_error())
        return Result.new_ok(wrapped.unwrap())
