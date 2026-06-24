"""Smoke test for ``backend_fallback_close``.

Minimal script-style test that validates one thing only:

1. ``main()`` starts a subprocess.
2. The child process constructs a mock ``KvClient`` and prints a sentinel text to stdout in ``close()``.
3. The child process only calls ``register_store_for_cleanup`` (does not call ``close()`` explicitly).
4. On process exit the atexit hook triggers ``close()``; the parent verifies it by checking the sentinel in stdout.
"""

import os
import subprocess
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


SENTINEL_TEXT = "BACKEND_FALLBACK_CLOSE_CALLED"


def main() -> None:
    """Run the fallback-close smoke test in a subprocess.

    The parent process only cares about two things:
    - the child process exits successfully (returncode == 0)
    - the child process stdout contains ``SENTINEL_TEXT``
    """

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

    script = """
import os
import sys

sys.path.insert(0, __PKG_ROOT__)

from fluxon_py.kvclient.backend_fallback_close import register_store_for_cleanup
from fluxon_py.kvclient.kvclient_interface import KvClient
from fluxon_py.api_error import Result, OkNone, ApiError


class _MockFuture:
    def is_waiting(self):
        return False

    def wait(self):
        return Result.new_ok(None)


class _MockStore(KvClient):
    def __init__(self):
        pass

    @classmethod
    def new(cls, config):
        return Result.new_error(ApiError("unused in fallback_close test"))

    def put(self, key, value, opts=None):
        return Result.new_ok(_MockFuture())

    def get(self, key):
        return Result.new_ok(_MockFuture())

    def get_size(self, key):
        return Result.new_ok(0)

    def is_exist(self, key):
        return Result.new_ok(False)

    def remove(self, key):
        return Result.new_ok(OkNone())

    def sync_kv_to_file(self, key, target_instance_key, filepath, file_offset, bytes_field_key, timeout_ms=60000):
        return Result.new_ok(_MockFuture())

    def instance_key(self):
        return Result.new_ok("mock")

    def close(self):
        # Print a sentinel to stdout so the parent can verify close() was called.
        print(__SENTINEL_TEXT__, flush=True)
        return Result.new_ok(OkNone())

    def is_write_once(self):
        return False

    def config(self):
        return None

    def get_cluster_name(self):
        return "mock_cluster"

    def get_etcd_config(self):
        return []

    def third_party_logs_dir(self):
        return Result.new_ok("/tmp/fluxon_third_party_logs")

    def ensure_zero_contribution_for_channel(self):
        return None


_cls = _MockStore
_cls._allow_init = True
try:
    store = _cls()
finally:
    _cls._allow_init = False

register_store_for_cleanup(store)
"""

    script = script.replace("__PKG_ROOT__", repr(repo_root))
    script = script.replace("__SENTINEL_TEXT__", repr(SENTINEL_TEXT))

    proc = subprocess.run(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if proc.returncode != 0:
        print("backend_fallback_close child process exited with error")
        print(f"exit code: {proc.returncode}")
        if proc.stdout:
            print("--- child stdout ---")
            print(proc.stdout)
        if proc.stderr:
            print("--- child stderr ---")
            print(proc.stderr)
        raise SystemExit(1)

    if SENTINEL_TEXT not in (proc.stdout or ""):
        print("backend_fallback_close did not print sentinel text")
        if proc.stdout:
            print("--- child stdout ---")
            print(proc.stdout)
        if proc.stderr:
            print("--- child stderr ---")
            print(proc.stderr)
        raise SystemExit(1)

    print("✅ backend_fallback_close: SUCCESS")


if __name__ == "__main__":
    main()
