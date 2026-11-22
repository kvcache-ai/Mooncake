"""Master-coordinated node placement and generation-fenced rendezvous."""

import atexit
import json
import os
from pathlib import Path
import threading
import time
import weakref

from .rendezvous import fingerprint


def master_address():
    """Reuse connectivity only; constructing Engram must not allocate KV pools."""
    for name in ("MOONCAKE_CONFIG_PATH", "SGLANG_HICACHE_MOONCAKE_CONFIG_PATH"):
        path = os.environ.get(name)
        if path:
            config = json.loads(Path(path).read_text())
            address = config.get("master_server_address")
            if not isinstance(address, str) or not address:
                raise ValueError(f"{name} requires master_server_address")
            return address
    address = os.environ.get("MOONCAKE_MASTER")
    if not address:
        raise ValueError("Set MOONCAKE_MASTER or an existing Mooncake config file")
    return address


def process_incarnation():
    # SGLang's scheduler children share a launch parent. Its start time guards
    # against PID reuse, while /proc boot_id distinguishes machine restarts.
    parent = os.getppid()
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    stat = Path(f"/proc/{parent}/stat").read_text().rsplit(")", 1)[1].split()
    return fingerprint([boot, parent, stat[19]])


_live = weakref.WeakSet()


@atexit.register
def _stop_heartbeats():
    for assignment in list(_live):
        assignment.close()


class Assignment:
    """One Store's immutable view of a node group; no per-lookup Master RPC."""

    def __init__(self, config):
        from mooncake._store import _EngramMasterClient

        self._client = _EngramMasterClient(master_address())
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._error = None
        self._thread = None
        self._base = {
            "pool": fingerprint(
                [config.backend, config.model_key, config.assignment, config.group_id]
            ),
            "node": config.node_id,
            "incarnation": process_incarnation(),
        }
        request = dict(
            self._base,
            action="join",
            mode=config.assignment,
            group=config.group_id or "",
            shard=-1 if config.shard_id is None else config.shard_id,
            group_size=config.group_size,
            num_callers=config.num_callers,
        )
        deadline = time.monotonic() + config.timeout
        while True:
            with self._lock:
                result = self._client.request(request)
            if result["error"] and not result["retry"]:
                raise RuntimeError(result["error"])
            if not result["error"] and result["full"]:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("Timed out waiting for a complete Engram node group")
            time.sleep(0.1)
        self.group_id = result["group"]
        self.shard_id = result["shard"]
        self.group_size = result["group_size"]
        self._base.update(boot=result["boot"], generation=result["generation"])
        self.identity = fingerprint(
            [self._base["pool"], self.group_id, result["boot"], result["generation"]]
        )
        self._thread = threading.Thread(target=self._heartbeat, daemon=True)
        self._thread.start()
        _live.add(self)

    def request(self, action, **kwargs):
        self.check()
        with self._lock:
            result = self._client.request(dict(self._base, action=action, **kwargs))
        if result["error"]:
            raise RuntimeError(result["error"])
        return result

    def check(self):
        if self._error is not None:
            raise RuntimeError(
                "Engram group is unavailable; rebuild every group member"
            ) from self._error

    def _heartbeat(self):
        while not self._stop.wait(2):
            try:
                self.request("heartbeat")
            except Exception as error:
                self._error = error
                return

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def rendezvous(self, namespace, caller_id, timeout):
        return GroupRendezvous(self, namespace, caller_id, timeout)


class GroupRendezvous:
    def __init__(self, assignment, namespace, caller_id, timeout):
        self.assignment = assignment
        self.prefix = fingerprint([namespace, str(caller_id)])
        self.deadline = time.monotonic() + timeout

    def _key(self, key):
        return self.prefix + "/" + key

    def publish(self, key, value):
        self.assignment.request(
            "put",
            key=self._key(key),
            value=json.dumps(value, sort_keys=True, separators=(",", ":")),
        )

    def wait(self, key):
        while True:
            if time.monotonic() >= self.deadline:
                raise TimeoutError(f"Timed out waiting for Engram metadata {key}")
            result = self.assignment.request("get", key=self._key(key))
            if result["found"]:
                return json.loads(result["value"])
            time.sleep(0.05)

    def close(self):
        pass
