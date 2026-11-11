"""Engram metadata control plane and in-process native session reuse."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
import uuid


def digest(value):
    return hashlib.sha256(encode(value)).hexdigest()


def encode(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


class Metadata:
    def __init__(self, endpoint):
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.netloc
            or parsed.query
            or parsed.fragment
            or parsed.username
        ):
            raise ValueError(
                "master_addr must be an HTTP(S) metadata endpoint without "
                "query parameters"
            )
        self.endpoint = endpoint.rstrip("/")
        self.token = os.environ.get("MC_METADATA_HTTP_TOKEN")

    def request(self, key, deadline, value=None):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Engram metadata operation timed out")
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        if value is not None:
            headers["If-None-Match"] = "*"
        request = Request(
            self.endpoint + "?" + urlencode({"key": key}),
            data=value,
            headers=headers,
            method="GET" if value is None else "PUT",
        )
        try:
            response = urlopen(request, timeout=min(remaining, 3.0))
        except HTTPError as exc:
            response = exc
        with response:
            body = response.read(8 * 1024 * 1024 + 1)
            if len(body) > 8 * 1024 * 1024:
                raise ValueError("Engram metadata response is too large")
            return response.code, response.headers, body

    def get(self, key, deadline):
        code, _, body = self.request(key, deadline)
        if code == 404:
            return None
        if code != 200:
            raise RuntimeError(f"Engram metadata GET failed, HTTP {code}")
        return json.loads(body)

    def create(self, key, value, deadline):
        body = encode(value)
        while True:
            try:
                code, headers, _ = self.request(key, deadline, body)
                # The C++ metadata server answers a failed create-only PUT
                # with 409 because its HTTP stack cannot emit 412.
                if code in (200, 201, 409, 412):
                    if headers.get("X-Mooncake-Conditional-Put") != "1":
                        raise RuntimeError(
                            "metadata server does not support Engram atomic "
                            "conditional publish; upgrade required"
                        )
                    if code not in (409, 412):
                        return
                    try:
                        current = self.get(key, deadline)
                    except (URLError, OSError):
                        pass  # Retry the create-only PUT; reads are harmless.
                    else:
                        if current == value:
                            return
                        raise RuntimeError(
                            f"Engram immutable metadata conflict: {key}"
                        )
                elif code < 500:
                    raise RuntimeError(f"Engram metadata PUT failed, HTTP {code}")
            except (URLError, OSError):
                pass  # Retry the same body; a lost response is not a second write.
            if time.monotonic() >= deadline:
                raise TimeoutError("Engram metadata publish timed out")
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))


_lock = threading.RLock()
_runtimes = {}
_abandoned = []


class Runtime:
    def __init__(self, native, key, master, device, hostname):
        self.key = key
        self.lock = threading.RLock()
        self.namespaces = set()
        self.initializing = None
        self.native = native.Session(
            device, master, "engram_" + uuid.uuid4().hex, hostname
        )


def acquire_runtime(
    master, session_id, shard_id, caller_id, device, hostname, namespace
):
    from mooncake import _engram_mnnvl as native

    key = (os.getpid(), master, session_id, shard_id, caller_id, device, hostname)
    with _lock:
        runtime = _runtimes.get(key)
        if runtime is None:
            runtime = Runtime(native, key, master, device, hostname)
            _runtimes[key] = runtime
        if namespace in runtime.namespaces:
            raise RuntimeError(
                "A namespace cannot be initialized twice for the same "
                "caller/session"
            )
        if runtime.initializing is not None:
            raise RuntimeError(
                "Stores in the same session must initialize sequentially; "
                "NUMA budgets cannot be allocated concurrently"
            )
        runtime.initializing = namespace
        runtime.namespaces.add(namespace)
        return runtime


def release_runtime(runtime, namespace):
    with _lock:
        if namespace not in runtime.namespaces:
            return
        if len(runtime.namespaces) == 1:
            runtime.native.close()
            _runtimes.pop(runtime.key, None)
        runtime.namespaces.remove(namespace)


def abandon(backend):
    # Retain resources until process exit when coordinated shutdown is
    # incomplete; do not release provider memory early during GC.
    with _lock:
        if backend not in _abandoned:
            _abandoned.append(backend)
