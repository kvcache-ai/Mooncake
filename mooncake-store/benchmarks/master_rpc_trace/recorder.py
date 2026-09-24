# Copyright 2026 Alibaba Cloud and its affiliates
# Licensed under the Apache License, Version 2.0.
"""Write physical-key RPC intents using an explicitly supplied logical clock.

This is an export interface for a simulator adapter, not an automatic SGLang
hook. AutoBench prompts or logical KV page hashes must first be converted by
the integration to actual master operations, keys, sizes and batch boundaries.
"""

import json
import threading


OPERATIONS = {
    "BatchExistKey",
    "BatchGetReplicaList",
    "BatchPutStart",
    "BatchPutEnd",
    "BatchPutRevoke",
    "BatchRemove",
}


def _uint64(value, name, *, positive=False):
    if type(value) is not int or not (int(positive) <= value < 2**64):
        raise ValueError(
            f"{name} must be a {'positive' if positive else 'nonnegative'} uint64"
        )


class RpcTraceWriter:
    """Append validated events to a new JSONL file; never use wall-clock time.

    Multiple logical clients share one writer. The caller is responsible for
    merging simulator events into nondecreasing logical timestamp order.
    """

    def __init__(self, path, *, metadata=None):
        if metadata is not None and not isinstance(metadata, dict):
            raise ValueError("metadata must be an object")
        header = json.dumps(
            {
                "type": "master_rpc_trace",
                "version": 1,
                "time_unit": "us",
                "metadata": metadata or {},
            },
            allow_nan=False,
        )
        self._file = open(path, "x", encoding="utf-8")
        self._file.write(header + "\n")
        self._lock = threading.Lock()
        self._events = {}
        self._open_puts = set()
        self._last_timestamp = 0

    def record(
        self,
        *,
        timestamp_us,
        client_id,
        op,
        keys,
        value_sizes=None,
        replica_num=None,
        depends_on=(),
        put_start=None,
    ):
        """Return an event id usable by depends_on or put_start in later calls."""
        _uint64(timestamp_us, "timestamp_us")
        if not isinstance(client_id, str) or not client_id:
            raise ValueError("client_id must be a nonempty string")
        if op not in OPERATIONS:
            raise ValueError(f"unsupported operation: {op}")
        if not isinstance(keys, (list, tuple)) or not keys:
            raise ValueError("keys must be a nonempty list")
        if any(not isinstance(key, str) or not key for key in keys):
            raise ValueError("keys must contain nonempty strings")
        if not isinstance(depends_on, (list, tuple)):
            raise ValueError("depends_on must be a list")
        row = {
            "timestamp_us": timestamp_us,
            "client_id": client_id,
            "op": op,
            "keys": list(keys),
        }
        if op == "BatchPutStart":
            if not isinstance(value_sizes, (list, tuple)) or len(value_sizes) != len(
                keys
            ):
                raise ValueError("value_sizes must match keys")
            if len(set(keys)) != len(keys):
                raise ValueError("duplicate write keys are not supported")
            for size in value_sizes:
                _uint64(size, "value size", positive=True)
            row["value_sizes"] = list(value_sizes)
            if replica_num is not None:
                _uint64(replica_num, "replica_num", positive=True)
                row["replica_num"] = replica_num
        elif value_sizes is not None or replica_num is not None:
            raise ValueError("value_sizes/replica_num are only valid for BatchPutStart")

        with self._lock:
            if self._file.closed:
                raise ValueError("writer is closed")
            if timestamp_us < self._last_timestamp:
                raise ValueError("timestamps must be nondecreasing")
            if any(
                not isinstance(dep, str) or dep not in self._events
                for dep in depends_on
            ):
                raise ValueError("dependencies must reference earlier event ids")
            if depends_on:
                row["depends_on"] = list(dict.fromkeys(depends_on))
            if op in {"BatchPutEnd", "BatchPutRevoke"}:
                if put_start not in self._open_puts:
                    raise ValueError("put_start must reference an unfinished write")
                start = self._events[put_start]
                if start["client_id"] != client_id or start["keys"] != list(keys):
                    raise ValueError("put_start must match client and ordered keys")
                row["put_start"] = put_start
            elif put_start is not None:
                raise ValueError("put_start is only valid for End/Revoke")
            event_id = f"rpc-{len(self._events)}"
            row["id"] = event_id
            self._file.write(json.dumps(row, ensure_ascii=True, allow_nan=False) + "\n")
            self._events[event_id] = row
            self._last_timestamp = timestamp_us
            if op == "BatchPutStart":
                self._open_puts.add(event_id)
            elif put_start is not None:
                self._open_puts.remove(put_start)
            return event_id

    def close(self):
        with self._lock:
            if self._file.closed:
                return
            self._file.close()
            if not self._events or self._open_puts:
                raise ValueError("trace is empty or contains unfinished writes")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self._file.close()
        else:
            self.close()
