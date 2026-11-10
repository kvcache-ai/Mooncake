"""Immutable byte tables with synchronous host and stream-ordered CUDA lookup.

The table API deliberately knows nothing about embedding dtypes or model layers.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import mmap
import operator
import os
import tempfile
import threading
from pathlib import Path

import numpy as np

__all__ = ["EngramStore"]


class EngramStore:
    """Own local immutable byte tables behind a backend-independent lookup API.

    ``tables`` maps names to ``{num_rows: N, row_bytes: R}``. New tables are
    create-only. A namespace identifies one immutable model/layout revision.
    Persistent local files survive close; anonymous tables (persistent=False)
    are released on close or process exit. CPU lookup is synchronous. CUDA
    lookup enqueues the complete operation on the supplied stream, including
    completion, and returns None.

    ``store_client`` is retained for a future KV Store backend. MNNVL EGM uses
    ``populate_shard_and_sync`` on a newly created anonymous Store and requires
    explicit coordinated initialization and shutdown.
    """

    def __init__(
        self,
        *,
        tables,
        namespace,
        local_dir=None,
        store_client=None,
        persistent=True,
    ):
        self._closed = False
        self._remote = None
        self._locally_used = False
        self._namespace = namespace
        self._lock = threading.RLock()
        self._mappings = {}
        self._cuda_tables = {}
        self._cuda_lookups = {}
        if not isinstance(namespace, str) or not namespace:
            raise ValueError("A nonempty namespace is required")
        if (local_dir is None) == (store_client is None):
            raise ValueError("Choose exactly one of local_dir and store_client")
        if store_client is not None:
            raise NotImplementedError("Remote EngramStore is not supported yet")
        if not isinstance(persistent, bool):
            raise ValueError("persistent must be a bool")
        self._persistent = persistent
        if not tables:
            raise ValueError("At least one table is required")
        self._specs = {}
        for name, spec in tables.items():
            if not isinstance(name, str) or not name:
                raise ValueError("Table names must be nonempty strings")
            if set(spec) != {"num_rows", "row_bytes"}:
                raise ValueError("Table specs require num_rows and row_bytes only")
            n, r = operator.index(spec["num_rows"]), operator.index(spec["row_bytes"])
            if n < 0 or r <= 0 or n * r > np.iinfo(np.intp).max:
                raise ValueError("Invalid table size")
            self._specs[name] = {"num_rows": n, "row_bytes": r}
        self._root = Path(local_dir).resolve()
        if persistent:
            self._root /= hashlib.sha256(namespace.encode()).hexdigest()
        self._root.mkdir(parents=True, exist_ok=True)

    def _spec(self, name):
        if self._closed:
            raise RuntimeError("EngramStore is closed")
        return self._specs[name]

    def get_table_spec(self, table_id):
        return dict(self._spec(table_id))

    def _path(self, name):
        return self._root / hashlib.sha256(name.encode()).hexdigest()

    @staticmethod
    def _array(value):
        if isinstance(value, np.ndarray):
            return value
        if getattr(value, "is_cuda", True):
            raise TypeError("Expected a NumPy array or CPU tensor")
        return value.detach().numpy()

    def populate_shard_and_sync(
        self,
        local_rows,
        *,
        backend,
        shard_id,
        num_shards,
        table_owners,
        master_addr,
        session_id,
        caller_id,
        num_callers,
        max_lookup_rows,
        max_egm_bytes_per_numa,
        device=None,
        host_id=None,
        numa_node=None,
        rpc_hostname="",
        init_timeout_s=300,
        close_timeout_s=60,
    ):
        """Publish this node's shard of EGM tables; every caller invokes this in
        the same order before graph capture.

        Only backend='mnnvl' is supported, and persistent=False is required.
        Each table is provided as complete CPU uint8 data by one NUMA writer on
        its owner node; non-writers pass an empty dict. Destroy the graph before
        closing; every caller must call close explicitly, and the deployment
        session cannot be reused after a timeout.
        """
        with self._lock:
            if self._closed or self._remote is not None or self._locally_used:
                raise RuntimeError(
                    "Only a new, unused Store can initialize the remote backend"
                )
            if backend != "mnnvl":
                raise ValueError(
                    "Coordinated initialization supports only backend='mnnvl'"
                )
            if self._persistent:
                raise ValueError("MNNVL Engram requires persistent=False")
            from ._engram_mnnvl_backend import Backend

            rows = {name: self._array(array) for name, array in local_rows.items()}
            self._remote = Backend(
                self._specs,
                self._namespace,
                shard_id=shard_id,
                num_shards=num_shards,
                table_owners=table_owners,
                master_addr=master_addr,
                session_id=session_id,
                caller_id=caller_id,
                num_callers=num_callers,
                max_lookup_rows=max_lookup_rows,
                max_egm_bytes_per_numa=max_egm_bytes_per_numa,
                device=device,
                host_id=host_id,
                numa_node=numa_node,
                rpc_hostname=rpc_hostname,
                init_timeout_s=init_timeout_s,
                close_timeout_s=close_timeout_s,
            )
            self._remote.initialize(rows)

    def populate(self, table_id, rows):
        with self._lock:
            if self._remote is not None:
                raise RuntimeError(
                    "Cannot override the already selected MNNVL backend"
                )
            self._locally_used = True
            spec = self._spec(table_id)
            rows = self._array(rows)
            shape = (spec["num_rows"], spec["row_bytes"])
            if (
                rows.dtype != np.uint8
                or rows.shape != shape
                or not rows.flags.c_contiguous
            ):
                raise ValueError(
                    "populate requires contiguous uint8 [num_rows,row_bytes]"
                )
            if not self._persistent:
                self._populate_anonymous(table_id, rows)
                return
            path = self._path(table_id)
            with open(str(path) + ".lock", "a+b") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                if path.exists():
                    raise FileExistsError(path)
                temporary = Path(str(path) + ".tmp")
                # Only the writer holding the table lock can recover a failed write.
                temporary.mkdir(exist_ok=True)
                try:
                    with open(temporary / "rows", "w+b") as f:
                        if rows.nbytes:
                            os.posix_fallocate(f.fileno(), 0, rows.nbytes)
                            with mmap.mmap(f.fileno(), rows.nbytes) as target:
                                target[:] = memoryview(rows).cast("B")
                    (temporary / "layout.json").write_text(json.dumps(spec))
                    os.rename(temporary, path)
                except BaseException:
                    for name in ("rows", "layout.json"):
                        (temporary / name).unlink(missing_ok=True)
                    temporary.rmdir()
                    raise

    def _populate_anonymous(self, name, rows):
        if name in self._mappings:
            raise FileExistsError(name)
        if not rows.nbytes:
            self._mappings[name] = (None, np.empty(rows.shape, np.uint8))
            return
        # The descriptor has no directory entry. The mmap owns its pages even
        # after the descriptor closes; SIGKILL cannot leave a named large file.
        with tempfile.TemporaryFile(dir=self._root) as f:
            os.posix_fallocate(f.fileno(), 0, rows.nbytes)
            mapping = mmap.mmap(f.fileno(), rows.nbytes)
        try:
            mapping[:] = memoryview(rows).cast("B")
            self._mappings[name] = (
                mapping,
                np.ndarray(rows.shape, np.uint8, buffer=mapping),
            )
        except BaseException:
            mapping.close()
            raise

    def _open(self, name):
        if name not in self._mappings:
            if not self._persistent:
                raise FileNotFoundError(f"Table has not been populated: {name}")
            spec, path = self._spec(name), self._path(name)
            if json.loads((path / "layout.json").read_text()) != spec:
                raise ValueError(f"Table layout mismatch: {name}")
            size = spec["num_rows"] * spec["row_bytes"]
            if (path / "rows").stat().st_size != size:
                raise ValueError(f"Table size mismatch: {name}")
            # A writable shared mapping permits portable CUDA host registration.
            # Published rows remain immutable through this API.
            if size:
                with open(path / "rows", "r+b") as f:
                    mapping = mmap.mmap(f.fileno(), size)
                array = np.ndarray(
                    (spec["num_rows"], spec["row_bytes"]), np.uint8, buffer=mapping
                )
            else:
                mapping, array = None, np.empty((0, spec["row_bytes"]), np.uint8)
            self._mappings[name] = (mapping, array)
        return self._mappings[name][1]

    def lookup(self, table_ids, row_ids, output, *, stream=None, offsets=None):
        """Read [T,K] IDs into byte rows [T,K,R], preserving rows with ID -1.

        The innermost output dimension must be contiguous; token-major and
        table-major outer strides are accepted. All selected tables have the
        same row width. Table names are snapshotted once. CPU lookup opens all
        selected tables before writing output. IDs and output stay alive until
        the supplied CUDA stream completes. Warm each table combination before
        graph capture; destroy captured graphs before closing this Store.
        """
        with self._lock:
            if self._remote is not None:
                self._remote.validate_lookup(row_ids, stream)
            else:
                self._locally_used = True
            table_ids = tuple(table_ids)
            specs = [self._spec(name) for name in table_ids]
            if not specs or any(s["row_bytes"] != specs[0]["row_bytes"] for s in specs):
                raise ValueError("Select nonempty tables of equal row_bytes")
            offsets = (
                tuple(0 for _ in specs)
                if offsets is None
                else tuple(map(operator.index, offsets))
            )
            if len(offsets) != len(specs) or any(
                x < 0 or x > np.iinfo(np.int64).max for x in offsets
            ):
                raise ValueError("Expected one nonnegative int64 offset per table")
            if stream is not None:
                self._lookup_cuda(table_ids, row_ids, output, int(stream), offsets)
                return
            ids, out = self._array(row_ids), self._array(output)
            if (
                ids.dtype not in (np.int32, np.int64)
                or ids.ndim != 2
                or ids.shape[1] != len(specs)
            ):
                raise ValueError("row_ids must be int32/int64 [tokens,tables]")
            if (
                out.dtype != np.uint8
                or out.shape != (*ids.shape, specs[0]["row_bytes"])
                or not out.flags.writeable
            ):
                raise ValueError(
                    "output must be writable uint8 [tokens,tables,row_bytes]"
                )
            if not ids.size:
                return
            self._validate_strides(out)
            if any(s < 0 for s in ids.strides) or np.shares_memory(ids, out):
                raise ValueError(
                    "IDs must have nonnegative strides and not overlap output"
                )
            columns = []
            for k, (spec, offset) in enumerate(zip(specs, offsets)):
                live = ids[:, k] != -1
                values = ids[live, k].astype(np.int64)
                if np.any(values < offset) or np.any(
                    values - offset >= spec["num_rows"]
                ):
                    raise IndexError(f"Row ID out of bounds in {table_ids[k]}")
                columns.append((live, values - offset))
            tables = [self._open(name) for name in table_ids]
            for k, (table, (live, values)) in enumerate(zip(tables, columns)):
                out[live, k, :] = table[values]

    @staticmethod
    def _validate_strides(out):
        if out.strides[-1] != 1 or any(s < 0 for s in out.strides):
            raise ValueError(
                "Output byte rows must be contiguous with nonnegative strides"
            )
        span = out.shape[-1]
        for stride, count in sorted(zip(out.strides[:2], out.shape[:2])):
            if count > 1:
                if stride < span:
                    raise ValueError("Output rows overlap")
                span += (count - 1) * stride

    def _lookup_cuda(self, names, ids, out, stream, offsets):
        from mooncake import _engram_cuda

        device = ids.device.index
        key = (device, names, offsets)
        if key not in self._cuda_lookups:
            _engram_cuda.require_warmup(stream)
            tables = []
            for name in names:
                if name not in self._cuda_tables:
                    self._cuda_tables[name] = (
                        self._remote.tables[name]
                        if self._remote is not None
                        else _engram_cuda.LocalTable(self._open(name))
                    )
                tables.append(self._cuda_tables[name])
            self._cuda_lookups[key] = _engram_cuda.Lookup(tables, offsets)
        self._cuda_lookups[key].run(ids, out, stream)

    def remove(self, table_id):
        with self._lock:
            self._spec(table_id)
            if self._remote is not None:
                raise RuntimeError("MNNVL tables are released by close()")
            if not self._persistent:
                raise RuntimeError("Anonymous tables are released by close()")
            if table_id in self._mappings:
                raise RuntimeError("Close readers before removing a mapped table")
            path = self._path(table_id)
            with open(str(path) + ".lock", "a+b") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                if path.exists():
                    (path / "rows").unlink()
                    (path / "layout.json").unlink()
                    path.rmdir()

    def close(self):
        with self._lock:
            if self._closed:
                return
            for lookup in self._cuda_lookups.values():
                lookup.synchronize()
            self._cuda_lookups.clear()
            self._cuda_tables.clear()
            if self._remote is not None:
                self._remote.close()
            for name in list(self._mappings):
                mapping, array = self._mappings.pop(name)
                del array
                if mapping is not None:
                    mapping.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):
        with contextlib.suppress(Exception):
            if self._remote is not None and not self._closed:
                # GC cannot replace cross-process coordinated shutdown, nor
                # discard resources used by the graph.
                from ._engram_session import abandon

                abandon(self)
            else:
                self.close()
