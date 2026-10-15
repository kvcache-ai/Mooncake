"""Immutable byte tables with synchronous host and stream-ordered CUDA lookup.

The table API deliberately knows nothing about embedding dtypes or model layers.
The existing layer API remains available through the same EngramStore class.
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
    """Own local tables, or read tables through an existing Mooncake Store client.

    ``tables`` maps names to ``{num_rows: N, row_bytes: R}``. New tables are
    create-only. A namespace identifies one immutable model/layout revision.
    Persistent local files survive close; anonymous tables (persistent=False)
    are released on close or process exit. CPU lookup is synchronous. CUDA
    lookup enqueues the complete operation on the supplied stream, including
    completion, and returns None.
    """

    def __init__(
        self,
        layers=None,
        store=None,
        *,
        tables=None,
        namespace=None,
        local_dir=None,
        store_client=None,
        persistent=True,
    ):
        self._legacy = None
        self._closed = False
        self._lock = threading.RLock()
        self._mappings = {}
        self._cuda_tables = {}
        self._cuda_lookups = {}
        if tables is None:
            if (
                namespace is not None
                or local_dir is not None
                or store_client is not None
                or not persistent
            ):
                raise ValueError("namespace/local_dir/store_client require tables")
            from mooncake import _store

            self._legacy = _store.EngramStore(layers, store)
            return
        if layers is not None or store is not None:
            raise ValueError("Use either the layer API or the table API")
        if not isinstance(namespace, str) or not namespace:
            raise ValueError("A nonempty namespace is required")
        if (local_dir is None) == (store_client is None):
            raise ValueError("Choose exactly one of local_dir and store_client")
        if not isinstance(persistent, bool) or (not persistent and local_dir is None):
            raise ValueError("persistent=False requires local_dir")
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
        self._namespace = namespace
        self._client = store_client
        self._root = None
        if local_dir is not None:
            self._root = Path(local_dir).resolve()
            if persistent:
                self._root /= hashlib.sha256(namespace.encode()).hexdigest()
            self._root.mkdir(parents=True, exist_ok=True)

    def __getattr__(self, name):
        legacy = self.__dict__.get("_legacy")
        if legacy is not None and not self.__dict__.get("_closed", False):
            return getattr(legacy, name)
        raise AttributeError(name)

    def _spec(self, name):
        if self._closed:
            raise RuntimeError("EngramStore is closed")
        if self._legacy is not None:
            raise TypeError("This operation requires the table API")
        return self._specs[name]

    def get_table_spec(self, table_id):
        return dict(self._spec(table_id))

    def _key(self, name):
        return f"engram:v1:{self._namespace.encode().hex()}:{name.encode().hex()}"

    def _path(self, name):
        return self._root / hashlib.sha256(name.encode()).hexdigest()

    @staticmethod
    def _array(value):
        if isinstance(value, np.ndarray):
            return value
        if getattr(value, "is_cuda", True):
            raise TypeError("Expected a NumPy array or CPU tensor")
        return value.detach().numpy()

    def populate(self, table_id, rows, config=None):
        if self._legacy is not None:
            if config is None:
                return self._legacy.populate(table_id, rows)
            return self._legacy.populate(table_id, rows, config)
        with self._lock:
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
            if self._root is None:
                self._populate_remote(table_id, rows, config)
                return
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

    def _populate_remote(self, name, rows, config):
        from mooncake.store import ReplicateConfig

        client, key = self._client, self._key(name)
        exists = client.is_exist(key)
        if exists < 0:
            raise RuntimeError(f"Cannot query table {name}: {exists}")
        if exists:
            raise FileExistsError(key)
        if not rows.nbytes:
            return
        address = rows.ctypes.data
        if client.register_buffer(address, rows.nbytes) != 0:
            raise RuntimeError("Cannot register populate input")
        try:
            result = client.batch_put_from(
                [key], [address], [rows.nbytes], config or ReplicateConfig()
            )
            if result != [0]:
                raise RuntimeError(f"populate failed: {result}")
        finally:
            if client.unregister_buffer(address) != 0:
                raise RuntimeError("Cannot unregister populate input")

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
        same row width. IDs and output stay alive until the supplied CUDA
        stream completes. Warm each table combination before graph capture;
        destroy captured graphs before closing this Store.

        Store-backed reads currently use the synchronous host path, with
        caller-registered output, as in the original Mooncake layer API.
        """
        with self._lock:
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
                if self._root is None:
                    raise NotImplementedError(
                        "CUDA lookup currently requires local_dir"
                    )
                self._lookup_cuda(
                    tuple(table_ids), row_ids, output, int(stream), offsets
                )
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
            if self._root is not None:
                for k, (name, (live, values)) in enumerate(zip(table_ids, columns)):
                    out[live, k, :] = self._open(name)[values]
            else:
                self._lookup_remote(table_ids, columns, out)

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
                table_key = name
                if table_key not in self._cuda_tables:
                    self._cuda_tables[table_key] = _engram_cuda.LocalTable(
                        self._open(name)
                    )
                tables.append(self._cuda_tables[table_key])
            self._cuda_lookups[key] = _engram_cuda.Lookup(tables, offsets)
        self._cuda_lookups[key].run(ids, out, stream)

    def _lookup_remote(self, names, columns, out):
        keys, dst, src, sizes = [], [], [], []
        width = out.shape[-1]
        for k, (name, (live, values)) in enumerate(zip(names, columns)):
            if not len(values):
                continue
            keys.append(self._key(name))
            dst.append(
                (np.flatnonzero(live) * out.strides[0] + k * out.strides[1]).tolist()
            )
            src.append((values * width).tolist())
            sizes.append([width] * len(values))
        if not keys:
            return
        result = self._client.get_into_ranges(
            [out.ctypes.data], [keys], [dst], [src], [sizes]
        )
        if result != [sizes]:
            raise RuntimeError("Engram ranged read failed; output is invalid")

    def remove(self, table_id):
        with self._lock:
            self._spec(table_id)
            if not self._persistent:
                raise RuntimeError("Anonymous tables are released by close()")
            if table_id in self._mappings:
                raise RuntimeError("Close readers before removing a mapped table")
            if self._root is None:
                rc = self._client.remove(self._key(table_id))
                if rc != 0:
                    raise RuntimeError(f"remove failed: {rc}")
            else:
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
            lookup = None
            self._cuda_lookups.clear()
            self._cuda_tables.clear()
            for name in list(self._mappings):
                mapping, array = self._mappings.pop(name)
                del array
                if mapping is not None:
                    mapping.close()
            self._legacy = None
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):
        with contextlib.suppress(Exception):
            self.close()
