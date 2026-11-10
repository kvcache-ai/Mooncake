"""GPU-free distributed control-plane/lifecycle regression; real FABRIC is
covered by bench_engram_mnnvl.py.
"""

import concurrent.futures
import copy
import sys
import threading
from types import SimpleNamespace

import numpy as np
import pytest

from mooncake import _engram_mnnvl_backend as backend
from mooncake import _engram_session as session
from mooncake.engram import EngramStore


class MemoryMetadata:
    values = {}
    lock = threading.Lock()

    def __init__(self, endpoint):
        self.endpoint = endpoint

    def create(self, key, value, deadline):
        with self.lock:
            if key in self.values and self.values[key] != value:
                raise RuntimeError("metadata conflict")
            self.values[key] = copy.deepcopy(value)

    def get(self, key, deadline):
        with self.lock:
            return copy.deepcopy(self.values.get(key))


class FakeNative:
    allocations = {}
    counter = 0
    lock = threading.Lock()

    def __init__(self, device, master, segment, hostname):
        self.device = device
        self.segment = segment
        self.closed = False
        self.owned = set()

    def info(self):
        return {
            "device": self.device,
            "gpu_uuid": f"gpu-{self.device}",
            "segment": self.segment,
            "numa": {0: 256},
        }

    def allocate(self, size, numa, budget):
        with self.lock:
            if sum(len(self.allocations[h]) for h in self.owned) + size > budget:
                raise ValueError("session NUMA budget")
            FakeNative.counter += 1
            handle = FakeNative.counter * 1048576
            self.allocations[handle] = bytearray(size)
            self.owned.add(handle)
            return handle

    def write(self, handle, offset, array):
        self.allocations[handle][offset : offset + array.nbytes] = array.tobytes()

    def publish(self, handle, numa):
        return {
            "address": handle,
            "length": len(self.allocations[handle]),
            "segment": self.segment,
        }

    def acquire(self, segment, address, length):
        assert len(self.allocations[address]) == length
        return address

    def table(self, handle, offset, count, width):
        return (
            memoryview(self.allocations[handle])[offset : offset + count * width]
            if count
            else b""
        )

    def sample(self, handle, offset, length):
        return bytes(self.allocations[handle][offset : offset + length])

    def synchronize(self):
        pass

    def release_mapping(self, handle):
        pass

    def free(self, handle):
        self.allocations.pop(handle, None)
        self.owned.discard(handle)

    def close(self):
        self.closed = True


@pytest.fixture
def fake(monkeypatch):
    MemoryMetadata.values = {}
    FakeNative.allocations = {}
    session._runtimes.clear()
    session._abandoned.clear()
    monkeypatch.setattr(session, "Metadata", MemoryMetadata)
    monkeypatch.setitem(
        sys.modules,
        "mooncake._engram_mnnvl",
        SimpleNamespace(Session=FakeNative, current_device=lambda: 0),
    )
    yield
    session._runtimes.clear()
    session._abandoned.clear()
    FakeNative.allocations = {}


def config(shard=0, caller=0, shards=2, callers=2, **extra):
    value = dict(
        backend="mnnvl",
        shard_id=shard,
        num_shards=shards,
        table_owners={"a": 0, "b": shards - 1, "empty": 0},
        master_addr="http://master/metadata",
        session_id="test-session",
        caller_id=caller,
        num_callers=callers,
        max_lookup_rows=64,
        max_egm_bytes_per_numa=4096,
        device=shard * callers + caller,
        host_id=f"node-{shard}",
        init_timeout_s=2,
        close_timeout_s=0.3,
    )
    return dict(value, **extra)


def store(path):
    return EngramStore(
        tables={
            "a": {"num_rows": 11, "row_bytes": 17},
            "b": {"num_rows": 7, "row_bytes": 19},
            "empty": {"num_rows": 0, "row_bytes": 17},
        },
        namespace="checkpoint/layer",
        local_dir=path,
        persistent=False,
    )


def initialize_group(path, mutate=None):
    stores = [store(path) for _ in range(4)]
    rows = [
        np.arange(187, dtype=np.uint8).reshape(11, 17),
        np.arange(133, dtype=np.uint8).reshape(7, 19),
    ]

    def initialize(index):
        shard, caller = divmod(index, 2)
        data = (
            (
                {"a": rows[0], "empty": np.empty((0, 17), np.uint8)}
                if shard == 0
                else {"b": rows[1]}
            )
            if caller == 0
            else {}
        )
        args = config(shard, caller)
        if mutate:
            mutate(index, data, args)
        stores[index].populate_shard_and_sync(data, **args)

    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        futures = [pool.submit(initialize, i) for i in range(4)]
        errors = []
        for future in futures:
            try:
                future.result(timeout=5)
            except Exception as exc:
                errors.append(exc)
    return stores, rows, errors


def close_group(stores):
    with concurrent.futures.ThreadPoolExecutor(len(stores)) as pool:
        return list(pool.map(lambda item: item.close(), stores))


def test_shared_tables_and_coordinated_close(fake, tmp_path):
    stores, rows, errors = initialize_group(tmp_path)
    assert not errors
    assert len(FakeNative.allocations) == 2  # Not one per GPU.
    expected = rows[0].tobytes()
    rows[0][:] = 0  # Do not borrow checkpoint backing after initialization.
    for item in stores:
        assert item._remote.state == "READY"
        assert bytes(item._remote.tables["a"]) == expected
        assert bytes(item._remote.tables["b"]) == rows[1].tobytes()
        assert len(item._remote.tables["empty"]) == 0
    close_group(stores)
    assert not FakeNative.allocations
    assert not session._runtimes
    close_group(stores)


@pytest.mark.parametrize(
    "case", ["duplicate_writer", "layout", "same_host", "capacity", "same_gpu"]
)
def test_invalid_deployment_does_not_become_ready(fake, tmp_path, case):
    def mutate(index, data, args):
        if index != 1:
            return
        if case == "duplicate_writer":
            data["a"] = np.zeros((11, 17), np.uint8)
        elif case == "layout":
            args["max_lookup_rows"] = 7
        elif case == "same_host":
            args["host_id"] = "node-1"
        elif case == "capacity":
            args["max_egm_bytes_per_numa"] = 1
            args["caller_id"] = 0
        else:
            args["device"] = 0

    stores, _, errors = initialize_group(tmp_path, mutate)
    assert errors
    assert all(item._remote.state != "READY" for item in stores)
    assert not FakeNative.allocations


def test_close_timeout_retains_provider_memory_for_retry(fake, tmp_path):
    stores, _, errors = initialize_group(tmp_path)
    assert not errors
    with pytest.raises(TimeoutError):
        stores[0].close()
    assert len(FakeNative.allocations) == 2
    close_group(stores)
    assert not FakeNative.allocations


def test_lookup_is_local_validation_only(fake, tmp_path):
    stores, _, errors = initialize_group(tmp_path)
    assert not errors
    ids = SimpleNamespace(is_cuda=True, device=SimpleNamespace(index=0), shape=(8, 2))
    stores[0]._remote.validate_lookup(ids, 1)
    with pytest.raises(NotImplementedError):
        stores[0]._remote.validate_lookup(ids, None)
    ids.shape = (65, 1)
    with pytest.raises(ValueError, match="max_lookup_rows"):
        stores[0]._remote.validate_lookup(ids, 1)
    ids.shape, ids.device.index = (1, 1), 3
    with pytest.raises(ValueError, match="IDs"):
        stores[0]._remote.validate_lookup(ids, 1)
    close_group(stores)


def test_local_backend_cannot_switch_after_population(fake, tmp_path):
    item = store(tmp_path)
    item.populate("a", np.zeros((11, 17), np.uint8))
    with pytest.raises(RuntimeError, match="new, unused Store"):
        item.populate_shard_and_sync({}, **config())
    item.close()


def test_requires_anonymous_store(tmp_path):
    item = EngramStore(
        tables={"a": {"num_rows": 1, "row_bytes": 1}}, namespace="n", local_dir=tmp_path
    )
    with pytest.raises(ValueError, match="persistent"):
        item.populate_shard_and_sync({}, **config())
    item.close()


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_rejects_unbounded_timeouts(value):
    with pytest.raises(ValueError):
        backend.timeout(value)


def test_numa_available_counts_free_and_file_pages(tmp_path):
    node = tmp_path / "node3"
    node.mkdir()
    (node / "meminfo").write_text(
        "Node 3 MemTotal:       1000 kB\n"
        "Node 3 MemFree:         100 kB\n"
        "Node 3 FilePages:        25 kB\n"
    )
    assert backend.numa_available(3, tmp_path) == 125 * 1024
    assert backend.numa_available(4, tmp_path) is None


def test_multiple_writers_on_same_numa_share_budget():
    specs = {
        "a": {"num_rows": 4, "row_bytes": 16},
        "b": {"num_rows": 4, "row_bytes": 16},
    }
    members = [
        {
            "shard": 0,
            "caller": i,
            "host": "node",
            "gpu_uuid": str(i),
            "numa": 0,
            "granularity": 256,
            "budget": 512,
            "tables": [name],
        }
        for i, name in enumerate(specs)
    ]
    _, layouts = backend.plan_layout(specs, {"a": 0, "b": 0}, members, 1, 2)
    assert sum(layout["length"] for layout in layouts.values()) == 512
    for member in members:
        member["budget"] = 256
    with pytest.raises(ValueError, match="budget"):
        backend.plan_layout(specs, {"a": 0, "b": 0}, members, 1, 2)


def test_gpu_readback_failure_is_never_ready(fake, tmp_path, monkeypatch):
    monkeypatch.setattr(
        FakeNative, "sample", lambda self, handle, offset, length: bytes([255] * length)
    )
    stores, _, errors = initialize_group(tmp_path)
    assert errors
    assert all(item._remote.state == "FAILED" for item in stores)
    close_group(stores)
    assert not FakeNative.allocations


def test_layout_uses_separate_arena_offsets_and_checks_overflow():
    specs = {
        "a": {"num_rows": 3, "row_bytes": 17},
        "b": {"num_rows": 7, "row_bytes": 19},
    }
    members = [
        {
            "shard": i,
            "caller": 0,
            "host": f"n{i}",
            "gpu_uuid": str(i),
            "numa": 0,
            "granularity": 256,
            "budget": 256,
            "tables": [name],
        }
        for i, name in enumerate(specs)
    ]
    _, layouts = backend.plan_layout(specs, {"a": 0, "b": 1}, members, 2, 1)
    assert layouts[(0, 0)]["tables"]["a"]["offset"] == 0
    assert layouts[(1, 0)]["tables"]["b"]["offset"] == 0
    with pytest.raises(ValueError, match="overflow"):
        backend.align(backend.LIMIT, 256)
    members[0]["budget"] = 1
    with pytest.raises(ValueError, match="budget"):
        backend.plan_layout(specs, {"a": 0, "b": 1}, members, 2, 1)


def test_missing_peer_times_out_before_allocating(fake, tmp_path):
    item = store(tmp_path)
    with pytest.raises(TimeoutError):
        item.populate_shard_and_sync(
            {"a": np.zeros((11, 17), np.uint8), "empty": np.empty((0, 17), np.uint8)},
            **config(init_timeout_s=0.05),
        )
    assert not FakeNative.allocations
    item.close()


def test_layout_accounts_for_previous_store_allocations():
    specs = {"a": {"num_rows": 4, "row_bytes": 16}}
    members = [
        {
            "shard": 0,
            "caller": 0,
            "host": "node",
            "gpu_uuid": "gpu",
            "numa": 0,
            "granularity": 256,
            "budget": 512,
            "used_bytes": 512,
            "tables": ["a"],
        }
    ]
    with pytest.raises(ValueError, match="budget"):
        backend.plan_layout(specs, {"a": 0}, members, 1, 1)


def test_metadata_client_rejects_server_without_conditional_support(monkeypatch):
    metadata = session.Metadata("http://server/metadata")
    monkeypatch.setattr(metadata, "request", lambda *args: (200, {}, b"updated"))
    with pytest.raises(RuntimeError, match="atomic"):
        metadata.create("key", {"value": 1}, 1e20)


def test_runtime_rejects_overlapping_initialization(fake):
    first = session.acquire_runtime(
        "http://master/metadata", "deployment", 0, 0, 0, "", "a"
    )
    with pytest.raises(RuntimeError, match="sequentially"):
        session.acquire_runtime(
            "http://master/metadata", "deployment", 0, 0, 0, "", "b"
        )
    first.initializing = None
    other = session.acquire_runtime(
        "http://master/metadata", "deployment", 0, 0, 0, "", "b"
    )
    assert first is other
    session.release_runtime(first, "a")
    assert not first.native.closed
    session.release_runtime(other, "b")
    assert first.native.closed
