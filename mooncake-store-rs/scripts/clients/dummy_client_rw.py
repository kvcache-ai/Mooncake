#!/usr/bin/env python3
"""DummyClient read/write verification for the current Mooncake store-rs stack.

This script validates dummy-mode put/get flows against a standalone
`mooncake-store-client` daemon. It keeps the validation black-box:

- `setup_dummy()` attaches to the daemon gRPC endpoint
- single-item mode uses high-level `put` / `get`
- batch mode validates shared-memory pointer APIs
- optional multi-buffer mode validates the raw multi-buffer shm path

Examples
--------

# single-item read/write
python3 scripts/clients/dummy_client_rw.py \\
  --daemon_addr 127.0.0.1:16590 \\
  --key_prefix dummy-smoke

# shm batch path
python3 scripts/clients/dummy_client_rw.py \\
  --daemon_addr 127.0.0.1:16590 \\
  --key_prefix dummy-batch \\
  --batch_size 8

# multi-buffer shm path
python3 scripts/clients/dummy_client_rw.py \\
  --daemon_addr 127.0.0.1:16590 \\
  --key_prefix dummy-multi \\
  --batch_size 4 \\
  --batch_api multi_buffer
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import sys
import time

from mooncake.store import (
    MooncakeDistributedStore,
    MooncakeHostMemAllocator,
    ReplicateConfig,
)


# ---------------------------------------------------------------------------
# cli helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DummyClient read/write verification for Mooncake store-rs"
    )
    parser.add_argument(
        "--daemon_addr",
        "--daemon-addr",
        required=True,
        help="Standalone dummy server address, e.g. 127.0.0.1:16590",
    )
    parser.add_argument(
        "--tenant",
        default="default",
        help="Tenant used for read/write operations (default: default)",
    )
    parser.add_argument(
        "--num_kv",
        type=int,
        default=30,
        help="Number of KV pairs to verify (default: 30)",
    )
    parser.add_argument(
        "--value_size",
        type=int,
        default=4096,
        help="Value size in bytes (default: 4096)",
    )
    parser.add_argument(
        "--key_prefix",
        required=True,
        help="Key prefix; keys become <prefix>-0, <prefix>-1, ...",
    )
    parser.add_argument(
        "--mode",
        choices=["write", "read", "both"],
        default="both",
        help="write / read / both (default: both)",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=1,
        help="Batch size; 1 uses single-item APIs (default: 1)",
    )
    parser.add_argument(
        "--batch-api",
        "--batch_api",
        choices=["auto", "single", "shm", "multi_buffer"],
        default="auto",
        help=(
            "Batch validation mode: auto selects shm for batch_size>1, "
            "single forces high-level put/get, multi_buffer validates raw multi-buffer shm"
        ),
    )
    parser.add_argument(
        "--replica_num",
        type=int,
        default=1,
        help="Replica count used for writes (default: 1)",
    )
    parser.add_argument(
        "--prefer-local",
        "--prefer_local",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Prefer local storage owner during placement when available (default: true)",
    )
    parser.add_argument(
        "--with-soft-pin",
        "--with_soft_pin",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable soft pin placement hints (default: true)",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Call remove_all() after validation",
    )
    parser.add_argument(
        "--local_buf_size",
        "--local-buf-size",
        type=int,
        default=128 * 1024 * 1024,
        help="Compatibility local buffer size passed into setup_dummy() (default: 128 MiB)",
    )
    parser.add_argument(
        "--scratch_size",
        "--scratch-size",
        type=int,
        default=16 * 1024 * 1024,
        help="Compatibility scratch size passed into setup_dummy() (default: 16 MiB)",
    )
    parser.add_argument(
        "--hold-seconds",
        "--hold_seconds",
        type=float,
        default=0.0,
        help="Keep the client alive after operations for the specified time",
    )
    args = parser.parse_args()
    validate_args(args)
    return args


def validate_args(args: argparse.Namespace) -> None:
    if args.batch_size <= 0:
        raise SystemExit("--batch_size must be greater than zero")
    if args.replica_num <= 0:
        raise SystemExit("--replica_num must be greater than zero")
    if args.value_size <= 0:
        raise SystemExit("--value_size must be greater than zero")
    if args.local_buf_size < 0 or args.scratch_size < 0:
        raise SystemExit("--local_buf_size and --scratch_size must be >= 0")
    if args.batch_api == "multi_buffer" and args.batch_size == 1:
        raise SystemExit("--batch_api=multi_buffer requires --batch_size greater than 1")


# ---------------------------------------------------------------------------
# payload helpers
# ---------------------------------------------------------------------------


def make_value(key: str, value_size: int) -> bytes:
    seed = hashlib.sha256(key.encode("utf-8")).digest()
    payload = bytearray()
    counter = 0
    while len(payload) < value_size:
        payload.extend(
            hashlib.sha256(seed + counter.to_bytes(8, byteorder="little")).digest()
        )
        counter += 1
    return bytes(payload[:value_size])


def chunked(items, chunk_size: int):
    for index in range(0, len(items), chunk_size):
        yield items[index : index + chunk_size]


def print_throughput(phase: str, item_count: int, value_size: int, elapsed: float) -> None:
    total_bytes = item_count * value_size
    throughput_mib = total_bytes / elapsed / (1024 * 1024) if elapsed > 0 else 0.0
    ops_per_sec = item_count / elapsed if elapsed > 0 else 0.0
    print(
        f"[{phase}] {item_count} KVs in {elapsed:.3f}s  "
        f"ops/s={ops_per_sec:.1f}  throughput={throughput_mib:.2f} MiB/s"
    )


def split_multi_buffer(value: bytes, pieces: int = 3) -> list[bytes]:
    if pieces <= 1 or len(value) <= 1:
        return [value]
    base = len(value) // pieces
    remainder = len(value) % pieces
    parts = []
    cursor = 0
    for index in range(pieces):
        take = base + (1 if index < remainder else 0)
        if take == 0:
            continue
        parts.append(value[cursor : cursor + take])
        cursor += take
    return parts or [value]


# ---------------------------------------------------------------------------
# store helpers
# ---------------------------------------------------------------------------


def apply_replication_config(
    config: ReplicateConfig,
    *,
    replica_num: int,
    prefer_local: bool,
    with_soft_pin: bool,
) -> None:
    config.replica_num = replica_num
    if hasattr(config, "prefer_local"):
        config.prefer_local = prefer_local
    if hasattr(config, "prefer_alloc_in_same_node"):
        config.prefer_alloc_in_same_node = prefer_local
    if hasattr(config, "with_soft_pin"):
        config.with_soft_pin = with_soft_pin


def wait_for_dummy_ready(store: MooncakeDistributedStore, timeout_seconds: float = 10.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            if int(store.health_check()) == 0:
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(
        f"dummy client health_check did not become ready within {timeout_seconds:.1f}s"
    )


class RegisteredAllocator:
    def __init__(self, store: MooncakeDistributedStore) -> None:
        self._store = store
        self._allocator = MooncakeHostMemAllocator()
        self._registered: list[tuple[int, int]] = []

    def alloc(self, size: int) -> int:
        pointer = int(self._allocator.alloc(size))
        self._store.register_buffer(pointer, size)
        self._registered.append((pointer, size))
        return pointer

    def free_all(self) -> None:
        while self._registered:
            pointer, size = self._registered.pop()
            try:
                self._store.unregister_buffer(pointer, size)
            except Exception:
                pass
            try:
                self._allocator.free(pointer)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# operation paths
# ---------------------------------------------------------------------------


def write_all_single(
    store: MooncakeDistributedStore,
    keys_vals: list[tuple[str, bytes]],
    tenant: str,
    config: ReplicateConfig,
) -> float:
    started = time.perf_counter()
    total = len(keys_vals)
    for index, (key, value) in enumerate(keys_vals, start=1):
        status = store.put(key, value, tenant=tenant, config=config)
        if int(status) != 0:
            raise RuntimeError(f"put failed key={key} status={status}")
        if index % 10 == 0 or index == total:
            print(f"[write] Progress: {index}/{total}")
    return time.perf_counter() - started


def write_all_shm(
    store: MooncakeDistributedStore,
    keys_vals: list[tuple[str, bytes]],
    tenant: str,
    config: ReplicateConfig,
    batch_size: int,
) -> float:
    started = time.perf_counter()
    total = len(keys_vals)
    written = 0
    for batch in chunked(keys_vals, batch_size):
        allocator = RegisteredAllocator(store)
        try:
            items = []
            for key, value in batch:
                pointer = allocator.alloc(len(value))
                ctypes.memmove(pointer, value, len(value))
                items.append((key, pointer, len(value)))
            statuses = store.batch_put_from(items, tenant=tenant, config=config)
            if any(int(status) != 0 for status in statuses):
                raise RuntimeError(
                    f"batch_put_from failed keys={[key for key, _, _ in items]} statuses={statuses}"
                )
        finally:
            allocator.free_all()
        written += len(batch)
        print(f"[write] Progress: {written}/{total} (batch_size={batch_size}, api=shm)")
    return time.perf_counter() - started


def write_all_multi_buffer(
    store: MooncakeDistributedStore,
    keys_vals: list[tuple[str, bytes]],
    tenant: str,
    config: ReplicateConfig,
    batch_size: int,
) -> float:
    started = time.perf_counter()
    total = len(keys_vals)
    written = 0
    for batch in chunked(keys_vals, batch_size):
        allocator = RegisteredAllocator(store)
        try:
            keys = []
            all_ptrs = []
            all_sizes = []
            for key, value in batch:
                parts = split_multi_buffer(value)
                keys.append(key)
                ptrs = []
                sizes = []
                for part in parts:
                    pointer = allocator.alloc(len(part))
                    ctypes.memmove(pointer, part, len(part))
                    ptrs.append(pointer)
                    sizes.append(len(part))
                all_ptrs.append(ptrs)
                all_sizes.append(sizes)
            statuses = store.batch_put_from_multi_buffers(
                keys,
                all_ptrs,
                all_sizes,
                tenant=tenant,
                config=config,
            )
            if any(int(status) != 0 for status in statuses):
                raise RuntimeError(
                    f"batch_put_from_multi_buffers failed keys={keys} statuses={statuses}"
                )
        finally:
            allocator.free_all()
        written += len(batch)
        print(
            f"[write] Progress: {written}/{total} "
            f"(batch_size={batch_size}, api=multi_buffer)"
        )
    return time.perf_counter() - started


def read_all_single(
    store: MooncakeDistributedStore,
    keys: list[str],
    tenant: str,
    value_size: int,
) -> float:
    started = time.perf_counter()
    total = len(keys)
    for index, key in enumerate(keys, start=1):
        value = store.get(key, tenant=tenant)
        expected = make_value(key, value_size)
        if value != expected:
            got_len = len(value) if value is not None else None
            raise RuntimeError(
                f"data mismatch key={key} expected_len={len(expected)} got_len={got_len}"
            )
        if index % 10 == 0 or index == total:
            print(f"[read] Progress: {index}/{total}")
    return time.perf_counter() - started


def read_all_shm(
    store: MooncakeDistributedStore,
    keys: list[str],
    tenant: str,
    value_size: int,
    batch_size: int,
) -> float:
    started = time.perf_counter()
    total = len(keys)
    read = 0
    for batch in chunked(keys, batch_size):
        allocator = RegisteredAllocator(store)
        try:
            items = []
            pointers = []
            for key in batch:
                pointer = allocator.alloc(value_size)
                items.append((key, pointer, value_size))
                pointers.append(pointer)
            lengths = store.batch_get_into(items, tenant=tenant)
            if len(lengths) != len(batch):
                raise RuntimeError(f"batch_get_into returned unexpected payload: {lengths!r}")
            for key, pointer, length in zip(batch, pointers, lengths):
                expected = make_value(key, value_size)
                actual_length = int(length)
                if actual_length != len(expected):
                    raise RuntimeError(
                        f"length mismatch key={key} expected_len={len(expected)} got_len={actual_length}"
                    )
                value = ctypes.string_at(pointer, actual_length)
                if value != expected:
                    raise RuntimeError(
                        f"data mismatch key={key} expected_len={len(expected)} got_len={actual_length}"
                    )
        finally:
            allocator.free_all()
        read += len(batch)
        print(f"[read] Progress: {read}/{total} (batch_size={batch_size}, api=shm)")
    return time.perf_counter() - started


def read_all_multi_buffer(
    store: MooncakeDistributedStore,
    keys: list[str],
    tenant: str,
    value_size: int,
    batch_size: int,
) -> float:
    started = time.perf_counter()
    total = len(keys)
    read = 0
    for batch in chunked(keys, batch_size):
        allocator = RegisteredAllocator(store)
        try:
            all_ptrs = []
            all_sizes = []
            expected_parts = []
            for key in batch:
                parts = split_multi_buffer(make_value(key, value_size))
                ptrs = []
                sizes = []
                for part in parts:
                    pointer = allocator.alloc(len(part))
                    ptrs.append(pointer)
                    sizes.append(len(part))
                all_ptrs.append(ptrs)
                all_sizes.append(sizes)
                expected_parts.append(parts)
            lengths = store.batch_get_into_multi_buffers(
                batch,
                all_ptrs,
                all_sizes,
                tenant=tenant,
            )
            if len(lengths) != len(batch):
                raise RuntimeError(
                    f"batch_get_into_multi_buffers returned unexpected payload: {lengths!r}"
                )
            for key, ptrs, sizes, expected, total_length in zip(
                batch,
                all_ptrs,
                all_sizes,
                expected_parts,
                lengths,
            ):
                actual_length = int(total_length)
                expected_length = sum(len(part) for part in expected)
                if actual_length != expected_length:
                    raise RuntimeError(
                        f"length mismatch key={key} expected_len={expected_length} got_len={actual_length}"
                    )
                value = bytearray()
                remaining = actual_length
                for pointer, size in zip(ptrs, sizes):
                    take = min(size, remaining)
                    if take <= 0:
                        break
                    value.extend(ctypes.string_at(pointer, take))
                    remaining -= take
                if bytes(value) != b"".join(expected):
                    raise RuntimeError(
                        f"data mismatch key={key} expected_len={expected_length} got_len={len(value)}"
                    )
        finally:
            allocator.free_all()
        read += len(batch)
        print(
            f"[read] Progress: {read}/{total} "
            f"(batch_size={batch_size}, api=multi_buffer)"
        )
    return time.perf_counter() - started


def delete_all(store: MooncakeDistributedStore) -> float:
    started = time.perf_counter()
    removed = int(store.remove_all(force=True))
    print(f"[delete] remove_all() removed {removed} key(s)")
    return time.perf_counter() - started


def resolve_batch_api(batch_size: int, batch_api: str) -> str:
    if batch_size == 1:
        return "single"
    if batch_api == "auto":
        return "shm"
    return batch_api


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> int:
    args = parse_args()
    store = MooncakeDistributedStore()
    rc = store.setup_dummy(args.local_buf_size, args.scratch_size, args.daemon_addr)
    if int(rc) != 0:
        raise RuntimeError(f"setup_dummy failed status={rc}")
    wait_for_dummy_ready(store)

    config = ReplicateConfig()
    apply_replication_config(
        config,
        replica_num=args.replica_num,
        prefer_local=args.prefer_local,
        with_soft_pin=args.with_soft_pin,
    )

    selected_api = resolve_batch_api(args.batch_size, args.batch_api)
    print("[INFO] Setting up DummyClient...")
    print(f"  daemon_addr:         {args.daemon_addr}")
    print(f"  tenant:              {args.tenant}")
    print(f"  key_prefix:          {args.key_prefix}")
    print(f"  mode:                {args.mode}")
    print(f"  batch_size:          {args.batch_size}")
    print(f"  batch_api:           {selected_api}")
    print(f"  replica_num:         {args.replica_num}")
    print(f"  prefer_local:        {args.prefer_local}")
    print(f"  with_soft_pin:       {args.with_soft_pin}")
    print(f"  local_buf_size:      {args.local_buf_size}")
    print(f"  scratch_size:        {args.scratch_size}")

    keys = [f"{args.key_prefix}-{index}" for index in range(args.num_kv)]

    try:
        if args.mode in ("write", "both"):
            items = [(key, make_value(key, args.value_size)) for key in keys]
            if selected_api == "single":
                elapsed = write_all_single(store, items, args.tenant, config)
            elif selected_api == "shm":
                elapsed = write_all_shm(
                    store,
                    items,
                    args.tenant,
                    config,
                    args.batch_size,
                )
            else:
                elapsed = write_all_multi_buffer(
                    store,
                    items,
                    args.tenant,
                    config,
                    args.batch_size,
                )
            print_throughput("write", args.num_kv, args.value_size, elapsed)
            print(f"[write] {args.num_kv} KVs written OK")

        if args.mode in ("read", "both"):
            if selected_api == "single":
                elapsed = read_all_single(store, keys, args.tenant, args.value_size)
            elif selected_api == "shm":
                elapsed = read_all_shm(
                    store,
                    keys,
                    args.tenant,
                    args.value_size,
                    args.batch_size,
                )
            else:
                elapsed = read_all_multi_buffer(
                    store,
                    keys,
                    args.tenant,
                    args.value_size,
                    args.batch_size,
                )
            print_throughput("read", args.num_kv, args.value_size, elapsed)
            print(f"[read] {args.num_kv} KVs verified OK")

        if args.delete:
            elapsed = delete_all(store)
            print(f"[delete] cleanup OK in {elapsed:.3f}s")

        if args.hold_seconds > 0:
            print(f"[hold] sleeping for {args.hold_seconds:.1f}s before exit")
            time.sleep(args.hold_seconds)

        print("OK")
        return 0
    finally:
        store.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("[INFO] interrupted", file=sys.stderr)
        sys.exit(130)
    except Exception as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        sys.exit(1)
