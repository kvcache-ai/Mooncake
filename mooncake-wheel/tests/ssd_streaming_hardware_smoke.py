"""SSD restore correctness for fixed CPU L2, TCP/RDMA and native io_uring.

With --ssd-push, exchange peer JSON files through a trusted setup channel.
Owner and requester both need --peer-info-file and --peer-file; start both
before reading. Peer files contain setup credentials and must stay private.
The normal read checks one Owner ReadInto RPC per whole/range read. Device
buffers without --ssd-push exercise legacy fallback. Requires an offload
master and a built wheel; this script reports correctness, not throughput.
"""

import argparse
import ctypes
import json
import os
import time
import uuid
from pathlib import Path


def pattern(size, offset=0):
    block = bytes(range(251))
    begin = offset % len(block)
    return (block * ((begin + size + 250) // 251))[begin : begin + size]


def poll(condition, timeout, description):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {description}")


def descriptors(store, key):
    result = store.batch_get_replica_desc([key]).get(key, [])
    return result if isinstance(result, (list, tuple)) else [result]


def disk_only(store, key):
    replicas = descriptors(store, key)
    return bool(replicas) and all(r.is_local_disk_replica() for r in replicas)


def private_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump(value, output)
    temp.replace(path)


def stats(store):
    info = json.loads(store.get_ssd_push_peer_info())
    return {
        field: info[field]
        for field in ("read_requests", "pending_requests", "registered_l2_pools")
    }


def configure_push(store, args):
    if args.peer_directory and not args.peer_file:
        # Native setup publishes both roles and imports reciprocal credentials.
        # Exercise FluentLLM's API surface without manual peer management.
        return
    info = json.loads(store.get_ssd_push_peer_info())
    assert info, "SSD Push requires a classic TCP/RDMA build and explicit opt-in"
    if args.role == "local":
        return
    private_json(args.peer_info_file, info)
    poll(
        lambda: all(Path(path).is_file() for path in args.peer_file),
        args.timeout,
        "trusted peer JSON files",
    )
    peers = [Path(path).read_text() for path in args.peer_file]
    assert store.configure_ssd_push_peers(peers) == 0, "Invalid trusted SSD peers"


def read_and_verify(store, args, key):
    torch = None
    if args.device != "cpu":
        import torch

        if args.device.startswith("npu"):
            import torch_npu  # noqa: F401

    guard = 16
    capacity = args.size + 2 * guard
    if torch is None:
        buffer = (ctypes.c_ubyte * capacity)()
        pointer = ctypes.addressof(buffer)

        def fill():
            ctypes.memset(pointer, 0xA5, capacity)

        def snapshot():
            return bytes(buffer)
    else:
        buffer = torch.empty(capacity, dtype=torch.uint8, device=args.device)
        pointer = buffer.data_ptr()

        def synchronize():
            if args.device.startswith("npu"):
                torch.npu.synchronize()
            else:
                torch.cuda.synchronize()

        def fill():
            buffer.fill_(0xA5)
            synchronize()

        def snapshot():
            synchronize()
            return bytes(buffer.cpu().tolist())

    assert store.register_buffer(pointer, capacity) == 0
    try:
        if args.ssd_push:
            assert stats(store)["registered_l2_pools"] == 1
            if args.owner_stats_file:
                poll(
                    lambda: Path(args.owner_stats_file).is_file(),
                    args.timeout,
                    "Owner stats",
                )
                before = json.loads(Path(args.owner_stats_file).read_text())[
                    "read_requests"
                ]
            elif args.role == "local":
                before = stats(store)["read_requests"]
            else:
                before = None
        if args.exercise_rdma_read:
            fill()
            result = store.batch_get_into(
                [key + "-rdma-memory"], [pointer + guard], [args.size]
            )
            assert result == [args.size], result
            assert snapshot()[guard : guard + args.size] == pattern(args.size)
        for _ in range(args.repetitions):
            assert disk_only(store, key), "Read must use only LOCAL_DISK replicas"
            fill()
            result = store.batch_get_into(
                [key], [pointer + guard], [args.size], args.read_timeout_ms
            )
            assert result == [args.size], result
            actual = snapshot()
            assert actual[:guard] == bytes([0xA5]) * guard
            assert actual[guard : guard + args.size] == pattern(args.size)
            assert actual[-guard:] == bytes([0xA5]) * guard
            if args.nested:
                fill()
                split = args.size // 2 + 7
                result = store.batch_get_into_multi_buffers(
                    [key],
                    [[pointer + guard, pointer + guard + split]],
                    [[split, args.size - split]],
                    False,
                    args.read_timeout_ms,
                )
                assert result == [args.size], result
                actual = snapshot()
                assert actual[:guard] == bytes([0xA5]) * guard
                assert actual[guard : guard + args.size] == pattern(args.size)
                assert actual[-guard:] == bytes([0xA5]) * guard
            for offset, length in [(12345, 65539), (args.size - 777, 777)]:
                fill()
                result = store.get_into_ranges(
                    [pointer + guard], [[key]], [[[0]]], [[[offset]]], [[[length]]]
                )
                assert result == [[[length]]], result
                actual = snapshot()
                assert actual[:guard] == bytes([0xA5]) * guard
                assert actual[guard : guard + length] == pattern(length, offset)
                assert actual[guard + length :] == bytes([0xA5]) * (
                    capacity - guard - length
                )
        if args.ssd_push:
            assert stats(store)["pending_requests"] == 0
            expected = (4 if args.nested else 3) * args.repetitions
            if args.owner_stats_file:
                poll(
                    lambda: json.loads(Path(args.owner_stats_file).read_text())[
                        "read_requests"
                    ]
                    >= before + expected,
                    args.timeout,
                    "Owner terminal RPC count",
                )
                after = json.loads(Path(args.owner_stats_file).read_text())[
                    "read_requests"
                ]
                assert after - before == expected, (before, after, expected)
            elif args.role == "local":
                assert stats(store)["read_requests"] - before == expected
            print(f"Expected Owner ReadInto RPC delta: {expected}", flush=True)
    finally:
        # Failed recovery/Busy means this external mapping must remain alive.
        pending = store.recover_ssd_push() if args.ssd_push else 0
        assert pending == 0, f"Retain L2 until recovery drains: {pending} pending"
        assert store.unregister_buffer(pointer) == 0
    print(f"PASS: {args.role}, {args.device}, {args.protocol}, key={key}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--role", choices=["local", "owner", "requester"], default="local"
    )
    parser.add_argument("--hostname", default="localhost:17813")
    parser.add_argument("--master", default="127.0.0.1:50051")
    parser.add_argument("--metadata", default="P2PHANDSHAKE")
    parser.add_argument("--protocol", choices=["tcp", "rdma"], default="tcp")
    parser.add_argument("--rdma-devices", default="")
    parser.add_argument("--device", default="cpu", help="cpu, npu:0 or cuda:0")
    parser.add_argument("--ssd-path", default="/tmp/mooncake-ssd-streaming-smoke")
    parser.add_argument("--tenant", default="default")
    parser.add_argument("--key", help="requester: key printed by the owner")
    parser.add_argument("--size", type=int, default=6 * 1024 * 1024)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--hold-seconds", type=float, default=600)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--read-timeout-ms", type=int, default=0)
    parser.add_argument(
        "--nested", action="store_true", help="Also exercise FluentLLM's nested Get API"
    )
    parser.add_argument(
        "--ssd-push",
        action="store_true",
        help="Fixed host L2, one terminal ReadInto RPC",
    )
    parser.add_argument("--io-driver", choices=["uring", "pread"], default="uring")
    parser.add_argument(
        "--peer-directory",
        help="Private shared directory for automatic native bootstrap",
    )
    parser.add_argument(
        "--peer-info-file", help="Write own bootstrap JSON here (private)"
    )
    parser.add_argument(
        "--peer-file", action="append", default=[], help="Read trusted peer JSON here"
    )
    parser.add_argument(
        "--stats-file", help="Publish own non-secret counters atomically"
    )
    parser.add_argument(
        "--owner-stats-file",
        help="Requester: shared Owner counters file for exact RPC assertion",
    )
    parser.add_argument(
        "--expected-read-rpcs", type=int, help="Owner: assert this total on normal exit"
    )
    parser.add_argument(
        "--exercise-rdma-read",
        action="store_true",
        help="Also verify remote MEMORY READ before SSD WRITE",
    )
    args = parser.parse_args()
    if args.ssd_push and args.device != "cpu":
        parser.error("--ssd-push requires a permanently registered CPU L2 buffer")
    if (
        args.ssd_push
        and args.role != "local"
        and not args.peer_directory
        and (not args.peer_info_file or not args.peer_file)
    ):
        parser.error(
            "two-host --ssd-push requires reciprocal --peer-info-file and --peer-file"
        )
    if args.exercise_rdma_read and (args.protocol != "rdma" or args.role == "local"):
        parser.error("--exercise-rdma-read requires two-host --protocol rdma")
    if args.size < 128 * 1024 or args.repetitions < 1:
        parser.error("size must be at least 128 KiB and repetitions positive")
    if args.role == "requester" and not args.key:
        parser.error("requester requires --key from the owner")
    from mooncake.store import MooncakeDistributedStore

    os.environ.setdefault("MOONCAKE_OFFLOAD_LOCAL_STREAMING_READ", "true")
    if args.ssd_push:
        os.environ["MOONCAKE_OFFLOAD_SSD_PUSH"] = "true"
        if args.peer_directory:
            os.environ["MOONCAKE_OFFLOAD_SSD_PEER_DIRECTORY"] = args.peer_directory
    os.environ["MOONCAKE_OFFLOAD_STREAM_USE_URING"] = (
        "true" if args.io_driver == "uring" else "false"
    )
    os.environ.setdefault("MOONCAKE_OFFLOAD_STREAM_SLOT_BYTES", "65536")
    os.environ.setdefault("MOONCAKE_OFFLOAD_STREAM_SLOTS", "4")
    # Fixed-L2 Push reads an object larger than the Owner arena.
    arena = (
        512 * 1024
        if args.ssd_push or (args.role == "local" and args.device == "cpu")
        else 8 * 1024 * 1024
    )
    os.environ.setdefault("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", str(arena))
    os.environ.setdefault("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "1")
    os.environ.setdefault("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "1")
    os.environ.setdefault(
        "MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR", "bucket_storage_backend"
    )
    producer = args.role != "requester"
    if producer:
        Path(args.ssd_path).mkdir(parents=True, exist_ok=True)
    store = MooncakeDistributedStore()
    key = args.key or f"ssd-streaming-{uuid.uuid4().hex}"
    try:
        result = store.setup(
            args.hostname,
            args.metadata,
            # A read-only requester must not receive the producer's MEMORY
            # replica: it has no SSD worker to offload it. Keep all source
            # placement on the sole producer, including the RDMA READ probe.
            max(16 * 1024 * 1024, args.size * 2) if producer else 0,
            16 * 1024 * 1024,
            args.protocol,
            args.rdma_devices,
            args.master,
            None,
            producer,
            args.ssd_path if producer else "",
            args.tenant,
        )
        assert result == 0, result
        if args.ssd_push:
            configure_push(store, args)
        if producer:
            if args.exercise_rdma_read:
                assert store.put(key + "-rdma-memory", pattern(args.size)) == 0
            assert store.put(key, pattern(args.size)) == 0
            poll(
                lambda: any(r.is_local_disk_replica() for r in descriptors(store, key)),
                args.timeout,
                "SSD offload",
            )
            # Querying may refresh the lease; clear after the master TTL and
            # retry rather than assuming the first clear request succeeds.
            poll(
                lambda: bool(
                    store.batch_replica_clear([key], segment_name=args.hostname)
                ),
                args.timeout,
                "MEMORY replica clear",
            )
        poll(lambda: disk_only(store, key), args.timeout, "LOCAL_DISK-only object")
        if args.role == "owner":
            print(f"READY key={key} size={args.size} tenant={args.tenant}", flush=True)
            end = time.monotonic() + args.hold_seconds
            while time.monotonic() < end:
                if args.stats_file and args.ssd_push:
                    private_json(args.stats_file, stats(store))
                time.sleep(0.1)
            if args.ssd_push and args.expected_read_rpcs is not None:
                assert stats(store)["read_requests"] == args.expected_read_rpcs
        else:
            read_and_verify(store, args, key)
    finally:
        if producer:
            store.remove(key)
            if args.exercise_rdma_read:
                store.remove(key + "-rdma-memory")
        if args.stats_file and args.ssd_push:
            private_json(args.stats_file, stats(store))
        assert (
            store.close() == 0
        ), "Close pending: retain registered L2 and recover before freeing it"


if __name__ == "__main__":
    main()
