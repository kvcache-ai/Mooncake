#!/usr/bin/env python3
"""Destination validation regressions against a running Store-RS dummy daemon.

Use --case undersized on an unfixed build: its backing allocation is deliberately
large enough for the old unchecked copy. Device rejection tests require the fix.
"""

import argparse
import struct
import sys
import uuid

import torch

from mooncake.store import (
    AXIS_TP,
    READ_MODE_AS_STORED,
    READ_MODE_FULL,
    READ_MODE_SHARD,
    MooncakeDistributedStore,
    ParallelAxis,
    ReadTarget,
    TensorParallelism,
)


def read_cases(store, tenant):
    prefix = f"destination-{uuid.uuid4().hex}"
    weight = torch.arange(4, dtype=torch.float32)
    assert store.put_tensor_with_parallelism(prefix, weight, tenant=tenant) == 0
    yield prefix, ReadTarget(READ_MODE_AS_STORED), weight

    par = TensorParallelism([ParallelAxis(AXIS_TP, 1, 2)])
    global_weight = torch.arange(8, dtype=torch.float32)
    assert (
        store.put_tensor_with_parallelism(
            prefix + "-shard", global_weight, parallelism=par, tenant=tenant
        )
        == 0
    )
    yield (
        prefix + "-shard",
        ReadTarget(READ_MODE_SHARD, parallelism=par),
        global_weight[4:],
    )

    for rank in range(2):
        assert (
            store.put_tensor_with_parallelism(
                prefix + "-full",
                weight,
                parallelism=TensorParallelism([ParallelAxis(AXIS_TP, rank, 2)]),
                tenant=tenant,
            )
            == 0
        )
    yield prefix + "-full", ReadTarget(READ_MODE_FULL), weight


def assert_rejected(operation, backing, message):
    before = backing.clone()
    try:
        operation()
    except ValueError as exc:
        assert message in str(exc), str(exc)
    else:
        raise AssertionError("invalid destination was accepted")
    finally:
        assert torch.equal(backing, before), "rejected read changed backing storage"


def test_undersized(store, tenant):
    for key, target, _ in read_cases(store, tenant):
        backing = torch.full((12,), -123.0)
        destination = backing[3:4]
        assert_rejected(
            lambda: store.get_tensor_with_parallelism(
                key, target=target, tensor=destination, tenant=tenant
            ),
            backing,
            "byte size mismatch",
        )


def test_layout_and_type(store, tenant):
    for key, target, _ in read_cases(store, tenant):
        for kind, message in (
            ("strided", "contiguous"),
            ("dtype", "dtype mismatch"),
            ("shape", "shape mismatch"),
            ("larger", "byte size mismatch"),
        ):
            backing = torch.full(
                (12,), -123, dtype=torch.int32 if kind == "dtype" else torch.float32
            )
            destination = backing[2:6]
            if kind == "strided":
                destination = backing[2:10:2]
            elif kind == "shape":
                destination = destination.reshape(2, 2)
            elif kind == "larger":
                destination = backing[2:10]
            assert_rejected(
                lambda: store.get_tensor_with_parallelism(
                    key, target=target, tensor=destination, tenant=tenant
                ),
                backing,
                message,
            )


def test_success(store, tenant):
    for key, target, expected in read_cases(store, tenant):
        for pinned in (False, True) if torch.cuda.is_available() else (False,):
            backing = torch.full((12,), -123.0, pin_memory=pinned)
            destination = backing[3:7]
            result = store.get_tensor_with_parallelism(
                key, target=target, tensor=destination, tenant=tenant
            )
            assert result is destination
            assert torch.equal(destination, expected)
            assert (backing[:3] == -123).all() and (backing[7:] == -123).all()
        result = store.get_tensor_with_parallelism(key, target=target, tenant=tenant)
        assert torch.equal(result, expected)


def test_devices(store, tenant):
    for key, target, _ in read_cases(store, tenant):
        # Meta has no backing allocation and must be rejected before pointer use.
        destination = torch.empty(4, device="meta")
        try:
            store.get_tensor_with_parallelism(
                key, target=target, tensor=destination, tenant=tenant
            )
        except ValueError as exc:
            assert "CPU" in str(exc), str(exc)
        else:
            raise AssertionError("meta destination was accepted")
        if torch.cuda.is_available():
            destination = torch.full((4,), -123.0, device="cuda")
            assert_rejected(
                lambda: store.get_tensor_with_parallelism(
                    key, target=target, tensor=destination, tenant=tenant
                ),
                destination,
                "CPU",
            )
    if not torch.cuda.is_available():
        print("SKIP: CUDA destination and pinned CPU checks (CUDA unavailable)")


def test_batch(store, tenant):
    for key, target, expected in read_cases(store, tenant):
        first = torch.full_like(expected, -123)
        backing = torch.full((12,), -123.0)
        last = torch.full_like(expected, -123)
        assert_rejected(
            lambda: store.batch_get_tensor_with_parallelism(
                [key, key, key],
                targets=[target, target, target],
                tensors=[first, backing[3:4], last],
                tenant=tenant,
            ),
            backing,
            "byte size mismatch",
        )
        assert torch.equal(first, expected)
        assert (last == -123).all()
        results = store.batch_get_tensor_with_parallelism(
            [key, key], targets=[target, target], tensors=[first, None], tenant=tenant
        )
        assert results[0] is first
        assert torch.equal(results[1], expected)


def test_metadata_and_empty(store, tenant):
    key = f"destination-metadata-{uuid.uuid4().hex}"
    assert (
        store.put_tensor_with_parallelism(
            key, torch.arange(4, dtype=torch.float32), tenant=tenant
        )
        == 0
    )
    raw = bytearray(store.get(key, tenant=tenant))
    data_offset = struct.unpack_from("=Q", raw, 24)[0]
    # A parseable header can have a local shape inconsistent with data_bytes.
    for data_bytes in (4, 24):
        malformed = raw[:data_offset] + bytearray(data_bytes)
        struct.pack_into("=Q", malformed, 32, data_bytes)
        bad_key = f"{key}-{data_bytes}"
        assert store.put(bad_key, bytes(malformed), tenant=tenant) == 0
        backing = torch.full((12,), -123.0)
        assert_rejected(
            lambda: store.get_tensor_with_parallelism(
                bad_key, tensor=backing[3:7], tenant=tenant
            ),
            backing,
            "byte size mismatch",
        )

    # Construct the empty object without exercising the separate tensor write
    # path's zero-length raw-pointer copy.
    empty = raw[:data_offset]
    struct.pack_into("=Q", empty, 32, 0)
    struct.pack_into("=q", empty, 40, 0)  # global_shape[0]
    struct.pack_into("=q", empty, 104, 0)  # local_shape[0]
    assert store.put(key + "-empty", bytes(empty), tenant=tenant) == 0
    destination = torch.empty(0)
    assert (
        store.get_tensor_with_parallelism(
            key + "-empty", tensor=destination, tenant=tenant
        )
        is destination
    )
    scalar = torch.tensor(3.25)
    assert (
        store.put_tensor_with_parallelism(key + "-scalar", scalar, tenant=tenant) == 0
    )
    destination = torch.empty(())
    assert (
        store.get_tensor_with_parallelism(
            key + "-scalar", tensor=destination, tenant=tenant
        )
        is destination
    )
    assert torch.equal(destination, scalar)


def test_overflow(store, tenant):
    key, target, _ = next(read_cases(store, tenant))
    backing = torch.full((12,), -123.0)

    class OverflowDestination:
        device = backing.device
        dtype = backing.dtype
        shape = (4,)

        def data_ptr(self):
            return backing.data_ptr()

        def numel(self):
            return sys.maxsize

        def element_size(self):
            return 4

        def is_contiguous(self):
            return True

    assert_rejected(
        lambda: store.get_tensor_with_parallelism(
            key, target=target, tensor=OverflowDestination(), tenant=tenant
        ),
        backing,
        "overflows",
    )


TESTS = {
    "undersized": test_undersized,
    "layout_and_type": test_layout_and_type,
    "success": test_success,
    "devices": test_devices,
    "batch": test_batch,
    "metadata_and_empty": test_metadata_and_empty,
    "overflow": test_overflow,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daemon_addr", "--daemon-addr", required=True)
    parser.add_argument("--tenant", default="default")
    parser.add_argument("--keyspace", default=None)
    parser.add_argument("--case", choices=TESTS)
    args = parser.parse_args()
    store = MooncakeDistributedStore()
    assert (
        store.setup_dummy(
            mem_pool_size=0,
            local_buffer_size=0,
            server_address=args.daemon_addr,
            keyspace=args.keyspace,
        )
        == 0
    )
    try:
        for name in [args.case] if args.case else TESTS:
            TESTS[name](store, args.tenant)
            print(f"PASS: {name}", flush=True)
    finally:
        store.close()


if __name__ == "__main__":
    main()
