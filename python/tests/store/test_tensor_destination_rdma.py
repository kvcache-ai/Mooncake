"""Real host/CUDA payload destinations; run against an isolated test master."""

import os
import ctypes

import pytest

torch = pytest.importorskip("torch")

from mooncake.store import MooncakeDistributedStore, BufferPool  # noqa: E402
from mooncake.structured_object_store import (  # noqa: E402
    BundleTransferPolicy,
    FieldSchema,
    MooncakeBundleTransfer,
    tensor_destination,
    tensor_object_buffer,
)


@pytest.fixture(scope="module")
def direct_store():
    master = os.getenv("MOONCAKE_ALIGNMENT_MASTER")
    if not master:
        pytest.skip("Set MOONCAKE_ALIGNMENT_MASTER to an isolated test master")
    store = MooncakeDistributedStore()
    assert (
        store.setup(
            os.environ["MOONCAKE_ALIGNMENT_HOST"],
            "P2PHANDSHAKE",
            128 * 1024 * 1024,
            64 * 1024 * 1024,
            "rdma",
            os.environ["MOONCAKE_ALIGNMENT_DEVICE"],
            master,
        )
        == 0
    )
    return store


@pytest.mark.parametrize("device", ["cpu", "cuda"])
@pytest.mark.parametrize("mode", ["copy", "auto", "zero_copy"])
@pytest.mark.parametrize("alignment", [64, 4096])
@pytest.mark.parametrize("rows", [[2, 0, 2], slice(1, 3), None, "member_slice"])
def test_direct_rdma_tensor_destination(direct_store, device, mode, alignment, rows):
    source = torch.full((3, 2, alignment // 4), -1, dtype=torch.int32)
    source[..., :8] = torch.arange(48, dtype=torch.int32).view(3, 2, 8)
    expected = (
        source
        if rows is None
        else source[slice(1, 3) if rows == "member_slice" else rows]
    )
    nbytes = expected.numel() * expected.element_size()
    backing = torch.empty(nbytes + alignment - 1, dtype=torch.uint8, device=device)
    offset = -backing.data_ptr() % alignment
    target = backing[offset : offset + nbytes].view(source.dtype).view(expected.shape)
    transfer = MooncakeBundleTransfer(
        direct_store, key_prefix="test-direct-destination"
    )
    lease = None
    if mode == "zero_copy":
        from mooncake._store import _serialize_tensor

        pool = BufferPool(direct_store, alignment=alignment)
        metadata, pointer, size, owner = _serialize_tensor(source)
        lease = pool.acquire(len(metadata) + size)
        view = lease.buffer
        view[: len(metadata)] = metadata
        ctypes.memmove(lease.ptr + len(metadata), pointer, size)
        view.release()
        value = tensor_object_buffer(
            lease.ptr, len(metadata) + size, lease, batch_size=3
        )
    else:
        value = source
    ref = None
    registered = False
    try:
        ref = transfer.put(
            {"batch": {"selection": value}, "non_tensor_batch": {}, "meta_info": {}},
            type="dataproto",
            policy=BundleTransferPolicy(copy_mode=mode),
            field_schemas={
                "selection": FieldSchema(
                    codec="torch_tensor", direct_alignment=alignment
                )
            },
        )
        assert direct_store.register_buffer(target.data_ptr(), nbytes) == 0
        registered = True
        destination = tensor_destination(
            target, alignment=alignment, pre_registered=True
        )
        if rows == "member_slice":
            location = ref.field_index["selection"]
            spec = transfer.read_spec(ref.stage_refs[location.stage]).slice_member(
                location.member,
                axis=0,
                start=1,
                end=3,
            )
            result = transfer.materialize_into(spec, {location.member: destination})
            assert result.objects[location.member] is target
        else:
            result = transfer.get(
                ref,
                type="dataproto",
                data_cls=dict,
                rows=rows,
                destinations={"selection": destination},
            )
            assert result["batch"]["selection"] is target
        assert target.data_ptr() % alignment == 0
        assert target.stride(1) * target.element_size() == alignment
        torch.testing.assert_close(target.cpu(), expected)
    finally:
        if registered:
            assert direct_store.unregister_buffer(target.data_ptr()) == 0
        if ref is not None:
            transfer.cleanup_dataproto(ref)
        if lease is not None:
            lease.release()
            pool.close()


def test_cuda_tcp_destination_is_rejected_before_payload_write():
    master = os.getenv("MOONCAKE_TCP_ALIGNMENT_MASTER")
    if not master:
        pytest.skip("Set MOONCAKE_TCP_ALIGNMENT_MASTER to a separate TCP test master")
    store = MooncakeDistributedStore()
    assert (
        store.setup(
            os.environ["MOONCAKE_ALIGNMENT_HOST"],
            "P2PHANDSHAKE",
            16 * 1024 * 1024,
            4 * 1024 * 1024,
            "tcp",
            "",
            master,
        )
        == 0
    )
    transfer = MooncakeBundleTransfer(store, key_prefix="test-reject-cuda-tcp")
    source = torch.arange(12, dtype=torch.int32).view(3, 4)
    target = torch.full_like(source, -999, device="cuda")
    ref = transfer.put(
        {"selection": source},
        type="dict",
        field_schemas={
            "selection": FieldSchema(codec="torch_tensor", direct_alignment=1),
        },
    )
    assert (
        store.register_buffer(target.data_ptr(), target.numel() * target.element_size())
        == 0
    )
    try:
        with pytest.raises(RuntimeError, match="get_into_ranges failed"):
            transfer.get(
                ref,
                type="dict",
                destinations={
                    "selection": tensor_destination(target, pre_registered=True),
                },
            )
        assert (target == -999).all().item()
    finally:
        assert store.unregister_buffer(target.data_ptr()) == 0
        transfer.cleanup_dataproto(ref)


def test_native_strict_rejects_uninitialized_client():
    store = MooncakeDistributedStore()
    with pytest.raises(RuntimeError, match="RealClient"):
        store.get_into_ranges([0], [[]], [[]], [[]], [[]], require_direct=True)
