from __future__ import annotations

import pytest

torch = pytest.importorskip("torch")

from mooncake.structured_object_store import (  # noqa: E402
    BundleTransferPolicy,
    FieldSchema,
    StructuredObjectPayload,
    tensor_destination,
)
from test_structured_object_store import NoTensorFastPathStore, make_transfer  # noqa: E402


class DirectStore(NoTensorFastPathStore):
    def get_into_ranges(self, *args, require_direct=False):
        assert require_direct is True
        return super().get_into_ranges(*args)


def aligned_tensor(shape, alignment=64):
    size = torch.empty([], dtype=torch.int32).element_size()
    count = 1
    for dim in shape:
        count *= dim
    backing = torch.empty(count * size + alignment, dtype=torch.uint8)
    offset = -backing.data_ptr() % alignment
    return backing[offset : offset + count * size].view(torch.int32).view(shape)


@pytest.mark.parametrize("mode", ["copy", "auto"])
@pytest.mark.parametrize("rows", [None, slice(1, 3), [2, 0, 2], []])
def test_direct_tensor_payload_uses_caller_tensor_without_deserialization(
    mode, rows, monkeypatch
):
    store, transfer = make_transfer(DirectStore())
    source = torch.full((3, 2, 16), -1, dtype=torch.int32)
    source[..., :9] = torch.arange(54, dtype=torch.int32).view(3, 2, 9)
    ref = transfer.put(
        {"value": source},
        type="dict",
        policy=BundleTransferPolicy(copy_mode=mode),
        field_schemas={"value": FieldSchema(codec="torch_tensor", direct_alignment=64)},
    )
    expected = source if rows is None else source[rows]
    target = aligned_tensor(expected.shape)
    monkeypatch.setattr(
        "mooncake.structured_object_store._deserialize_tensor_payload",
        lambda *_: pytest.fail("direct payload must not be deserialized"),
    )
    result = transfer.get(
        ref,
        type="dict",
        rows=rows,
        destinations={
            "value": tensor_destination(target, alignment=64, pre_registered=True)
        },
    )
    assert result["value"] is target
    torch.testing.assert_close(target, expected)
    assert target.data_ptr() % 64 == 0
    assert store.get_tensor_calls == 0
    assert store.get_into_ranges_calls == (0 if target.numel() == 0 else 1)


def test_tensor_destination_rejects_invalid_contracts():
    target = aligned_tensor((2, 16))
    with pytest.raises(ValueError, match="pre_registered"):
        tensor_destination(target, alignment=64)
    for alignment in (0, 3, True):
        with pytest.raises(ValueError, match="alignment"):
            tensor_destination(target, alignment=alignment, pre_registered=True)
    with pytest.raises(ValueError, match="aligned"):
        tensor_destination(target.view(-1)[1:], alignment=64, pre_registered=True)
    with pytest.raises(ValueError, match="contiguous"):
        tensor_destination(target.t(), pre_registered=True)


@pytest.mark.parametrize("change", ["shape", "dtype", "transport"])
def test_direct_tensor_read_fails_without_staging(change):
    store, transfer = make_transfer(DirectStore())
    source = torch.zeros((2, 16), dtype=torch.int32)
    ref = transfer.put(
        {"value": source},
        type="dict",
        policy=BundleTransferPolicy(copy_mode="copy"),
        field_schemas={"value": FieldSchema(codec="torch_tensor", direct_alignment=1)},
    )
    target = aligned_tensor((2, 16) if change != "shape" else (1, 32))
    if change == "dtype":
        target = target.view(torch.float32)
    if change == "transport":
        transfer._transport._get_into_ranges = None
    with pytest.raises((ValueError, RuntimeError)):
        transfer.get(
            ref,
            type="dict",
            destinations={"value": tensor_destination(target, pre_registered=True)},
        )
    assert store.get_into_ranges_calls == 0


def test_tensor_destination_rejects_mutation_after_registration():
    target = aligned_tensor((2, 16))
    destination = tensor_destination(target, alignment=64, pre_registered=True)
    target.as_strided_((2, 16), (1, 2))
    with pytest.raises(ValueError, match="changed since registration"):
        destination.validate(
            {"encoding": "torch_tensor", "dtype": "torch.int32", "element_size": 4},
            (2, 16),
        )


@pytest.mark.parametrize("offset", [0, 4])
def test_direct_destinations_reject_aliases_before_payload_read(offset):
    store, transfer = make_transfer(DirectStore())
    source = torch.ones((2, 16), dtype=torch.int32)
    ref = transfer.put(
        {"a": source, "b": source * 2},
        type="dict",
        policy=BundleTransferPolicy(copy_mode="copy"),
        field_schemas={
            key: FieldSchema(codec="torch_tensor", direct_alignment=1)
            for key in ("a", "b")
        },
    )
    allocation = aligned_tensor((40,))
    first = allocation[:32].view(2, 16)
    second = allocation[offset : offset + 32].view(2, 16)
    with pytest.raises(ValueError, match="overlap"):
        transfer.get(
            ref,
            type="dict",
            destinations={
                "a": tensor_destination(first, pre_registered=True),
                "b": tensor_destination(second, pre_registered=True),
            },
        )
    assert store.get_into_ranges_calls == 0


def test_only_declared_schema_enables_direct_reads(monkeypatch):
    store, transfer = make_transfer(NoTensorFastPathStore())
    source = torch.arange(32, dtype=torch.int32).view(2, 16)
    plain = transfer.put({"value": source}, type="dict")
    target = aligned_tensor(source.shape)
    with pytest.raises(ValueError, match="schema with direct_alignment"):
        transfer.get(
            plain,
            type="dict",
            destinations={
                "value": tensor_destination(target, alignment=64, pre_registered=True),
            },
        )
    torch.testing.assert_close(transfer.get(plain, type="dict")["value"], source)
    special = transfer.put(
        {"value": source},
        type="dict",
        field_schemas={
            "value": FieldSchema(codec="torch_tensor", direct_alignment=64),
        },
    )
    with pytest.raises(ValueError, match="registered tensor destination"):
        transfer.get(special, type="dict")
    with pytest.raises(ValueError, match="weaker than its schema"):
        transfer.get(
            special,
            type="dict",
            destinations={
                "value": tensor_destination(target, alignment=4, pre_registered=True),
            },
        )


@pytest.mark.parametrize("alignment", [True, 0, -1, 3, "64"])
def test_schema_rejects_invalid_direct_alignment(alignment):
    with pytest.raises(ValueError, match="direct_alignment"):
        FieldSchema(codec="torch_tensor", direct_alignment=alignment)


def test_structured_schema_requires_dense_tensors():
    with pytest.raises(ValueError, match="torch_tensor codec"):
        FieldSchema(codec="utf8_ragged", direct_alignment=64)
    store, transfer = make_transfer(NoTensorFastPathStore())
    with pytest.raises(ValueError, match="dense torch tensor"):
        transfer.put_structured_object(
            StructuredObjectPayload(
                buffers={"value": b"abc"},
                field_schemas={
                    "value": FieldSchema(codec="torch_tensor", direct_alignment=64)
                },
            )
        )


def test_direct_scatter_keeps_schema_alignment_on_output_allocation():
    store, transfer = make_transfer(DirectStore())
    source = torch.arange(6, dtype=torch.int32).view(3, 2)
    ref = transfer.put(
        {"value": source},
        type="dict",
        field_schemas={
            "value": FieldSchema(codec="torch_tensor", direct_alignment=64),
        },
    )
    allocation = aligned_tensor((3, 2))
    destination = tensor_destination(
        allocation[1:], alignment=64, pre_registered=True, allocation=allocation
    )
    result = transfer.get(
        ref, type="dict", rows=[2, 0], destinations={"value": destination}
    )
    assert result["value"] is destination.tensor
    torch.testing.assert_close(allocation[1:], source[[2, 0]])
    with pytest.raises(ValueError, match="inside its allocation"):
        tensor_destination(
            aligned_tensor((2, 2)),
            alignment=64,
            pre_registered=True,
            allocation=allocation,
        )


def test_scatter_fragment_requires_natural_dtype_alignment():
    import ctypes

    root = aligned_tensor((32,))
    buffer = (ctypes.c_ubyte * (root.numel() * root.element_size())).from_address(
        root.data_ptr()
    )
    fragment = torch.frombuffer(buffer, dtype=torch.int32, count=2, offset=1)
    with pytest.raises(ValueError, match="natural alignment"):
        tensor_destination(fragment, alignment=64, pre_registered=True, allocation=root)


@pytest.mark.parametrize("flag", ["negative", "conjugate"])
def test_direct_destination_rejects_logical_view_flags(flag):
    root = torch.empty(
        (2, 4), dtype=torch.int32 if flag == "negative" else torch.complex64
    )
    view = torch._neg_view(root) if flag == "negative" else root.conj()
    with pytest.raises(ValueError, match="unresolved"):
        tensor_destination(view, pre_registered=True)


def test_direct_destination_rejects_logical_allocation_root():
    target = aligned_tensor((2, 4))
    with pytest.raises(ValueError, match="allocation"):
        tensor_destination(
            target, pre_registered=True, allocation=torch._neg_view(target)
        )


def test_direct_destination_detects_logical_flag_mutation():
    root = aligned_tensor((2, 4))
    destination = tensor_destination(root, alignment=64, pre_registered=True)
    torch._C._set_neg(root, True)
    try:
        with pytest.raises(ValueError, match="changed since registration"):
            destination.validate(
                {
                    "encoding": "torch_tensor",
                    "dtype": "torch.int32",
                    "element_size": 4,
                    "direct_alignment": 64,
                },
                (2, 4),
            )
    finally:
        torch._C._set_neg(root, False)


@pytest.mark.parametrize("mode", ["copy", "auto"])
@pytest.mark.parametrize(
    "unsupported", ["scalar", "missing_native_codec", "negative", "conjugate", "sparse"]
)
def test_direct_writer_rejects_unreadable_payload_before_publishing(
    monkeypatch, mode, unsupported
):
    store, transfer = make_transfer(NoTensorFastPathStore())
    value = (
        torch.tensor(3, dtype=torch.int32)
        if unsupported == "scalar"
        else torch.arange(8, dtype=torch.int32)
    )
    if unsupported == "negative":
        value = torch._neg_view(value)
    elif unsupported == "conjugate":
        value = value.to(torch.complex64).conj()
    elif unsupported == "sparse":
        value = value.to_sparse()
    if unsupported == "missing_native_codec":
        monkeypatch.setattr(
            "mooncake.structured_object_store._has_tensor_codec_helpers", lambda: False
        )
    with pytest.raises(
        ValueError, match="native tensor payload codec|unresolved|strided"
    ):
        transfer.put_structured_object(
            StructuredObjectPayload(
                buffers={"value": value},
                field_schemas={
                    "value": FieldSchema(codec="torch_tensor", direct_alignment=64)
                },
            ),
            policy=BundleTransferPolicy(copy_mode=mode),
        )
    assert store.objects == {}


@pytest.mark.parametrize("dtype", [torch.uint8, torch.int16, torch.int32, torch.int64])
@pytest.mark.parametrize("shape", [(0, 2, 4), (2, 0, 4), (2, 2, 4)])
def test_direct_schema_empty_dimensions_and_dtypes(dtype, shape):
    store, transfer = make_transfer(DirectStore())
    source = torch.arange(torch.Size(shape).numel(), dtype=dtype).view(shape)
    ref = transfer.put(
        {"batch": {"value": source}},
        type="dataproto",
        policy=BundleTransferPolicy(copy_mode="copy"),
        field_schemas={"value": FieldSchema(codec="torch_tensor", direct_alignment=64)},
    )
    rows = list(range(shape[0] - 1, -1, -1))
    count = source.numel() * source.element_size()
    backing = torch.empty(count + 64, dtype=torch.uint8)
    offset = -backing.data_ptr() % 64
    target = backing[offset : offset + count].view(dtype).view(shape)
    result = transfer.get(
        ref,
        type="dataproto",
        data_cls=dict,
        rows=rows,
        destinations={
            "value": tensor_destination(target, alignment=64, pre_registered=True)
        },
    )
    assert result["batch"]["value"] is target
    torch.testing.assert_close(target, source[rows])
    assert store.get_into_ranges_calls == (1 if count else 0)


class BoundedPayloadStore(DirectStore):
    """A copy buffer holds at most4096 bytes; large native tensors cannot fit."""

    def put(self, key, value, config=None):
        if "/buffer/" in key and len(value) > 4096:
            return -600
        return super().put(key, value, config=config)

    def put_tensor(self, *args, **kwargs):
        pytest.fail("large direct tensor used the single-object native path")

    pub_tensor = put_tensor

    def put_from(self, key, ptr, size, config=None, *, require_direct=False):
        if not require_direct:
            return super().put_from(key, ptr, size, config=config)
        source, host = self.device_source
        if source.data_ptr() not in self.registered:
            return -500
        offset = ptr - source.data_ptr()
        assert 0 <= offset <= source.nbytes - size
        assert size <= 4096
        self.strict_put_calls += 1
        return self.put(
            key,
            memoryview(host.numpy()).cast("B")[offset : offset + size],
            config=config,
        )


@pytest.mark.parametrize("mode", ["copy", "auto"])
@pytest.mark.parametrize("rows", [None, [2, 0, 2]])
@pytest.mark.parametrize("chunk_bytes", [128, 257, 4096])
@pytest.mark.parametrize("non_contiguous", [False, True])
@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_large_direct_schema_respects_chunk_bytes(
    mode, rows, chunk_bytes, non_contiguous, device
):
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    store, transfer = make_transfer(BoundedPayloadStore())
    source = torch.arange(3 * 2 * 1024, dtype=torch.int32, device=device).view(
        3, 2, 1024
    )
    if non_contiguous:
        source = source.transpose(1, 2)
    direct_device = device == "cuda" and mode == "auto" and not non_contiguous
    store.strict_put_calls = 0
    if direct_device:
        store.device_source = (source, source.cpu())
        assert store.register_buffer(source.data_ptr(), source.nbytes) == 0
    ref = transfer.put(
        {"value": source},
        type="dict",
        chunk_bytes=chunk_bytes,
        policy=BundleTransferPolicy(copy_mode=mode),
        field_schemas={"value": FieldSchema(codec="torch_tensor", direct_alignment=64)},
    )
    if direct_device:
        assert store.unregister_buffer(source.data_ptr()) == 0
        assert store.strict_put_calls > 0
    location = ref.field_index["value"]
    manifest = transfer._bundle_store.resolve_manifest(ref.stage_refs[location.stage])
    chunks = manifest["buffers"][location.member]["chunks"]
    assert len(chunks) > 1
    assert max(chunk["bytes"] for chunk in chunks) <= chunk_bytes
    expected = (source if rows is None else source[rows]).cpu()
    target = aligned_tensor(expected.shape)
    result = transfer.get(
        ref,
        type="dict",
        rows=rows,
        destinations={
            "value": tensor_destination(target, alignment=64, pre_registered=True)
        },
    )
    assert result["value"] is target
    torch.testing.assert_close(target, expected)
    assert store.get_tensor_calls == 0


def test_registered_gpu_source_rejects_missing_registration_and_cleans_header():
    if not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    store, transfer = make_transfer(BoundedPayloadStore())
    source = torch.arange(1024, device="cuda", dtype=torch.int32)
    store.device_source = (source, source.cpu())
    store.strict_put_calls = 0
    with pytest.raises(RuntimeError, match="require_direct=True"):
        transfer.put(
            {"value": source},
            type="dict",
            chunk_bytes=128,
            policy=BundleTransferPolicy(copy_mode="auto"),
            field_schemas={
                "value": FieldSchema(codec="torch_tensor", direct_alignment=64)
            },
        )
    assert not store.objects
    assert not store.registered


@pytest.mark.parametrize("mode", ["copy", "auto"])
def test_ordinary_tensor_keeps_single_object_storage(mode):
    store, transfer = make_transfer(DirectStore())
    source = torch.arange(6144, dtype=torch.int32).view(3, 2, 1024)
    ref = transfer.put(
        {"value": source},
        type="dict",
        chunk_bytes=4096,
        policy=BundleTransferPolicy(copy_mode=mode),
        field_schemas={"value": FieldSchema(codec="torch_tensor")},
    )
    location = ref.field_index["value"]
    manifest = transfer._bundle_store.resolve_manifest(ref.stage_refs[location.stage])
    assert len(manifest["buffers"][location.member]["chunks"]) == 1
    torch.testing.assert_close(transfer.get(ref, type="dict")["value"], source)


def test_copy_chunks_are_assembled_on_demand_and_cleaned_on_failure():
    from mooncake.structured_object_store import _MooncakePayloadTransport

    events = []

    class ObservedGroup:
        def __init__(self, index, fail=False):
            self.index, self.fail = index, fail

        def __iter__(self):
            events.append(("copy", self.index))
            if self.fail:
                raise MemoryError("injected next-chunk allocation failure")
            return iter([memoryview(b"payload")])

    class Store(DirectStore):
        def put(self, key, value, config=None):
            events.append(("put", key))
            return super().put(key, value, config=config)

    store = Store()
    transport = _MooncakePayloadTransport(store)
    transport.put_multi_buffer_payload_chunks(
        ["first", "second"],
        [ObservedGroup(0), ObservedGroup(1)],
        BundleTransferPolicy(copy_mode="copy"),
    )
    assert events == [("copy", 0), ("put", "first"), ("copy", 1), ("put", "second")]
    store.objects.clear()
    events.clear()
    with pytest.raises(MemoryError, match="next-chunk"):
        transport.put_multi_buffer_payload_chunks(
            ["first", "second"],
            [ObservedGroup(0), ObservedGroup(1, fail=True)],
            BundleTransferPolicy(copy_mode="copy"),
        )
    assert store.objects == {}
    assert events == [("copy", 0), ("put", "first"), ("copy", 1)]
