"""CPU codec tests; no Store service required unless MC_CODEC_INTEGRATION=1."""

import os
import struct
import uuid
import zlib

import pytest
import torch

from mooncake._kv_codec import Dtype, InvalidRecord, ScaledFp8Codec
from mooncake.codec_store import CodecStoreClient, StoreReadError


class ByteStore:
    """Exercise only the public put/get contract, including insert-only Put."""

    def __init__(self):
        self.records = {}
        self.put_calls = []
        self.get_calls = []
        self.put_status = 0

    def put(self, key, record, config=None):
        self.put_calls.append((key, record, config))
        if self.put_status == 0:
            self.records.setdefault(key, record)
        return self.put_status

    def get(self, key):
        self.get_calls.append(key)
        return self.records.get(key, b"")


def bytes_view(tensor):
    return memoryview(tensor.view(torch.uint8).numpy()).cast("B")


def reseal(record):
    """Independent CRC32/IEEE implementation for malformed-record tests."""
    result = bytearray(record)
    result[24:28] = bytes(4)
    struct.pack_into("<I", result, 24, zlib.crc32(result))
    return bytes(result)


def reference(source, group_size):
    quantized, scales, decoded = [], [], []
    for group in source.flatten().float().split(group_size):
        maximum = group.abs().max()
        scale = maximum / 448 if maximum else torch.tensor(1.0)
        # The wire contract specifies division using the persisted FP32 scale.
        q = (group / scale).clamp(-448, 448).to(torch.float8_e4m3fn)
        quantized.append(q.view(torch.uint8))
        scales.append(scale)
        restored = (q.double() * scale.double()).clamp(
            -torch.finfo(source.dtype).max, torch.finfo(source.dtype).max
        )
        decoded.append(restored.float().to(source.dtype))
    return (
        torch.cat(quantized),
        torch.stack(scales),
        torch.cat(decoded).reshape(source.shape),
    )


@pytest.mark.parametrize("dtype", [torch.float16, torch.bfloat16])
@pytest.mark.parametrize("group_size", [1, 7, 128, 10000])
def test_roundtrip_matches_independent_torch_reference(dtype, group_size):
    source = torch.randn(2, 3, 137, generator=torch.Generator().manual_seed(42)).to(
        dtype
    )
    source.flatten()[0:2] = torch.tensor([0.0, -0.0], dtype=dtype)
    store = ByteStore()
    client = CodecStoreClient(store, group_size)
    config = object()
    assert client.put("model/shard/tokens", source, config) == 0
    assert store.put_calls[-1][2] is config
    record = store.records[client.storage_key("model/shard/tokens")]
    q, scales, expected = reference(source, group_size)
    offset = 32 + source.ndim * 8
    assert (
        record[offset : offset + scales.numel() * 4]
        == scales.numpy().astype("<f4").tobytes()
    )
    assert record[offset + scales.numel() * 4 :] == bytes(q.tolist())
    destination = torch.full_like(source, 99)
    assert client.get_into("model/shard/tokens", destination) is destination
    assert torch.equal(destination.view(torch.uint8), expected.view(torch.uint8))
    assert store.get_calls == [client.storage_key("model/shard/tokens")]
    assert record == reseal(record)


@pytest.mark.parametrize("dtype", [torch.float16, torch.bfloat16])
def test_all_finite_input_bit_patterns(dtype):
    # Includes every subnormal, extreme magnitude, both signs and signed zero.
    source = torch.arange(65536, dtype=torch.int32).to(torch.uint16).view(dtype)
    source = source[torch.isfinite(source)]
    client = CodecStoreClient(ByteStore(), 257)
    assert client.put("exhaustive", source) == 0
    destination = torch.empty_like(source)
    client.get_into("exhaustive", destination)
    _, _, expected = reference(source, 257)
    assert torch.equal(destination.view(torch.uint8), expected.view(torch.uint8))


def test_wire_golden_and_ties_to_even():
    codec = ScaledFp8Codec(4)
    source = torch.tensor([448, -448, 1, -0.0], dtype=torch.float16)
    # Fixed v1 header, shape, scale=1, canonical E4M3FN payload.
    expected = struct.pack("<8sHBBIQIIQf", b"MCKVFP8\0", 1, 1, 1, 4, 4, 0, 0, 4, 1.0)
    expected = reseal(expected + bytes([0x7E, 0xFE, 0x38, 0x80]))
    assert codec.encode(bytes_view(source), Dtype.FLOAT16, [4]) == expected
    # FP8 adjacent codes: 1.0, 1.125, 1.25; midpoint ties round to even.
    source = torch.tensor([448, 1.0625, 1.1875, -1.0625], dtype=torch.float16)
    record = codec.encode(bytes_view(source), Dtype.FLOAT16, [4])
    assert record[-4:] == bytes([0x7E, 0x38, 0x3A, 0xB8])


def test_encoding_namespace_raw_and_existing_key_semantics():
    store = ByteStore()
    source = torch.ones(1024, dtype=torch.bfloat16)
    original = bytes(bytes_view(source))
    store.put("key", original)
    a, b = CodecStoreClient(store, 128), CodecStoreClient(store, 64)
    assert a.storage_key("key") != b.storage_key("key")
    assert a.storage_key("key") == CodecStoreClient(store, 128).storage_key("key")
    assert a.put("key", source) == b.put("key", source) == 0
    record = store.records[a.storage_key("key")]
    assert len(record) == 32 + 8 + 8 * 4 + 1024
    assert len(record) < len(original)
    assert a.put("key", source * 2) == 0
    assert store.records[a.storage_key("key")] == record
    assert store.get("key") == original


@pytest.mark.parametrize("group_size", [0, -1, 2**32, True, 1.5])
def test_invalid_group(group_size):
    with pytest.raises(ValueError):
        CodecStoreClient(ByteStore(), group_size)


@pytest.mark.parametrize(
    "source",
    [
        torch.ones(3),
        torch.ones(3, dtype=torch.uint8),
        torch.ones(2, 3, dtype=torch.float16).t(),
        torch.empty(0, dtype=torch.float16),
        torch.tensor(1, dtype=torch.float16),
        torch.ones(1, dtype=torch.float16, requires_grad=True),
        torch.ones(3, dtype=torch.float16, device="meta"),
        torch.ones((1,) * 9, dtype=torch.float16),
    ],
)
def test_unsupported_tensors_never_reach_store(source):
    store = ByteStore()
    with pytest.raises(ValueError):
        CodecStoreClient(store).put("key", source)
    assert not store.put_calls


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
@pytest.mark.parametrize("dtype", [torch.float16, torch.bfloat16])
def test_nonfinite_never_reaches_store(value, dtype):
    store = ByteStore()
    with pytest.raises(ValueError, match="Non-finite"):
        CodecStoreClient(store).put("key", torch.tensor([1, value], dtype=dtype))
    assert not store.put_calls


def test_put_error_and_empty_read():
    store = ByteStore()
    store.put_status = -200
    client = CodecStoreClient(store)
    source = torch.ones(5, dtype=torch.float16)
    assert client.put("key", source) == -200
    assert not store.records
    target = torch.full_like(source, 7)
    with pytest.raises(StoreReadError):
        client.get_into("key", target)
    assert torch.equal(target, torch.full_like(source, 7))


@pytest.mark.parametrize("offset", [0, 8, 10, 12, 16, 24, 28, 32, 40, 44])
def test_corruption_does_not_modify_destination(offset):
    store = ByteStore()
    client = CodecStoreClient(store)
    source = torch.ones(16, dtype=torch.float16)
    client.put("key", source)
    record = bytearray(store.records[client.storage_key("key")])
    record[offset] ^= 1
    store.records[client.storage_key("key")] = bytes(record)
    target = torch.full_like(source, 7)
    with pytest.raises(InvalidRecord):
        client.get_into("key", target)
    assert torch.equal(target, torch.full_like(source, 7))


@pytest.mark.parametrize(
    "kind",
    [
        "version",
        "reserved",
        "negative_scale",
        "nan_scale",
        "zero_scale",
        "nan_payload",
        "truncated",
        "trailing",
    ],
)
def test_malformed_records_with_valid_checksum(kind):
    store = ByteStore()
    client = CodecStoreClient(store)
    source = torch.ones(16, dtype=torch.float16)
    client.put("key", source)
    record = bytearray(store.records[client.storage_key("key")])
    if kind == "version":
        record[8] = 2
    elif kind == "reserved":
        record[28] = 1
    elif kind.endswith("scale"):
        value = {"negative_scale": -1, "nan_scale": float("nan"), "zero_scale": 0}[kind]
        struct.pack_into("<f", record, 40, value)
    elif kind == "nan_payload":
        record[-1] = 0xFF
    elif kind == "truncated":
        record = record[:-1]
    else:
        record += b"x"
    store.records[client.storage_key("key")] = reseal(record)
    target = torch.full_like(source, 7)
    with pytest.raises(InvalidRecord):
        client.get_into("key", target)
    assert torch.equal(target, torch.full_like(source, 7))


@pytest.mark.parametrize(
    "destination",
    [
        torch.full((16,), 7, dtype=torch.bfloat16),
        torch.full((4, 4), 7, dtype=torch.float16),
        torch.full((17,), 7, dtype=torch.float16),
    ],
)
def test_descriptor_mismatch_preserves_destination(destination):
    client = CodecStoreClient(ByteStore())
    client.put("key", torch.ones(16, dtype=torch.float16))
    before = destination.clone()
    with pytest.raises(ValueError, match="mismatch"):
        client.get_into("key", destination)
    assert torch.equal(before, destination)


def test_codec_config_mismatch_not_silently_decoded():
    source = torch.ones(16, dtype=torch.float16)
    record = ScaledFp8Codec(128).encode(bytes_view(source), Dtype.FLOAT16, [16])
    with pytest.raises(ValueError, match="mismatch"):
        ScaledFp8Codec(64).decode_into(record, bytes_view(source), Dtype.FLOAT16, [16])


def test_binding_sizes_overflow_and_writable_buffer():
    codec = ScaledFp8Codec()
    for shape in ([], [0], [2**63], [2**32, 2**32], [1] * 9):
        with pytest.raises(ValueError):
            codec.encoded_size(Dtype.FLOAT16, shape)
    with pytest.raises(ValueError):
        codec.encode(b"\0", Dtype.FLOAT16, [1])
    with pytest.raises(ValueError, match="source size"):
        codec.encode(b"\0\0", Dtype.FLOAT16, [2**40])
    record = codec.encode(b"\0\0", Dtype.FLOAT16, [1])
    with pytest.raises(ValueError):
        codec.decode_into(record, bytearray(1), Dtype.FLOAT16, [1])
    with pytest.raises((ValueError, BufferError)):
        codec.decode_into(record, bytes(2), Dtype.FLOAT16, [1])
    for end in range(32):
        with pytest.raises(InvalidRecord):
            codec.decode_into(record[:end], bytearray(2), Dtype.FLOAT16, [1])


@pytest.mark.skipif(
    os.getenv("MC_CODEC_INTEGRATION") != "1", reason="requires an isolated Store master"
)
def test_real_store_tcp_roundtrip():
    from mooncake.store import MooncakeDistributedStore

    store = MooncakeDistributedStore()
    assert (
        store.setup(
            "127.0.0.1",
            "P2PHANDSHAKE",
            32 * 1024**2,
            16 * 1024**2,
            "tcp",
            "",
            os.environ["MASTER_SERVER"],
        )
        == 0
    )
    client = CodecStoreClient(store)
    key = "codec-test-" + uuid.uuid4().hex
    source = torch.randn(2, 4096, generator=torch.Generator().manual_seed(5)).bfloat16()
    try:
        assert client.put(key, source) == 0
        destination = torch.empty_like(source)
        client.get_into(key, destination)
        _, _, expected = reference(source, 128)
        assert torch.equal(destination, expected)
        # Confirm that the real Store holds encoded bytes, not native tensor bytes.
        assert len(store.get(client.storage_key(key))) < source.numel() * 2
    finally:
        store.remove(client.storage_key(key))
        store.close()
