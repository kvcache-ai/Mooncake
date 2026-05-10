import ctypes

import numpy as np
import torch
from tensordict import TensorDict

from mooncake.dataproto_transfer import (
    DataProtoMaterializePolicy,
    DataProtoShardPolicy,
    InMemoryMooncakeStore,
    MooncakeDataProtoTransferBackend,
)
import mooncake.dataproto_transfer as dataproto_transfer
from roll.distributed.scheduler.protocol import DataProto


def make_skewed_dataproto() -> DataProto:
    row_count = 8
    batch = TensorDict(
        {"input_ids": torch.arange(row_count * 2, dtype=torch.int64).reshape(row_count, 2)},
        batch_size=[row_count],
    )
    non_tensors = {
        "text": np.array(["x", "y" * 4096, "z", "w" * 4096, "a", "b", "c", "d"], dtype=object),
        "ragged": np.array([torch.arange(index + 1, dtype=torch.float32) for index in range(row_count)], dtype=object),
    }
    return DataProto.from_dict(tensors=dict(batch.items()), non_tensors=non_tensors, meta_info={"source": "test"})


class CountingInMemoryMooncakeStore(InMemoryMooncakeStore):
    def __init__(self) -> None:
        super().__init__()
        self.get_into_count = 0
        self.batch_get_into_count = 0
        self.get_into_ranges_count = 0
        self.fail_register_size: int | None = None
        self.removed_keys: list[str] = []
        self.fail_put_parts = False

    def register_buffer(self, buffer_ptr: int, size: int) -> int:
        if self.fail_register_size is not None and size == self.fail_register_size:
            return -1
        return super().register_buffer(buffer_ptr, size)

    def remove(self, key: str, force: bool = False) -> int:
        self.removed_keys.append(key)
        return super().remove(key, force)

    def put_parts(self, key: str, *buffers, config=None) -> int:
        joined = bytearray(sum(len(buffer) for buffer in buffers))
        offset = 0
        for buffer in buffers:
            joined[offset : offset + len(buffer)] = buffer
            offset += len(buffer)
        self.objects[key] = bytes(joined)
        return -1 if self.fail_put_parts else 0

    def get_into(self, key: str, buffer_ptr: int, size: int) -> int:
        self.get_into_count += 1
        return super().get_into(key, buffer_ptr, size)

    def batch_get_into(self, keys: list[str], buffer_ptrs: list[int], sizes: list[int]) -> list[int]:
        self.batch_get_into_count += 1
        return super().batch_get_into(keys, buffer_ptrs, sizes)

    def get_into_ranges(self, buffers, all_keys, all_dst_offsets, all_src_offsets, all_sizes):
        self.get_into_ranges_count += 1
        results = []
        for ptr, keys, key_dst_offsets, key_src_offsets, key_sizes in zip(
            buffers,
            all_keys,
            all_dst_offsets,
            all_src_offsets,
            all_sizes,
        ):
            buffer_results = []
            for key, dst_offsets, src_offsets, sizes in zip(keys, key_dst_offsets, key_src_offsets, key_sizes):
                data = self.objects[key]
                key_results = []
                for dst_offset, src_offset, size in zip(dst_offsets, src_offsets, sizes):
                    chunk = data[src_offset : src_offset + size]
                    ctypes.memmove(ptr + dst_offset, chunk, len(chunk))
                    key_results.append(len(chunk) if len(chunk) == size else -1)
                buffer_results.append(key_results)
            results.append(buffer_results)
        return results


def test_dataproto_sharding_byte_balances_skewed_rows_and_roundtrips():
    store = InMemoryMooncakeStore()
    backend = MooncakeDataProtoTransferBackend(store)
    data = make_skewed_dataproto()

    policy = DataProtoShardPolicy(enabled=True, num_shards=4, byte_balanced=True, max_inflight_get=2)
    ref = backend.put_dataproto(data, partition="test", shard_policy=policy)

    assert ref.manifest["policy"]["byte_balanced"] is True
    ranges = [tuple(item) for item in ref.manifest["profile"]["shard_ranges"]]
    assert ranges != [(0, 2), (2, 4), (4, 6), (6, 8)]
    assert ranges[0][0] == 0
    assert ranges[-1][1] == len(data)
    assert all(left[1] == right[0] for left, right in zip(ranges, ranges[1:]))

    materialized = backend.materialize_dataproto(
        ref,
        materialize_policy=DataProtoMaterializePolicy(max_inflight_get=2, pool_prewarm=True),
    )

    assert torch.equal(materialized.batch["input_ids"], data.batch["input_ids"])
    assert materialized.non_tensor_batch["text"].tolist() == data.non_tensor_batch["text"].tolist()
    for actual, expected in zip(materialized.non_tensor_batch["ragged"], data.non_tensor_batch["ragged"]):
        assert torch.equal(actual, expected)
    assert materialized.meta_info["source"] == "test"
    assert materialized.meta_info["mooncake_materialize_profile"]["max_inflight_get"] == 2


def test_adaptive_policy_skips_small_dense_payload_sharding():
    store = CountingInMemoryMooncakeStore()
    backend = MooncakeDataProtoTransferBackend(store)
    data = DataProto.from_dict(
        tensors={"input_ids": torch.arange(8, dtype=torch.int64).reshape(4, 2)},
        non_tensors={},
        meta_info={"source": "small"},
    )

    ref = backend.put_dataproto(data, partition="test", shard_policy=DataProtoShardPolicy(adaptive=True))

    assert not hasattr(ref, "manifest") or ref.manifest.get("layout") != "row_sharded_v1"


def test_adaptive_policy_uses_bounded_parallelism_for_large_payload(monkeypatch):
    monkeypatch.setattr(dataproto_transfer, "RAGGED_TENSOR_PART_CHUNK_BYTES", 16)
    store = CountingInMemoryMooncakeStore()
    backend = MooncakeDataProtoTransferBackend(store)
    data = make_skewed_dataproto()
    policy = DataProtoShardPolicy(
        adaptive=True,
        target_shard_bytes=1024,
        small_payload_threshold=1,
        max_shards=8,
        max_inflight_get=8,
    )

    ref = backend.put_dataproto(data, partition="test", shard_policy=policy)
    materialized = backend.materialize_dataproto(ref)

    assert ref.manifest["policy"]["selected_shards"] > 1
    assert ref.manifest["policy"]["max_inflight_get"] <= 4
    assert ref.manifest["policy"]["byte_balanced"] is False
    profile = ref.manifest["profile"]["transfer_profile"]
    assert profile["total_bytes"] > 0
    assert materialized.meta_info["mooncake_materialize_profile"]["max_inflight_get"] <= 4


def test_materialize_feedback_throttles_next_parallelism(monkeypatch):
    monkeypatch.setattr(dataproto_transfer, "RAGGED_TENSOR_PART_CHUNK_BYTES", 16)
    store = CountingInMemoryMooncakeStore()
    backend = MooncakeDataProtoTransferBackend(store)
    data = make_skewed_dataproto()
    policy = DataProtoShardPolicy(
        adaptive=True,
        target_shard_bytes=1024,
        small_payload_threshold=1,
        max_shards=8,
        max_inflight_get=4,
    )
    ref = backend.put_dataproto(data, partition="test", shard_policy=policy)

    first = backend.materialize_dataproto(ref)
    state = dataproto_transfer._TRANSFER_ADAPTIVE_STATES[id(store)]
    state.pressure_level = "throttle"
    second = backend.materialize_dataproto(ref)

    first_inflight = first.meta_info["mooncake_materialize_profile"]["max_inflight_get"]
    second_profile = second.meta_info["mooncake_materialize_profile"]
    assert second_profile["max_inflight_get"] <= max(1, first_inflight // 2)
    assert second_profile["parallel_feedback"]["max_inflight_get"] == second_profile["max_inflight_get"]


def test_adaptive_policy_byte_balances_large_skewed_rows():
    store = CountingInMemoryMooncakeStore()
    backend = MooncakeDataProtoTransferBackend(store)
    row_count = 4
    large = "x" * (65 * 1024 * 1024)
    data = DataProto.from_dict(
        tensors={"input_ids": torch.arange(row_count, dtype=torch.int64).reshape(row_count, 1)},
        non_tensors={"text": np.array(["a", large, "b", "c"], dtype=object)},
        meta_info={"source": "skewed"},
    )
    policy = DataProtoShardPolicy(
        adaptive=True,
        target_shard_bytes=32 * 1024 * 1024,
        small_payload_threshold=1,
        max_shards=4,
        max_inflight_get=8,
    )

    ref = backend.put_dataproto(data, partition="test", shard_policy=policy)

    assert ref.manifest["policy"]["byte_balanced"] is True
    assert ref.manifest["profile"]["transfer_profile"]["row_skewed"] is True
    assert [tuple(item) for item in ref.manifest["profile"]["shard_ranges"]] != [(0, 1), (1, 2), (2, 3), (3, 4)]


def test_dense_batch_tensors_use_batch_get_into():
    store = CountingInMemoryMooncakeStore()
    backend = MooncakeDataProtoTransferBackend(store)
    row_count = 4
    data = DataProto.from_dict(
        tensors={
            "input_ids": torch.arange(row_count * 2, dtype=torch.int64).reshape(row_count, 2),
            "attention_mask": torch.ones((row_count, 2), dtype=torch.int64),
        },
        non_tensors={},
        meta_info={"source": "dense"},
    )

    ref = backend.put_dataproto(data, partition="test")
    materialized = backend.materialize_dataproto(ref)

    assert store.batch_get_into_count == 1
    assert torch.equal(materialized.batch["input_ids"], data.batch["input_ids"])
    assert torch.equal(materialized.batch["attention_mask"], data.batch["attention_mask"])


def test_shard_level_planner_batches_dense_and_whole_key_leaf_reads():
    store = CountingInMemoryMooncakeStore()
    backend = MooncakeDataProtoTransferBackend(store)
    row_count = 4
    data = DataProto.from_dict(
        tensors={"input_ids": torch.arange(row_count * 2, dtype=torch.int64).reshape(row_count, 2)},
        non_tensors={"text": np.array([f"row-{index}" for index in range(row_count)], dtype=object)},
        meta_info={"source": "planner"},
    )

    ref = backend.put_dataproto(data, partition="test")
    materialized = backend.materialize_dataproto(ref)

    assert store.batch_get_into_count == 1
    assert torch.equal(materialized.batch["input_ids"], data.batch["input_ids"])
    assert materialized.non_tensor_batch["text"].tolist() == data.non_tensor_batch["text"].tolist()


def make_chunked_tensor_dataproto() -> DataProto:
    row_count = 4
    ragged = np.empty(row_count, dtype=object)
    ragged[:] = [torch.arange(16, dtype=torch.float32) + index for index in range(row_count)]
    return DataProto.from_dict(
        tensors={"input_ids": torch.arange(row_count, dtype=torch.int64).reshape(row_count, 1)},
        non_tensors={"ragged": ragged},
        meta_info={"source": "chunked"},
    )


def test_chunked_tensor_payload_uses_get_into_ranges(monkeypatch):
    monkeypatch.setattr(dataproto_transfer, "RAGGED_TENSOR_PART_CHUNK_BYTES", 16)
    store = CountingInMemoryMooncakeStore()
    backend = MooncakeDataProtoTransferBackend(store)
    data = make_chunked_tensor_dataproto()

    ref = backend.put_dataproto(data, partition="test")
    materialized = backend.materialize_dataproto(ref)

    profile = materialized.meta_info["mooncake_materialize_profile"]
    assert store.get_into_ranges_count > 0
    assert profile["direct_range_group_count"] > 0
    assert profile["direct_range_read_s"] >= 0.0
    assert profile["direct_range_fallback_count"] == 0
    assert profile["scratch_copy_s"] == 0.0
    assert torch.equal(materialized.batch["input_ids"], data.batch["input_ids"])
    for actual, expected in zip(materialized.non_tensor_batch["ragged"], data.non_tensor_batch["ragged"]):
        assert torch.equal(actual, expected)


def test_chunked_tensor_payload_falls_back_to_scratch_pool(monkeypatch):
    monkeypatch.setattr(dataproto_transfer, "RAGGED_TENSOR_PART_CHUNK_BYTES", 16)
    store = CountingInMemoryMooncakeStore()
    backend = MooncakeDataProtoTransferBackend(store)
    data = make_chunked_tensor_dataproto()
    tensor_bytes = sum(tensor.element_size() * tensor.numel() for tensor in data.non_tensor_batch["ragged"])
    store.fail_register_size = tensor_bytes

    ref = backend.put_dataproto(data, partition="test")
    materialized = backend.materialize_dataproto(ref)

    profile = materialized.meta_info["mooncake_materialize_profile"]
    assert profile["direct_range_fallback_count"] == 1
    assert profile["range_group_count"] > 0
    assert profile["scratch_copy_s"] >= 0.0
    for actual, expected in zip(materialized.non_tensor_batch["ragged"], data.non_tensor_batch["ragged"]):
        assert torch.equal(actual, expected)


def test_remove_sharded_dataproto_removes_child_manifests_before_parent_manifest():
    store = CountingInMemoryMooncakeStore()
    backend = MooncakeDataProtoTransferBackend(store)
    data = make_skewed_dataproto()
    ref = backend.put_dataproto(
        data,
        partition="test",
        shard_policy=DataProtoShardPolicy(enabled=True, num_shards=2, byte_balanced=False),
    )

    child_manifest_keys = [shard["manifest_key"] for shard in ref.manifest["shards"]]
    backend.remove_dataproto(ref)

    assert all(key in store.removed_keys for key in child_manifest_keys)
    assert store.removed_keys[-1] == ref.manifest_key
    assert not store.objects


def test_put_dataproto_cleans_partial_batch_put_failure():
    class FailingBatchPutStore(CountingInMemoryMooncakeStore):
        def batch_put_from(self, keys: list[str], buffer_ptrs: list[int], sizes: list[int], config=None) -> list[int]:
            results = []
            for index, (key, buffer_ptr, size) in enumerate(zip(keys, buffer_ptrs, sizes)):
                if index == 1:
                    results.append(-1)
                    continue
                self.put_from(key, buffer_ptr, size, config)
                results.append(0)
            return results

    store = FailingBatchPutStore()
    backend = MooncakeDataProtoTransferBackend(store)
    data = DataProto.from_dict(
        tensors={
            "input_ids": torch.arange(8, dtype=torch.int64).reshape(4, 2),
            "attention_mask": torch.ones((4, 2), dtype=torch.int64),
        },
        non_tensors={},
        meta_info={"source": "partial-failure"},
    )

    try:
        backend.put_dataproto(data, partition="test")
    except RuntimeError as exc:
        assert "batch_put_from failed" in str(exc)
    else:
        raise AssertionError("put_dataproto should fail")

    assert not store.objects


def test_put_parts_failure_cleans_partial_chunk_key(monkeypatch):
    monkeypatch.setattr(dataproto_transfer, "RAGGED_TENSOR_PART_CHUNK_BYTES", 16)
    store = CountingInMemoryMooncakeStore()
    store.fail_put_parts = True
    backend = MooncakeDataProtoTransferBackend(store)
    data = make_chunked_tensor_dataproto()

    try:
        backend.put_dataproto(data, partition="test")
    except RuntimeError as exc:
        assert "put_parts failed" in str(exc)
    else:
        raise AssertionError("put_dataproto should fail")

    assert not store.objects


def test_typed_ragged_dtype_uses_non_null_values_when_first_row_is_none():
    store = CountingInMemoryMooncakeStore()
    backend = MooncakeDataProtoTransferBackend(store)
    values = np.array([None, [1, 2], np.array([3, 4], dtype=np.int32)], dtype=object)
    data = DataProto.from_dict(
        tensors={"input_ids": torch.arange(3, dtype=torch.int64).reshape(3, 1)},
        non_tensors={"numbers": values},
        meta_info={"source": "typed-ragged"},
    )

    materialized = backend.materialize_dataproto(backend.put_dataproto(data, partition="test"))

    assert materialized.non_tensor_batch["numbers"][0] is None
    assert materialized.non_tensor_batch["numbers"][1] == [1, 2]
    assert materialized.non_tensor_batch["numbers"][2] == [3, 4]
    leaf = next(entry for entry in backend.put_dataproto(data, partition="test").manifest["leaves"] if entry["path"] == "non_tensor_batch.numbers")
    assert leaf["metadata"]["dtype"] == "int64"
