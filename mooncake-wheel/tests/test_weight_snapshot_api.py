"""Installed-wheel API checks for model-weight snapshot entry points."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import mooncake.store as native_store
from mooncake.reshard.weight.store import WeightStoreWriter


class TestWeightSnapshotApi(unittest.TestCase):
    def test_native_store_forwards_snapshot_factory(self) -> None:
        store = native_store.MooncakeDistributedStore()
        snapshot = object()
        adapter = object()
        expected = object()

        with patch(
            "mooncake.reshard.weight.store.begin_weight_snapshot",
            return_value=expected,
        ) as factory:
            result = store.begin_weight_snapshot(snapshot, adapter)

        self.assertIs(result, expected)
        self.assertEqual(factory.call_args.args[1:], (snapshot, adapter))
        self.assertIsInstance(
            factory.call_args.args[0], native_store.MooncakeDistributedStore
        )

    def test_weight_store_writer_replaces_parallelism_api(self) -> None:
        store = native_store.MooncakeDistributedStore()

        self.assertTrue(callable(store.begin_weight_snapshot))
        self.assertTrue(issubclass(WeightStoreWriter, object))

        for name in (
            "get_tensor_with_parallelism",
            "batch_get_tensor_with_parallelism",
            "get_tensor_with_parallelism_into",
            "batch_get_tensor_with_parallelism_into",
            "put_tensor_with_parallelism",
            "batch_put_tensor_with_parallelism",
            "put_tensor_with_parallelism_from",
            "batch_put_tensor_with_parallelism_from",
            "upsert_tensor_with_parallelism",
            "upsert_tensor_with_parallelism_from",
            "batch_upsert_tensor_with_parallelism",
            "batch_upsert_tensor_with_parallelism_from",
        ):
            self.assertFalse(hasattr(store, name), name)

        for name in ("ParallelAxis", "TensorParallelism", "ReadTarget"):
            self.assertFalse(hasattr(native_store, name), name)
