# SPDX-License-Identifier: Apache-2.0
"""Unit tests for Mooncake remote model loader (FakeStore, no live cluster)."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from typing import Dict

import torch
import torch.nn as nn

from mooncake.model_loader.config import (
    MooncakeStoreLoaderConfig,
    parse_model_name,
    relative_rank_weight_key,
)
from mooncake.model_loader.mooncake_store_connector import MooncakeStoreConnector
from mooncake.model_loader.remote_weight_io import (
    RemoteWeightIO,
    filter_subtensors,
)


class _FakeReplicateConfig:
    def __init__(self):
        self.replica_num = 1
        self.preferred_segments = []


class FakeStore:
    """In-memory stand-in for MooncakeDistributedStore (standalone path)."""

    def __init__(self):
        self._kv: Dict[str, bytes] = {}

    def is_exist(self, key: str) -> int:
        return 1 if key in self._kv else 0

    def get(self, key: str) -> bytes:
        if key not in self._kv:
            raise KeyError(key)
        return self._kv[key]

    def put(self, key: str, data: bytes, config=None) -> int:
        del config
        self._kv[key] = bytes(data)
        return 0

    def register_buffer(self, ptr: int, size: int) -> int:
        del ptr, size
        return 0

    def batch_put_from(self, keys, ptrs, sizes, config=None):
        raise AssertionError("FakeStore uses standalone put path only")

    def batch_get_into(self, keys, ptrs, sizes):
        raise AssertionError("FakeStore uses standalone get path only")


class TinyModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.w = nn.Parameter(torch.randn(4, 4))
        self.b = nn.Parameter(torch.randn(4))


def _standalone_config() -> MooncakeStoreLoaderConfig:
    return MooncakeStoreLoaderConfig(
        local_hostname="localhost",
        metadata_server="P2PHANDSHAKE",
        global_segment_size=1024,
        local_buffer_size=1024,
        protocol="tcp",
        device_name="",
        master_server_address="localhost:50051",
        standalone_storage=True,
        client_server_address="localhost:50052",
    )


class TestParseModelName(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(parse_model_name("mooncake:///qwen3-tp8"), "qwen3-tp8")
        self.assertEqual(
            parse_model_name("mooncake://host/my-model"), "my-model"
        )

    def test_rejects_other_scheme(self):
        with self.assertRaises(ValueError):
            parse_model_name("s3://bucket/model")


class TestFilterSubtensors(unittest.TestCase):
    def test_keeps_unique(self):
        a = torch.randn(2, 2)
        b = torch.randn(3, 3)
        out = filter_subtensors({"a": a, "b": b})
        self.assertEqual(set(out), {"a", "b"})


class TestRemoteWeightIORoundTrip(unittest.TestCase):
    def setUp(self):
        self.store = FakeStore()
        self.config = _standalone_config()
        self.url = "mooncake:///unit-model"

        # Inject ReplicateConfig when the C++ binding omits it (test-only).
        try:
            import mooncake.store as store_mod

            self._had_rep = hasattr(store_mod, "ReplicateConfig")
            if not self._had_rep:
                store_mod.ReplicateConfig = _FakeReplicateConfig
            self.store_mod = store_mod
        except ImportError:
            self.skipTest("mooncake.store not available")

        self.connector = MooncakeStoreConnector(
            self.url, config=self.config, store=self.store
        )

    def tearDown(self):
        self.connector.close()

    def test_save_load_roundtrip(self):
        model = TinyModel()
        src = {k: v.detach().clone() for k, v in model.state_dict().items()}

        with RemoteWeightIO(self.url, connector=self.connector) as io:
            io.save_model(model, model_path=None, rank=0)

            # Corrupt parameters then reload
            with torch.no_grad():
                model.w.zero_()
                model.b.zero_()
            io.load_weights_into(model, rank=0)

        for name, tensor in model.state_dict().items():
            self.assertTrue(torch.allclose(tensor, src[name]))

    def test_sidecar_files(self):
        model = TinyModel()
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = os.path.join(tmp, "config.json")
            with open(cfg_path, "w", encoding="utf-8") as f:
                json.dump({"architectures": ["Tiny"]}, f)

            with RemoteWeightIO(self.url, connector=self.connector) as io:
                io.save_model(model, model_path=tmp, rank=1)
                local = io.materialize_files()
                out = os.path.join(local, "config.json")
                self.assertTrue(os.path.isfile(out))
                with open(out, encoding="utf-8") as f:
                    self.assertEqual(json.load(f)["architectures"], ["Tiny"])

    def test_rank_key_isolation(self):
        model = TinyModel()
        with RemoteWeightIO(self.url, connector=self.connector) as io:
            io.save_model(model, rank=0)
            key = relative_rank_weight_key(0, "w")
            full = f"unit-model/{key}"
            self.assertEqual(self.store.is_exist(full), 1)
            self.assertEqual(
                self.store.is_exist("unit-model/keys/rank_1/w"), 0
            )


class TestConfigFromMapping(unittest.TestCase):
    def test_from_mapping(self):
        cfg = MooncakeStoreLoaderConfig.from_mapping(
            {
                "master": "10.0.0.1:50051",
                "protocol": "rdma",
                "device_name": "mlx5_0",
                "standalone_storage": False,
            }
        )
        self.assertEqual(cfg.master_server_address, "10.0.0.1:50051")
        self.assertEqual(cfg.protocol, "rdma")
        self.assertEqual(cfg.device_name, "mlx5_0")


class TestSeedFromFiles(unittest.TestCase):
    def test_seed_safetensors(self):
        try:
            from safetensors.torch import save_file
        except ImportError:
            self.skipTest("safetensors not installed")

        from mooncake.model_loader.seed_from_files import seed_model_from_files

        store = FakeStore()
        config = _standalone_config()
        url = "mooncake:///seed-model"
        connector = MooncakeStoreConnector(url, config=config, store=store)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "model.safetensors")
                w = torch.ones(4, 4)
                b = torch.ones(4)
                save_file({"w": w, "b": b}, path)
                with open(os.path.join(tmp, "config.json"), "w", encoding="utf-8") as f:
                    json.dump({"ok": True}, f)

                seed_model_from_files(tmp, url, tp_rank=0, connector=connector)

                model = TinyModel()
                with torch.no_grad():
                    model.w.zero_()
                    model.b.zero_()
                with RemoteWeightIO(url, connector=connector) as io:
                    io.load_weights_into(model, rank=0)

                self.assertTrue(torch.allclose(model.w, w))
                self.assertTrue(torch.allclose(model.b, b))
        finally:
            connector.close()


if __name__ == "__main__":
    unittest.main()
