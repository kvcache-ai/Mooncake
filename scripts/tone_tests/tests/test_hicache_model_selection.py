#!/usr/bin/env python3
import importlib.util
import os
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


class FakeHiCacheStorageBaseMixin:
    @classmethod
    def _get_model_name(cls):
        return "upstream-default-model"


def load_test_module():
    requests = types.ModuleType("requests")
    requests.RequestException = Exception
    sys.modules["requests"] = requests

    file_backend = types.ModuleType("test_hicache_storage_file_backend")
    file_backend.HiCacheStorageBaseMixin = FakeHiCacheStorageBaseMixin
    sys.modules["test_hicache_storage_file_backend"] = file_backend

    sglang = types.ModuleType("sglang")
    sglang.__path__ = []
    sglang_test = types.ModuleType("sglang.test")
    sglang_test.__path__ = []
    test_utils = types.ModuleType("sglang.test.test_utils")
    test_utils.DEFAULT_MLA_MODEL_NAME_FOR_TEST = "upstream-mla-model"
    test_utils.CustomTestCase = unittest.TestCase
    test_utils.find_available_port = lambda base: base
    test_utils.is_in_ci = lambda: False
    sys.modules.update(
        {
            "sglang": sglang,
            "sglang.test": sglang_test,
            "sglang.test.test_utils": test_utils,
        }
    )

    module_path = (
        Path(__file__).resolve().parents[1]
        / "python"
        / "test_hicache_storage_mooncake_backend.py"
    )
    spec = importlib.util.spec_from_file_location("mooncake_hicache_test", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestModelSelection(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_test_module()

    def test_cuda_inherits_upstream_default(self):
        with mock.patch.dict(os.environ, {"CI_ACCELERATOR": "cuda"}):
            self.assertEqual(
                self.module.HiCacheStorageMooncakeBackendBaseMixin._get_model_name(),
                "upstream-default-model",
            )

    def test_rocm_uses_ci_model(self):
        with mock.patch.dict(os.environ, {"CI_ACCELERATOR": "rocm"}):
            self.assertEqual(
                self.module.HiCacheStorageMooncakeBackendBaseMixin._get_model_name(),
                "Qwen/Qwen3-8B",
            )


if __name__ == "__main__":
    unittest.main()
