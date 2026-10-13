"""Unit tests for mooncake.tpu against fake adapter and provider objects.

The native adapter is covered by the TENT C++ tests (tent_tpu_pjrt_adapter_test,
built with -DUSE_TPU=ON); these tests need neither a TPU nor that build.
"""

import os
import unittest
from unittest import mock

from mooncake import tpu


class FakeAdapterLib:
    """Mimics the adapter's ctypes surface; tracks ownership of handles."""

    def __init__(self):
        self.provider_path = None
        self.registered = {}  # token -> handle
        self.released = []
        self.next_token = 0x7F0000000000
        self.fail_handles = set()

    def mc_tpu_adapter_load_provider(self, path):
        self.provider_path = path.decode()
        return 0

    def mc_tpu_adapter_register(self, handle, token_ref, size_ref, device_ref):
        if handle in self.fail_handles:
            self.released.append(handle)  # adapter owns it even on failure
            return 1
        token = self.next_token
        self.next_token += 1 << 30
        self.registered[token] = handle
        token_ref._obj.value = token
        size_ref._obj.value = 4096 * handle
        device_ref._obj.value = handle % 4
        return 0

    def mc_tpu_adapter_unregister(self, token):
        if token not in self.registered:
            return 1
        self.released.append(self.registered.pop(token))
        return 0

    def mc_tpu_adapter_release_buffer(self, handle):
        self.released.append(handle)
        return 0


class FakeProvider:
    """Arrays are lists of handles, exported as-is."""

    def library_path(self):
        return "/fake/libprovider.so"

    def export_shards(self, array):
        return list(array)


class TpuTest(unittest.TestCase):
    def setUp(self):
        self.lib = FakeAdapterLib()
        self.adapter = tpu.TpuAdapter(
            path="/fake/libmooncake_tpu_pjrt.so", lib=self.lib
        )
        self.provider = FakeProvider()

    def test_prepare_registers_every_shard_and_binds_provider(self):
        shards = tpu.prepare(
            [[1, 2], [3]], provider=self.provider, adapter=self.adapter
        )

        self.assertEqual(self.lib.provider_path, "/fake/libprovider.so")
        self.assertEqual([len(s) for s in shards], [2, 1])
        self.assertEqual(shards[0][1].size, 4096 * 2)
        self.assertEqual(shards[1][0].device, 3)
        self.assertEqual(sorted(self.lib.registered.values()), [1, 2, 3])

    def test_failure_unregisters_and_releases_everything(self):
        self.lib.fail_handles = {5}

        with self.assertRaises(RuntimeError):
            tpu.prepare(
                [[1, 2], [4, 5, 6, 7]], provider=self.provider, adapter=self.adapter
            )
        self.assertEqual(self.lib.registered, {})
        # 1, 2, 4 unregistered; 5 released by the failed register; 6, 7 never
        # adopted, so released explicitly.
        self.assertEqual(sorted(self.lib.released), [1, 2, 4, 5, 6, 7])

    def test_unregister(self):
        shards = tpu.prepare([[1, 2]], provider=self.provider, adapter=self.adapter)

        tpu.unregister(shards[0], adapter=self.adapter)
        self.assertEqual(self.lib.registered, {})
        with self.assertRaises(ValueError):
            tpu.unregister(shards[0], adapter=self.adapter)

    def test_register_with_engine_passes_tokens_and_sizes(self):
        shards = tpu.prepare([[1], [2]], provider=self.provider, adapter=self.adapter)
        engine = mock.Mock()
        engine.batch_register_memory.return_value = 0

        tpu.register_with_engine(engine, shards)
        engine.batch_register_memory.assert_called_once_with(
            [shards[0][0].token, shards[1][0].token], [4096, 8192]
        )

    def test_register_with_engine_raises_on_error(self):
        shards = tpu.prepare([[1]], provider=self.provider, adapter=self.adapter)
        engine = mock.Mock()
        engine.batch_register_memory.return_value = -1

        with self.assertRaisesRegex(RuntimeError, "batch_register_memory"):
            tpu.register_with_engine(engine, shards)

    def test_register_with_engine_skips_empty(self):
        engine = mock.Mock()
        tpu.register_with_engine(engine, [[]])
        engine.batch_register_memory.assert_not_called()

    def test_get_adapter_rejects_a_different_configured_library(self):
        with mock.patch.object(tpu, "_adapter", None), mock.patch.dict(
            os.environ, {tpu.ADAPTER_LIB_ENV: "/somewhere/else.so"}
        ):
            with self.assertRaisesRegex(RuntimeError, "same library"):
                tpu.get_adapter("/fake/libmooncake_tpu_pjrt.so")

    def test_get_adapter_points_tent_at_the_loaded_file(self):
        with mock.patch.object(tpu, "_adapter", None), mock.patch.dict(
            os.environ, {}, clear=True
        ), mock.patch.object(tpu, "_load_adapter", return_value=self.lib):
            adapter = tpu.get_adapter("/fake/dir/libmooncake_tpu_pjrt.so")
            self.assertEqual(
                os.environ[tpu.ADAPTER_LIB_ENV], "/fake/dir/libmooncake_tpu_pjrt.so"
            )
            self.assertIs(tpu.get_adapter(), adapter)

    def test_missing_default_provider_has_actionable_error(self):
        with mock.patch.dict("sys.modules", {"tpu_sync": None}):
            with self.assertRaisesRegex(ImportError, "provider"):
                tpu.prepare([[1]], adapter=self.adapter)

    def test_default_adapter_path_is_next_to_the_module(self):
        self.assertEqual(
            os.path.dirname(tpu.default_adapter_path()),
            os.path.dirname(os.path.abspath(tpu.__file__)),
        )


if __name__ == "__main__":
    unittest.main()
