#!/usr/bin/env python3
import asyncio
import json
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from aiohttp import web as _unused_web  # noqa: F401
except ModuleNotFoundError:
    aiohttp_module = types.ModuleType("aiohttp")
    web_module = types.ModuleType("aiohttp.web")

    class Response:
        def __init__(self, status=200, text="", content_type=None):
            self.status = status
            self.text = text
            self.content_type = content_type

    web_module.Response = Response
    aiohttp_module.web = web_module
    sys.modules["aiohttp"] = aiohttp_module
    sys.modules["aiohttp.web"] = web_module

try:
    from mooncake.store import MooncakeDistributedStore as _unused_store  # noqa: F401
except ModuleNotFoundError:
    store_module = types.ModuleType("mooncake.store")

    class MooncakeDistributedStore:
        pass

    store_module.MooncakeDistributedStore = MooncakeDistributedStore
    sys.modules["mooncake.store"] = store_module

from mooncake.mooncake_store_service import MooncakeStoreService, _shm_name_to_path


class FakeStore:
    def __init__(self):
        self.mounted = {}
        self.mount_calls = []
        self.unmount_calls = []
        self.fail_mount = False
        self.unmount_failures = set()
        self.allocated_mount_calls = []
        self.free_unmount_calls = []

    def mount_segment(self, path, size, offset, protocol, location):
        self.mount_calls.append((path, size, offset, protocol, location))
        if self.fail_mount:
            return {"ret": -1, "segment_ids": []}
        segment_id = "00000000-0000-0000-0000-000000000001"
        self.mounted[segment_id] = {
            "path": path,
            "size": size,
            "offset": offset,
            "protocol": protocol,
            "location": location,
        }
        return {"ret": 0, "segment_ids": [segment_id]}

    def unmount_segment(self, segment_ids):
        self.unmount_calls.append(list(segment_ids))
        for segment_id in segment_ids:
            if segment_id in self.unmount_failures:
                return -1
        for segment_id in segment_ids:
            self.mounted.pop(segment_id, None)
        return 0

    def allocate_and_mount_segment(self, size, protocol, location):
        self.allocated_mount_calls.append((size, protocol, location))
        segment_id = "00000000-0000-0000-0000-000000000002"
        return {
            "ret": 0,
            "segment_ids": [segment_id],
            "allocated_size": 4096,
        }

    def unmount_and_free_segment(self, segment_ids):
        self.free_unmount_calls.append(list(segment_ids))
        return 0


class FakeRequest:
    def __init__(self, body):
        self.body = body

    async def json(self):
        return self.body


class StoreServiceApiTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake_store = FakeStore()
        self.service = MooncakeStoreService.__new__(MooncakeStoreService)
        self.service.store = self.fake_store
        self.service.config = SimpleNamespace(protocol="tcp")
        self.service.current_mode = "prefill"
        self.service.mounted_segment_ids = []
        self.service.last_mount_info = {}
        self.service._state_lock = asyncio.Lock()

    async def test_mount_shm_then_unmount_shm_api(self):
        mount_resp = await self.service.handle_mount_shm(
            FakeRequest(
                {
                    "name": "mooncake-segment",
                    "size": 4096,
                    "offset": 128,
                    "protocol": "tcp",
                    "location": "cpu:0",
                }
            )
        )
        self.assertEqual(mount_resp.status, 200)
        mount_body = json.loads(mount_resp.text)
        self.assertEqual(mount_body["status"], "success")
        self.assertEqual(
            mount_body["segment_ids"],
            ["00000000-0000-0000-0000-000000000001"],
        )
        self.assertEqual(
            self.fake_store.mount_calls,
            [("/dev/shm/mooncake-segment", 4096, 128, "tcp", "cpu:0")],
        )

        unmount_resp = await self.service.handle_unmount_shm(
            FakeRequest({"segment_ids": mount_body["segment_ids"]})
        )
        self.assertEqual(unmount_resp.status, 200)
        unmount_body = json.loads(unmount_resp.text)
        self.assertEqual(unmount_body["status"], "success")
        self.assertEqual(
            self.fake_store.unmount_calls,
            [["00000000-0000-0000-0000-000000000001"]],
        )
        self.assertEqual(self.service.current_mode, "prefill")

    async def test_unmount_shm_updates_state_for_partial_success(self):
        succeeded_id = "00000000-0000-0000-0000-000000000001"
        failed_id = "00000000-0000-0000-0000-000000000002"
        self.service.current_mode = "decode"
        self.service.mounted_segment_ids = [succeeded_id, failed_id]
        self.fake_store.unmount_failures = {failed_id}

        resp = await self.service.handle_unmount_shm(
            FakeRequest({"segment_ids": [succeeded_id, failed_id]})
        )

        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertEqual(body["failed_segment_ids"], [failed_id])
        self.assertEqual(
            self.fake_store.unmount_calls,
            [[succeeded_id], [failed_id]],
        )
        self.assertEqual(self.service.mounted_segment_ids, [failed_id])
        self.assertEqual(self.service.current_mode, "decode")

    async def test_reconfigure_decode_mount_failure_rolls_back_to_prefill(self):
        old_id = "00000000-0000-0000-0000-000000000001"
        self.service.current_mode = "decode"
        self.service.mounted_segment_ids = [old_id]
        self.service.last_mount_info = {
            "path": "/dev/shm/old",
            "offset": 0,
            "size": 4096,
            "protocol": "tcp",
            "location": "",
        }
        self.fake_store.fail_mount = True

        resp = await self.service.handle_reconfigure(
            FakeRequest({"mode": "decode", "path": "/dev/shm/new", "size": 4096})
        )

        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertEqual(body["mode"], "prefill")
        self.assertIn("rolled back to prefill", body["error"])
        self.assertEqual(self.fake_store.unmount_calls, [[old_id]])
        self.assertEqual(self.service.mounted_segment_ids, [])
        self.assertEqual(self.service.current_mode, "prefill")
        self.assertEqual(self.service.last_mount_info, {})

    async def test_mount_allocates_and_frees_on_unmount(self):
        mount_resp = await self.service.handle_mount(
            FakeRequest({"size": 1, "protocol": "tcp", "location": "cpu:0"})
        )
        self.assertEqual(mount_resp.status, 200)
        mount_body = json.loads(mount_resp.text)
        self.assertEqual(mount_body["status"], "success")
        self.assertEqual(
            mount_body["segment_ids"],
            ["00000000-0000-0000-0000-000000000002"],
        )
        self.assertEqual(mount_body["allocated_size"], 4096)
        self.assertEqual(self.fake_store.allocated_mount_calls, [(1, "tcp", "cpu:0")])

        unmount_resp = await self.service.handle_unmount(
            FakeRequest({"segment_ids": mount_body["segment_ids"]})
        )
        self.assertEqual(unmount_resp.status, 200)
        unmount_body = json.loads(unmount_resp.text)
        self.assertEqual(unmount_body["status"], "success")
        self.assertEqual(
            self.fake_store.free_unmount_calls,
            [["00000000-0000-0000-0000-000000000002"]],
        )

    async def test_mount_shm_requires_name_and_size(self):
        resp = await self.service.handle_mount_shm(
            FakeRequest({"name": "mooncake-segment"})
        )
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("name or size", body["error"])

    async def test_mount_shm_rejects_path_like_name(self):
        resp = await self.service.handle_mount_shm(
            FakeRequest({"name": "../foo", "size": 4096})
        )
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("name or size", body["error"])

    async def test_mount_requires_positive_size(self):
        resp = await self.service.handle_mount(FakeRequest({"size": 0}))
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Invalid size", body["error"])

    async def test_unmount_shm_requires_segment_ids(self):
        resp = await self.service.handle_unmount_shm(FakeRequest({}))
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Missing segment_ids", body["error"])

    # ==================== /api/put tests ====================

    async def test_handle_put_success(self):
        self.fake_store.put = lambda key, value: 0
        resp = await self.service.handle_put(
            FakeRequest({"key": "test_key", "value": "test_value"})
        )
        self.assertEqual(resp.status, 200)
        body = json.loads(resp.text)
        self.assertEqual(body["status"], "success")

    async def test_handle_put_missing_key(self):
        self.fake_store.put = lambda key, value: 0
        resp = await self.service.handle_put(FakeRequest({"value": "test_value"}))
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Missing key or value", body["error"])

    async def test_handle_put_missing_value(self):
        self.fake_store.put = lambda key, value: 0
        resp = await self.service.handle_put(FakeRequest({"key": "k"}))
        self.assertEqual(resp.status, 500)

    async def test_handle_put_store_failure(self):
        self.fake_store.put = lambda key, value: -1
        resp = await self.service.handle_put(FakeRequest({"key": "k", "value": "v"}))
        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertIn("PUT operation failed", body["error"])

    async def test_handle_put_empty_key(self):
        self.fake_store.put = lambda key, value: 0
        resp = await self.service.handle_put(FakeRequest({"key": "", "value": "v"}))
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Missing key or value", body["error"])

    # ==================== /api/get/{key} tests ====================

    async def test_handle_get_success(self):
        self.fake_store.get = lambda key: b"payload_bytes"
        request = FakeRequest({})
        request.match_info = {"key": "my_key"}
        resp = await self.service.handle_get(request)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.body, b"payload_bytes")

    async def test_handle_get_not_found(self):
        self.fake_store.get = lambda key: None
        request = FakeRequest({})
        request.match_info = {"key": "missing_key"}
        resp = await self.service.handle_get(request)
        self.assertEqual(resp.status, 404)
        body = json.loads(resp.text)
        self.assertIn("Key not found", body["error"])

    async def test_handle_get_empty_bytes(self):
        self.fake_store.get = lambda key: b""
        request = FakeRequest({})
        request.match_info = {"key": "empty_value_key"}
        resp = await self.service.handle_get(request)
        self.assertEqual(resp.status, 404)
        body = json.loads(resp.text)
        self.assertIn("Key not found", body["error"])

    async def test_handle_get_store_exception(self):
        def raise_error(key):
            raise RuntimeError("store crashed")

        self.fake_store.get = raise_error
        request = FakeRequest({})
        request.match_info = {"key": "crash_key"}
        resp = await self.service.handle_get(request)
        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertIn("store crashed", body["error"])

    # ==================== /api/exist/{key} tests ====================

    async def test_handle_exist_true(self):
        self.fake_store.is_exist = lambda key: True
        request = FakeRequest({})
        request.match_info = {"key": "existing_key"}
        resp = await self.service.handle_exist(request)
        self.assertEqual(resp.status, 200)
        body = json.loads(resp.text)
        self.assertTrue(body["exists"])

    async def test_handle_exist_false(self):
        self.fake_store.is_exist = lambda key: False
        request = FakeRequest({})
        request.match_info = {"key": "missing_key"}
        resp = await self.service.handle_exist(request)
        self.assertEqual(resp.status, 200)
        body = json.loads(resp.text)
        self.assertFalse(body["exists"])

    async def test_handle_exist_store_exception(self):
        def raise_error(key):
            raise RuntimeError("exist check crashed")

        self.fake_store.is_exist = raise_error
        request = FakeRequest({})
        request.match_info = {"key": "crash_key"}
        resp = await self.service.handle_exist(request)
        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertIn("exist check crashed", body["error"])

    # ==================== /api/remove/{key} tests ====================

    async def test_handle_remove_success(self):
        self.fake_store.remove = lambda key: 0
        request = FakeRequest({})
        request.match_info = {"key": "removable_key"}
        resp = await self.service.handle_remove(request)
        self.assertEqual(resp.status, 200)
        body = json.loads(resp.text)
        self.assertEqual(body["status"], "success")

    async def test_handle_remove_failure(self):
        self.fake_store.remove = lambda key: -1
        request = FakeRequest({})
        request.match_info = {"key": "stuck_key"}
        resp = await self.service.handle_remove(request)
        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertIn("Remove operation failed", body["error"])

    async def test_handle_remove_store_exception(self):
        def raise_error(key):
            raise RuntimeError("remove crashed")

        self.fake_store.remove = raise_error
        request = FakeRequest({})
        request.match_info = {"key": "crash_key"}
        resp = await self.service.handle_remove(request)
        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertIn("remove crashed", body["error"])

    # ==================== /api/remove_all tests ====================

    async def test_handle_remove_all_success(self):
        self.fake_store.remove_all = lambda: 5
        resp = await self.service.handle_remove_all(FakeRequest({}))
        self.assertEqual(resp.status, 200)
        body = json.loads(resp.text)
        self.assertIn("5", body["status"])

    async def test_handle_remove_all_zero_keys(self):
        self.fake_store.remove_all = lambda: 0
        resp = await self.service.handle_remove_all(FakeRequest({}))
        self.assertEqual(resp.status, 200)
        body = json.loads(resp.text)
        self.assertIn("0", body["status"])

    async def test_handle_remove_all_failure(self):
        self.fake_store.remove_all = lambda: -1
        resp = await self.service.handle_remove_all(FakeRequest({}))
        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertIn("RemoveAll operation failed", body["error"])

    async def test_handle_remove_all_store_exception(self):
        def raise_error():
            raise RuntimeError("remove_all crashed")

        self.fake_store.remove_all = raise_error
        resp = await self.service.handle_remove_all(FakeRequest({}))
        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertIn("remove_all crashed", body["error"])

    # ==================== /api/reconfigure tests ====================

    async def test_reconfigure_decode_success(self):
        self.fake_store.fail_mount = False
        resp = await self.service.handle_reconfigure(
            FakeRequest(
                {
                    "mode": "decode",
                    "path": "/dev/shm/test",
                    "size": 4096,
                }
            )
        )
        self.assertEqual(resp.status, 200)
        body = json.loads(resp.text)
        self.assertEqual(body["mode"], "decode")
        self.assertEqual(body["status"], "success")
        self.assertIn("segment_ids", body)
        self.assertEqual(self.service.current_mode, "decode")

    async def test_reconfigure_decode_missing_path(self):
        resp = await self.service.handle_reconfigure(
            FakeRequest({"mode": "decode", "size": 4096})
        )
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Missing path or size", body["error"])

    async def test_reconfigure_decode_missing_size(self):
        resp = await self.service.handle_reconfigure(
            FakeRequest({"mode": "decode", "path": "/dev/shm/test"})
        )
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Missing path or size", body["error"])

    async def test_reconfigure_prefill_success(self):
        self.service.current_mode = "decode"
        self.service.mounted_segment_ids = []
        resp = await self.service.handle_reconfigure(FakeRequest({"mode": "prefill"}))
        self.assertEqual(resp.status, 200)
        body = json.loads(resp.text)
        self.assertEqual(body["mode"], "prefill")
        self.assertEqual(self.service.current_mode, "prefill")

    async def test_reconfigure_prefill_unmounts_segments(self):
        sid = "00000000-0000-0000-0000-000000000001"
        self.service.current_mode = "decode"
        self.service.mounted_segment_ids = [sid]
        resp = await self.service.handle_reconfigure(FakeRequest({"mode": "prefill"}))
        self.assertEqual(resp.status, 200)
        self.assertEqual(self.fake_store.unmount_calls, [[sid]])
        self.assertEqual(self.service.mounted_segment_ids, [])
        self.assertEqual(self.service.current_mode, "prefill")

    async def test_reconfigure_prefill_unmount_failure(self):
        sid = "00000000-0000-0000-0000-000000000002"
        self.service.current_mode = "decode"
        self.service.mounted_segment_ids = [sid]
        self.fake_store.unmount_failures = {sid}
        resp = await self.service.handle_reconfigure(FakeRequest({"mode": "prefill"}))
        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertIn("Unmount failed", body["error"])

    async def test_reconfigure_invalid_mode(self):
        resp = await self.service.handle_reconfigure(FakeRequest({"mode": "invalid"}))
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Invalid mode", body["error"])

    async def test_reconfigure_empty_mode(self):
        resp = await self.service.handle_reconfigure(FakeRequest({}))
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Invalid mode", body["error"])

    async def test_reconfigure_decode_remount_unmounts_previous(self):
        old_id = "00000000-0000-0000-0000-000000000001"
        self.service.current_mode = "decode"
        self.service.mounted_segment_ids = [old_id]
        self.fake_store.fail_mount = False
        resp = await self.service.handle_reconfigure(
            FakeRequest(
                {
                    "mode": "decode",
                    "path": "/dev/shm/new",
                    "size": 8192,
                }
            )
        )
        self.assertEqual(resp.status, 200)
        self.assertEqual(self.fake_store.unmount_calls, [[old_id]])
        self.assertEqual(self.service.current_mode, "decode")

    # ==================== /api/mount edge cases ====================

    async def test_mount_negative_size(self):
        resp = await self.service.handle_mount(FakeRequest({"size": -1}))
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Invalid size", body["error"])

    async def test_mount_float_size(self):
        resp = await self.service.handle_mount(FakeRequest({"size": 1.5}))
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Invalid size", body["error"])

    async def test_mount_string_size(self):
        resp = await self.service.handle_mount(FakeRequest({"size": "1024"}))
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Invalid size", body["error"])

    async def test_mount_missing_size(self):
        resp = await self.service.handle_mount(FakeRequest({}))
        self.assertEqual(resp.status, 400)

    # ==================== /api/unmount edge cases ====================

    async def test_unmount_requires_segment_ids(self):
        resp = await self.service.handle_unmount(FakeRequest({}))
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Missing segment_ids", body["error"])

    async def test_unmount_empty_list(self):
        resp = await self.service.handle_unmount(FakeRequest({"segment_ids": []}))
        self.assertEqual(resp.status, 400)
        body = json.loads(resp.text)
        self.assertIn("Missing segment_ids", body["error"])

    async def test_unmount_success(self):
        resp = await self.service.handle_unmount(
            FakeRequest({"segment_ids": ["00000000-0000-0000-0000-000000000002"]})
        )
        self.assertEqual(resp.status, 200)
        body = json.loads(resp.text)
        self.assertEqual(body["status"], "success")

    # ==================== /api/mount_shm edge cases ====================

    async def test_mount_shm_store_failure(self):
        self.fake_store.fail_mount = True
        resp = await self.service.handle_mount_shm(
            FakeRequest({"name": "test-seg", "size": 4096})
        )
        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertIn("Mount failed", body["error"])

    async def test_mount_shm_with_defaults(self):
        resp = await self.service.handle_mount_shm(
            FakeRequest({"name": "minimal-seg", "size": 2048})
        )
        self.assertEqual(resp.status, 200)
        self.assertEqual(
            self.fake_store.mount_calls,
            [("/dev/shm/minimal-seg", 2048, 0, "tcp", "")],
        )

    async def test_mount_shm_empty_name(self):
        resp = await self.service.handle_mount_shm(
            FakeRequest({"name": "", "size": 4096})
        )
        self.assertEqual(resp.status, 400)

    async def test_mount_shm_dot_name(self):
        resp = await self.service.handle_mount_shm(
            FakeRequest({"name": ".", "size": 4096})
        )
        self.assertEqual(resp.status, 400)

    async def test_mount_shm_dotdot_name(self):
        resp = await self.service.handle_mount_shm(
            FakeRequest({"name": "..", "size": 4096})
        )
        self.assertEqual(resp.status, 400)

    # ==================== /api/unmount_shm edge cases ====================

    async def test_unmount_shm_string_segment_id_coercion(self):
        resp = await self.service.handle_unmount_shm(
            FakeRequest({"segment_ids": "00000000-0000-0000-0000-000000000001"})
        )
        self.assertEqual(resp.status, 200)
        self.assertEqual(
            self.fake_store.unmount_calls,
            [["00000000-0000-0000-0000-000000000001"]],
        )

    async def test_unmount_shm_empty_list(self):
        resp = await self.service.handle_unmount_shm(FakeRequest({"segment_ids": []}))
        self.assertEqual(resp.status, 400)


class ShmNameToPathTest(unittest.TestCase):
    def test_valid_simple_name(self):
        self.assertEqual(_shm_name_to_path("my-segment"), "/dev/shm/my-segment")

    def test_valid_leading_slash(self):
        self.assertEqual(_shm_name_to_path("/my-segment"), "/dev/shm/my-segment")

    def test_rejects_empty(self):
        self.assertIsNone(_shm_name_to_path(""))

    def test_rejects_none(self):
        self.assertIsNone(_shm_name_to_path(None))

    def test_rejects_non_string(self):
        self.assertIsNone(_shm_name_to_path(123))

    def test_rejects_dot(self):
        self.assertIsNone(_shm_name_to_path("."))

    def test_rejects_dotdot(self):
        self.assertIsNone(_shm_name_to_path(".."))

    def test_rejects_path_traversal(self):
        self.assertIsNone(_shm_name_to_path("../etc/passwd"))

    def test_rejects_nested_path(self):
        self.assertIsNone(_shm_name_to_path("subdir/file"))

    def test_rejects_slash_only(self):
        self.assertIsNone(_shm_name_to_path("/"))

    def test_rejects_slash_dot(self):
        self.assertIsNone(_shm_name_to_path("/."))

    def test_rejects_slash_dotdot(self):
        self.assertIsNone(_shm_name_to_path("/.."))


if __name__ == "__main__":
    unittest.main()
