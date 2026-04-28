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
    from aiohttp import web as _unused_web
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
    from mooncake.store import MooncakeDistributedStore as _unused_store
except ModuleNotFoundError:
    store_module = types.ModuleType("mooncake.store")

    class MooncakeDistributedStore:
        pass

    store_module.MooncakeDistributedStore = MooncakeDistributedStore
    sys.modules["mooncake.store"] = store_module

from mooncake.mooncake_store_service import MooncakeStoreService


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

    def unmount_segment(self, segment_ids, grace_period_seconds=0):
        self.unmount_calls.append((list(segment_ids), grace_period_seconds))
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

    def unmount_and_free_segment(self, segment_ids, grace_period_seconds=0):
        self.free_unmount_calls.append((list(segment_ids), grace_period_seconds))
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
            [(["00000000-0000-0000-0000-000000000001"], 0)],
        )
        self.assertEqual(self.service.current_mode, "prefill")

    async def test_unmount_shm_passes_grace_period(self):
        segment_id = "00000000-0000-0000-0000-000000000001"
        self.service.current_mode = "decode"
        self.service.mounted_segment_ids = [segment_id]

        resp = await self.service.handle_unmount_shm(
            FakeRequest({"segment_ids": [segment_id], "grace_period_seconds": 3})
        )

        self.assertEqual(resp.status, 200)
        self.assertEqual(self.fake_store.unmount_calls, [([segment_id], 3)])
        self.assertEqual(self.service.mounted_segment_ids, [])
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
            [([succeeded_id], 0), ([failed_id], 0)],
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
        self.assertEqual(self.fake_store.unmount_calls, [([old_id], 0)])
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
        self.assertEqual(self.fake_store.allocated_mount_calls,
                         [(1, "tcp", "cpu:0")])

        unmount_resp = await self.service.handle_unmount(
            FakeRequest({"segment_ids": mount_body["segment_ids"]})
        )
        self.assertEqual(unmount_resp.status, 200)
        unmount_body = json.loads(unmount_resp.text)
        self.assertEqual(unmount_body["status"], "success")
        self.assertEqual(
            self.fake_store.free_unmount_calls,
            [(["00000000-0000-0000-0000-000000000002"], 0)],
        )

    async def test_unmount_allocated_passes_grace_period(self):
        segment_id = "00000000-0000-0000-0000-000000000002"

        resp = await self.service.handle_unmount(
            FakeRequest({"segment_ids": [segment_id], "grace_period_seconds": 4})
        )

        self.assertEqual(resp.status, 200)
        self.assertEqual(self.fake_store.free_unmount_calls, [([segment_id], 4)])

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


if __name__ == "__main__":
    unittest.main()
