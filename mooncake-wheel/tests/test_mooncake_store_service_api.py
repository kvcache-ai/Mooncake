#!/usr/bin/env python3
import asyncio
import json
import logging
import signal
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from aiohttp import web as _unused_web  # noqa: F401
except ModuleNotFoundError:
    aiohttp_module = types.ModuleType("aiohttp")
    web_module = types.ModuleType("aiohttp.web")

    class Response:
        def __init__(self, status=200, text="", body=None, content_type=None):
            self.status = status
            self.text = text
            self.body = body
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

from mooncake.mooncake_store_service import (
    MooncakeStoreService,
    _install_shutdown_signal_handlers,
    _shm_name_to_path,
    main as store_service_main,
)


class FakeStore:
    def __init__(self):
        self.mounted = {}
        self.mount_calls = []
        self.unmount_calls = []
        self.fail_mount = False
        self.unmount_failures = set()
        self.allocated_mount_calls = []
        self.free_unmount_calls = []
        self.setup_calls = []

    def setup(self, *args):
        self.setup_calls.append(args)
        return 0

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

    async def test_start_store_service_passes_tenant_id_to_setup(self):
        fake_store = FakeStore()
        self.service.config = SimpleNamespace(
            local_hostname="localhost",
            metadata_server="P2PHANDSHAKE",
            global_segment_size=1024,
            local_buffer_size=2048,
            protocol="tcp",
            device_name="",
            master_server_address="127.0.0.1:50051",
            enable_ssd_offload=False,
            ssd_offload_path="",
            tenant_id="tenant-a",
            enable_client_http_server=False,
            client_http_port=9300,
        )

        with patch(
            "mooncake.mooncake_store_service.MooncakeDistributedStore",
            return_value=fake_store,
        ):
            result = await self.service.start_store_service(max_wait_time=1)

        self.assertTrue(result)
        self.assertEqual(
            fake_store.setup_calls,
            [
                (
                    {
                        "local_hostname": "localhost",
                        "metadata_server": "P2PHANDSHAKE",
                        "global_segment_size": 1024,
                        "local_buffer_size": 2048,
                        "protocol": "tcp",
                        "rdma_devices": "",
                        "master_server_addr": "127.0.0.1:50051",
                        "enable_ssd_offload": False,
                        "ssd_offload_path": "",
                        "tenant_id": "tenant-a",
                        "enable_client_http_server": False,
                        "client_http_port": 9300,
                    },
                )
            ],
        )

    async def test_cli_config_can_override_tenant_id(self):
        config = {
            "local_hostname": "localhost",
            "metadata_server": "P2PHANDSHAKE",
            "master_server_address": "127.0.0.1:50051",
            "tenant_id": "tenant-from-file",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            config_path.write_text(json.dumps(config))
            service = MooncakeStoreService(
                str(config_path), {"tenant_id": "tenant-from-cli"}
            )

        self.assertEqual(service.config.tenant_id, "tenant-from-cli")

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
        # Fresh prefill -> decode mount that fails: there are no previously
        # serving segments to preserve, so the node still rolls back to prefill.
        self.service.current_mode = "prefill"
        self.service.mounted_segment_ids = []
        self.service.last_mount_info = {}
        self.fake_store.fail_mount = True

        resp = await self.service.handle_reconfigure(
            FakeRequest({"mode": "decode", "path": "/dev/shm/new", "size": 4096})
        )

        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertEqual(body["mode"], "prefill")
        self.assertIn("rolled back to prefill", body["error"])
        self.assertEqual(self.fake_store.unmount_calls, [])
        self.assertEqual(self.service.mounted_segment_ids, [])
        self.assertEqual(self.service.current_mode, "prefill")
        self.assertEqual(self.service.last_mount_info, {})

    async def test_reconfigure_decode_remount_failure_keeps_previous_segments(self):
        # A remount to a DIFFERENT path that fails to mount must not destroy the
        # still-healthy previous segments: the node keeps serving from them and
        # stays in decode mode (make-before-break).
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
        self.assertEqual(body["mode"], "decode")
        self.assertIn("keeping previous decode segments", body["error"])
        # Nothing was unmounted, capacity preserved, mode unchanged.
        self.assertEqual(self.fake_store.unmount_calls, [])
        self.assertEqual(self.service.mounted_segment_ids, [old_id])
        self.assertEqual(self.service.current_mode, "decode")
        # last_mount_info still points at the previous (working) path so a
        # subsequent same-path detection keeps working.
        self.assertEqual(self.service.last_mount_info["path"], "/dev/shm/old")

    async def test_reconfigure_decode_same_path_remount_failure_keeps_previous_segments(self):
        # A remount to the SAME path that fails to mount must not destroy the
        # still-healthy previous segments: the node keeps serving from them and
        # stays in decode mode (make-before-break). Same-path MBB is safe here
        # because /api/reconfigure binds through MasterClient::MountSegment,
        # which mints a fresh UUID per mount and does not enter the NoF
        # te_endpoint-dedup path, so old and new cannot collide on the same path.
        old_id = "00000000-0000-0000-0000-000000000001"
        self.service.current_mode = "decode"
        self.service.mounted_segment_ids = [old_id]
        self.service.last_mount_info = {
            "path": "/dev/shm/same",
            "offset": 0,
            "size": 4096,
            "protocol": "tcp",
            "location": "",
        }
        self.fake_store.fail_mount = True

        resp = await self.service.handle_reconfigure(
            FakeRequest({"mode": "decode", "path": "/dev/shm/same", "size": 4096})
        )

        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertEqual(body["mode"], "decode")
        self.assertIn("keeping previous decode segments", body["error"])
        # Nothing was unmounted, capacity preserved, mode unchanged.
        self.assertEqual(self.fake_store.unmount_calls, [])
        self.assertEqual(self.service.mounted_segment_ids, [old_id])
        self.assertEqual(self.service.current_mode, "decode")
        # last_mount_info still points at the previous (working) same path so a
        # subsequent remount keeps working.
        self.assertEqual(self.service.last_mount_info["path"], "/dev/shm/same")

    async def test_reconfigure_decode_remount_success_is_make_before_break(self):
        # A successful remount to a DIFFERENT path must mount the new segment
        # BEFORE retiring the old one (make-before-break), then leave only the
        # new segment serving. Distinct ids let old and new be told apart.
        old_id = "00000000-0000-0000-0000-000000000001"
        new_id = "00000000-0000-0000-0000-000000000002"
        self.service.current_mode = "decode"
        self.service.mounted_segment_ids = [old_id]
        self.service.last_mount_info = {
            "path": "/dev/shm/old",
            "offset": 0,
            "size": 4096,
            "protocol": "tcp",
            "location": "",
        }

        order = {"old_still_mounted_at_new_mount": None}

        def mount_new(path, size, offset, protocol, location):
            # The old segment must still be serving when the new one is mounted.
            order["old_still_mounted_at_new_mount"] = (
                old_id in self.service.mounted_segment_ids
            )
            return {"ret": 0, "segment_ids": [new_id]}

        self.fake_store.mount_segment = mount_new

        resp = await self.service.handle_reconfigure(
            FakeRequest({"mode": "decode", "path": "/dev/shm/new", "size": 8192})
        )

        self.assertEqual(resp.status, 200)
        body = json.loads(resp.text)
        self.assertEqual(body["mode"], "decode")
        # Make-before-break: the old segment was still mounted when the new one
        # was created, and it is retired only after the new mount succeeds.
        self.assertTrue(order["old_still_mounted_at_new_mount"])
        self.assertEqual(self.fake_store.unmount_calls, [([old_id], 0)])
        # Only the new segment is left serving; the old one is gone.
        self.assertEqual(self.service.mounted_segment_ids, [new_id])
        self.assertEqual(self.service.current_mode, "decode")
        self.assertEqual(self.service.last_mount_info["path"], "/dev/shm/new")

    async def test_reconfigure_decode_partial_unmount_failure_keeps_only_failed_ids(self):
        # New mount succeeds, but retiring the previous segments only PARTIALLY
        # fails. unmount_segment reports the first error for a batch, so the old
        # ids must be unmounted individually; mounted_segment_ids must then hold
        # the new id plus ONLY the id whose cleanup actually failed -- not the
        # whole previous set (which would retain a stale, already-freed id) and
        # not none of it (which would silently leak the still-live old segment).
        old_ok = "00000000-0000-0000-0000-0000000000a1"
        old_fail = "00000000-0000-0000-0000-0000000000a2"
        new_id = "00000000-0000-0000-0000-0000000000b1"
        self.service.current_mode = "decode"
        self.service.mounted_segment_ids = [old_ok, old_fail]
        self.service.last_mount_info = {
            "path": "/dev/shm/old",
            "offset": 0,
            "size": 4096,
            "protocol": "tcp",
            "location": "",
        }
        self.fake_store.unmount_failures = {old_fail}

        def mount_new(path, size, offset, protocol, location):
            return {"ret": 0, "segment_ids": [new_id]}

        self.fake_store.mount_segment = mount_new

        resp = await self.service.handle_reconfigure(
            FakeRequest({"mode": "decode", "path": "/dev/shm/new", "size": 8192})
        )

        self.assertEqual(resp.status, 200)
        # Each previous id was unmounted on its own, not as a single batch.
        self.assertEqual(
            self.fake_store.unmount_calls, [([old_ok], 0), ([old_fail], 0)]
        )
        # New id serves; the freed id is dropped, the un-freed id stays tracked.
        self.assertEqual(self.service.mounted_segment_ids, [new_id, old_fail])
        self.assertEqual(self.service.current_mode, "decode")

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
        self.fake_store.is_exist = lambda key: True
        self.fake_store.get = lambda key: b"payload_bytes"
        request = FakeRequest({})
        request.match_info = {"key": "my_key"}
        resp = await self.service.handle_get(request)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.body, b"payload_bytes")

    async def test_handle_get_not_found(self):
        self.fake_store.is_exist = lambda key: 0
        self.fake_store.get = lambda key: b""
        request = FakeRequest({})
        request.match_info = {"key": "missing_key"}
        resp = await self.service.handle_get(request)
        self.assertEqual(resp.status, 404)
        body = json.loads(resp.text)
        self.assertIn("Key not found", body["error"])

    async def test_handle_get_exist_check_failure(self):
        self.fake_store.is_exist = lambda key: -1
        self.fake_store.get = lambda key: b""
        request = FakeRequest({})
        request.match_info = {"key": "error_key"}
        resp = await self.service.handle_get(request)
        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertIn("Exist check failed", body["error"])

    async def test_handle_get_empty_bytes(self):
        self.fake_store.is_exist = lambda key: True
        self.fake_store.get = lambda key: b""
        request = FakeRequest({})
        request.match_info = {"key": "empty_value_key"}
        resp = await self.service.handle_get(request)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.body, b"")

    async def test_handle_get_empty_bytes_rechecks_existence(self):
        existence_results = iter([1, 0])
        self.fake_store.is_exist = lambda key: next(existence_results)
        self.fake_store.get = lambda key: b""
        request = FakeRequest({})
        request.match_info = {"key": "empty_value_key"}
        resp = await self.service.handle_get(request)
        self.assertEqual(resp.status, 404)
        body = json.loads(resp.text)
        self.assertIn("Key not found", body["error"])

    async def test_handle_get_none_value_is_store_failure(self):
        self.fake_store.is_exist = lambda key: True
        self.fake_store.get = lambda key: None
        request = FakeRequest({})
        request.match_info = {"key": "empty_value_key"}
        resp = await self.service.handle_get(request)
        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        self.assertIn("GET operation failed", body["error"])

    async def test_handle_get_store_exception(self):
        def raise_error(key):
            raise RuntimeError("store crashed")

        self.fake_store.is_exist = lambda key: True
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

    async def test_handle_exist_store_error(self):
        # is_exist returns -1 when the store is unhealthy; this should surface
        # as HTTP 500, not as HTTP 200 {"exists": true} (bool(-1) == True).
        self.fake_store.is_exist = lambda key: -1
        request = FakeRequest({})
        request.match_info = {"key": "some_key"}
        resp = await self.service.handle_exist(request)
        self.assertEqual(resp.status, 500)
        body = json.loads(resp.text)
        # Assert the specific message so this pins the exists < 0 branch rather
        # than any 500 (the except path returns {"error": str(e)} too).
        self.assertEqual(body["error"], "Exist check failed")

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


class StoreServiceShutdownTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = MooncakeStoreService.__new__(MooncakeStoreService)
        self.service.store = None
        self.service.config = SimpleNamespace(
            local_hostname="localhost",
            metadata_server="P2PHANDSHAKE",
            global_segment_size=1,
            local_buffer_size=0,
            protocol="tcp",
            device_name="",
            master_server_address="localhost:50051",
            enable_ssd_offload=False,
            ssd_offload_path="",
            tenant_id="",
        )

    async def test_shutdown_event_stops_startup_retry_sleep(self):
        class FailingStore:
            def setup(self, *_args):
                raise RuntimeError("setup failed")

        shutdown_event = asyncio.Event()

        async def trigger_shutdown():
            await asyncio.sleep(0)
            shutdown_event.set()

        with mock.patch(
            "mooncake.mooncake_store_service.MooncakeDistributedStore",
            FailingStore,
        ):
            shutdown_task = asyncio.create_task(trigger_shutdown())
            start_time = time.perf_counter()
            with self.assertLogs(level=logging.WARNING):
                result = await self.service.start_store_service(
                    max_wait_time=2, shutdown_event=shutdown_event
                )
            elapsed = time.perf_counter() - start_time
            await shutdown_task

        self.assertFalse(result)
        self.assertLess(elapsed, 0.5)

    async def test_shutdown_during_setup_is_observed_before_success(self):
        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        class SchedulingStore(FakeStore):
            def __init__(self):
                super().__init__()
                self.close_calls = 0

            def setup(self, *args):
                ret = super().setup(*args)
                loop.call_soon(shutdown_event.set)
                return ret

            def close(self):
                self.close_calls += 1
                return 0

        store = SchedulingStore()
        with mock.patch(
            "mooncake.mooncake_store_service.MooncakeDistributedStore",
            return_value=store,
        ):
            result = await self.service.start_store_service(
                max_wait_time=1, shutdown_event=shutdown_event
            )

        self.assertFalse(result)
        self.assertEqual(store.close_calls, 1)
        self.assertIsNone(self.service.store)

    async def test_stop_logs_nonzero_close_return(self):
        store = mock.Mock()
        store.close.return_value = 7
        self.service.store = store

        with self.assertLogs(level=logging.WARNING) as logs:
            await self.service.stop()

        self.assertIsNone(self.service.store)
        self.assertTrue(
            any("close returned 7" in message for message in logs.output)
        )
        await self.service.stop()
        store.close.assert_called_once_with()

    async def test_signal_handler_requests_shutdown(self):
        loop = mock.Mock()
        shutdown_event = asyncio.Event()

        _install_shutdown_signal_handlers(loop, shutdown_event)

        sigterm_call = next(
            call
            for call in loop.add_signal_handler.call_args_list
            if call.args[0] == signal.SIGTERM
        )
        sigterm_call.args[1](sigterm_call.args[2])
        self.assertTrue(shutdown_event.is_set())

    async def test_main_closes_store_when_shutdown_requested_during_startup(self):
        args = SimpleNamespace(
            config=None,
            define=[],
            max_wait_time=60,
            port=8080,
        )
        service = mock.Mock()
        service.start_store_service = mock.AsyncMock(return_value=False)
        service.start_http_service = mock.AsyncMock(return_value=True)
        service.stop = mock.AsyncMock()

        startup_calls = []

        def request_shutdown(_loop, shutdown_event):
            startup_calls.append("install")
            shutdown_event.set()

        def unblock_shutdown_signals():
            startup_calls.append("unblock")

        with (
            mock.patch(
                "mooncake.mooncake_store_service.parse_arguments",
                return_value=args,
            ),
            mock.patch(
                "mooncake.mooncake_store_service.MooncakeStoreService",
                return_value=service,
            ),
            mock.patch(
                "mooncake.mooncake_store_service._unblock_shutdown_signals",
                side_effect=unblock_shutdown_signals,
            ),
            mock.patch(
                "mooncake.mooncake_store_service._install_shutdown_signal_handlers",
                side_effect=request_shutdown,
            ),
        ):
            await store_service_main()

        service.start_store_service.assert_awaited_once()
        service.start_http_service.assert_not_awaited()
        service.stop.assert_awaited_once()
        self.assertEqual(startup_calls, ["install", "unblock"])


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
