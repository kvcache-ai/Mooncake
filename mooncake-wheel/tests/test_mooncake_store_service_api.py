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
    main as store_service_main,
)


class FakeStore:
    def __init__(self):
        self.setup_calls = []

    def setup(self, *args):
        self.setup_calls.append(args)
        return 0


class FakeRequest:
    def __init__(self, body, json_error=None):
        self.body = body
        self.json_error = json_error

    async def json(self):
        if self.json_error is not None:
            raise self.json_error
        return self.body


class StoreServiceApiTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake_store = FakeStore()
        self.service = MooncakeStoreService.__new__(MooncakeStoreService)
        self.service.store = self.fake_store

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


class StoreServiceShutdownTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = MooncakeStoreService.__new__(MooncakeStoreService)
        self.service.store = None
        self.service.is_p2p = False
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


if __name__ == "__main__":
    unittest.main()
