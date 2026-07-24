#!/usr/bin/env python3
import asyncio
import logging
import socket
import sys
import unittest
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mooncake.http_metadata_server import KVBootstrapServer, KVPoll


class HttpMetadataServerStartupTest(unittest.TestCase):
    def _close_and_join(self, server):
        server.close()
        thread = server.thread
        if thread is None:
            return

        if thread.is_alive():
            loop = server._loop
            if loop is not None and not loop.is_closed():
                try:
                    loop.call_soon_threadsafe(loop.stop)
                except RuntimeError:
                    pass
        thread.join(timeout=2)

    def test_run_propagates_bind_failure_without_logging_success(self):
        host = "127.0.0.1"

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
            reservation.bind((host, 0))
            reservation.listen(1)
            server = KVBootstrapServer(
                port=reservation.getsockname()[1], host=host
            )

            try:
                with self.assertLogs(level=logging.INFO) as captured_logs:
                    with self.assertRaises(OSError):
                        server.run()

                thread = server.thread
                self.assertIsNotNone(thread)
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
                self.assertFalse(
                    any(
                        "HTTP Metadata Server started" in message
                        for message in captured_logs.output
                    )
                )
            finally:
                self._close_and_join(server)

    def test_run_reports_ready_server_and_close_stops_thread(self):
        server = KVBootstrapServer(port=0, host="127.0.0.1")
        thread = None

        try:
            with self.assertLogs(level=logging.INFO) as captured_logs:
                thread = server.run()

            self.assertTrue(thread.is_alive())
            self.assertEqual(server.poll(), KVPoll.Success)
            self.assertTrue(
                any(
                    "HTTP Metadata Server started" in message
                    for message in captured_logs.output
                )
            )
        finally:
            self._close_and_join(server)

        self.assertIsNotNone(thread)
        self.assertFalse(thread.is_alive())

    def test_runtime_base_exception_is_not_swallowed(self):
        server = KVBootstrapServer(port=0, host="127.0.0.1")
        startup = Future()
        loop = asyncio.new_event_loop()
        run_forever = loop.run_forever

        # Stop the loop as soon as the server signals readiness (the callback
        # runs in the loop thread when notify_started sets the result), so the
        # wrapper below can raise once startup has completed. Keying off the
        # startup future keeps this independent of how many times
        # run_until_complete internally invokes run_forever.
        startup.add_done_callback(lambda _: loop.call_soon(loop.stop))

        def interrupt_after_readiness():
            run_forever()
            if startup.done():
                # Restore the real run_forever so cleanup in _run_server's
                # finally block is not interrupted a second time.
                loop.run_forever = run_forever
                raise KeyboardInterrupt

        loop.run_forever = interrupt_after_readiness

        try:
            with patch.object(asyncio, "new_event_loop", return_value=loop):
                with self.assertRaises(KeyboardInterrupt):
                    server._run_server(startup)
        finally:
            asyncio.set_event_loop(None)

        self.assertIsNone(startup.result())


if __name__ == "__main__":
    unittest.main()
