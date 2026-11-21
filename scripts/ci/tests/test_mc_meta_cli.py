#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import os
import threading
import unittest
import urllib.parse
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch


_SCRIPT = Path(__file__).resolve().parents[2] / "management" / "mc_meta_cli.py"
_SPEC = importlib.util.spec_from_file_location("mc_meta_cli", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
mc_meta_cli = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(mc_meta_cli)


@contextmanager
def metadata_server(*, require_auth: bool):
    received = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            authorization = self.headers.get("Authorization")
            received.append((self.path, authorization))
            if require_auth and authorization != "Bearer fixture-token":
                status = 401
            elif urllib.parse.unquote(self.path).endswith(":missing"):
                status = 404
            else:
                status = 200
            self.send_response(status)
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/metadata", received
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


class MetadataQueryAuthTest(unittest.TestCase):
    def test_token_is_sent_for_existing_and_missing_keys(self):
        with patch.dict(
            os.environ, {"MC_METADATA_HTTP_TOKEN": "fixture-token"}, clear=True
        ):
            with metadata_server(require_auth=True) as (endpoint, received):
                self.assertTrue(
                    mc_meta_cli.query_key_by_http(
                        endpoint, "mooncake/ram/node1:12345", 2
                    )
                )
                self.assertFalse(
                    mc_meta_cli.query_key_by_http(
                        endpoint, "mooncake/ram/node1:missing", 2
                    )
                )

        self.assertEqual(
            [authorization for _, authorization in received],
            ["Bearer fixture-token"] * 2,
        )

    def test_empty_token_omits_authorization_header(self):
        with patch.dict(os.environ, {}, clear=True):
            with metadata_server(require_auth=False) as (endpoint, received):
                self.assertTrue(
                    mc_meta_cli.query_key_by_http(
                        endpoint, "mooncake/ram/node1:12345", 2
                    )
                )

        self.assertEqual(received, [(received[0][0], None)])

    def test_unauthorized_error_names_token_variable_not_token_value(self):
        with patch.dict(
            os.environ, {"MC_METADATA_HTTP_TOKEN": "wrong-fixture-token"}, clear=True
        ):
            with metadata_server(require_auth=True) as (endpoint, _):
                with self.assertRaisesRegex(
                    RuntimeError, "MC_METADATA_HTTP_TOKEN"
                ) as raised:
                    mc_meta_cli.query_key_by_http(
                        endpoint, "mooncake/ram/node1:12345", 2
                    )

        self.assertNotIn("wrong-fixture-token", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
