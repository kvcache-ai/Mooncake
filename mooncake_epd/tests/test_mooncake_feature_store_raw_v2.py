from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import torch

from mooncake_epd.core.state import (
    FeatureBundle,
    FeatureHandleProvider,
    FeatureHandleProviderConfig,
    MooncakeFeatureBundleStore,
    MooncakeFeatureBundleStoreConfig,
)


class _KVHandler(BaseHTTPRequestHandler):
    store: dict[str, bytes] = {}

    def do_PUT(self):  # noqa: N802
        binary_prefix = "/api/put_bytes/"
        if self.path.startswith(binary_prefix):
            key = self.path[len(binary_prefix) :]
            length = int(self.headers.get("content-length", "0"))
            self.store[key] = self.rfile.read(length)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        if self.path != "/api/put":
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("content-length", "0"))
        payload = json.loads(self.rfile.read(length).decode("utf-8"))
        self.store[str(payload["key"])] = str(payload["value"]).encode("ascii")
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def do_GET(self):  # noqa: N802
        prefix = "/api/get/"
        if not self.path.startswith(prefix):
            self.send_response(404)
            self.end_headers()
            return
        key = self.path[len(prefix) :]
        if key not in self.store:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(self.store[key])

    def log_message(self, *args, **kwargs):
        return


@pytest.fixture()
def local_store_url():
    _KVHandler.store = {}
    server = ThreadingHTTPServer(("127.0.0.1", 0), _KVHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=2)


def _bundle(fid: str = "raw-img") -> FeatureBundle:
    return FeatureBundle(
        image_hash=fid,
        last_hidden=torch.arange(12, dtype=torch.bfloat16).reshape(3, 4),
        intermediates=[
            (1, torch.arange(6, dtype=torch.float16).reshape(3, 2)),
            (4, torch.arange(3, dtype=torch.float32).reshape(3, 1)),
        ],
        grid_thw=torch.tensor([[1, 1, 3]], dtype=torch.long),
        metadata={
            "model_fingerprint": "model-raw",
            "processor_fingerprint": "processor-raw",
            "non_json_value": torch.device("cpu"),
        },
    )


def test_raw_v2_publish_load_roundtrip(local_store_url):
    store = MooncakeFeatureBundleStore(
        MooncakeFeatureBundleStoreConfig(
            store_id="store-raw",
            store_url=local_store_url,
            serialization_format="raw_v2",
        )
    )

    handle = store.publish_bundle(_bundle(), checksum=True)
    loaded = store.load_bundle(handle.uri)

    assert loaded.image_hash == "raw-img"
    assert torch.equal(loaded.last_hidden, _bundle().last_hidden)
    assert torch.equal(loaded.grid_thw, _bundle().grid_thw)
    assert [idx for idx, _ in loaded.intermediates] == [1, 4]
    assert torch.equal(loaded.intermediates[0][1], _bundle().intermediates[0][1])
    assert loaded.metadata["model_fingerprint"] == "model-raw"
    assert loaded.metadata["processor_fingerprint"] == "processor-raw"
    assert isinstance(loaded.metadata["non_json_value"], str)
    handle.descriptor.validate_bundle(loaded, require_checksum=True)


def test_raw_v2_feature_handle_provider_resolves(local_store_url):
    store = MooncakeFeatureBundleStore(
        MooncakeFeatureBundleStoreConfig(
            store_id="store-raw",
            store_url=local_store_url,
            serialization_format="raw_v2",
        )
    )
    handle = store.publish_bundle(_bundle("provider-img"), checksum=True)
    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            device="cpu",
            strict=True,
            mooncake_store_url=local_store_url,
            mooncake_store_id="store-raw",
            expected_model_fingerprint="model-raw",
            expected_processor_fingerprint="processor-raw",
            require_checksum=True,
            bundle_cache_entries=0,
            resolved_cache_entries=0,
        )
    )

    resolved = provider.resolve_from_sources({"mm_feature_handles": [handle.as_control_payload()]})

    assert resolved is not None
    assert resolved.image_embeds.shape == (3, 7)
    assert torch.equal(resolved.image_grid_thw, torch.tensor([[1, 1, 3]], dtype=torch.long))


def test_legacy_torch_pickle_payload_still_loads(local_store_url):
    legacy_store = MooncakeFeatureBundleStore(
        MooncakeFeatureBundleStoreConfig(
            store_id="store-legacy",
            store_url=local_store_url,
            serialization_format="torch_pickle",
        )
    )
    raw_store = MooncakeFeatureBundleStore(
        MooncakeFeatureBundleStoreConfig(
            store_id="store-legacy",
            store_url=local_store_url,
            serialization_format="raw_v2",
        )
    )

    handle = legacy_store.publish_bundle(_bundle("legacy-img"), checksum=True)
    loaded = raw_store.load_bundle(handle.uri)

    assert loaded.image_hash == "legacy-img"
    assert torch.equal(loaded.last_hidden, _bundle("legacy-img").last_hidden)
    handle.descriptor.validate_bundle(loaded, require_checksum=True)


def test_raw_v2_binary_http_payload_avoids_base64(local_store_url):
    store = MooncakeFeatureBundleStore(
        MooncakeFeatureBundleStoreConfig(
            store_id="store-binary",
            store_url=local_store_url,
            serialization_format="raw_v2",
            http_binary_payload=True,
        )
    )

    handle = store.publish_bundle(_bundle("binary-img"), checksum=True)
    loaded = store.load_bundle(handle.uri)

    stored_payload = next(iter(_KVHandler.store.values()))
    assert stored_payload.startswith(MooncakeFeatureBundleStore.RAW_V2_MAGIC)
    assert loaded.image_hash == "binary-img"
    handle.descriptor.validate_bundle(loaded, require_checksum=True)
