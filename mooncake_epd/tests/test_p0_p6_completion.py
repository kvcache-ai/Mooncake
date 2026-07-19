from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest
import torch

from mooncake_epd.agent import AdmissionAction, AgentPDScheduler, AgentRequest, AgentType, HiddenStatePrefixCache
from mooncake_epd.core.omni_pipeline import OmniPipeline
from mooncake_epd.core.state import (
    FeatureBundle,
    FeatureHandleProvider,
    FeatureHandleProviderConfig,
    MooncakeFeatureBundleStore,
    MooncakeFeatureBundleStoreConfig,
    MooncakeKVStateStore,
    MooncakeRemoteKVMaterializer,
    PagedKVManager,
    parse_mooncake_feature_uri,
)
from mooncake_epd.core.transfer import Channel, Mode, TransferEngine, TransferPolicy


class _KVHandler(BaseHTTPRequestHandler):
    store: dict[str, bytes] = {}

    def do_PUT(self):  # noqa: N802
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
        key = self.path[len(prefix):]
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


def _bundle(fid="img-a", base=1.0):
    return FeatureBundle(
        image_hash=fid,
        last_hidden=torch.tensor([[base, base + 1.0]], dtype=torch.float32),
        intermediates=[(1, torch.tensor([[base + 2.0]], dtype=torch.float32))],
        grid_thw=torch.tensor([[1, 1, 1]], dtype=torch.long),
        metadata={"model_fingerprint": "m", "processor_fingerprint": "p"},
    )


def test_p0_mooncake_feature_handle_publish_and_provider_resolve(local_store_url):
    store = MooncakeFeatureBundleStore(
        MooncakeFeatureBundleStoreConfig(store_id="store-a", store_url=local_store_url)
    )
    handle = store.publish_bundle(_bundle(), checksum=True, metadata={"source_mm_hash": "stable"})
    assert handle.uri.startswith("mooncake://store-a/")
    assert parse_mooncake_feature_uri(handle.uri)[0] == "store-a"

    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            device="cpu",
            strict=True,
            mooncake_store_url=local_store_url,
            mooncake_store_id="store-a",
            expected_model_fingerprint="m",
            expected_processor_fingerprint="p",
            require_checksum=True,
        )
    )
    resolved = provider.resolve_from_sources({"mm_feature_handles": [handle.as_control_payload()]})
    assert resolved is not None
    assert torch.equal(resolved.image_embeds, torch.tensor([[1.0, 2.0, 3.0]]))


def test_p0_mooncake_feature_keys_include_descriptor_digest(local_store_url):
    store = MooncakeFeatureBundleStore(
        MooncakeFeatureBundleStoreConfig(store_id="store-a", store_url=local_store_url)
    )
    base = _bundle(fid="same-image", base=1.0)
    deepstack = FeatureBundle(
        image_hash="same-image",
        last_hidden=base.last_hidden,
        intermediates=[
            (1, torch.tensor([[3.0]], dtype=torch.float32)),
            (2, torch.tensor([[4.0]], dtype=torch.float32)),
        ],
        grid_thw=base.grid_thw,
        metadata=dict(base.metadata),
    )

    h1 = store.publish_bundle(base, checksum=True)
    h2 = store.publish_bundle(deepstack, checksum=True)

    assert h1.feature_id == h2.feature_id == "same-image"
    assert h1.uri != h2.uri
    assert "-v2-" in h1.uri and "-v2-" in h2.uri


def _pm(node_id: str) -> PagedKVManager:
    return PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=1,
        head_dim=2,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id=node_id,
    )


def test_p1_agent_cross_node_materializer_installs_and_releases_remote_pages():
    src = _pm("node-a")
    dst = _pm("node-b")
    refs = src.allocate_pages(2, filled=4)
    for i, ref in enumerate(refs):
        key = torch.full((2, 1, 4, 2), float(i + 1))
        val = torch.full((2, 1, 4, 2), float(i + 10))
        src.write_page_slots(ref, key, val, offset=0)
    materializer = MooncakeRemoteKVMaterializer(src, dst, transfer_engine=TransferEngine(protocol="local"))
    store = MooncakeKVStateStore(
        src,
        node_id="node-a",
        remote_materializer=materializer,
        page_managers_by_node={"node-b": dst},
    )
    parent = store.register_state(refs, workflow_id="wf", state_id="parent")
    child = store.clone_state("parent", child_state_id="child", target_node_id="node-b")
    assert child.owner_node_id == "node-b"
    child_refs = store.get_refs("child")
    assert [r.physical_node_id for r in child_refs] == ["node-b", "node-b"]
    for src_ref, dst_ref in zip(refs, child_refs):
        sk, sv = src.get_page(src_ref)
        dk, dv = dst.get_page(dst_ref)
        assert torch.equal(sk, dk)
        assert torch.equal(sv, dv)
    assert store.release_state("child") == 2
    assert store.release_state("parent") == 2


def test_p3_omni_pipeline_moves_nested_tensors_and_feature_bundle():
    class StageA:
        name = "ar"
        def run(self, inputs):
            return [torch.ones(2), {"bundle": _bundle("img-omni", 4.0)}]

    class StageB:
        name = "generation"
        def run(self, inputs):
            assert isinstance(inputs[1]["bundle"], FeatureBundle)
            return [inputs[0] + inputs[1]["bundle"].last_hidden.flatten()[:2]]

    pipe = OmniPipeline(
        [StageA(), StageB()],
        transfer=TransferEngine(protocol="local"),
        device_per_stage=["cpu", "cpu"],
        policy_per_edge=[TransferPolicy(Mode.SHM, channel=Channel.AGENT_TO_AGENT, extra={"force_copy": True})],
    )
    out = pipe.process([])
    assert torch.equal(out[0], torch.tensor([5.0, 6.0]))
    stats = pipe.stats()
    assert stats["edges"]["ar->generation"]["transfers"] == 1


def test_p4_scheduler_admission_backpressure_and_reject():
    sched = AgentPDScheduler(prefill_workers=["p0"], decode_workers=["d0"])
    sched.update_load("p0", current_load=9, max_capacity=10, queue_size=4, queue_capacity=10, critical_rho=0.8)
    sched.update_load("d0", current_load=1, max_capacity=10, queue_size=1, queue_capacity=10)
    decision = sched.route_with_admission(AgentRequest("r1", AgentType.THINKING))
    assert decision.action is AdmissionAction.BACKPRESSURE
    assert decision.degrade_level in {"disable_approx_reuse", "raise_compression"}
    sched.update_load("p0", queue_size=10, queue_capacity=10)
    rejected = sched.route_with_admission(AgentRequest("r2", AgentType.THINKING))
    assert rejected.action is AdmissionAction.REJECT


def test_p5_prefix_cache_uses_full_hash_and_stable_keys():
    cache = HiddenStatePrefixCache(max_cache_size_bytes=1024 * 1024, max_entries=8)
    a = torch.zeros(5000, dtype=torch.float32)
    b = a.clone()
    b[-1] = 1.0  # differs outside the old sampled prefix window
    cache.put(a, torch.tensor([1.0]), {"id": "a"})
    assert cache.get(b) is None
    cache.put(b, torch.tensor([2.0]), {"id": "b"}, stable_key="image://same")
    got = cache.get(a, stable_key="image://same")
    assert got is not None
    assert torch.equal(got[0], torch.tensor([2.0]))
    assert got[1]["cache_key_mode"] == "stable"


def test_p5_prefix_cache_rejects_oversize_entries_and_isolates_hits():
    cache = HiddenStatePrefixCache(max_cache_size_bytes=8, max_entries=1)
    pixels = torch.zeros(1)
    assert cache.put(pixels, torch.zeros(4, dtype=torch.float32), {}) is False
    assert cache.get_stats()["total_entries"] == 0
    assert cache.get_stats()["rejected_entries"] == 1

    cache = HiddenStatePrefixCache(max_cache_size_bytes=1024, max_entries=2)
    assert cache.put(pixels, torch.tensor([1.0]), {}, stable_key="asset") is True
    first = cache.get(pixels, stable_key="asset")
    assert first is not None
    first[0].fill_(99)
    first[1]["mutated"] = True
    second = cache.get(pixels, stable_key="asset")
    assert second is not None
    assert torch.equal(second[0], torch.tensor([1.0]))
    assert "mutated" not in second[1]
