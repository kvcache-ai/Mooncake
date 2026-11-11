"""Plugin opt-in, layout, owner grouping, and real CUDA/Graph row-gather
regression.
"""

import json
import sys
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest
import torch

from mooncake import engram_plugins as plugin
from mooncake.engram import EngramStore


def configuration(engine="sglang"):
    return {
        "engine": engine,
        "namespace": "checkpoint-v1",
        "master_addr": "http://master/metadata",
        "session_id": "test",
        "shard_id": 0,
        "num_shards": 2,
        "num_callers": 2,
        "max_lookup_rows": 1024,
        "max_egm_bytes_per_numa": 4096,
    }


def fake_module(name, origin, **attributes):
    cls = type(name, (torch.nn.Module,), {"__module__": origin})
    module = cls()
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


def test_plugins_are_disabled_without_explicit_configuration(monkeypatch):
    monkeypatch.delenv("MOONCAKE_ENGRAM_CONFIG", raising=False)
    plugin.register_sglang()
    assert plugin.configuration("sglang") is None


def test_configuration_is_engine_scoped_and_rejects_unknown_fields(monkeypatch):
    config = configuration()
    monkeypatch.setenv("MOONCAKE_ENGRAM_CONFIG", json.dumps(config))
    assert plugin.configuration("sglang")["namespace"] == "checkpoint-v1"
    config["engine"] = "vllm"
    monkeypatch.setenv("MOONCAKE_ENGRAM_CONFIG", json.dumps(config))
    with pytest.raises(ValueError, match="engine must be sglang"):
        plugin.configuration("sglang")
    config["engine"] = "sglang"
    config["unexpected"] = True
    monkeypatch.setenv("MOONCAKE_ENGRAM_CONFIG", json.dumps(config))
    with pytest.raises(ValueError, match="Unknown"):
        plugin.configuration("sglang")


def test_weight_and_scale_choose_same_owner_and_balance_replicas():
    tables = {}
    for layer in ("layer0", "layer1"):
        for plane, width in (("weight", 256), ("scale", 8)):
            tables[f"{layer}/{plane}"] = {
                "num_rows": 16,
                "row_bytes": width,
                "dtype": "uint8",
                "group": layer,
            }
    manifests = [{"shard": i, "caller": 0, "tables": tables} for i in (0, 1)]
    _, owners = plugin.choose_owners(manifests)
    assert owners["layer0/weight"] == owners["layer0/scale"]
    assert owners["layer1/weight"] == owners["layer1/scale"]
    assert owners["layer0/weight"] != owners["layer1/weight"]


def test_conflicting_plane_layout_is_rejected():
    table = {"weight": {"num_rows": 8, "row_bytes": 16}}
    manifests = [
        {"shard": 0, "caller": 0, "tables": table},
        {
            "shard": 1,
            "caller": 0,
            "tables": {"weight": {"num_rows": 8, "row_bytes": 8}},
        },
    ]
    with pytest.raises(ValueError, match="inconsistent"):
        plugin.choose_owners(manifests)


@pytest.mark.parametrize("mxfp4", [False, True])
def test_sglang_deepseek_plane_bytes_follow_storage_format(mxfp4):
    dtype = torch.uint8 if mxfp4 else torch.float8_e4m3fn
    module = fake_module(
        "EngramEmbedding",
        "sglang.srt.layers.engram",
        weight=torch.nn.Parameter(
            torch.zeros((5, 128 if mxfp4 else 256), dtype=dtype), requires_grad=False
        ),
        scale=torch.nn.Parameter(
            torch.ones((5, 8), dtype=torch.uint8), requires_grad=False
        ),
        row_start=10,
        rows=5,
        peer=None,
        _shared=False,
    )
    root = torch.nn.Module()
    root.add_module("embed", module)
    (adapter,) = plugin.discover(root, "sglang")
    entries = adapter.manifest()
    assert entries["embed/weight/10:15"]["row_bytes"] == (128 if mxfp4 else 256)
    assert entries["embed/scale/10:15"]["row_bytes"] == 8
    assert adapter.byte_rows("weight").dtype == np.uint8


def test_sglang_registers_before_precompile(monkeypatch):
    monkeypatch.setenv("MOONCAKE_ENGRAM_CONFIG", json.dumps(configuration()))
    registry = SimpleNamespace(register=mock.Mock())
    module = SimpleNamespace(
        HookRegistry=registry,
        HookType=SimpleNamespace(BEFORE="before", AFTER="after"),
    )
    monkeypatch.setitem(sys.modules, "sglang.srt.plugins.hook_registry", module)
    monkeypatch.setattr(plugin, "_registered", set())
    monkeypatch.setattr(plugin.atexit, "register", mock.Mock())
    plugin.register_sglang()
    plugin.register_sglang()
    assert registry.register.call_count == 2
    target, prepare, kind = registry.register.call_args_list[0].args
    assert target.endswith("ModelRunner.maybe_precompile_model_kernels_after_loading")
    assert kind == "before"
    runner = SimpleNamespace(
        is_draft_worker=False,
        server_args=SimpleNamespace(pp_size=1),
        startup_weight_load=object(),
    )
    with pytest.raises(ValueError, match="asynchronous"):
        prepare(runner)
    target, release, kind = registry.register.call_args_list[1].args
    assert target.endswith("Scheduler.release_host_resources")
    assert kind == "after"
    closed = []
    state = SimpleNamespace(close=lambda: closed.append("close"))
    scheduler = SimpleNamespace(
        tp_worker=SimpleNamespace(
            model_runner=SimpleNamespace(
                model=SimpleNamespace(_mooncake_engram_state=state)
            )
        )
    )
    release(None, scheduler)
    assert closed == ["close"]
    release(None, SimpleNamespace(tp_worker=None))
    assert closed == ["close"]


@pytest.mark.parametrize("dtype", [torch.bfloat16, torch.float8_e4m3fn])
def test_plugin_cuda_lookup_masks_ids_and_replays_graph(tmp_path, dtype):
    pytest.importorskip("mooncake._engram_cuda")
    if not torch.cuda.is_available():
        pytest.skip("CUDA required")
    from mooncake import _engram_plugin_ops as ops

    rows = torch.arange(128, dtype=torch.float32).reshape(8, 16).to(dtype)
    module = fake_module(
        "Qwen4ExpPinnedHostEmbedding",
        "sglang.srt.models.qwen4_exp",
        weight=torch.nn.Parameter(rows, requires_grad=False),
        embedding_dim=16,
    )
    module.allocate_output = lambda shape, device: torch.empty(
        shape, dtype=torch.bfloat16, device=device
    )
    adapter = plugin.Adapter("ple", module, "sglang_qwen", 100, 8, ("weight",))
    table = adapter.planes["weight"]["name"]
    store = EngramStore(
        tables={table: {"num_rows": 8, "row_bytes": 16 * rows.element_size()}},
        namespace="test",
        local_dir=tmp_path,
        persistent=False,
    )
    store.populate(table, adapter.byte_rows("weight"))
    adapter.store = store
    adapter.state = plugin.State([adapter])
    adapter.state.stage(store, [adapter])
    ops.install_ops()
    ops.bind(adapter)
    ids = torch.tensor([[100, 107], [99, -1]], device="cuda", dtype=torch.int64)
    out = torch.empty((2, 2, 16), device="cuda", dtype=torch.bfloat16)
    module.gather(ids, out)
    compiled = torch.compile(module.gather, backend="eager", fullgraph=True)
    compiled(ids, out)
    expected = (
        torch.stack(
            (
                rows[0],
                rows[7],
                torch.zeros(16, dtype=dtype),
                torch.zeros(16, dtype=dtype),
            )
        )
        .view(2, 2, 16)
        .bfloat16()
    )
    torch.testing.assert_close(out.cpu(), expected, rtol=0, atol=0)
    graph = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        module.gather(ids, out)
    ids.copy_(torch.tensor([[101, 102], [106, 103]], device="cuda"))
    graph.replay()
    torch.cuda.synchronize()
    torch.testing.assert_close(
        out.cpu(), rows.bfloat16()[[1, 2, 6, 3]].view(2, 2, 16), rtol=0, atol=0
    )
    del graph
    adapter.state.close()


def test_state_releases_stages_in_reverse_order_even_after_failure(monkeypatch):
    calls = []

    class Store:
        def __init__(self, name, fail=False):
            self.name, self.fail = name, fail

        def close(self):
            calls.append(self.name)
            if self.fail:
                raise RuntimeError(self.name)

    monkeypatch.setattr(torch.cuda, "synchronize", lambda: None)
    state = plugin.State([])
    state.stage(Store("layer1"), [])
    state.stage(Store("layer14", fail=True), [])
    with pytest.raises(RuntimeError, match="layer14"):
        state.close()
    assert calls == ["layer14", "layer1"]
    assert state.closed
    state.close()
    assert calls == ["layer14", "layer1"]


def test_attach_migrates_one_adapter_per_stage_then_releases_native(monkeypatch):
    if not torch.cuda.is_available():
        pytest.skip("CUDA required")
    import mooncake.engram as engram_module
    from mooncake import _engram_plugin_ops as ops

    events = []

    class Store:
        def __init__(self, *, tables, namespace, local_dir, persistent):
            self.namespace = namespace
            self.tables = tables
            events.append(("store", namespace, tuple(sorted(tables))))

        def populate_shard_and_sync(self, rows, **kwargs):
            events.append(("populate", self.namespace, sorted(rows)))

        def lookup(self, tables, ids, out, *, offsets, stream):
            events.append(("lookup", self.namespace))

        def close(self):
            events.append(("close", self.namespace))

    class FakeAdapter:
        def __init__(self, path, rows, start=0):
            self.path, self.count, self.start = path, rows, start
            self.kind = "sglang_deepseek"
            self.state = self.store = None
            self.op_id = -1
            self.planes = {
                plane: {
                    "name": f"{path}/{plane}/{start}:{start + rows}",
                    "width": 8,
                    "dtype": torch.uint8,
                }
                for plane in ("weight", "scale")
            }

        def manifest(self):
            group = f"{self.path}/{self.start}:{self.start + self.count}"
            return {
                entry["name"]: {
                    "num_rows": self.count,
                    "row_bytes": 8,
                    "dtype": "torch.uint8",
                    "kind": self.kind,
                    "group": group,
                }
                for entry in self.planes.values()
            }

        def byte_rows(self, plane):
            events.append(("bytes", self.path, plane))
            return np.zeros((self.count, self.planes[plane]["width"]), dtype=np.uint8)

        def release_native(self):
            events.append(("release", self.path))

    small, large = FakeAdapter("layer1", 4), FakeAdapter("layer14", 8)
    remote = (FakeAdapter("layer1", 4, 100), FakeAdapter("layer14", 8, 100))
    config = dict(configuration(), num_callers=2, caller_id=0)
    monkeypatch.setattr(plugin, "discover", lambda model, engine: [small, large])
    monkeypatch.setattr(plugin, "Metadata", lambda endpoint: None)

    def exchange(metadata, prefix, manifest, config, deadline):
        peer = {"tables": {}, "shard": 0, "caller": 1, "nonce": "peer"}
        for adapter in remote:
            peer["tables"].update(adapter.manifest())
        return [manifest, peer]

    monkeypatch.setattr(plugin, "_exchange", exchange)
    monkeypatch.setattr(engram_module, "EngramStore", Store)
    monkeypatch.setattr(ops, "install_ops", lambda: None)
    monkeypatch.setattr(
        ops, "bind", lambda adapter: events.append(("bind", adapter.path))
    )
    monkeypatch.setattr(torch.cuda, "is_current_stream_capturing", lambda: False)
    monkeypatch.setattr(torch.distributed, "is_initialized", lambda: False)
    model = SimpleNamespace()
    plugin.attach(model, "sglang", config)
    expected = []
    for path, rows in (
        ("layer14", 8),
        ("layer1", 4),
    ):  # Migrate large tables first to release the largest native tables early.
        namespace = f"checkpoint-v1/sglang/{path}"
        tables = tuple(
            sorted(
                f"{path}/{plane}/{start}:{start + rows}"
                for plane in ("weight", "scale")
                for start in (0, 100)
            )
        )
        expected += [
            (
                "store",
                namespace,
                tables,
            ),  # Include the peer caller's table specs so the layout digest matches.
            ("bytes", path, "weight"),
            ("bytes", path, "scale"),
            (
                "populate",
                namespace,
                [f"{path}/scale/0:{rows}", f"{path}/weight/0:{rows}"],
            ),
            ("lookup", namespace),
            ("lookup", namespace),  # Sample-verify both planes after migration.
            ("bind", path),
            ("lookup", namespace),
            ("lookup", namespace),
            ("release", path),
        ]
    assert events == expected
    state = model._mooncake_engram_state
    assert [store.namespace for store, _ in state.stages] == [
        "checkpoint-v1/sglang/layer14",
        "checkpoint-v1/sglang/layer1",
    ]
    assert small.store is state.stages[1][0] and large.store is state.stages[0][0]
    state.close()
    assert events[-2:] == [
        ("close", "checkpoint-v1/sglang/layer1"),
        ("close", "checkpoint-v1/sglang/layer14"),
    ]


def test_verify_rows_uses_absolute_ids_and_accepts_matching_bytes():
    if not torch.cuda.is_available():
        pytest.skip("CUDA required")
    source = np.arange(64, dtype=np.uint8).reshape(8, 8)
    seen = []

    class Store:
        def lookup(self, tables, ids, out, *, offsets, stream):
            absolute = ids.cpu().numpy().reshape(-1)
            seen.append(absolute)
            out.copy_(torch.from_numpy(source[absolute - offsets[0]]).unsqueeze(1))

    adapter = SimpleNamespace(start=100)
    plugin._verify_rows(Store(), adapter, {"layer/weight/100:108": source})
    assert seen and seen[0].min() >= 100 and seen[0].max() < 108


def test_verify_rows_rejects_mismatched_bytes():
    if not torch.cuda.is_available():
        pytest.skip("CUDA required")
    source = np.arange(64, dtype=np.uint8).reshape(8, 8)

    class Store:
        def lookup(self, tables, ids, out, *, offsets, stream):
            out.fill_(1)

    adapter = SimpleNamespace(start=0)
    with pytest.raises(RuntimeError, match="differ from the source"):
        plugin._verify_rows(Store(), adapter, {"layer/weight/0:8": source})


def test_model_comparison_uses_token_ids_not_decoded_text(monkeypatch):
    import io

    import test_engram_sglang as client

    payload = {
        "meta_info": {"output_token_logprobs": [[-0.1, 7, "a"], [-0.2, 9, "b"]]}
    }

    def response(request, timeout):
        assert request.full_url.endswith("/generate")
        return io.BytesIO(json.dumps(payload).encode())

    monkeypatch.setattr(client, "urlopen", response)
    tokens, probabilities, _ = client.generate("http://server", "prompt", 2, 1)
    assert tokens == [7, 9]
    assert probabilities == [-0.1, -0.2]
