from __future__ import annotations

import json
import os
import subprocess
import sys
import importlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _run(code: str) -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    env["MOONCAKE_EPD_ENABLE_VLLM_PATCHES"] = "0"
    output = subprocess.check_output(
        [sys.executable, "-c", code],
        cwd=ROOT,
        env=env,
        text=True,
    )
    return json.loads(output.splitlines()[-1])


def test_vllm_import_does_not_patch_without_explicit_adapter_activation():
    report = _run(
        "import json; import vllm.v1.worker.gpu_model_runner as m; "
        "print(json.dumps({'patched': bool(getattr(m.GPUModelRunner._batch_mm_inputs_from_scheduler, '_mooncake_epd_feature_handle_patch', False))}))"
    )
    assert report == {"patched": False}


def test_explicit_adapter_reports_and_installs_required_hooks():
    report = _run(
        "import json; from mooncake_epd.integrations.vllm.adapter import install_vllm_epd_adapter; "
        "import os; "
        "import torch; "
        "from vllm.v1.worker.gpu_model_runner import GPUModelRunner; "
        "from vllm.model_executor.models.qwen2_5_omni_thinker import Qwen2_5OmniThinkerForConditionalGeneration; "
        "r=install_vllm_epd_adapter(strict=True); "
        "V=type('V', (), {'dtype': torch.float32}); F=type('F', (), {'visual': V()}); "
        "out=Qwen2_5OmniThinkerForConditionalGeneration._process_image_input(F(), {'type':'image_embeds','image_embeds':torch.ones((2,3),dtype=torch.bfloat16)}); "
        "print(json.dumps({'installed': r['installed'], 'adapter_version': r['adapter_version'], 'prompt_only_patch_installed': r['prompt_only_patch_installed'], 'prompt_only_protocol_version': r['prompt_only_protocol_version'], 'decode_token_multimodal_patch_installed': r['decode_token_multimodal_patch_installed'], 'patched': bool(getattr(GPUModelRunner._batch_mm_inputs_from_scheduler, '_mooncake_epd_feature_handle_patch', False)), 'omni_patched': bool(getattr(Qwen2_5OmniThinkerForConditionalGeneration.embed_multimodal, '_mooncake_epd_feature_handle_patch', False)), 'omni_contract_patched': bool(getattr(Qwen2_5OmniThinkerForConditionalGeneration._process_image_input, '_mooncake_epd_qwen25_omni_image_embed_contract_patch', False)), 'omni_contract_result': [isinstance(out, tuple), len(out), list(out[0].shape)], 'env_restored': os.getenv('MOONCAKE_EPD_ENABLE_VLLM_PATCHES') == '0'}))"
    )
    assert report == {
        "installed": True,
        "adapter_version": "v2",
        "prompt_only_patch_installed": True,
        "prompt_only_protocol_version": 2,
        "decode_token_multimodal_patch_installed": True,
        "patched": True,
        "omni_patched": True,
        "omni_contract_patched": True,
        "omni_contract_result": [True, 1, [2, 3]],
        "env_restored": True,
    }


def test_explicit_adapter_records_model_side_precomputed_qwen25_image_embeds(tmp_path):
    metrics_dir = tmp_path / "metrics"
    report = _run(
        "import json, os, torch; from pathlib import Path; "
        "os.environ['MOONCAKE_EPD_CONNECTOR_METRICS_DIR']=" + repr(str(metrics_dir)) + "; "
        "os.environ['MOONCAKE_EPD_ENGINE_ID']='adapter-test'; "
        "os.environ['MOONCAKE_EPD_KV_ROLE']='kv_producer'; "
        "os.environ['MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_METRICS_INTERVAL_S']='0'; "
        "from mooncake_epd.integrations.vllm.adapter import install_vllm_epd_adapter; "
        "from vllm.model_executor.models.qwen2_5_omni_thinker import Qwen2_5OmniThinkerForConditionalGeneration as Omni; "
        "from vllm.model_executor.models.qwen2_5_vl import Qwen2_5_VLForConditionalGeneration as VL; "
        "install_vllm_epd_adapter(strict=True); "
        "V=type('V', (), {'dtype': torch.float32, 'spatial_merge_size': 2}); "
        "F=type('F', (), {'visual': V()}); "
        "item={'type':'image_embeds','image_embeds':torch.ones((1,3),dtype=torch.bfloat16),'image_grid_thw':torch.tensor([[1,2,2]])}; "
        "omni=Omni._process_image_input(F(), item); vl=VL._process_image_input(F(), item); "
        "payload=json.loads(next(Path(" + repr(str(metrics_dir)) + ").glob('*.mm_hidden.json')).read_text()); "
        "print(json.dumps({'omni_tuple': isinstance(omni, tuple), 'vl_tuple': isinstance(vl, tuple), 'precomputed': payload['metrics']['precomputed_image_embeds_hits'], 'hits': payload['metrics']['hits'], 'misses': payload['metrics']['misses']}))"
    )
    assert report == {
        "omni_tuple": True,
        "vl_tuple": True,
        "precomputed": 2,
        "hits": 2,
        "misses": 0,
    }


def test_launcher_propagates_explicit_adapter_to_spawned_engine_children(monkeypatch):
    from mooncake_epd.integrations.vllm import launcher

    cli_main = importlib.import_module("vllm.entrypoints.cli.main")
    observed: dict[str, object] = {}
    monkeypatch.setenv("MOONCAKE_EPD_ENABLE_VLLM_PATCHES", "0")
    monkeypatch.setattr(
        launcher,
        "install_vllm_epd_adapter",
        lambda *, strict=None: observed.setdefault("strict", strict),
    )
    monkeypatch.setattr(
        cli_main,
        "main",
        lambda: observed.update(
            child_bootstrap=os.environ.get("MOONCAKE_EPD_ENABLE_VLLM_PATCHES"),
            argv=list(sys.argv),
        ),
    )

    launcher.main(["--", "serve", "fake-model"])

    assert observed == {
        "strict": None,
        "child_bootstrap": "1",
        "argv": ["vllm", "serve", "fake-model"],
    }
