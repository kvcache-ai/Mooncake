from __future__ import annotations

from mooncake_epd.scripts.check_vllm_feature_handle_patch import build_report


def test_feature_handle_patch_checker_reports_provider_status():
    report = build_report()
    assert report["provider_available"] is True
    assert "vllm_available" in report
    assert "gpu_model_runner_hook" in report
    assert "direct_feature_buffer_build_app_hook" in report


def test_feature_handle_patch_checker_covers_qwen_omni_thinker_target():
    report = build_report()
    target = next(
        item
        for item in report["qwen_hooks"]
        if item["module"] == "vllm.model_executor.models.qwen2_5_omni_thinker"
    )

    assert target["class"] == "Qwen2_5OmniThinkerForConditionalGeneration"
    assert target["methods"]["embed_multimodal"]["exists"] is True
    assert isinstance(report["direct_feature_buffer_build_app_hook"], bool)
    assert isinstance(report["qwen25_omni_precomputed_image_embed_contract"], bool)
    assert isinstance(report["qwen25_vl_precomputed_image_embed_metric"], bool)
    assert isinstance(report["qwen_hooks"], list)
    assert isinstance(report["errors"], list)
