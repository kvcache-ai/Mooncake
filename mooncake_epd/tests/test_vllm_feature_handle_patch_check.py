from __future__ import annotations

from mooncake_epd.scripts.check_vllm_feature_handle_patch import build_report


def test_feature_handle_patch_checker_reports_provider_status():
    report = build_report()
    assert report["provider_available"] is True
    assert "vllm_available" in report
    assert "gpu_model_runner_hook" in report
    assert "direct_feature_buffer_build_app_hook" in report
    assert isinstance(report["direct_feature_buffer_build_app_hook"], bool)
    assert isinstance(report["qwen_hooks"], list)
    assert isinstance(report["errors"], list)
