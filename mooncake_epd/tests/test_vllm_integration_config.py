from __future__ import annotations

import pytest

from mooncake_epd.core.control.vllm_incarnation import (
    VLLM_INCARNATION_ENDPOINT,
    VLLM_INCARNATION_MIDDLEWARE,
)
from mooncake_epd.demo.vllm_integration import CONNECTOR_MODULE_PATH, VLLMDisaggConfig, generate_configs


def test_generate_configs_enables_repo_local_connector(tmp_path):
    config = VLLMDisaggConfig(local_hostname="127.0.0.1", layers_per_group=6, group_delay_ms=1.25)
    files = generate_configs(str(tmp_path), config)

    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")
    decode_script = (tmp_path / "start_decode.sh").read_text(encoding="utf-8")
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert f'"kv_connector_module_path":"{CONNECTOR_MODULE_PATH}"' in prefill_script
    assert f'"kv_connector_module_path":"{CONNECTOR_MODULE_PATH}"' in decode_script
    assert '"connector_metrics_dir":"' in prefill_script
    assert '"connector_metrics_dir":"' in decode_script
    assert '"max_transfer_descriptors":64' in prefill_script
    assert '"max_transfer_descriptors":64' in decode_script
    assert 'export PYTHONPATH=' in prefill_script
    assert 'export MOONCAKE_EPD_CONNECTOR_METRICS_DIR=' in prefill_script
    assert 'export MOONCAKE_EPD_CONNECTOR_METRICS_DIR=' in decode_script
    assert 'export MOONCAKE_EPD_CONNECTOR_METRICS_DIR=' in proxy_script
    assert 'export VLLM_MOONCAKE_BOOTSTRAP_PORT=' in prefill_script
    assert 'export VLLM_MOONCAKE_BOOTSTRAP_PORT=' in decode_script
    assert 'export VLLM_MOONCAKE_BOOTSTRAP_PORT=' not in proxy_script
    assert prefill_script.split('export VLLM_MOONCAKE_BOOTSTRAP_PORT=', 1)[1].splitlines()[0] != decode_script.split('export VLLM_MOONCAKE_BOOTSTRAP_PORT=', 1)[1].splitlines()[0]
    assert '--layers-per-group 6' in proxy_script
    assert '--group-delay-ms 1.25' in proxy_script
    assert '--workflow-registry-wal ' in proxy_script
    assert '--connector-metrics-dir ' in proxy_script
    assert '--enable-agent-state-clone' in proxy_script
    assert "proxy_workflow_registry" in files
    assert "connector_metrics_dir" in files
    assert files["prefill"].endswith("start_prefill.sh")


def test_generate_configs_can_enable_feature_handle_proxy_mode(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        mm_prefetch_mode="feature_handle",
        prefill_supports_feature_handles=True,
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--mm-prefetch-mode feature_handle" in proxy_script
    assert "--prefill-supports-feature-handles" in proxy_script


def test_generate_configs_enables_prefill_direct_feature_buffer_routes(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        mm_prefetch_mode="feature_handle",
        prefill_supports_feature_handles=True,
        enable_prefill_direct_feature_buffer_routes=True,
        encoder_service_url="http://127.0.0.1:8330",
    )
    generate_configs(str(tmp_path), config)
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "export MOONCAKE_EPD_ENABLE_DIRECT_FEATURE_BUFFER=1" in prefill_script
    assert "export MOONCAKE_EPD_DIRECT_BUFFER_WORKER_ID=prefill-0" in prefill_script
    assert "export MOONCAKE_EPD_FEATURE_HANDLE_WORKER_ID=prefill-0" in prefill_script
    assert "export MOONCAKE_EPD_DIRECT_BUFFER_DEVICE=cuda" in prefill_script
    assert "--encoder-service-url http://127.0.0.1:8330" in proxy_script
    assert f"--prefill-direct-buffer-service-url {config.prefill_direct_buffer_service_url}" in proxy_script
    assert "--release-direct-feature-buffers-after-prefill" in proxy_script


def test_generate_configs_can_enforce_strict_no_fallback(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        mm_prefetch_mode="feature_handle",
        prefill_supports_feature_handles=True,
        enable_prefill_direct_feature_buffer_routes=True,
        strict_no_fallback=True,
    )
    generate_configs(str(tmp_path), config)
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")
    decode_script = (tmp_path / "start_decode.sh").read_text(encoding="utf-8")
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    for script in (prefill_script, decode_script, proxy_script):
        assert "export MOONCAKE_EPD_STRICT=1" in script
        assert "export MOONCAKE_EPD_VLLM_FEATURE_HANDLE_STRICT=1" in script
        assert "export MOONCAKE_EPD_ALLOW_TRANSFER_FALLBACK=0" in script
    assert "--strict-no-fallback" in proxy_script


def test_generate_configs_emits_descriptor_coalescing_flag(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        enable_descriptor_coalescing=False,
    )
    generate_configs(str(tmp_path), config)
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")
    decode_script = (tmp_path / "start_decode.sh").read_text(encoding="utf-8")

    assert '"enable_descriptor_coalescing":false' in prefill_script
    assert '"enable_descriptor_coalescing":false' in decode_script


def test_generate_configs_emits_connector_metrics_batching_profile(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        connector_metrics_flush_interval_s=0.5,
        connector_metrics_max_pending_records=96,
    )
    generate_configs(str(tmp_path), config)
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")

    assert '"connector_metrics_flush_interval_s":0.5' in prefill_script
    assert '"connector_metrics_max_pending_records":96' in prefill_script


def test_generate_configs_emits_workflow_registry_wal_batching_profile(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        workflow_registry_wal_fsync_interval_s=0.5,
        workflow_registry_wal_max_pending_records=48,
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--workflow-registry-wal-fsync-interval-s 0.5" in proxy_script
    assert "--workflow-registry-wal-max-pending 48" in proxy_script


def test_generate_configs_can_enable_early_decode_pipeline(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        enable_decode_pipeline=True,
        decode_pipeline_max_inflight=2,
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--enable-decode-pipeline" in proxy_script
    assert "--decode-pipeline-max-inflight 2" in proxy_script


def test_generate_configs_can_enable_prerendered_decode_fast_path(tmp_path):
    config = VLLMDisaggConfig(
        model="/models/qwen3-vl",
        local_hostname="127.0.0.1",
        enable_prerendered_decode=True,
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--enable-prerendered-decode" in proxy_script
    assert "--prerendered-decode-model /models/qwen3-vl" in proxy_script


def test_generate_configs_can_enable_decode_mm_hash_fast_path(tmp_path):
    config = VLLMDisaggConfig(
        model="/models/qwen3-vl",
        local_hostname="127.0.0.1",
        enable_prerendered_decode=True,
        enable_decode_mm_hash_cache=True,
        decode_mm_hash_cache_max_entries=24,
        decode_mm_hash_cache_ttl_s=90.0,
        decode_mm_hash_epoch_poll_s=0.75,
        decode_mm_hash_epoch_probe_timeout_s=0.25,
        decode_mm_hash_epoch_freshness_s=0.4,
        decode_mm_hash_epoch_endpoint=VLLM_INCARNATION_ENDPOINT,
        enable_decode_mm_hash_epoch_guard=True,
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")
    decode_script = (tmp_path / "start_decode.sh").read_text(encoding="utf-8")

    assert "--enable-decode-mm-hash-cache" in proxy_script
    assert "--decode-mm-hash-cache-max-entries 24" in proxy_script
    assert "--decode-mm-hash-cache-ttl-s 90.0" in proxy_script
    assert "--decode-mm-hash-epoch-poll-s 0.75" in proxy_script
    assert "--decode-mm-hash-epoch-probe-timeout-s 0.25" in proxy_script
    assert "--decode-mm-hash-epoch-freshness-s 0.4" in proxy_script
    assert (
        f"--decode-mm-hash-epoch-endpoint {VLLM_INCARNATION_ENDPOINT}"
        in proxy_script
    )
    assert f"--middleware {VLLM_INCARNATION_MIDDLEWARE}" in decode_script
    assert "--decode-mm-hash-epoch-guard" in proxy_script


def test_generate_configs_emits_mm_prefetch_source_of_truth(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        enable_mm_prefetch=False,
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--no-enable-mm-prefetch" in proxy_script
    assert "--enable-mm-prefetch" not in proxy_script


def test_generate_configs_can_enable_bounded_direct_handle_reuse(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        mm_prefetch_mode="feature_handle",
        prefill_supports_feature_handles=True,
        enable_prefill_direct_feature_buffer_routes=True,
        enable_direct_feature_handle_cache=True,
        direct_feature_handle_cache_max_entries=32,
        direct_feature_handle_cache_max_bytes=2 * 1024**3,
        direct_feature_handle_cache_ttl_s=900.0,
        prefill_incarnation_poll_s=1.0,
        prefill_incarnation_poll_jitter_ratio=0.2,
        prefill_incarnation_failure_threshold=3,
        prefill_incarnation_probe_timeout_s=0.75,
        prefill_incarnation_freshness_s=2.0,
        prefill_incarnation_endpoint=VLLM_INCARNATION_ENDPOINT,
        enable_prefill_incarnation_guard=True,
        enable_prefill_render_cache=True,
        prefill_render_cache_max_entries=64,
        prefill_render_cache_max_bytes=1024**3,
        prefill_render_cache_ttl_s=450.0,
        release_direct_feature_buffers_after_prefill=False,
        upstream_max_connections=48,
        upstream_max_keepalive_connections=24,
        upstream_keepalive_expiry_s=30.0,
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")

    assert "--enable-direct-feature-handle-cache" in proxy_script
    assert "--direct-feature-handle-cache-max-entries 32" in proxy_script
    assert f"--direct-feature-handle-cache-max-bytes {2 * 1024**3}" in proxy_script
    assert "--direct-feature-handle-cache-ttl-s 900.0" in proxy_script
    assert "--prefill-incarnation-poll-s 1.0" in proxy_script
    assert "--prefill-incarnation-poll-jitter-ratio 0.2" in proxy_script
    assert "--prefill-incarnation-failure-threshold 3" in proxy_script
    assert "--prefill-incarnation-probe-timeout-s 0.75" in proxy_script
    assert "--prefill-incarnation-freshness-s 2.0" in proxy_script
    assert (
        f"--prefill-incarnation-endpoint {VLLM_INCARNATION_ENDPOINT}"
        in proxy_script
    )
    assert "--prefill-incarnation-guard" in proxy_script
    assert f"--middleware {VLLM_INCARNATION_MIDDLEWARE}" in prefill_script
    assert "--enable-prefill-render-cache" in proxy_script
    assert "--prefill-render-cache-max-entries 64" in proxy_script
    assert f"--prefill-render-cache-max-bytes {1024**3}" in proxy_script
    assert "--prefill-render-cache-ttl-s 450.0" in proxy_script
    assert "--no-release-direct-feature-buffers-after-prefill" in proxy_script
    assert "--upstream-max-connections 48" in proxy_script
    assert "--upstream-max-keepalive-connections 24" in proxy_script
    assert "--upstream-keepalive-expiry-s 30.0" in proxy_script


@pytest.mark.parametrize(
    "endpoint",
    [
        "//other-host/mooncake_epd/incarnation",
        "/mooncake_epd/incarnation?stale=1",
        "/mooncake_epd/incarnation#fragment",
    ],
)
def test_generate_configs_rejects_nonlocal_prefill_incarnation_endpoint(
    tmp_path,
    endpoint,
):
    with pytest.raises(ValueError, match="absolute URL path"):
        generate_configs(
            str(tmp_path),
            VLLMDisaggConfig(prefill_incarnation_endpoint=endpoint),
        )


def test_generate_configs_shell_quotes_prefill_incarnation_endpoint(tmp_path):
    endpoint = "/mooncake_epd/incarnation;echo unsafe"
    generate_configs(
        str(tmp_path),
        VLLMDisaggConfig(
            prefill_incarnation_endpoint=endpoint,
            enable_prefill_incarnation_guard=False,
        ),
    )

    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")
    assert (
        "--prefill-incarnation-endpoint "
        "'/mooncake_epd/incarnation;echo unsafe'"
    ) in proxy_script


def test_generate_configs_can_enable_client_mm_uuid_references(tmp_path):
    config = VLLMDisaggConfig(enable_client_mm_uuid_references=True)

    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--enable-client-mm-uuid-references" in proxy_script


def test_generate_configs_auto_keeps_same_host_workers_off_the_rnic(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        protocol="auto",
    )
    generate_configs(str(tmp_path), config)
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")

    assert "export MOONCAKE_PROTOCOL=tcp" in prefill_script
    assert "export MOONCAKE_EPD_DATA_PROTOCOL=local" in prefill_script
    assert '"transport_backend":"mooncake_engine_direct"' in prefill_script


def test_generate_configs_emits_real_iwarp_rdmacm_profile(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        protocol="rdmacm",
        rdmacm_bind_address="192.168.100.1",
        rdmacm_remote_address="192.168.100.2",
        rdmacm_port_offset=3100,
    )
    generate_configs(str(tmp_path), config)
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")
    mooncake = (tmp_path / "mooncake.json").read_text(encoding="utf-8")

    # Mooncake remains the TCP control/registration plane because its legacy
    # verbs backend cannot establish Intel iWARP QPs.
    assert '"protocol": "tcp"' in mooncake
    assert "export MOONCAKE_PROTOCOL=tcp" in prefill_script
    assert "export MOONCAKE_EPD_DATA_PROTOCOL=rdmacm" in prefill_script
    assert "export MOONCAKE_EPD_RDMACM_BIND_ADDRESS=192.168.100.1" in prefill_script
    assert "export MOONCAKE_EPD_RDMACM_REMOTE_ADDRESS=192.168.100.2" in prefill_script
    assert '"transport_backend":"rdmacm_staged"' in prefill_script
    assert '"rdmacm_port_offset":3100' in prefill_script
    assert "--transport-backend rdmacm_staged" in proxy_script
