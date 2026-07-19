from __future__ import annotations

from pathlib import Path

import pytest

from mooncake_epd.demo.vllm_integration import (
    CONNECTOR_MODULE_PATH,
    REPO_ROOT,
    VLLMDisaggConfig,
    _BOOTSTRAP_PORT_MAX,
    _BOOTSTRAP_PORT_MIN,
    _resolve_venv_root,
    generate_configs,
)


def test_config_resolves_repository_relative_model_from_environment(monkeypatch):
    monkeypatch.setenv("MOONCAKE_EPD_MODEL", "models/custom-qwen-vl")

    config = VLLMDisaggConfig()

    assert Path(config.model) == REPO_ROOT.parent / "models" / "custom-qwen-vl"


def test_venv_resolution_prefers_explicit_deployment_root(tmp_path, monkeypatch):
    venv_root = tmp_path / "runtime-env"
    (venv_root / "bin").mkdir(parents=True)
    (venv_root / "bin" / "python").touch()
    monkeypatch.setenv("MOONCAKE_EPD_VENV_ROOT", str(venv_root))

    assert _resolve_venv_root() == venv_root


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
    assert 'export PYTHONPATH=' in prefill_script
    assert 'unset http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY' in prefill_script
    assert 'export NO_PROXY=127.0.0.1,localhost,::1' in prefill_script
    assert 'export no_proxy=${NO_PROXY}' in prefill_script
    assert 'export MOONCAKE_EPD_CONNECTOR_METRICS_DIR=' in prefill_script
    assert 'export MOONCAKE_EPD_CONNECTOR_METRICS_DIR=' in decode_script
    assert 'export MOONCAKE_EPD_CONNECTOR_METRICS_DIR=' in proxy_script
    assert 'export MC_TCP_WRITE_COMPLETION_ACK=1' in prefill_script
    assert 'export MC_TCP_WRITE_COMPLETION_ACK=1' in decode_script
    assert 'export MC_TCP_ENABLE_CONNECTION_POOL=0' in prefill_script
    assert 'export MC_TCP_ENABLE_CONNECTION_POOL=0' in decode_script
    assert 'export VLLM_CACHE_ROOT=' in prefill_script
    assert 'export TORCHINDUCTOR_CACHE_DIR=${VLLM_CACHE_ROOT}/torch_compile_cache' in prefill_script
    assert 'export TRITON_CACHE_DIR=${VLLM_CACHE_ROOT}/triton' in prefill_script
    assert 'export VLLM_MOONCAKE_BOOTSTRAP_PORT=' in prefill_script
    assert 'export VLLM_MOONCAKE_BOOTSTRAP_PORT=' in decode_script
    assert 'export VLLM_MOONCAKE_BOOTSTRAP_PORT=' not in proxy_script
    assert prefill_script.split('export VLLM_MOONCAKE_BOOTSTRAP_PORT=', 1)[1].splitlines()[0] != decode_script.split('export VLLM_MOONCAKE_BOOTSTRAP_PORT=', 1)[1].splitlines()[0]
    assert '--layers-per-group 6' in proxy_script
    assert '--group-delay-ms 1.25' in proxy_script
    assert '--workflow-registry-wal ' in proxy_script
    assert '--connector-metrics-dir ' in proxy_script
    assert '--enable-agent-state-clone' in proxy_script
    assert '--prefill-dispatch-mode render_generate' in proxy_script
    assert "--no-prefill-http-keepalive" in proxy_script
    assert '--no-allow-unverified-openai-prompt-only' in proxy_script
    assert "--enable-rendered-prefill-cache" in proxy_script
    assert "--rendered-prefill-cache-max-entries 64" in proxy_script
    assert "--rendered-prefill-cache-max-bytes 268435456" in proxy_script
    assert "--no-enable-direct-feature-adaptive-lease-prefetch" in proxy_script
    assert "--direct-feature-adaptive-lease-prefetch-max 2" in proxy_script
    assert "proxy_workflow_registry" in files
    assert "connector_metrics_dir" in files
    assert files["prefill"].endswith("start_prefill.sh")


def test_generate_configs_uses_unique_non_ephemeral_bootstrap_ports(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        prefill_gpus=(0, 1),
        decode_gpus=(2, 3),
    )
    generate_configs(str(tmp_path), config)

    scripts = [
        tmp_path / "start_prefill.sh",
        tmp_path / "start_prefill_1.sh",
        tmp_path / "start_decode.sh",
        tmp_path / "start_decode_1.sh",
    ]
    ports = [
        int(
            script.read_text(encoding="utf-8")
            .split("export VLLM_MOONCAKE_BOOTSTRAP_PORT=", 1)[1]
            .splitlines()[0]
        )
        for script in scripts
    ]

    assert len(ports) == len(set(ports))
    assert all(_BOOTSTRAP_PORT_MIN <= port <= _BOOTSTRAP_PORT_MAX for port in ports)


def test_generate_configs_can_enable_adaptive_direct_feature_leases(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        direct_feature_adaptive_lease_prefetch=True,
        direct_feature_adaptive_lease_prefetch_max=2,
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--enable-direct-feature-adaptive-lease-prefetch" in proxy_script
    assert "--direct-feature-adaptive-lease-prefetch-max 2" in proxy_script


def test_generate_configs_exports_selected_scheduler_policy(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        scheduler_policy="least_loaded",
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--scheduler-policy least_loaded" in proxy_script


def test_generate_configs_rejects_unknown_scheduler_policy(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        scheduler_policy="opaque_magic",
    )

    with pytest.raises(ValueError, match="unsupported scheduler_policy"):
        generate_configs(str(tmp_path), config)


def test_generate_configs_can_disable_sync_workflow_wal_for_serving_hot_path(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        enable_workflow_registry_wal=False,
    )
    files = generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--workflow-registry-wal" not in proxy_script
    assert files["proxy_workflow_registry"] is None


def test_generate_configs_can_pin_vllm_multimodal_hidden_cache_mode(tmp_path):
    disabled = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        vllm_mm_hidden_cache=False,
    )
    generate_configs(str(tmp_path / "disabled"), disabled)
    disabled_script = (tmp_path / "disabled" / "start_prefill.sh").read_text(
        encoding="utf-8"
    )
    assert "export MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE=0" in disabled_script

    enabled = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        vllm_mm_hidden_cache=True,
    )
    generate_configs(str(tmp_path / "enabled"), enabled)
    enabled_script = (tmp_path / "enabled" / "start_prefill.sh").read_text(
        encoding="utf-8"
    )
    assert "export MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE=1" in enabled_script


def test_generate_configs_can_pin_vllm_generation_config(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        generation_config="vllm",
    )
    generate_configs(str(tmp_path), config)
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")
    decode_script = (tmp_path / "start_decode.sh").read_text(encoding="utf-8")

    assert "--generation-config vllm" in prefill_script
    assert "--generation-config vllm" in decode_script


def test_generate_configs_pairs_list_aligned_nvlink_workers_for_pd_locality(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        protocol="nvlink_intra",
        prefill_gpus=(0, 1),
        decode_gpus=(2, 3),
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--prefill-decode-affinity prefill-0=decode-0 prefill-1=decode-1" in proxy_script
    assert config.resolved_prefill_decode_affinity == (
        ("prefill-0", "decode-0"),
        ("prefill-1", "decode-1"),
    )


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


def test_generate_configs_exports_feature_handle_store_endpoint(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        mm_prefetch_mode="feature_handle",
        prefill_supports_feature_handles=True,
        feature_handle_store_url="http://127.0.0.1:8089",
        feature_handle_store_id="real-mm-store",
        feature_handle_store_timeout_s=12.5,
        feature_handle_require_checksum=True,
    )
    generate_configs(str(tmp_path), config)
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")

    assert "export MOONCAKE_EPD_FEATURE_HANDLE_STORE_URL=http://127.0.0.1:8089" in prefill_script
    assert "export MOONCAKE_STORE_URL=http://127.0.0.1:8089" in prefill_script
    assert "export MOONCAKE_EPD_FEATURE_HANDLE_STORE_ID=real-mm-store" in prefill_script
    assert "export MOONCAKE_EPD_FEATURE_HANDLE_STORE_TIMEOUT_S=12.500" in prefill_script
    assert "export MOONCAKE_EPD_FEATURE_HANDLE_TIMEOUT_S=12.500" in prefill_script
    assert "export MOONCAKE_EPD_FEATURE_HANDLE_REQUIRE_CHECKSUM=1" in prefill_script


def test_online_encoder_checksum_flag_is_shared_with_prefill_validation(tmp_path):
    """A diagnostic checksum must be emitted for both E and P, not just P."""

    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        feature_handle_require_checksum=True,
    )
    generate_configs(str(tmp_path), config)

    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")
    assert "export MOONCAKE_EPD_FEATURE_HANDLE_REQUIRE_CHECKSUM=1" in prefill_script


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
    assert "export MOONCAKE_EPD_DIRECT_BUFFER_ROOT_ROUTES=0" in prefill_script
    assert "export MOONCAKE_EPD_DIRECT_BUFFER_AUTH_TOKEN=" in prefill_script
    assert "export MOONCAKE_EPD_DIRECT_BUFFER_AUTH_TOKEN=" in proxy_script
    assert config.direct_feature_buffer_auth_token
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
    for script in (prefill_script, decode_script):
        assert '"require_kv_transfer_manifest_v2":true' in script
        assert '"require_kv_transfer_manifest_generation":true' in script
        assert "export MOONCAKE_EPD_WORKER_GENERATION=" in script
        assert "export MOONCAKE_EPD_GENERATION_DIR=" in script
        assert '$(date +%s%N)' in script
        assert 'mv "$MOONCAKE_EPD_GENERATION_DIR/' in script
    assert f"--worker-generation-dir {tmp_path / 'worker_generations'}" in proxy_script
    assert (tmp_path / "worker_generations").is_dir()
    assert "--strict-no-fallback" in proxy_script


def test_generate_configs_can_explicitly_allow_verified_prompt_only_mode(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        prefill_dispatch_mode="openai_prompt_only",
        allow_unverified_openai_prompt_only=True,
        strict_no_fallback=True,
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--prefill-dispatch-mode openai_prompt_only" in proxy_script
    assert "--allow-unverified-openai-prompt-only" in proxy_script


def test_generate_configs_wires_decode_token_envelope_mode(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        decode_dispatch_mode="token_ids",
        allow_decode_token_fallback=False,
    )

    files = generate_configs(str(tmp_path), config)
    proxy_script = Path(files["proxy"]).read_text(encoding="utf-8")

    assert "--decode-dispatch-mode token_ids" in proxy_script
    assert "--no-allow-decode-token-fallback" in proxy_script


def test_generate_configs_can_enable_prefill_http_keepalive(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        prefill_http_keepalive=True,
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--prefill-http-keepalive" in proxy_script
    assert "--no-prefill-http-keepalive" not in proxy_script


def test_generate_configs_exposes_bounded_direct_release_batching(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        direct_feature_release_batch_max_jobs=7,
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--direct-feature-release-batch-max-jobs 7" in proxy_script


def test_generate_configs_can_disable_rendered_prefill_cache_for_ablation(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        enable_rendered_prefill_cache=False,
        rendered_prefill_cache_max_entries=7,
        rendered_prefill_cache_max_bytes=123456,
        rendered_prefill_cache_ttl_s=12.5,
    )
    generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "--no-enable-rendered-prefill-cache" in proxy_script
    assert "--rendered-prefill-cache-max-entries 7" in proxy_script
    assert "--rendered-prefill-cache-max-bytes 123456" in proxy_script
    assert "--rendered-prefill-cache-ttl-s 12.5" in proxy_script


def test_generate_configs_exports_rdma_and_shm_protocols(tmp_path):
    rdma_dir = tmp_path / "rdma"
    rdma_cfg = VLLMDisaggConfig(local_hostname="127.0.0.1", protocol="rdma")
    generate_configs(str(rdma_dir), rdma_cfg)
    rdma_prefill = (rdma_dir / "start_prefill.sh").read_text(encoding="utf-8")
    rdma_proxy = (rdma_dir / "start_proxy.sh").read_text(encoding="utf-8")
    assert "export MOONCAKE_PROTOCOL=rdma" in rdma_prefill
    assert "export MOONCAKE_EPD_DIRECT_ENGINE_PROTOCOL=rdma" in rdma_prefill
    assert "export RDMAV_FORK_SAFE=1" in rdma_prefill
    assert "export IBV_FORK_SAFE=1" in rdma_prefill
    assert "unset MC_FORCE_TCP" in rdma_prefill
    assert "--mooncake-protocol rdma" in rdma_proxy
    assert '"mooncake_protocol":"rdma"' in rdma_prefill
    assert "unset MC_TCP_WRITE_COMPLETION_ACK" in rdma_prefill

    shm_dir = tmp_path / "shm"
    shm_cfg = VLLMDisaggConfig(local_hostname="127.0.0.1", protocol="shm")
    generate_configs(str(shm_dir), shm_cfg)
    shm_prefill = (shm_dir / "start_prefill.sh").read_text(encoding="utf-8")
    shm_proxy = (shm_dir / "start_proxy.sh").read_text(encoding="utf-8")
    assert "export MOONCAKE_PROTOCOL=shm" in shm_prefill
    assert "export MOONCAKE_EPD_DIRECT_ENGINE_PROTOCOL=tcp" in shm_prefill
    assert "unset MC_FORCE_TCP" in shm_prefill
    assert "--mooncake-protocol shm" in shm_proxy
    assert '"mooncake_protocol":"shm"' in shm_prefill
    assert "export MC_TCP_WRITE_COMPLETION_ACK=1" in shm_prefill

    split_dir = tmp_path / "rdma-kv-tcp-feature"
    split_cfg = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        protocol="rdma",
        direct_engine_protocol="tcp",
    )
    generate_configs(str(split_dir), split_cfg)
    split_prefill = (split_dir / "start_prefill.sh").read_text(encoding="utf-8")
    assert "export MOONCAKE_PROTOCOL=rdma" in split_prefill
    assert "export MOONCAKE_EPD_DIRECT_ENGINE_PROTOCOL=tcp" in split_prefill
    assert "export MC_TCP_WRITE_COMPLETION_ACK=1" in split_prefill


def test_generate_configs_can_disable_tcp_write_completion_ack_for_ablation(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        protocol="tcp",
        tcp_write_completion_ack=False,
    )
    generate_configs(str(tmp_path), config)
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")

    assert "unset MC_TCP_WRITE_COMPLETION_ACK" in prefill_script


def test_generate_configs_requires_explicit_tcp_connection_pool_opt_in(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        protocol="tcp",
        tcp_connection_pool=True,
    )
    generate_configs(str(tmp_path), config)
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")

    assert "export MC_TCP_ENABLE_CONNECTION_POOL=1" in prefill_script


def test_generate_configs_exports_intranode_nvlink_protocol(tmp_path):
    cfg = VLLMDisaggConfig(local_hostname="127.0.0.1", protocol="nvlink_intra")
    generate_configs(str(tmp_path), cfg)
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")

    assert "export MOONCAKE_PROTOCOL=nvlink_intra" in prefill_script
    assert "export MOONCAKE_EPD_DIRECT_ENGINE_PROTOCOL=nvlink_intra" in prefill_script
    assert "export MC_INTRANODE_NVLINK=1" in prefill_script
    assert "unset MC_FORCE_TCP" in prefill_script
    assert "--mooncake-protocol nvlink_intra" in proxy_script
    assert '"num_workers":2' in prefill_script


def test_generate_configs_allows_explicit_nvlink_sender_worker_override(tmp_path):
    cfg = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        protocol="nvlink_intra",
        transfer_workers=3,
    )
    generate_configs(str(tmp_path), cfg)
    prefill_script = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")

    assert cfg.effective_transfer_workers == 3
    assert '"num_workers":3' in prefill_script


def test_generate_configs_rejects_mixed_nvlink_direct_protocol(tmp_path):
    cfg = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        protocol="tcp",
        direct_engine_protocol="nvlink_intra",
    )

    with pytest.raises(RuntimeError, match="process-scoped"):
        generate_configs(str(tmp_path), cfg)


def test_default_network_sender_workers_remain_four():
    cfg = VLLMDisaggConfig(local_hostname="127.0.0.1", protocol="tcp")

    assert cfg.effective_transfer_workers == 4


def test_default_nvlink_sender_workers_use_two_submitters():
    cfg = VLLMDisaggConfig(local_hostname="127.0.0.1", protocol="nvlink_intra")

    assert cfg.effective_transfer_workers == 2


def test_generate_configs_supports_multiple_decode_workers(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        decode_gpus=(2, 3),
        decode_ports=(18200, 18201),
        mm_prefetch_mode="feature_handle",
        prefill_supports_feature_handles=True,
    )
    files = generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")
    decode0 = (tmp_path / "start_decode.sh").read_text(encoding="utf-8")
    decode1 = (tmp_path / "start_decode_1.sh").read_text(encoding="utf-8")

    assert len(files["decode_scripts"]) == 2
    assert files["decode_scripts"][0].endswith("start_decode.sh")
    assert files["decode_scripts"][1].endswith("start_decode_1.sh")
    assert "CUDA_VISIBLE_DEVICES=2" in decode0
    assert "CUDA_VISIBLE_DEVICES=3" in decode1
    assert "epd-decode-0" in decode0
    assert "epd-decode-1" in decode1
    assert "--decoder-hosts 127.0.0.1 127.0.0.1" in proxy_script
    assert "--low-latency-decode-worker-ids decode-0" in proxy_script
    assert "--standard-decode-worker-ids decode-1" in proxy_script


def test_generate_configs_supports_multiple_prefill_direct_buffer_routes(tmp_path):
    config = VLLMDisaggConfig(
        local_hostname="127.0.0.1",
        prefill_gpus=(0, 1),
        prefill_ports=(18100, 18101),
        decode_gpus=(2,),
        mm_prefetch_mode="feature_handle",
        prefill_supports_feature_handles=True,
        enable_prefill_direct_feature_buffer_routes=True,
        encoder_service_url="http://127.0.0.1:8330",
    )
    files = generate_configs(str(tmp_path), config)
    proxy_script = (tmp_path / "start_proxy.sh").read_text(encoding="utf-8")
    prefill0 = (tmp_path / "start_prefill.sh").read_text(encoding="utf-8")
    prefill1 = (tmp_path / "start_prefill_1.sh").read_text(encoding="utf-8")

    assert len(files["prefill_scripts"]) == 2
    assert "CUDA_VISIBLE_DEVICES=0" in prefill0
    assert "CUDA_VISIBLE_DEVICES=1" in prefill1
    assert "export MOONCAKE_EPD_DIRECT_BUFFER_WORKER_ID=prefill-0" in prefill0
    assert "export MOONCAKE_EPD_DIRECT_BUFFER_WORKER_ID=prefill-1" in prefill1
    assert "--prefiller-ports 18100 18101" in proxy_script
    assert "--prefill-direct-buffer-service-urls http://127.0.0.1:18100 http://127.0.0.1:18101" in proxy_script
    assert config.prefill_direct_buffer_service_urls == (
        "http://127.0.0.1:18100",
        "http://127.0.0.1:18101",
    )
