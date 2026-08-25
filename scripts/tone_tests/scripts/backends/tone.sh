#!/bin/bash

configure_integration_test_backend() {
    CI_ACCELERATOR=cuda
    REGISTRY_ADDR_SGLANG=${REGISTRY_ADDR_SGLANG:-"lmsysorg/sglang:latest"}
    REGISTRY_ADDR_VLLM=${REGISTRY_ADDR_VLLM:-"vllm/vllm-openai:latest"}
    MOONCAKE_SGLANG_BASE_GPU_ID=${MOONCAKE_SGLANG_BASE_GPU_ID:-6}
    MOONCAKE_EPD_ENCODER_GPU_ID=${MOONCAKE_EPD_ENCODER_GPU_ID:-0}
    MOONCAKE_EPD_PREFILL_GPU_ID=${MOONCAKE_EPD_PREFILL_GPU_ID:-4}
    MOONCAKE_EPD_DECODE_GPU_ID=${MOONCAKE_EPD_DECODE_GPU_ID:-6}
    MOONCAKE_VLLM_VISIBLE_DEVICES=${MOONCAKE_VLLM_VISIBLE_DEVICES:-6,7}
    MOONCAKE_SGLANG_MEM_FRACTION_STATIC=${MOONCAKE_SGLANG_MEM_FRACTION_STATIC:-}
    MOONCAKE_CI_TIER=full
    MOONCAKE_RENDER_DEVICES=""
    MOONCAKE_GPU_INDICES=""
    MOONCAKE_CPUSET_CPUS=""
    MOONCAKE_CPUSET_MEMS=""
    MOONCAKE_RDMA_DEVICES=""
    MOONCAKE_RDMA_NETDEVS=""
    MOONCAKE_TRANSFER_DEVICE=""
    MOONCAKE_GID_INDEX=""
    AINIC_VERSION=""
    USE_HUGGINGFACE_MIRROR=${USE_HUGGINGFACE_MIRROR:-true}

    All_TEST_SCRIPTS_SGLANG=(
        "test_hicache_storage_mooncake_backend.sh"
        "test_disaggregation_different_tp.sh"
        "test_1p1d_erdma.sh"
        "test_epd_sglang.sh"
        "test_moe_mooncake.sh"
    )
    All_TEST_SCRIPTS_VLLM=("test_vllm_1p1d_erdma.sh")

    REMOTE_SSH_TARGET=${REMOTE_SSH_TARGET:-"$REMOTE_IP"}
    SSH_CMD=${SSH_CMD:-"ssh -o StrictHostKeyChecking=no"}
    RSYNC_RSH=${RSYNC_RSH:-"ssh -o StrictHostKeyChecking=no"}
    SCP_CMD=${SCP_CMD:-"scp -o StrictHostKeyChecking=no"}
}
