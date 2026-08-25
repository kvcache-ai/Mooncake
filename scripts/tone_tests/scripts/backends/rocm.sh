#!/bin/bash

configure_integration_test_backend() {
    CI_ACCELERATOR=rocm
    : "${REGISTRY_ADDR_SGLANG:?REGISTRY_ADDR_SGLANG is required for ROCm}"
    : "${REGISTRY_ADDR_VLLM:?REGISTRY_ADDR_VLLM is required for ROCm}"
    : "${MOONCAKE_CI_TIER:?MOONCAKE_CI_TIER is required for ROCm}"
    : "${MOONCAKE_RENDER_DEVICES:?MOONCAKE_RENDER_DEVICES is required for ROCm}"
    : "${MOONCAKE_GPU_INDICES:?MOONCAKE_GPU_INDICES is required for ROCm}"
    : "${MOONCAKE_CPUSET_CPUS:?MOONCAKE_CPUSET_CPUS is required for ROCm}"
    : "${MOONCAKE_CPUSET_MEMS:?MOONCAKE_CPUSET_MEMS is required for ROCm}"
    : "${MOONCAKE_RDMA_DEVICES:?MOONCAKE_RDMA_DEVICES is required for ROCm}"
    : "${MOONCAKE_RDMA_NETDEVS:?MOONCAKE_RDMA_NETDEVS is required for ROCm}"
    : "${MOONCAKE_TRANSFER_DEVICE:?MOONCAKE_TRANSFER_DEVICE is required for ROCm}"
    : "${MOONCAKE_GID_INDEX:?MOONCAKE_GID_INDEX is required for ROCm}"
    : "${MOONCAKE_SGLANG_BASE_GPU_ID:?MOONCAKE_SGLANG_BASE_GPU_ID is required for ROCm}"
    : "${MOONCAKE_EPD_ENCODER_GPU_ID:?MOONCAKE_EPD_ENCODER_GPU_ID is required for ROCm}"
    : "${MOONCAKE_EPD_PREFILL_GPU_ID:?MOONCAKE_EPD_PREFILL_GPU_ID is required for ROCm}"
    : "${MOONCAKE_EPD_DECODE_GPU_ID:?MOONCAKE_EPD_DECODE_GPU_ID is required for ROCm}"
    : "${MOONCAKE_VLLM_VISIBLE_DEVICES:?MOONCAKE_VLLM_VISIBLE_DEVICES is required for ROCm}"
    : "${MOONCAKE_SGLANG_MEM_FRACTION_STATIC:?MOONCAKE_SGLANG_MEM_FRACTION_STATIC is required for ROCm}"
    : "${AINIC_VERSION:?AINIC_VERSION is required for ROCm}"
    : "${REMOTE_SSH_TARGET:?REMOTE_SSH_TARGET is required for ROCm}"
    : "${MOONCAKE_SSH_CONFIG:?MOONCAKE_SSH_CONFIG is required for ROCm}"
    USE_HUGGINGFACE_MIRROR=${USE_HUGGINGFACE_MIRROR:-false}

    case "$MOONCAKE_CI_TIER" in
        core-4gpu)
            # The heterogeneous-TP case needs eight GPUs. Mooncake Elastic EP
            # is CUDA-only, so neither case belongs in the 4+4 ROCm tier.
            All_TEST_SCRIPTS_SGLANG=(
                "test_hicache_storage_mooncake_backend.sh"
                "test_1p1d_erdma.sh"
                "test_epd_sglang.sh"
            )
            ;;
        full)
            All_TEST_SCRIPTS_SGLANG=(
                "test_hicache_storage_mooncake_backend.sh"
                "test_disaggregation_different_tp.sh"
                "test_1p1d_erdma.sh"
                "test_epd_sglang.sh"
                "test_moe_mooncake.sh"
            )
            ;;
        *)
            echo "ERROR: unsupported MOONCAKE_CI_TIER: $MOONCAKE_CI_TIER" >&2
            return 2
            ;;
    esac
    All_TEST_SCRIPTS_VLLM=("test_vllm_1p1d_erdma.sh")

    SSH_CMD=${SSH_CMD:-"ssh -F ${MOONCAKE_SSH_CONFIG}"}
    RSYNC_RSH=${RSYNC_RSH:-"ssh -F ${MOONCAKE_SSH_CONFIG}"}
    SCP_CMD=${SCP_CMD:-"scp -F ${MOONCAKE_SSH_CONFIG}"}
}
