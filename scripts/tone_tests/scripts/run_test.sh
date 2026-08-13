#!/bin/bash

CONTAINER_NAME=${CONTAINER_NAME:-"mooncake-ci-test"}
MODEL_CACHE=${MODEL_CACHE:-"/root/.cache"}
HF_TOKEN_FILE=${HF_TOKEN_FILE:-"/etc/mooncake-ci/huggingface.token"}
CI_ACCELERATOR=${CI_ACCELERATOR:-"cuda"}
if [ "$CI_ACCELERATOR" = "rocm" ]; then
    REGISTRY_ADDR_SGLANG=${REGISTRY_ADDR_SGLANG:-"lmsysorg/sglang:v0.5.13.post1-rocm720-mi35x@sha256:552e7038a9309796ad0cd6c62e80568eca4f88ef1e1eb42db2f7ffaafcd79693"}
    REGISTRY_ADDR_VLLM=${REGISTRY_ADDR_VLLM:-"vllm/vllm-openai-rocm:v0.21.0@sha256:98a77b20df03adeb1cfc0ced009b4df6dd52b0a994ab99a32421f30876a9ae0c"}
    MOONCAKE_SGLANG_BASE_GPU_ID=${MOONCAKE_SGLANG_BASE_GPU_ID:-0}
    MOONCAKE_EPD_ENCODER_GPU_ID=${MOONCAKE_EPD_ENCODER_GPU_ID:-0}
    MOONCAKE_EPD_PREFILL_GPU_ID=${MOONCAKE_EPD_PREFILL_GPU_ID:-2}
    MOONCAKE_EPD_DECODE_GPU_ID=${MOONCAKE_EPD_DECODE_GPU_ID:-0}
    MOONCAKE_VLLM_VISIBLE_DEVICES=${MOONCAKE_VLLM_VISIBLE_DEVICES:-0,1}
    MOONCAKE_SGLANG_MEM_FRACTION_STATIC=${MOONCAKE_SGLANG_MEM_FRACTION_STATIC:-0.5}
    MOONCAKE_CI_TIER=${MOONCAKE_CI_TIER:-"core-4gpu"}
    MOONCAKE_RDMA_DEVICES=${MOONCAKE_RDMA_DEVICES:-"ionic_0,ionic_1,ionic_2,ionic_3"}
    MOONCAKE_RDMA_NETDEVS=${MOONCAKE_RDMA_NETDEVS:-"eth2,eth3,eth4,eth5"}
    MOONCAKE_TRANSFER_DEVICE=${MOONCAKE_TRANSFER_DEVICE:-"ionic_0"}
    MOONCAKE_GID_INDEX=${MOONCAKE_GID_INDEX:-1}
    USE_HUGGINGFACE_MIRROR=${USE_HUGGINGFACE_MIRROR:-false}
else
    REGISTRY_ADDR_SGLANG=${REGISTRY_ADDR_SGLANG:-"lmsysorg/sglang:latest"}
    REGISTRY_ADDR_VLLM=${REGISTRY_ADDR_VLLM:-"vllm/vllm-openai:latest"}
    MOONCAKE_SGLANG_BASE_GPU_ID=${MOONCAKE_SGLANG_BASE_GPU_ID:-6}
    MOONCAKE_EPD_ENCODER_GPU_ID=${MOONCAKE_EPD_ENCODER_GPU_ID:-0}
    MOONCAKE_EPD_PREFILL_GPU_ID=${MOONCAKE_EPD_PREFILL_GPU_ID:-4}
    MOONCAKE_EPD_DECODE_GPU_ID=${MOONCAKE_EPD_DECODE_GPU_ID:-6}
    MOONCAKE_VLLM_VISIBLE_DEVICES=${MOONCAKE_VLLM_VISIBLE_DEVICES:-6,7}
    MOONCAKE_SGLANG_MEM_FRACTION_STATIC=${MOONCAKE_SGLANG_MEM_FRACTION_STATIC:-}
    MOONCAKE_CI_TIER=${MOONCAKE_CI_TIER:-"full"}
    MOONCAKE_RDMA_DEVICES=${MOONCAKE_RDMA_DEVICES:-}
    MOONCAKE_RDMA_NETDEVS=${MOONCAKE_RDMA_NETDEVS:-}
    MOONCAKE_TRANSFER_DEVICE=${MOONCAKE_TRANSFER_DEVICE:-}
    MOONCAKE_GID_INDEX=${MOONCAKE_GID_INDEX:-}
    USE_HUGGINGFACE_MIRROR=${USE_HUGGINGFACE_MIRROR:-true}
fi
MOONCAKE_RENDER_DEVICES=${MOONCAKE_RENDER_DEVICES:-"/dev/dri/renderD129 /dev/dri/renderD137 /dev/dri/renderD145 /dev/dri/renderD153"}
MOONCAKE_GPU_INDICES=${MOONCAKE_GPU_INDICES:-"0,1,2,3"}
MOONCAKE_CPUSET_CPUS=${MOONCAKE_CPUSET_CPUS:-"0-95"}
MOONCAKE_CPUSET_MEMS=${MOONCAKE_CPUSET_MEMS:-"0"}
AINIC_VERSION="1.117.5"
HUGGINGFACE_MIRROR=${HUGGINGFACE_MIRROR:-"https://hf-mirror.com"}
USE_MODELSCOPE=${USE_MODELSCOPE:-false}
REMOTE_TEST_DIR=${REMOTE_TEST_DIR:-"/tmp/Mooncake_tone/mooncake_ci_test"}
LOCAL_IP=${LOCAL_IP}
REMOTE_IP=${REMOTE_IP}
ARTIFACT_ID=${ARTIFACT_ID:-}
ARTIFACT_ID_SGLANG=${ARTIFACT_ID_SGLANG:-$ARTIFACT_ID}
ARTIFACT_ID_VLLM=${ARTIFACT_ID_VLLM:-$ARTIFACT_ID}
WHEEL_DIR=${WHEEL_DIR:-}
WHEEL_DIR_SGLANG=${WHEEL_DIR_SGLANG:-$WHEEL_DIR}
WHEEL_DIR_VLLM=${WHEEL_DIR_VLLM:-$WHEEL_DIR}
GIT_REPO=${GIT_REPO:-}

if [ "$MOONCAKE_CI_TIER" = "core-4gpu" ]; then
    # The upstream heterogeneous-TP test starts TP4 and TP2 workers on the
    # same host and therefore needs eight GPUs. Keep the permanent 4+4 ROCm
    # allocation honest: all other SGLang and vLLM external-PD cases run here,
    # while this one remains an explicitly reported capacity exception.
    All_TEST_SCRIPTS_SGLANG=(
        "test_hicache_storage_mooncake_backend.sh"
        "test_1p1d_erdma.sh"
        "test_epd_sglang.sh"
        "test_moe_mooncake.sh"
    )
elif [ "$MOONCAKE_CI_TIER" = "full" ]; then
    All_TEST_SCRIPTS_SGLANG=(
        "test_hicache_storage_mooncake_backend.sh"
        "test_disaggregation_different_tp.sh"
        "test_1p1d_erdma.sh"
        "test_epd_sglang.sh"
        "test_moe_mooncake.sh"
    )
else
    echo "ERROR: unsupported MOONCAKE_CI_TIER: $MOONCAKE_CI_TIER" >&2
    exit 2
fi

All_TEST_SCRIPTS_VLLM=(
    "test_vllm_1p1d_erdma.sh"
)

if [ "$CI_ACCELERATOR" = "rocm" ]; then
    # The ROCm cluster uses a dedicated CI identity and pinned host key. Keep
    # the serving/RDMA address separate from the SSH management endpoint.
    REMOTE_SSH_TARGET=${REMOTE_SSH_TARGET:-"mooncake-worker"}
    MOONCAKE_SSH_CONFIG=${MOONCAKE_SSH_CONFIG:-"/etc/mooncake-ci/ssh_config"}
    SSH_CMD=${SSH_CMD:-"ssh -F ${MOONCAKE_SSH_CONFIG}"}
    RSYNC_RSH=${RSYNC_RSH:-"ssh -F ${MOONCAKE_SSH_CONFIG}"}
    SCP_CMD=${SCP_CMD:-"scp -F ${MOONCAKE_SSH_CONFIG}"}
else
    REMOTE_SSH_TARGET=${REMOTE_SSH_TARGET:-"$REMOTE_IP"}
    SSH_CMD=${SSH_CMD:-"ssh -o StrictHostKeyChecking=no"}
    RSYNC_RSH=${RSYNC_RSH:-"ssh -o StrictHostKeyChecking=no"}
    SCP_CMD=${SCP_CMD:-"scp -o StrictHostKeyChecking=no"}
fi
readonly REMOTE_SSH_TARGET SSH_CMD RSYNC_RSH SCP_CMD

TONE_TESTS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)
RUN_DIR="$TONE_TESTS_DIR/run"

. $TONE_TESTS_DIR/scripts/common.sh

get_test_type() {
    local test_name=$1
    
    if [ ! -f "$TONE_TESTS_DIR/scripts/$test_name" ]; then
        echo "unknown"
        return 1
    fi
    
    local test_type=$(grep "^TEST_TYPE=" "$TONE_TESTS_DIR/scripts/$test_name" | head -n 1 | cut -d'"' -f2)
    
    if [ -z "$test_type" ]; then
        echo "unknown"
        return 1
    fi
    
    echo "$test_type"
    return 0
}

get_framework_from_test_array() {
    local test_array_name=$1
    
    case $test_array_name in
        "All_TEST_SCRIPTS_SGLANG")
            echo "SGLANG"
            ;;
        "All_TEST_SCRIPTS_VLLM")
            echo "VLLM"
            ;;
        *)
            echo "SGLANG"
            ;;
    esac
}

get_framework_from_test_name() {
    local test_name=$1
    
    for vllm_test in "${All_TEST_SCRIPTS_VLLM[@]}"; do
        if [ "$test_name" = "$vllm_test" ]; then
            echo "VLLM"
            return 0
        fi
    done
    
    # default SGLANG
    echo "SGLANG"
}

get_registry_addr_for_framework() {
    local framework_type=$1
    case $framework_type in
        "SGLANG")
            echo "$REGISTRY_ADDR_SGLANG"
            ;;
        "VLLM")
            echo "$REGISTRY_ADDR_VLLM"
            ;;
        *)
            echo "$REGISTRY_ADDR_SGLANG"  # default
            ;;
    esac
}

is_base_env_prepared() {
    if [ ! -f "$RUN_DIR/.shrc" ]; then
        return 1  # config file doesn't exist
    fi

    # Check if the wheel source matches the current environment.
    local current_artifact_id=$(grep "^export ARTIFACT_ID=" "$RUN_DIR/.shrc" | cut -d'=' -f2-)
    local current_wheel_dir=$(grep "^export WHEEL_DIR=" "$RUN_DIR/.shrc" | cut -d'=' -f2-)
    if [ "$current_artifact_id" != "$ARTIFACT_ID" ]; then
        return 1  # ARTIFACT_ID mismatch
    fi
    if [ "$current_wheel_dir" != "$WHEEL_DIR" ]; then
        return 1  # self-hosted artifact path mismatch
    fi

    # Check if whl package exists
    local whl_count=$(find "$RUN_DIR/whls/" -name "*.whl" -type f 2>/dev/null | wc -l)
    [ $whl_count -gt 0 ]
}

prepare_single_env(){
    local registry_addr=$1 
    local framework_type=$2

    if is_base_env_prepared; then
        echo "Environment already prepared for current ARTIFACT_ID, skipping setup"
        return 0
    fi

    echo "===== Preparing environment for $framework_type (registry: $registry_addr) ====="
    
    setup_directory $RUN_DIR
    setup_directory $RUN_DIR/logs

    cat > $RUN_DIR/.shrc << EOF
# Mooncake CI Test Environment Variables - Main Controller
export CONTAINER_NAME=${CONTAINER_NAME}
export CI_ACCELERATOR=${CI_ACCELERATOR}
export MOONCAKE_CI_TIER=${MOONCAKE_CI_TIER}
export MODEL_CACHE=${MODEL_CACHE}
export HF_TOKEN_FILE=${HF_TOKEN_FILE}
export REGISTRY_ADDR_SGLANG=${REGISTRY_ADDR_SGLANG}
export REGISTRY_ADDR_VLLM=${REGISTRY_ADDR_VLLM}
export USE_HUGGINGFACE_MIRROR=${USE_HUGGINGFACE_MIRROR}
export HUGGINGFACE_MIRROR=${HUGGINGFACE_MIRROR}
export USE_MODELSCOPE=${USE_MODELSCOPE}
export ARTIFACT_ID=${ARTIFACT_ID}
export WHEEL_DIR=${WHEEL_DIR}
export GIT_REPO=${GIT_REPO}
export LOCAL_IP=${LOCAL_IP}
export REMOTE_IP=${REMOTE_IP}
export BASE_DIR=${TONE_TESTS_DIR}
export TEST_RUN_DIR=${RUN_DIR}
export TEST_RESULT_DIR=${RUN_DIR}/logs
export REMOTE_TEST_DIR=${REMOTE_TEST_DIR}
export MOONCAKE_RENDER_DEVICES="${MOONCAKE_RENDER_DEVICES}"
export MOONCAKE_GPU_INDICES=${MOONCAKE_GPU_INDICES}
export MOONCAKE_CPUSET_CPUS=${MOONCAKE_CPUSET_CPUS}
export MOONCAKE_CPUSET_MEMS=${MOONCAKE_CPUSET_MEMS}
export MOONCAKE_SGLANG_BASE_GPU_ID=${MOONCAKE_SGLANG_BASE_GPU_ID}
export MOONCAKE_EPD_ENCODER_GPU_ID=${MOONCAKE_EPD_ENCODER_GPU_ID}
export MOONCAKE_EPD_PREFILL_GPU_ID=${MOONCAKE_EPD_PREFILL_GPU_ID}
export MOONCAKE_EPD_DECODE_GPU_ID=${MOONCAKE_EPD_DECODE_GPU_ID}
export MOONCAKE_VLLM_VISIBLE_DEVICES=${MOONCAKE_VLLM_VISIBLE_DEVICES}
export MOONCAKE_SGLANG_MEM_FRACTION_STATIC=${MOONCAKE_SGLANG_MEM_FRACTION_STATIC}
export MOONCAKE_RDMA_DEVICES=${MOONCAKE_RDMA_DEVICES}
export MOONCAKE_RDMA_NETDEVS=${MOONCAKE_RDMA_NETDEVS}
export MOONCAKE_TRANSFER_DEVICE=${MOONCAKE_TRANSFER_DEVICE}
export MOONCAKE_GID_INDEX=${MOONCAKE_GID_INDEX}
export AINIC_VERSION=${AINIC_VERSION}
EOF

    echo "===== Preparing local machine ====="
    echo "Get mooncake whl on local machine..."
    source $RUN_DIR/.shrc && get_whl $RUN_DIR
    if [ $? -ne 0 ]; then
        echo "Failed to get mooncake whl on local machine"
        return 1
    fi
    echo "Local preparation completed successfully"

    return 0
}

prepare_double_env(){
    local registry_addr=$1
    local framework_type=$2

    echo "===== Preparing Double-Machine Environment (local + remote) ====="

    if ! prepare_single_env "$registry_addr" "$framework_type"; then
        echo "ERROR: prepare_single_env failed"
        return 1
    fi

    if [ -z "$REMOTE_IP" ]; then
        echo "ERROR: REMOTE_IP must be set for double-machine prepare"
        return 1
    fi

    echo "Preparing remote machine $REMOTE_IP..."
    ${SSH_CMD} "$REMOTE_SSH_TARGET" "rm -rf ${REMOTE_TEST_DIR} && mkdir -p ${REMOTE_TEST_DIR}"
    
    rsync -av -e "$RSYNC_RSH" ${TONE_TESTS_DIR}/ "$REMOTE_SSH_TARGET:${REMOTE_TEST_DIR}/"
    if [ $? -ne 0 ]; then
        echo "Failed to sync files to remote server"
        return 1
    fi
    
    ${SSH_CMD} "$REMOTE_SSH_TARGET" "sed -i 's|^export BASE_DIR=.*$|export BASE_DIR=${REMOTE_TEST_DIR}|' ${REMOTE_TEST_DIR}/run/.shrc && \
                            sed -i 's|^export TEST_RUN_DIR=.*$|export TEST_RUN_DIR=${REMOTE_TEST_DIR}/run|' ${REMOTE_TEST_DIR}/run/.shrc && \
                            sed -i 's|^export TEST_RESULT_DIR=.*$|export TEST_RESULT_DIR=${REMOTE_TEST_DIR}/logs|' ${REMOTE_TEST_DIR}/run/.shrc"
    
    echo "Remote preparation completed successfully"

    return 0
}

setup_env_for_test() {
    local target=$1
    local framework_type=$2
    local type=$(get_test_type "$target")
    [ "$target" = "all" ] && type="double"

    local registry_addr=$(get_registry_addr_for_framework "$framework_type")
    case "$framework_type" in
        SGLANG)
            ARTIFACT_ID=$ARTIFACT_ID_SGLANG
            WHEEL_DIR=$WHEEL_DIR_SGLANG
            ;;
        VLLM)
            ARTIFACT_ID=$ARTIFACT_ID_VLLM
            WHEEL_DIR=$WHEEL_DIR_VLLM
            ;;
        *) echo "ERROR: unknown framework type: $framework_type" >&2; return 1 ;;
    esac
    [ -n "$ARTIFACT_ID" ] || [ -n "$WHEEL_DIR" ] || {
        echo "ERROR: no wheel source configured for $framework_type" >&2
        return 1
    }
    export ARTIFACT_ID WHEEL_DIR

    if [ "$type" = "double" ]; then
        prepare_double_env "$registry_addr" "$framework_type" || return 1
    else
        prepare_single_env "$registry_addr" "$framework_type" || return 1
    fi

    source "$RUN_DIR/.shrc" && setup_node_env "$registry_addr" || return 1

    if [ "$type" = "double" ]; then
        echo "Initializing remote node $REMOTE_IP..."
        ${SSH_CMD} "$REMOTE_SSH_TARGET" "
            source ${REMOTE_TEST_DIR}/run/.shrc && \
            source ${REMOTE_TEST_DIR}/scripts/common.sh && \
            setup_node_env '${registry_addr}'
        " || { echo "ERROR: Remote setup failed"; return 1; }
    fi
    
    echo "All environments are ready."
}

run_single_test(){
    local test_name=$1
    shift
    echo "===== Running Single Test: $test_name ====="

    local framework_type=$(get_framework_from_test_name "$test_name")
    echo "Test $test_name will use framework: $framework_type"
    
    setup_env_for_test "$test_name" "$framework_type" || return 1
    
    source "$RUN_DIR/.shrc"
    cd "$TONE_TESTS_DIR/scripts"
    source "./$test_name"

    local log_dir="${BASE_DIR}/run/logs/$(basename "$test_name" .sh)"
    setup_log_directory "$log_dir"

    local exit_code=0
    run_test "$@" || exit_code=1
    
    if declare -f parse >/dev/null 2>&1; then
        parse "$exit_code" || exit_code=1
    fi

    local type=$(get_test_type "$test_name")
    cleanup_test_env "$type"
    return $exit_code
}

run_all_tests(){
    local input_tests=$1
    if ! declare -p "$input_tests" >/dev/null 2>&1; then
        echo "ERROR: Variable '$input_tests' does not exist"
        return 1
    fi
    local tests_array_ref="${input_tests}[@]"
    local tests=("${!tests_array_ref}")

    local framework_type=$(get_framework_from_test_array "$input_tests")
    echo "===== Running All Tests for $framework_type Framework (Double Machine Mode) ====="
    
    setup_env_for_test "all" "$framework_type" || return 1
    
    source "$RUN_DIR/.shrc"
    cd "$TONE_TESTS_DIR/scripts"
    
    local all_passed=true
    local test_index=0
    local test_count=${#tests[@]}
    for test_name in "${tests[@]}"; do
        test_index=$((test_index + 1))
        if [[ ! -f "./$test_name" ]]; then
            echo "WARNING: test case $test_name is not found, skipping"
            continue
        fi
        echo "Executing: $test_name"

        local log_dir="${BASE_DIR}/run/logs/$(basename "$test_name" .sh)"
        setup_log_directory "$log_dir"

        source "./$test_name"
        local exit_code=0
        run_test || exit_code=1
        
        if declare -f parse >/dev/null 2>&1; then
            parse "$exit_code" || exit_code=1
        fi
        
        [ $exit_code -ne 0 ] && all_passed=false

        # Container is shared across cases in run-all. Do not schedule another
        # case unless both nodes have been reset successfully.
        if [ "$test_index" -lt "$test_count" ] && ! drain_gpu_between_tests; then
            local remaining_count=$((test_count - test_index))
            echo "ERROR: Shared test environment is unhealthy after ${test_name}." >&2
            echo "ERROR: Skipping ${remaining_count} remaining test case(s); node intervention is required." >&2
            all_passed=false
            break
        fi
    done
    
    cleanup_test_env "double"
    $all_passed && return 0 || return 1
}

show_help(){
    echo "Mooncake CI Controller"
    echo "Usage: $0 <command> [args]"
    echo ""
    echo "Commands:"
    echo "  run-single <test_name>             - Full lifecycle: setup -> run -> parse -> cleanup"
    echo "  run-all [SGLANG|VLLM]              - Run all tests for specific framework"
    echo "  run-all                            - Run all tests for both SGLANG and VLLM frameworks"
    echo "  run-all SGLANG                     - Run all SGLANG tests (using SGLANG image)"
    echo "  run-all VLLM                       - Run all VLLM tests (using VLLM image)"
}

case "$1" in
    "run-single")
        shift
        run_single_test "$@"
        ;;
 "run-all")
    shift
    if [ -z "$1" ]; then
        # No parameter specified, run both SGLANG and VLLM tests
        echo "No framework specified, running all SGLANG tests..."
        run_all_tests "All_TEST_SCRIPTS_SGLANG"
        
        echo "Running all VLLM tests..."
        run_all_tests "All_TEST_SCRIPTS_VLLM"
    else
        FRAMEWORK=$1
        if [ "$FRAMEWORK" = "VLLM" ]; then
            run_all_tests "All_TEST_SCRIPTS_VLLM"
        elif [ "$FRAMEWORK" = "SGLANG" ]; then
            run_all_tests "All_TEST_SCRIPTS_SGLANG"
        else
            echo "ERROR: Unknown framework '$FRAMEWORK'. Use SGLANG or VLLM."
            show_help
            return 1
        fi
    fi
    ;;
    *)
        show_help
        ;;
esac
