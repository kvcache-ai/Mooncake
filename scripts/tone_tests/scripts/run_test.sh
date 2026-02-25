#!/bin/bash

CONTAINER_NAME=${CONTAINER_NAME:-"mooncake-ci-test"}
MODEL_CACHE=${MODEL_CACHE:-"/root/.cache"}
REGISTRY_ADDR_SGLANG=${REGISTRY_ADDR_SGLANG:-"lmsysorg/sglang:latest"}
REGISTRY_ADDR_VLLM=${REGISTRY_ADDR_VLLM:-"vllm/vllm-openai:latest"}
USE_HUGGINGFACE_MIRROR=${USE_HUGGINGFACE_MIRROR:-true}
HUGGINGFACE_MIRROR=${HUGGINGFACE_MIRROR:-"https://hf-mirror.com"}
USE_MODELSCOPE=${USE_MODELSCOPE:-false}
REMOTE_TEST_DIR=${REMOTE_TEST_DIR:-"/tmp/Mooncake_tone/mooncake_ci_test"}
LOCAL_IP=${LOCAL_IP}
REMOTE_IP=${REMOTE_IP}
ARTIFACT_ID=${ARTIFACT_ID}
GIT_REPO=${GIT_REPO}

All_TEST_SCRIPTS_SGLANG=(
    "test_hicache_storage_mooncake_backend.sh"
    "test_disaggregation_different_tp.sh"
    "test_1p1d_erdma.sh"
    "test_epd_sglang.sh"
    "test_ep_moe_mooncake.sh"
)

All_TEST_SCRIPTS_VLLM=(
    "test_vllm_1p1d_erdma.sh"
)

readonly SSH_CMD="ssh -o StrictHostKeyChecking=no"

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

    # Check if ARTIFACT_ID matches current environment
    local current_artifact_id=$(grep "^export ARTIFACT_ID=" "$RUN_DIR/.shrc" | cut -d'=' -f2-)
    if [ "$current_artifact_id" != "$ARTIFACT_ID" ]; then
        return 1  # ARTIFACT_ID mismatch
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
export MODEL_CACHE=${MODEL_CACHE}
export REGISTRY_ADDR_SGLANG=${REGISTRY_ADDR_SGLANG}
export REGISTRY_ADDR_VLLM=${REGISTRY_ADDR_VLLM}
export USE_HUGGINGFACE_MIRROR=${USE_HUGGINGFACE_MIRROR}
export HUGGINGFACE_MIRROR=${HUGGINGFACE_MIRROR}
export USE_MODELSCOPE=${USE_MODELSCOPE}
export ARTIFACT_ID=${ARTIFACT_ID}
export GIT_REPO=${GIT_REPO}
export LOCAL_IP=${LOCAL_IP}
export REMOTE_IP=${REMOTE_IP}
export BASE_DIR=${TONE_TESTS_DIR}
export TEST_RUN_DIR=${RUN_DIR}
export TEST_RESULT_DIR=${RUN_DIR}/logs
export REMOTE_TEST_DIR=${REMOTE_TEST_DIR}
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
    ${SSH_CMD} $REMOTE_IP "rm -rf ${REMOTE_TEST_DIR} && mkdir -p ${REMOTE_TEST_DIR}"
    
    rsync -av ${TONE_TESTS_DIR}/ $REMOTE_IP:${REMOTE_TEST_DIR}/
    if [ $? -ne 0 ]; then
        echo "Failed to sync files to remote server"
        return 1
    fi
    
    ${SSH_CMD} $REMOTE_IP "sed -i 's|^export BASE_DIR=.*$|export BASE_DIR=${REMOTE_TEST_DIR}|' ${REMOTE_TEST_DIR}/run/.shrc && \
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

    if [ "$type" = "double" ]; then
        prepare_double_env "$registry_addr" "$framework_type" || return 1
    else
        prepare_single_env "$registry_addr" "$framework_type" || return 1
    fi

    source "$RUN_DIR/.shrc" && setup_node_env "$registry_addr" || return 1

    if [ "$type" = "double" ]; then
        echo "Initializing remote node $REMOTE_IP..."
        ${SSH_CMD} "$REMOTE_IP" "
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
    if ! [[ -v "$input_tests" ]]; then
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
    for test_name in "${tests[@]}"; do
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