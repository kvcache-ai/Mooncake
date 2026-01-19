#!/bin/bash

# This test case is adapted from https://github.com/sgl-project/sglang/blob/main/test/srt/test_disaggregation_different_tp.py
# The original MLA model has been replaced with deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct

test_case_name="test_disaggregation_different_tp"
CONTAINER_NAME=${CONTAINER_NAME:-"mooncake-ci-test"}
MODEL_CACHE=${MODEL_CACHE:-"/root/.cache"}
REGISTRY_ADDR=${REGISTRY_ADDR:-"lmsysorg/sglang:latest"}
USE_HUGGINGFACE_MIRROR=${USE_HUGGINGFACE_MIRROR:-true}
HUGGINGFACE_MIRROR=${HUGGINGFACE_MIRROR:-"https://hf-mirror.com"}
USE_MODELSCOPE=${USE_MODELSCOPE:-false}
ARTIFACT_ID=
GIT_REPO=

TONE_TESTS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)
echo "TONE_TESTS_DIR: $TONE_TESTS_DIR"
. $TONE_TESTS_DIR/scripts/common.sh

setup()
{
    echo "===== Setting up test environment ====="
    echo "Setup test result directory..."
    setup_test_result_directory $TONE_TESTS_DIR/$TEST_CASE_RESULT_PATH

    cat > $TONE_TESTS_DIR/$TEST_CASE_RESULT_PATH/.shrc << EOF
# Mooncake CI Test Environment Variables - ${test_case_name}
export CONTAINER_NAME=${CONTAINER_NAME}
export MODEL_CACHE=${MODEL_CACHE}
export REGISTRY_ADDR=${REGISTRY_ADDR}
export USE_HUGGINGFACE_MIRROR=${USE_HUGGINGFACE_MIRROR}
export HUGGINGFACE_MIRROR=${HUGGINGFACE_MIRROR}
export USE_MODELSCOPE=${USE_MODELSCOPE}
export ARTIFACT_ID=${ARTIFACT_ID}
export GIT_REPO=${GIT_REPO}
export BASE_DIR=${TONE_TESTS_DIR}
export TEST_CASE_RESULT_DIR=${TONE_TESTS_DIR}/${TEST_CASE_RESULT_PATH}
EOF

    echo "Get mooncake whl..."
    source $TONE_TESTS_DIR/$TEST_CASE_RESULT_PATH/.shrc && get_whl $TONE_TESTS_DIR/$TEST_CASE_RESULT_PATH
    if [ $? -ne 0 ]; then
        echo "Failed to get mooncake whl"
        return 1
    fi

    echo "Get the latest sglang image..."
    if ! get_image; then
        echo "ERROR: Failed to get the required image"
        return 1
    fi

    echo "Quit old container..."
    if ! clean_container ${CONTAINER_NAME}; then
        echo "ERROR: Failed to clean up container"
        return 1
    fi

    extra_args=""
    extra_args="$extra_args --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/uverbs1 --device=/dev/infiniband/rdma_cm "
    if ${USE_HUGGINGFACE_MIRROR}; then
        extra_args="$extra_args -e HF_ENDPOINT=${HUGGINGFACE_MIRROR}"
        extra_args="$extra_args -e HF_HUB_ENABLE_HF_TRANSFER=1"
    fi
    if ${USE_MODELSCOPE}; then
        extra_args="$extra_args -e SGLANG_USE_MODELSCOPE=true"
    fi
    echo "extra_args: $extra_args"

    echo "Launching docker container..."
    if ! docker_launch "$extra_args"; then
        echo "ERROR: Failed to launch docker container"
        return 1
    fi

    return 0
}

run_test()
{ 
    echo "===== Running pytest tests ====="
    local log_dir="${TONE_TESTS_DIR}/${TEST_CASE_RESULT_PATH}/logs"
    if [ -d "$log_dir" ]; then
        echo "Removing existing log directory: $log_dir"
        rm -rf "$log_dir"
    fi
    mkdir -p "$log_dir"
    local log_file="${log_dir}/${test_case_name}.log"

    echo "Running tests in container and saving output to: $log_file"
    ${docker_exec} "\
        cd /sgl-workspace/sglang/test/srt && \
        sed -i '0,/^class /s|^class |DEFAULT_MODEL_NAME_FOR_TEST_MLA = \"deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct\"\n&|' test_disaggregation_different_tp.py && \
        echo 'Model override applied successfully' && \
        python3 -m pytest test_disaggregation_different_tp.py -v -s --tb=long" | tee "$log_file"
    
    return ${PIPESTATUS[0]}
}

parse()
{
    local test_exit_code=$1

    echo "===== Parsing test results ====="
    local result_json="${TONE_TESTS_DIR}/${TEST_CASE_RESULT_PATH}/test_results.json"

    if [ $test_exit_code -eq 0 ]; then
        echo "All tests passed"
        echo "{\"test_case\": \"$test_case_name\", \"status\": \"Pass\", \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" > "$result_json"
        echo "$test_case_name: Pass"
    else
        echo "Tests failed - check log file for details"
        echo "{\"test_case\": \"$test_case_name\", \"status\": \"Fail\", \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" > "$result_json"
        echo "$test_case_name: Fail"
    fi
    
    echo "Test results saved to: $result_json"
}

cleanup()
{
    echo "===== Cleaning up ====="
    stop_container "${CONTAINER_NAME}"
}

main()
{
    local exit_code=0
    
    echo "===== Step 1: Setup ====="
    if ! setup; then
        echo "ERROR: Setup failed"
        exit_code=1
    fi
    
    if [ $exit_code -eq 0 ]; then
        echo "===== Step 2: Run Test ====="
        if ! run_test; then
            echo "ERROR: Run test failed"
            exit_code=1
        fi
    fi
    
    echo "===== Step 3: Parse Results ====="
    parse $exit_code
    
    echo "===== Step 4: Cleanup ====="
    cleanup
    
    if [ $exit_code -ne 0 ]; then
        echo "===== Test completed with errors ====="
        exit 1
    else
        echo "===== Test completed successfully ====="
    fi
}

main