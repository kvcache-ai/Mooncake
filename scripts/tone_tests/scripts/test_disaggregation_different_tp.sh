#!/bin/bash

# This test case is adapted from https://github.com/sgl-project/sglang/blob/main/test/registered/distributed/test_disaggregation_different_tp.py
# The original test has been updated to use deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct as DEFAULT_MODEL_NAME_FOR_TEST_MLA
# and meta-llama/Llama-3.2-3B-Instruct as DEFAULT_MODEL_NAME_FOR_TEST

test_case_name="test_disaggregation_different_tp"
TEST_TYPE="single"

BASE_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}
. ${BASE_DIR}/scripts/common.sh

run_test()
{
    echo "===== Running pytest tests ====="
    local log_file="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${test_case_name}.log"

    echo "Running tests in container and saving output to: $log_file"

    local offline_prefix=""
    local cache_diagnostic=""
    if [ "${USE_MODELSCOPE}" = "true" ]; then
        cache_diagnostic="Model cache policy: online (USE_MODELSCOPE=true)"
    else
        local off_mla
        local off_llama
        local off_qwen
        off_mla=$(hf_offline_prefix "deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct")
        off_llama=$(hf_offline_prefix "meta-llama/Llama-3.2-3B-Instruct")
        off_qwen=$(hf_offline_prefix "Qwen/Qwen3.5-4B")
        if [ -z "$off_mla" ]; then
            cache_diagnostic="Model cache policy: online (missing cache for deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct)"
        elif [ -z "$off_llama" ]; then
            cache_diagnostic="Model cache policy: online (missing cache for meta-llama/Llama-3.2-3B-Instruct)"
        elif [ -z "$off_qwen" ]; then
            cache_diagnostic="Model cache policy: online (missing cache for Qwen/Qwen3.5-4B)"
        else
            offline_prefix="$off_mla"
            cache_diagnostic="Model cache policy: offline (all required models are cached)"
        fi
    fi

    {
        echo "$cache_diagnostic"
        ${docker_exec} "\
            cd /sgl-workspace/sglang/test/registered/disaggregation && \
            sed -i '0,/^class /s|^class |DEFAULT_MODEL_NAME_FOR_TEST_MLA = \"deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct\"\nDEFAULT_MODEL_NAME_FOR_TEST = \"meta-llama/Llama-3.2-3B-Instruct\"\n&|' test_disaggregation_different_tp.py && \
            echo 'Model override applied successfully' && \
            ${offline_prefix}python3 -m pytest test_disaggregation_different_tp.py -v -s --tb=long"
    } 2>&1 | tee "$log_file"

    return ${PIPESTATUS[0]}
}

parse()
{
    local test_exit_code=$1

    echo "===== Parsing test results ====="
    if [ $test_exit_code -eq 0 ]; then
        save_test_result "$test_case_name" "Pass" "${BASE_DIR}/${TEST_CASE_RESULT_PATH}"
        echo "✓ Test PASSED"
        return 0
    else
        save_test_result "$test_case_name" "Fail" "${BASE_DIR}/${TEST_CASE_RESULT_PATH}"
        echo "✗ Test FAILED"
        return 1
    fi
}

if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    exit_code=0
    if ! run_test; then
        exit_code=1
    fi

    parse $exit_code
    exit $?
fi
