#!/bin/bash

# This test case is adapted from https://github.com/sgl-project/sglang/blob/main/test/srt/test_disaggregation_different_tp.py
# The original MLA model has been replaced with deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct

test_case_name="test_disaggregation_different_tp"
TEST_TYPE="single"

BASE_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}
. ${BASE_DIR}/scripts/common.sh

run_test()
{ 
    echo "===== Running pytest tests ====="
    local log_file="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${test_case_name}.log"

    echo "Running tests in container and saving output to: $log_file"
    ${docker_exec} "\
        cd /sgl-workspace/sglang/test/srt && \
        sed -i '0,/^class /s|^class |DEFAULT_MODEL_NAME_FOR_TEST_MLA = \"deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct\"\nDEFAULT_MODEL_NAME_FOR_TEST = \"meta-llama/Llama-3.2-3B-Instruct\"\n&|' test_disaggregation_different_tp.py && \
        echo 'Model override applied successfully' && \
        python3 -m pytest test_disaggregation_different_tp.py -v -s --tb=long" | tee "$log_file"
    
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