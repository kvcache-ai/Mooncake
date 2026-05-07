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
    ${docker_exec} "\
        cd /sgl-workspace/sglang/test/registered/distributed && \
        sed -i '0,/^class /s|^class |DEFAULT_MODEL_NAME_FOR_TEST_MLA = \"deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct\"\nDEFAULT_MODEL_NAME_FOR_TEST = \"meta-llama/Llama-3.2-3B-Instruct\"\n&|' test_disaggregation_different_tp.py && \
        echo 'Model override applied successfully' && \
        python3 -m pytest test_disaggregation_different_tp.py -v -s --tb=long" | tee "$log_file"
    
    return ${PIPESTATUS[0]}
}

if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    run_test_case_with_parse "$test_case_name" run_test
    exit $?
fi
