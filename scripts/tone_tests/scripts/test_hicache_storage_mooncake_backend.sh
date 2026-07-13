#!/bin/bash

test_case_name="test_hicache_storage_mooncake_backend"
TEST_TYPE="single"

BASE_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}
. ${BASE_DIR}/scripts/common.sh

run_test()
{
    echo "===== Running pytest tests ====="
    local log_file="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${test_case_name}.log"

    echo "Running tests in container and saving output to: $log_file"
    ${docker_exec} "\
        export PYTHONPATH=/sgl-workspace/sglang/test/registered/hicache:\$PYTHONPATH && \
        cd /test_run/python && \
        python3 -m pytest test_hicache_storage_mooncake_backend.py -v -s --tb=long" | tee "$log_file"
    
    return ${PIPESTATUS[0]}
}

if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    run_test_case_with_parse "$test_case_name" run_test
    exit $?
fi
