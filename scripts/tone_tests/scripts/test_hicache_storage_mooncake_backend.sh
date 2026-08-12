#!/bin/bash

test_case_name="test_hicache_storage_mooncake_backend"
TEST_TYPE="single"

BASE_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}
. ${BASE_DIR}/scripts/common.sh

collect_failure_diagnostics()
{
    local log_file=$1

    {
        echo
        echo "===== HiCache host failure diagnostics ====="
        date -u '+timestamp=%Y-%m-%dT%H:%M:%SZ'

        echo "--- Host memory ---"
        free -h 2>&1 || true

        echo "--- Docker state ---"
        docker inspect --format \
            'state={{json .State}} restart_count={{.RestartCount}}' \
            "$CONTAINER_NAME" 2>&1 || true
        docker stats --no-stream --format \
            'name={{.Name}} memory={{.MemUsage}} memory_percent={{.MemPerc}} pids={{.PIDs}}' \
            "$CONTAINER_NAME" 2>&1 || true
        docker top "$CONTAINER_NAME" -eo pid,ppid,stat,rss,vsz,comm,args 2>&1 || true

        echo "--- Recent kernel messages ---"
        if command -v journalctl >/dev/null 2>&1; then
            journalctl -k --since '-15 minutes' --no-pager 2>&1 | tail -n 200 || true
        elif command -v dmesg >/dev/null 2>&1; then
            dmesg --ctime 2>&1 | tail -n 200 || true
        else
            echo "Neither journalctl nor dmesg is available"
        fi
        echo "===== End HiCache host failure diagnostics ====="
    } | tee -a "$log_file"
}

run_test()
{
    echo "===== Running pytest tests ====="
    local log_file="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${test_case_name}.log"

    echo "Running tests in container and saving output to: $log_file"
    ${docker_exec} "\
        export PYTHONPATH=/sgl-workspace/sglang/test/registered/hicache:\$PYTHONPATH && \
        cd /test_run/python && \
        python3 -m pytest test_hicache_storage_mooncake_backend.py -v -s --tb=long" | tee "$log_file"

    local test_exit_code=${PIPESTATUS[0]}
    if [ "$test_exit_code" -ne 0 ]; then
        collect_failure_diagnostics "$log_file"
    fi

    return "$test_exit_code"
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
