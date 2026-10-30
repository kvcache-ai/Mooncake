#!/usr/bin/env bash

test_case_name="test_hicache_storage_mooncake_store_rs"
TEST_TYPE="single"

BASE_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}
# shellcheck source=common.sh
. "${BASE_DIR}/scripts/common.sh"

run_test() {
    echo "===== Running SGLang HiCache with Store-RS ====="
    local log_file="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${test_case_name}.log"
    local transport_backend=${MC_STORE_RS_TRANSPORT_BACKEND:-classic-te}

    if ! ${docker_exec} "MOONCAKE_STORE_BACKEND=rs python3 -c 'import mooncake._store_rs; import mooncake.store as store; assert store._BACKEND == \"rs\"'"; then
        echo "ERROR: SGLang image does not have the Store-RS backend" >&2
        return 1
    fi

    if ! ${docker_exec} "command -v redis-server >/dev/null && command -v redis-cli >/dev/null"; then
        echo "Installing Redis for Store-RS metadata..."
        if ! ${docker_exec} "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y redis-server redis-tools"; then
            echo "ERROR: failed to install Redis in the SGLang test image" >&2
            return 1
        fi
    fi

    ${docker_exec} "\
        export PYTHONPATH=/sgl-workspace/sglang/test/registered/hicache:\$PYTHONPATH && \
        export MOONCAKE_STORE_BACKEND=rs && \
        export MC_STORE_RS_TRANSPORT_BACKEND=${transport_backend} && \
        export MOONCAKE_TRANSFER_DEVICE='${MOONCAKE_TRANSFER_DEVICE:-}' && \
        cd /test_run/e2e/python && \
        python3 -m pytest test_hicache_storage_mooncake_backend.py -v -s --tb=long" \
        2>&1 | tee "$log_file"

    return "${PIPESTATUS[0]}"
}

parse() {
    local test_exit_code=$1
    if [[ "$test_exit_code" -eq 0 ]]; then
        save_test_result "$test_case_name" "Pass" "${BASE_DIR}/${TEST_CASE_RESULT_PATH}"
        return 0
    fi
    save_test_result "$test_case_name" "Fail" "${BASE_DIR}/${TEST_CASE_RESULT_PATH}"
    return 1
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    exit_code=0
    run_test || exit_code=$?
    parse "$exit_code"
fi
