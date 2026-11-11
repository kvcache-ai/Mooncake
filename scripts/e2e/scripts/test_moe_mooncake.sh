#!/bin/bash


test_case_name="test_moe_mooncake"
TEST_TYPE="single"

BASE_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}
. ${BASE_DIR}/scripts/common.sh

run_model_test()
{
    echo "===== Running pytest tests ====="
    if ! ${docker_exec} "command -v sgl-eval >/dev/null 2>&1"; then
        echo "ERROR: sgl-eval is missing from the SGLang test container" >&2
        echo "The T-One setup must install the pinned sgl-eval dependency before pytest." >&2
        return 1
    fi

    # Use local HF cache (offline) when the model is already downloaded.
    local offline_prefix=$(hf_offline_prefix "deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct")

    ${docker_exec} "\
        export PYTHONPATH=/sgl-workspace/sglang:\$PYTHONPATH && \
        cd /test_run/e2e/python && \
        ${offline_prefix}python3 -m pytest test_moe_mooncake.py -v -s --tb=long"
}

run_ep_tests()
{
    local source_root
    source_root=$(cd "${E2E_DIR:-${BASE_DIR}/../e2e}/../.." && pwd) || return 1
    if [ ! -f "$source_root/python/tests/ep/test_ep_grid.py" ]; then
        # TONE stages scripts separately from the checkout. The exact-wheel
        # artifact also carries the matching test sources.
        local archive="${BASE_DIR}/run/whls/mooncake-ep-tests.tar.gz"
        if [ ! -f "$archive" ]; then
            echo "ERROR: EP test source bundle is missing from the wheel artifact: $archive" >&2
            return 1
        fi
        source_root="${BASE_DIR}/run/ep-test-sources"
        mkdir -p "$source_root" || return 1
        tar -xzf "$archive" -C "$source_root" || return 1
    fi

    # Copy tests only: imports of mooncake still resolve to the installed wheel.
    docker cp "${source_root}/python/tests/ep" \
        "${CONTAINER_NAME}:/tmp/mooncake-ep-tests" || return 1
    docker cp "${source_root}/mooncake-ep/benchmarks/ep_benchmark" \
        "${CONTAINER_NAME}:/tmp/mooncake-ep-benchmark" || return 1

    ${docker_exec} "
        set -e
        echo '===== Existing EP unit checks ====='
        python3 -m pytest /tmp/mooncake-ep-tests/test_fp8_cast.py \
            /tmp/mooncake-ep-tests/test_mooncake_elastic_buffer_transport.py -v
        echo '===== Existing EP grid: single-node native P2P ====='
        timeout --signal=TERM --kill-after=30s 900s \
            python3 /tmp/mooncake-ep-tests/test_ep_grid.py \
            --local-p2p --num-ranks 2 --iterations 3
        echo '===== Existing legacy EP correctness and bandwidth ====='
        timeout --signal=TERM --kill-after=30s 900s \
            python3 /tmp/mooncake-ep-tests/test_mooncake_ep.py \
            --pg-backend nccl --num-ranks 2 --require-native
        echo '===== Existing EP benchmark: short routing sweep ====='
        for route in uniform k_hot zipf; do
            timeout --signal=TERM --kill-after=30s 300s \
                python3 -m torch.distributed.run --standalone --nproc-per-node=2 \
                --max-restarts=0 /tmp/mooncake-ep-benchmark/run_ep_benchmark.py \
                --pg-backend nccl --require-native --num-tokens 128 \
                --hidden-size 2048 --num-experts 32 --top-k 4 --dtype bf16 \
                --routing-mode \$route --hot-experts 4 --warmup-iters 3 --iters 10
        done
    "
}

run_checks()
{
    local ep_status=0 model_status=0
    run_ep_tests || ep_status=$?
    run_model_test || model_status=$?
    echo "Native EP checks exit code: $ep_status"
    echo "Elastic EP backend model test exit code: $model_status"
    [ "$ep_status" -eq 0 ] && [ "$model_status" -eq 0 ]
}

run_test()
{
    local log_file="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${test_case_name}.log"
    echo "Running model and native EP checks; saving output to: $log_file"
    run_checks 2>&1 | tee "$log_file"
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
