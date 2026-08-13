#!/bin/bash

test_case_name="test_1p1d_erdma"
TEST_TYPE="double"
SUPPORT_MODELS=("Qwen/Qwen3-8B" "deepseek-ai/DeepSeek-V2-Lite")

PID_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}/run/pids/${test_case_name}
BASE_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}
. ${BASE_DIR}/scripts/common.sh

detect_remote_mode
mkdir -p "$PID_DIR"

start_server()
{   
    local host
    local model_name=$1
    local model_name_clean=$2
    local sglang_server_log_path
    local mode_name
    if [ "$ISREMOTE" == "0" ]; then 
        host=$LOCAL_IP
        sglang_server_log_path=/test_run/run/logs/$test_case_name/$model_name_clean/sglang_server_local.log
        mode_name=prefill
    else
        host=$REMOTE_IP
        sglang_server_log_path=/test_run/run/logs/$test_case_name/$model_name_clean/sglang_server_remote.log
        mode_name=decode
    fi

    local extra_args="--disaggregation-mode $mode_name --tp-size 2 --base-gpu-id=${MOONCAKE_SGLANG_BASE_GPU_ID:-6}"
    if ! launch_sglang_server "$model_name" "$host" "30001" "$sglang_server_log_path" "$mode_name" "$extra_args"; then
        return 1
    fi

    return 0
}

run_proxy(){
    local model_name=$1
    local proxy_log_path="/test_run/run/logs/$test_case_name/$model_name/load_balancer.log"

    if ! launch_sglang_router "http://${LOCAL_IP}:30001" "http://${REMOTE_IP}:30001" "0.0.0.0" "8000" "$proxy_log_path"; then
        echo "ERROR: Failed to start SGLang Router"
        return 1
    fi
    
    return 0
}

run_request(){
    local model_name=$1
    echo "===== Sending Test Request ====="
    curl_response=$(curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:8000/generate -H "Content-Type: application/json" -d '{
      "text": "Let me tell you a short story ",
      "sampling_params": {
        "temperature": 0
      }
    }' --max-time 30)
    response_body=$(echo "$curl_response" | head -n -1)
    status_code=$(echo "$curl_response" | tail -n 1)
    echo "Curl Response:"
    echo "$response_body"
    echo "Status Code: $status_code"

    echo "$response_body" > $BASE_DIR/run/logs/$test_case_name/$model_name/curl_response.log

    if validate_api_response "$response_body" "$status_code"; then
        echo "Test request successful!"
        return 0
    else
        echo "Test request failed"
        return 1
    fi
}

kill_model_processes() {
    cleanup_model_processes "$PID_DIR" "$test_case_name"
}

run_single_model()
{
    local model_name=$1
    local model_name_clean=$(sanitize_model_name "$model_name")
    local status=0

    setup_log_directory_dual "$test_case_name" "$model_name_clean"   

    echo "===== Run MODEL NAME: $model_name ====="    
    # Local start server
    if ! start_server $model_name $model_name_clean; then
        echo "ERROR: Failed to start local server for model $model_name"
        status=1
    else
        # Remote start server
        if ! ${SSH_CMD} "${REMOTE_SSH_TARGET:-$REMOTE_IP}" "source $REMOTE_TEST_DIR/run/.shrc; cd \$BASE_DIR/scripts && ./$test_case_name.sh start_server $model_name $model_name_clean"; then
            echo "ERROR: Failed to start remote server for model $model_name"
            status=1
        else
            # Local run proxy
            if ! run_proxy $model_name_clean; then
                echo "ERROR: Failed to start local proxy for model $model_name"
                status=1
            else
                sleep 5
                # Local Sending Test Request
                if ! run_request $model_name_clean; then
                    echo "ERROR: Failed to send test request for model $model_name"
                    status=1
                fi
            fi
        fi
    fi

    echo "===== Cleaning up model processes for $model_name ====="
    if ! kill_model_processes; then
        echo "ERROR: Model process cleanup failed for $model_name" >&2
        status=1
    fi

    return $status
}

run_test()
{
    if [ -z "$REMOTE_IP" ] || [ -z "$LOCAL_IP" ]; then
        echo "Please specify client and server IPs"
        return 1
    fi
    echo "===== Running test case: $test_case_name for all supported models ====="

    if [ "$#" -gt 0 ]; then
        SUPPORT_MODELS=("$@")
    fi

    local test_failed=false
    local model_index=0
    local model_count=${#SUPPORT_MODELS[@]}
    for model in "${SUPPORT_MODELS[@]}"; do
        model_index=$((model_index + 1))
        if ! run_single_model "$model"; then
            echo "ERROR: Test case $test_case_name failed for model $model"
            test_failed=true
        fi
        if [ "$model_index" -lt "$model_count" ] && ! drain_gpu_between_tests; then
            echo "ERROR: Failed to isolate the next model from $model" >&2
            return 1
        fi
    done

    if [ "$test_failed" = true ]; then
        return 1
    fi
    
    return 0
}

parse()
{
    echo "===== Parsing test results ====="
    
    if collect_and_validate_model_results "SUPPORT_MODELS" "sglang_server_remote.log" "$test_case_name"; then
        save_test_result "$test_case_name" "Pass" "${BASE_DIR}/${TEST_CASE_RESULT_PATH}"
        echo "✓ Test PASSED"
        return 0
    else
        save_test_result "$test_case_name" "Fail" "${BASE_DIR}/${TEST_CASE_RESULT_PATH}"
        echo "✗ Test FAILED"
        return 1
    fi
}

case "$1" in
    "start_server")
        shift
        start_server "$@"
        exit $?
        ;;
    "stop_server")
        kill_model_processes
        exit $?
        ;;
    *)
        if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
            run_test && parse || parse
        fi
        ;;
esac
