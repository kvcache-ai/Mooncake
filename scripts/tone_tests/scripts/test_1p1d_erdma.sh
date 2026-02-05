#!/bin/bash

test_case_name="test_1p1d_erdma"
TEST_TYPE="double"
SUPPORT_MODELS=("Qwen/Qwen3-8B" "deepseek-ai/DeepSeek-V2-Lite")

PID_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}/run/pids/${test_case_name}

if [ -z "${ISREMOTE}" ]; then
    if [ -n "${REMOTE_IP}" ] && [ -n "${REMOTE_TEST_DIR}" ] && [[ "$PWD" == "${REMOTE_TEST_DIR}"* ]]; then
        ISREMOTE=1
    else
        ISREMOTE=0
    fi
    export ISREMOTE
fi

BASE_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}
. ${BASE_DIR}/scripts/common.sh

mkdir -p "$PID_DIR"

start_server()
{   
    local host
    local model_name=$1
    local model_name_clean=$2
    local sglang_server_log_path
    if [ "$ISREMOTE" == "0" ]; then 
        host=$LOCAL_IP
        sglang_server_log_path=/test_run/run/logs/$test_case_name/$model_name_clean/sglang_server_local.log
        mode_name=prefill
    else
        host=$REMOTE_IP
        sglang_server_log_path=/test_run/run/logs/$test_case_name/$model_name_clean/sglang_server_remote.log
        mode_name=decode
    fi

    sglang_start_server_cmd="
    ${docker_exec} \
    \"python -m sglang.launch_server --model-path ${model_name} \
    --disaggregation-mode $mode_name --port 30001 --host ${host} --tp-size 2 --base-gpu-id=0 > ${sglang_server_log_path} 2>&1 &\""

    local pid_file="${PID_DIR}/server_${mode_name}.pid"
    local grep_pattern="python -m sglang.launch_server.*${model_name}"

    echo "Starting SGLang Server..."
    if ! launch_and_track_process "$sglang_start_server_cmd" "$grep_pattern" "$pid_file"; then
        return 1
    fi

    exactly_sglang_server_log_path=$(echo "$sglang_server_log_path" | sed "s|/test_run/|$BASE_DIR/|")
    if ! check_server_ready "$exactly_sglang_server_log_path"; then
        return 1
    fi

    return 0
}

run_proxy(){
    local model_name=$1
    local proxy_log_path="/test_run/run/logs/$test_case_name/$model_name/load_balancer.log"

    echo "===== Proxy Run ====="
    lb_cmd="${docker_exec} \"python3 -m sglang_router.launch_router --pd-disaggregation \
    --prefill http://${LOCAL_IP}:30001 --decode http://${REMOTE_IP}:30001 --host 0.0.0.0 \
    --port 8000 > $proxy_log_path 2>&1 &\""

    local pid_file="${PID_DIR}/proxy.pid"
    local grep_pattern="sglang::router"
    echo "Load balancer starting..."
    if ! launch_and_track_process "$lb_cmd" "$grep_pattern" "$pid_file"; then
        return 1
    fi

    exactly_proxy_log_path=$(echo "$proxy_log_path" | sed "s|/test_run/|$BASE_DIR/|")
    if ! check_proxy_ready "$exactly_proxy_log_path"; then
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

    if [ $status_code -eq 200 ]; then
        echo "Test request successful!"
        return 0
    else
        echo "Test request failed with status code $status_code"
        return 1
    fi
}

kill_model_processes() {
    echo "===== Killing model processes ====="
    
    if [ -d "$PID_DIR" ]; then
        echo "Cleaning up by PID files in $PID_DIR..."
        for pid_file in "${PID_DIR}"/*.pid; do
            if [ -f "$pid_file" ]; then
                local service_name=$(basename "$pid_file" .pid)
                kill_process "$pid_file" "$service_name"
            fi
        done
    fi
    
    if [ "$ISREMOTE" == "0" ] && [ -n "$REMOTE_IP" ]; then
        echo "===== Killing model processes (remote: $REMOTE_IP) ====="
        ${SSH_CMD} "$REMOTE_IP" "source $REMOTE_TEST_DIR/run/.shrc; cd \$BASE_DIR/scripts && ./$test_case_name.sh stop_server" 2>/dev/null || true
    fi
    
    echo "Process cleanup completed."
}

run_single_model()
{
    local model_name=$1
    local model_name_clean=$(echo "$model_name" | sed 's/\//__/g')
    local status=0

    setup_log_directory "$TEST_RUN_DIR/logs/$test_case_name/$model_name_clean"
    ${SSH_CMD} $REMOTE_IP "source $REMOTE_TEST_DIR/run/.shrc; cd \$BASE_DIR/scripts && source ./common.sh && setup_log_directory \"\$TEST_RUN_DIR/logs/$test_case_name/$model_name_clean\""
    
    echo "===== Run MODEL NAME: $model_name ====="    
    # Local start server
    if ! start_server $model_name $model_name_clean; then
        echo "ERROR: Failed to start local server for model $model_name"
        status=1
    else
        # Remote start server
        if ! ${SSH_CMD} $REMOTE_IP "source $REMOTE_TEST_DIR/run/.shrc; cd \$BASE_DIR/scripts && ./$test_case_name.sh start_server $model_name $model_name_clean"; then
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
    kill_model_processes
    sleep 2

    return $status
}

run_test()
{
    if [ -z "$REMOTE_IP" ] || [ -z "$LOCAL_IP" ]; then
        echo "Please specify client and server IPs"
        return 1
    fi
    echo "===== Running test case: $test_case_name for all supported models ====="

    local test_failed=false
    for model in "${SUPPORT_MODELS[@]}"; do
        if ! run_single_model "$model"; then
            echo "ERROR: Test case $test_case_name failed for model $model"
            test_failed=true
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
    local all_passed=true

    if [ -n "$REMOTE_IP" ]; then
        echo "Getting remote results from remote server..."
        for model in "${SUPPORT_MODELS[@]}"; do
            local model_name_clean=$(echo "$model" | sed 's/\//__/g')
            
            local remote_log_dir="${REMOTE_TEST_DIR}/${TEST_CASE_RESULT_PATH}/${model_name_clean}"
            local local_log_dir="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${model_name_clean}"
            
            echo "Processing model: $model_name_clean"
            echo "  Remote log dir: $remote_log_dir"
            echo "  Local log dir: $local_log_dir"
            
            echo "  Copying remote sglang_server_remote.log..."
            scp ${REMOTE_IP}:${remote_log_dir}/sglang_server_remote.log \
                ${local_log_dir}/ 2>/dev/null
            
            if [ $? -eq 0 ]; then
                echo "  ✓ Successfully copied sglang_server_remote.log for $model_name_clean"
            else
                echo "  ✗ Failed to copy sglang_server_remote.log for $model_name_clean (file may not exist)"
            fi

            local log_file="${local_log_dir}/curl_response.log"
            
            echo "  Checking results for model: $model"
            
            if [ -f "$log_file" ]; then
                curl_response=$(cat "$log_file")
                
                if echo "$curl_response" | grep -q "\"object\":\"error\""; then
                    error_message=$(echo "$curl_response" | grep -o '"message":"[^"]*"' | sed 's/"message":"//' | sed 's/"$//')
                    echo "  ERROR: $error_message"
                    echo "  $model: Fail"
                    all_passed=false
                else
                    echo "  $model:Pass"
                fi
            else
                echo "  ERROR: Curl response log not found at $log_file"
                echo "  $model:Fail"
                all_passed=false
            fi
            
            echo ""
        done
        
        echo "Remote log collection completed"
    else
        echo "No client specified, skipping result parsing"
        all_passed=false
    fi

    if [ "$all_passed" = true ]; then
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
        exit 0
        ;;
    *)
        if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
            run_test && parse || parse
        fi
        ;;
esac
