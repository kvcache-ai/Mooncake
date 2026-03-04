#!/bin/bash

test_case_name="test_vllm_1p1d_erdma"
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
    local vllm_server_log_path
    local port
    local kv_role

    if [ "$ISREMOTE" == "0" ]; then 
        host=$LOCAL_IP
        vllm_server_log_path=/test_run/run/logs/$test_case_name/$model_name_clean/vllm_server_local.log
        port=8020
        kv_role="kv_consumer"
    else
        host=$REMOTE_IP
        vllm_server_log_path=/test_run/run/logs/$test_case_name/$model_name_clean/vllm_server_remote.log
        port=8010
        kv_role="kv_producer"
    fi

    local kv_config_json="{\\\"kv_connector\\\":\\\"MooncakeConnector\\\",\\\"kv_role\\\":\\\"$kv_role\\\"}"

    local extra_args="--tensor-parallel-size 2 --max-model-len 32768 --no-enable-prefix-caching --kv-transfer-config '$kv_config_json'"
    
    local env_vars="CUDA_VISIBLE_DEVICES=0,1"
    
    if ! launch_vllm_server "$model_name" "$host" "$port" "$vllm_server_log_path" "$kv_role" "$extra_args" "$env_vars"; then
        return 1
    fi

    return 0
}

run_proxy()
{
    local model_name=$1
    local proxy_log_path="/test_run/run/logs/$test_case_name/$model_name/proxy.log"

    echo "===== Proxy Run ====="
    
    # Get vLLM version and decide which proxy to use
    local vllm_version=$(${docker_exec} "python3 -c 'import vllm; print(vllm.__version__)' 2>/dev/null" || echo "0.15.0")
    echo "Detected vLLM version: $vllm_version"
    
    # Determine proxy script and ready check strategy
    local proxy_script
    local ready_pattern
    local use_health_check=0
    if python3 -c "from packaging import version; import sys; sys.exit(0 if version.parse('$vllm_version') >= version.parse('0.16.0') else 1)" 2>/dev/null; then
        echo "Using Mooncake Connector Proxy (vLLM >= 0.16.0)"
        proxy_script="python3 -u /vllm-workspace/examples/online_serving/disaggregated_serving/mooncake_connector/mooncake_connector_proxy.py --prefill http://$REMOTE_IP:8010 --decode http://$LOCAL_IP:8020 --host 0.0.0.0 --port 8000"
        ready_pattern="All prefiller instances are ready."
    else
        echo "Using NIXL Proxy (vLLM < 0.16.0)"
        proxy_script="python3 -u /test_run/python/toy_proxy_server.py --host 0.0.0.0 --port 8000 --prefiller-host $REMOTE_IP --prefiller-port 8010 --decoder-host $LOCAL_IP --decoder-port 8020"
        ready_pattern="Application startup complete."
        use_health_check=1
    fi

    # Launch proxy
    local lb_cmd="${docker_exec} \"$proxy_script > $proxy_log_path 2>&1 &\""
    local pid_file="${PID_DIR}/proxy.pid"
    # Extract Python script path (second word) and get filename
    local grep_pattern=$(echo "$proxy_script" | grep -oE '[^/]+\.py')
    
    echo "Starting proxy server..."
    if ! launch_and_track_process "$lb_cmd" "$grep_pattern" "$pid_file"; then
        return 1
    fi

    # Check proxy ready status
    exactly_proxy_log_path=$(convert_container_path_to_host "$proxy_log_path")
    if ! check_vllm_proxy_ready "$exactly_proxy_log_path" "$ready_pattern"; then
        return 1
    fi

    # Additional health check for toy_proxy_server
    if [ "$use_health_check" -eq 1 ]; then
        if ! wait_for_server_ready "$LOCAL_IP" "8000" "/health"; then
            return 1
        fi
    fi

    return 0
}

run_request()
{
    local model_name=$1
    local model_clean_name=$2
    echo "===== Sending Test Request ====="
    curl_response=$(curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:8000/v1/chat/completions -H "Content-Type: application/json" -d "{
    \"model\": \"$model_name\",
    \"messages\": [
      {\"role\": \"user\", \"content\": \"Where is the capital of France?\"}
    ],
    \"stream\": false,
    \"max_tokens\": 200,
    \"temperature\": 0
    }" --max-time 120)
    response_body=$(echo "$curl_response" | head -n -1)
    status_code=$(echo "$curl_response" | tail -n 1)
    echo "Curl Response:"
    echo "$response_body"
    echo "Status Code: $status_code"

    echo "$response_body" > $BASE_DIR/run/logs/$test_case_name/$model_clean_name/curl_response.log

    if validate_api_response "$response_body" "$status_code" ".choices[0].message.content" "paris"; then
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
                if ! run_request $model_name $model_name_clean; then
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

    if collect_and_validate_model_results "SUPPORT_MODELS" "vllm_server_remote.log" "$test_case_name" "paris"; then
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