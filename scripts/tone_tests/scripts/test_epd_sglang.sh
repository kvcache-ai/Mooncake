#!/bin/bash

test_case_name="test_epd_sglang"
TEST_TYPE="double"
SUPPORT_MODELS=("Qwen/Qwen2.5-VL-7B-Instruct")

BASE_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}
. ${BASE_DIR}/scripts/common.sh

detect_remote_mode

PID_DIR=${BASE_DIR}/run/pids/${test_case_name}
mkdir -p "$PID_DIR"

start_epd_component()
{
    local component_type=$1  # encoder，prefill, decode
    local model_name=$2
    local model_name_clean=$3
    
    local host
    local port
    local log_path
    local pid_suffix
    local extra_args
    local ready_pattern
    
    case $component_type in
        "encoder")
            host=$LOCAL_IP
            port=30000
            log_path="/test_run/run/logs/$test_case_name/$model_name_clean/encoder.log"
            pid_suffix="encoder"
            extra_args="--encoder-only --encoder-transfer-backend mooncake --tp-size 2"
            ready_pattern="Application startup complete."
            echo "Starting Encoder..."
            ;;
        "prefill")
            host=$LOCAL_IP
            port=30002
            log_path="/test_run/run/logs/$test_case_name/$model_name_clean/sglang_server_prefill.log"
            pid_suffix="prefill"
            extra_args="--disaggregation-mode prefill --language-only --encoder-urls http://${LOCAL_IP}:30000 --tp-size 2 --encoder-transfer-backend mooncake --base-gpu-id=4"
            ready_pattern="The server is fired up and ready to roll!"
            echo "Starting Prefill Server..."
            ;;
        "decode")
            host=$REMOTE_IP
            port=30003
            log_path="/test_run/run/logs/$test_case_name/$model_name_clean/sglang_server_decode.log"
            pid_suffix="decode"
            extra_args="--disaggregation-mode decode --tp-size 2 --base-gpu-id=6"
            ready_pattern="The server is fired up and ready to roll!"
            echo "Starting Decode Server..."
            ;;
        *)
            echo "ERROR: Unknown component type: $component_type" >&2
            return 1
            ;;
    esac
    
    if ! launch_sglang_server "$model_name" "$host" "$port" "$log_path" "$pid_suffix" "$extra_args" "$ready_pattern"; then
        echo "ERROR: Failed to start $component_type"
        return 1
    fi
    
    return 0
}

run_proxy()
{
    local model_name=$1
    local proxy_log_path="/test_run/run/logs/$test_case_name/$model_name/load_balancer.log"

    launch_sglang_router "http://${LOCAL_IP}:30002" "http://${REMOTE_IP}:30003" "0.0.0.0" "8000" "$proxy_log_path"
}

run_request()
{
    local model_name=$1
    local image_file_path=${2:-"${BASE_DIR}/assets/test_cat.jpg"}
    echo "===== Sending Test Request ====="
    
    if [ ! -f "$image_file_path" ]; then
        echo "ERROR: Image file not found: $image_file_path"
        return 1
    fi
    local temp_json_file=$(mktemp)

    echo "Generating JSON request file..."
    python3 << EOF
import json
import base64

with open('$image_file_path', 'rb') as f:
    img_data = base64.b64encode(f.read()).decode('utf-8')

request = {
    'model': 'default',
    'messages': [
        {
            'role': 'user',
            'content': [
                {'type': 'text', 'text': 'What animal is in the picture?'},
                {
                    'type': 'image_url',
                    'image_url': {
                        'url': f'data:image/jpeg;base64,{img_data}'
                    }
                }
            ]
        }
    ],
    'temperature': 0,
    'max_tokens': 64
}

with open('$temp_json_file', 'w') as f:
    json.dump(request, f)

print("✓ JSON file generated successfully")
EOF
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to generate JSON request file"
        rm -f "$temp_json_file"
        return 1
    fi
    
    curl_response=$(curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:8000/v1/chat/completions \
      -H "Content-Type: application/json" \
      -d @$temp_json_file \
      --max-time 60)
    
    rm -f "$temp_json_file"
    
    response_body=$(echo "$curl_response" | head -n -1)
    status_code=$(echo "$curl_response" | tail -n 1)
    echo "Curl Response:"
    echo "$response_body"
    echo "Status Code: $status_code"

    echo "$response_body" > $BASE_DIR/run/logs/$test_case_name/$model_name/curl_response.log

    if validate_api_response "$response_body" "$status_code" ".choices[0].message.content" "cat|kitten|feline"; then
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


start_local_components()
{
    local model_name=$1
    local model_name_clean=$2
    local components=("encoder" "prefill")
    
    for component in "${components[@]}"; do
        start_epd_component "$component" "$model_name" "$model_name_clean" || return 1
    done
    return 0
}

start_remote_decode()
{
    local model_name=$1
    local model_name_clean=$2
    
    ${SSH_CMD} $REMOTE_IP "source $REMOTE_TEST_DIR/run/.shrc; cd \$BASE_DIR/scripts && ./$test_case_name.sh start_component decode $model_name $model_name_clean"
}

run_single_model()
{
    local model_name=$1
    local model_name_clean=$(sanitize_model_name "$model_name")

    setup_log_directory_dual "$test_case_name" "$model_name_clean"
    
    echo "===== Run MODEL NAME: $model_name ====="
    
    # Encoders → Local Prefill → Remote Decode → Router → Test
    if start_local_components "$model_name" "$model_name_clean" && \
       start_remote_decode "$model_name" "$model_name_clean" && \
       run_proxy "$model_name_clean"; then
        sleep 5
        run_request "$model_name_clean"
        local status=$?
    else
        local status=1
    fi

    echo "===== Cleaning up model processes for $model_name ====="
    kill_model_processes
    sleep 2

    return $status
}

run_test()
{
    if [ -z "$REMOTE_IP" ] || [ -z "$LOCAL_IP" ]; then
        echo "ERROR: Please specify LOCAL_IP and REMOTE_IP"
        return 1
    fi
    
    echo "===== Running EPD test case: $test_case_name for all supported models ====="

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
    
    if collect_and_validate_model_results "SUPPORT_MODELS" "sglang_server_decode.log" "$test_case_name" "cat|kitten|feline"; then
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
    "start_component")
        shift
        start_epd_component "$@"
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
