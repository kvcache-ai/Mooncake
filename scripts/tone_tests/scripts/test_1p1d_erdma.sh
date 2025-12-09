#!/bin/bash

set -e

test_case_name="test_1p1d_erdma"

LOCAL_IP=
REMOTE_IP=

readonly SSH_CMD="ssh -o StrictHostKeyChecking=no"
SUPPORT_MODELS=("Qwen/Qwen3-8B" "deepseek-ai/DeepSeek-V2-Lite")
CONTAINER_NAME=${CONTAINER_NAME:-"mooncake-ci-test"}
MODEL_CACHE=${MODEL_CACHE:-"/root/.cache"}
REGISTRY_ADDR=${REGISTRY_ADDR:-"lmsysorg/sglang:latest"}
USE_HUGGINGFACE_MIRROR=${USE_HUGGINGFACE_MIRROR:-true}
HUGGINGFACE_MIRROR=${HUGGINGFACE_MIRROR:-"https://hf-mirror.com"}
USE_MODELSCOPE=${USE_MODELSCOPE:-false}
REMOTE_TEST_DIR=${REMOTE_TEST_DIR:-"/tmp/Mooncake_tone/mooncake_ci_test"}
ARTIFACT_ID=
GIT_REPO=

TONE_TESTS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)

. $TONE_TESTS_DIR/scripts/common.sh

prepare(){
    # get client server ip
    echo "===== Getting Local and Remote IP ====="
    echo "LOCAL: $LOCAL_IP, REMOTE: $REMOTE_IP" 

    setup_test_result_directory $TONE_TESTS_DIR/$TEST_CASE_RESULT_PATH

    cat > $TONE_TESTS_DIR/$TEST_CASE_RESULT_PATH/.shrc << EOF
# Mooncake CI Test Environment Variables - ${test_case_name}
export CONTAINER_NAME=${CONTAINER_NAME}
export MODEL_CACHE=${MODEL_CACHE}
export ARTIFACT_ID=${ARTIFACT_ID}
export REGISTRY_ADDR=${REGISTRY_ADDR} 
export ISREMOTE=0
export LOCAL_IP=${LOCAL_IP}
export REMOTE_IP=${REMOTE_IP}
export USE_HUGGINGFACE_MIRROR=${USE_HUGGINGFACE_MIRROR}
export HUGGINGFACE_MIRROR=${HUGGINGFACE_MIRROR}
export USE_MODELSCOPE=${USE_MODELSCOPE}
export BASE_DIR=${TONE_TESTS_DIR}
export GIT_REPO=${GIT_REPO}
export TEST_CASE_RESULT_DIR=${TONE_TESTS_DIR}/${TEST_CASE_RESULT_PATH}
EOF

    echo "===== Starting Local Installation ====="
    echo "Get mooncake whl on local machine..."
    echo "Verifying local environment variables:"
    cat $TONE_TESTS_DIR/$TEST_CASE_RESULT_PATH/.shrc
    source $TONE_TESTS_DIR/$TEST_CASE_RESULT_PATH/.shrc && get_whl $TEST_CASE_RESULT_DIR
    if [ $? -ne 0 ]; then
        echo "Failed to get mooncake whl on local machine"
        exit 1
    fi
    echo "Local installation completed successfully"

    echo "===== Starting Remote Installation ====="
    echo "Local: $LOCAL_IP, Remote: $REMOTE_IP"
    if [ -n "$REMOTE_IP" ]; then
        echo "Copying test script to remote server $REMOTE_IP..."
        ${SSH_CMD} $REMOTE_IP "rm -rf ${REMOTE_TEST_DIR} && mkdir -p ${REMOTE_TEST_DIR}"

        rsync -av ${TONE_TESTS_DIR}/ $REMOTE_IP:${REMOTE_TEST_DIR}/
        if [ $? -ne 0 ]; then
            echo "Failed to copy files to remote server"
            exit 1
        fi
        
        ${SSH_CMD} $REMOTE_IP "sed -i 's|^export ISREMOTE=0|export ISREMOTE=1|' ${REMOTE_TEST_DIR}/${TEST_CASE_RESULT_PATH}/.shrc && \
                              sed -i 's|^export BASE_DIR=.*$|export BASE_DIR=${REMOTE_TEST_DIR}|' ${REMOTE_TEST_DIR}/${TEST_CASE_RESULT_PATH}/.shrc && \
                              sed -i 's|^export TEST_CASE_RESULT_DIR=.*$|export TEST_CASE_RESULT_DIR=${REMOTE_TEST_DIR}/${TEST_CASE_RESULT_PATH}|' ${REMOTE_TEST_DIR}/${TEST_CASE_RESULT_PATH}/.shrc"
        echo "Verifying remote environment variables:"
        ${SSH_CMD} $REMOTE_IP "cat ${REMOTE_TEST_DIR}/${TEST_CASE_RESULT_PATH}/.shrc"

        echo "Remote installation completed successfully"
    else
        echo "No client specified, skipping remote installation"
    fi

    echo "===== Installation Completed ====="
}

setup()
{   
    local model_name=$1
    echo "===== Setting up Mooncake CI test ====="
    echo "mkdir log path..."
    setup_log_directory $TEST_CASE_RESULT_PATH $model_name

    echo "Get the latest sglang image..."
    if ! get_image; then
        echo "ERROR: Failed to get the required image"
        exit 1
    fi

    # qit old container
    echo "Quit old container..."
    if ! clean_container ${CONTAINER_NAME}; then
        echo "ERROR: Failed to clean up container"
        exit 1
    fi

    extra_args=""
    extra_args="$extra_args --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/uverbs1 --device=/dev/infiniband/rdma_cm "
    if ${USE_HUGGINGFACE_MIRROR}; then
       extra_args="$extra_args -e HF_ENDPOINT=${HUGGINGFACE_MIRROR}"
       extra_args="$extra_args -e HF_HUB_ENABLE_HF_TRANSFER=1"
    fi
    if ${USE_MODELSCOPE}; then
       extra_args="$extra_args -e SGLANG_USE_MODELSCOPE=true"
    fi
    echo "extra_args: $extra_args"

    echo "Launching docker container..."
    if ! docker_launch "$extra_args"; then
        echo "ERROR: Failed to launch docker container"
        exit 1
    fi
}

start_server()
{   
    local host
    local model_name=$1
    local model_name_clean=$2
    local sglang_server_log_path
    if [ "$ISREMOTE" == "0" ]; then 
        host=$LOCAL_IP
        sglang_server_log_path=/test_run/run/$test_case_name/logs/$model_name_clean/sglang_server_local.log
        mode_name=prefill
    else
        host=$REMOTE_IP
        sglang_server_log_path=/test_run/run/$test_case_name/logs/$model_name_clean/sglang_server_remote.log
        mode_name=decode
    fi

    sglang_start_server_cmd_remote="
    ${docker_exec} \
    \"python -m sglang.launch_server --model-path ${model_name} \
    --disaggregation-mode $mode_name --port 30001 --host ${host} --tp-size 2 --base-gpu-id=6 > ${sglang_server_log_path} 2>&1 &\""
    echo "Start sglang server command:"
    echo "$sglang_start_server_cmd_remote"
    eval "$sglang_start_server_cmd_remote"

    exactly_sglang_server_log_path=$(echo "$sglang_server_log_path" | sed "s|/test_run/|$BASE_DIR/|")
    if ! check_server_ready "$exactly_sglang_server_log_path"; then
        return 1
    fi
}

run_proxy(){
    local model_name=$1
    local proxy_log_path="/test_run/run/$test_case_name/logs/$model_name/load_balancer.log"

    echo "===== Proxy Run ====="
    lb_cmd="${docker_exec} \"python3 -m sglang_router.launch_router --pd-disaggregation \
    --prefill http://${LOCAL_IP}:30001 --decode http://${REMOTE_IP}:30001 --host 0.0.0.0 \
    --port 8000 > $proxy_log_path 2>&1 &\""
    echo "Load balancer command:"
    echo "$lb_cmd"
    eval "$lb_cmd"

    exactly_proxy_log_path=$(echo "$proxy_log_path" | sed "s|/test_run/|$BASE_DIR/|")
    if ! check_proxy_ready "$exactly_proxy_log_path"; then
        return 1
    fi
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

    echo "$response_body" > $BASE_DIR/run/$test_case_name/logs/$model_name/curl_response.log

    if [ $status_code -eq 200 ]; then
        echo "Test request successful!"
    else
        echo "Test request failed with status code $status_code"
    fi
}

run_single_model()
{
    local model_name=$1
    local model_name_clean=$(echo "$model_name" | sed 's/\//__/g')
    
    echo "===== Run MODEL NAME: $model_name ====="
    # launch docker
    echo "Launching docker container on local machine..."
    setup $model_name_clean
    echo -e "\n\nLaunching docker container on remote machine..."
    ${SSH_CMD} $REMOTE_IP "source $REMOTE_TEST_DIR/$TEST_CASE_RESULT_PATH/.shrc; cd \$BASE_DIR/scripts && bash ./$test_case_name.sh setup $model_name_clean"
    
    # Local start server
    if ! start_server $model_name $model_name_clean; then
        echo "ERROR: Failed to start local server for model $model_name"
        exit 1
    fi
    # Remote start server
    if ! ${SSH_CMD} $REMOTE_IP "source $REMOTE_TEST_DIR/$TEST_CASE_RESULT_PATH/.shrc; cd \$BASE_DIR/scripts && ./$test_case_name.sh start_server $model_name $model_name_clean"; then
        echo "ERROR: Failed to start remote server for model $model_name"
        exit 1
    fi

    # Local run proxy
    if ! run_proxy $model_name_clean; then
        echo "ERROR: Failed to start local proxy for model $model_name"
        exit 1
    fi
    sleep 5
    # Local Sending Test Request
    run_request $model_name_clean
}

run_test()
{
    source $TONE_TESTS_DIR/$TEST_CASE_RESULT_PATH/.shrc
    if [ -z "$REMOTE_IP" ] || [ -z "$LOCAL_IP" ]; then
        echo "Please specify client and server IPs"
        exit 1
    fi
    echo "===== Running test case: $test for all supported models ====="

    for model in "${SUPPORT_MODELS[@]}"; do
        run_single_model "$model" 
    done
}

cleanup()
{
    echo "===== Cleaning up ====="
    # Stop the Docker container
    stop_container "${CONTAINER_NAME}"
    
    if [ -n "$REMOTE_IP" ]; then
        stop_container "${CONTAINER_NAME}" "$REMOTE_IP"
    fi
}

parse()
{
    echo "===== Parsing test results ====="
    source $TONE_TESTS_DIR/$TEST_CASE_RESULT_PATH/.shrc

    local all_passed=true
    local result_json="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/test_results.json"

    if [ -n "$REMOTE_IP" ]; then
        echo "Getting remote results from remote server..."
        for model in "${SUPPORT_MODELS[@]}"; do
            local model_name_clean=$(echo "$model" | sed 's/\//__/g')
            
            local remote_log_dir="${REMOTE_TEST_DIR}/${TEST_CASE_RESULT_PATH}/logs/${model_name_clean}"
            local local_log_dir="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/logs/${model_name_clean}"
            
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
    fi

    if [ "$all_passed" = true ]; then
        echo "{\"test_case\": \"$test_case_name\", \"status\": \"Pass\", \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" > "$result_json"
        echo "$test_case_name: Pass"
    else
        echo "{\"test_case\": \"$test_case_name\", \"status\": \"Fail\", \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" > "$result_json"
        echo "$test_case_name: Fail"
    fi
    echo "Test results saved to: $result_json"
}


main()
{
    local exit_code=0
    
    echo "===== Step 1: Prepare ====="
    if ! prepare; then
        echo "ERROR: Prepare failed"
        exit_code=1
    fi
    
    if [ $exit_code -eq 0 ]; then
        echo "===== Step 2: Run Test ====="
        if ! run_test; then
            echo "ERROR: Run test failed"
            exit_code=1
        fi
    fi

    echo "===== Step 3: Parse Results ====="
    parse
    
    echo "===== Step 4: Cleanup ====="
    cleanup
    
    if [ $exit_code -ne 0 ]; then
        echo "===== Test completed with errors ====="
        exit 1
    else
        echo "===== Test completed successfully ====="
    fi
}

# 处理命令行参数
case "$1" in
    "prepare")
        shift
        prepare "$@"
        exit 0
        ;;
    "setup")
        shift
        setup "$@"
        exit 0
        ;;
    "start_server")
        shift
        start_server "$@"
        exit 0
        ;;
    "run_proxy")
        shift
        run_proxy "$@"
        exit 0
        ;;
    "run_request")
        shift
        run_request "$@"
        exit 0
        ;;
    "main")
        shift
        main "$@"
        exit 0
        ;;
    "")
        main
        exit 0
        ;;
    *)
        echo "Usage: $0 {prepare|setup|start_server|run_proxy|run_request|parse|cleanup|main}"
        echo "  prepare      - Prepare test environment and sync code to remote"
        echo "  setup        - Setup docker container and install dependencies"
        echo "  start_server - Start the SGLang server (prefill/decode mode)"
        echo "  run_proxy    - Start the load balancer/proxy"
        echo "  run_request  - Send test request to the server"
        echo "  parse        - Collect remote logs to local machine"
        echo "  cleanup      - Stop and cleanup docker containers (local and remote)"
        echo "  main         - Run complete test workflow (prepare -> run_test -> parse -> cleanup)"
        exit 1
        ;;
esac