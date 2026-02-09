#!/bin/bash

TEST_CASE_RESULT_PATH="run/logs/$test_case_name"
docker_exec="docker exec ${CONTAINER_NAME} bash -c"

setup_directory(){
    local dir_path=$1
    
    if [ -z "$dir_path" ]; then
        echo "ERROR: Directory path not provided" >&2
        return 1
    fi
    
    if [ -d "$dir_path" ]; then
        echo "Directory already exists: $dir_path"
        return 0
    fi

    if mkdir -p "$dir_path"; then
        echo "Directory created successfully: $dir_path"
        return 0
    else
        echo "ERROR: Failed to create directory: $dir_path" >&2
        return 1
    fi
}

setup_log_directory(){
    local log_dir="$1"
    
    if [ -d "$log_dir" ]; then
        echo "Removing existing log directory: $log_dir"
        rm -rf "$log_dir"
    fi
    mkdir -p "$log_dir"
    echo "Log directory set up at: $log_dir"
}

docker_launch(){
    local registry_addr=$1
    local extra_args=$2

    docker_run_cmd="docker run  --init --name ${CONTAINER_NAME} \
    -d --ipc=host --cap-add=SYS_PTRACE --network=host --gpus all \
    --ulimit memlock=-1 --ulimit stack=67108864 --shm-size=128g \
    -v ${MODEL_CACHE}:/root/.cache $extra_args --privileged \
    -v $BASE_DIR:/test_run \
    -v /root/test.jsonl:/tmp/test.jsonl \
    --entrypoint bash \
    ${registry_addr} -c \"hostname;sleep 360000\""

    echo "Executing Docker run command:"
    echo "$docker_run_cmd"
    if ! eval "$docker_run_cmd"; then
        echo "ERROR: Failed to launch docker container" >&2
        return 1
    fi

    pip_cmd=""

    # detect ubuntu codename and set appropriate ERDMA repository
    ubuntu_codename=$(${docker_exec} "cat /etc/os-release | grep UBUNTU_CODENAME | cut -d'=' -f2" 2>/dev/null | tr -d '"' || echo "")

    if [ "$ubuntu_codename" = "noble" ]; then
        # Ubuntu 24.04
        erdma_repo_codename="noble"
        echo "Detected Ubuntu 24.04 (noble), using noble ERDMA repository"
    elif [ "$ubuntu_codename" = "jammy" ]; then
        # Ubuntu 22.04
        erdma_repo_codename="jammy"
        echo "Detected Ubuntu 22.04 (jammy), using jammy ERDMA repository"
    else
        # Default to jammy if codename detection fails
        erdma_repo_codename="jammy"
        echo "Could not detect Ubuntu codename, defaulting to jammy ERDMA repository"
    fi

    erdma_driver_cmd='curl -fsSL http://mirrors.cloud.aliyuncs.com/erdma/GPGKEY | gpg --dearmour -o /etc/apt/trusted.gpg.d/erdma.gpg && \
    echo "deb [ ] http://mirrors.cloud.aliyuncs.com/erdma/apt/ubuntu '"${erdma_repo_codename}"'/erdma main" | tee /etc/apt/sources.list.d/erdma.list && \
    apt update && \
    apt install libibverbs1 ibverbs-providers ibverbs-utils librdmacm1 -y'
    mooncake_whl_file=$(ls $TEST_RUN_DIR/whls/*.whl 2>/dev/null | xargs -n 1 basename | head -n 1)
    if [ -z "$mooncake_whl_file" ]; then
        echo "No wheel file found in $TEST_RUN_DIR/whls/"
        return 1
    fi
    local relative_path=${TEST_RUN_DIR#$BASE_DIR}
    local cleaned_path=${relative_path#/}
    pip_cmd=$(append_str "${pip_cmd}" "pip install /test_run/$cleaned_path/whls/$mooncake_whl_file")

    echo "Installing ERDMA drivers"
    echo "Executing ERDMA driver installation command:"
    echo "${erdma_driver_cmd}"
    if ! ${docker_exec} "${erdma_driver_cmd}"; then
        echo "ERROR: Failed to install ERDMA drivers" >&2
        return 1
    fi

    echo "Checking RDMA devices"
    echo "Executing ibv_devinfo check command:"
    echo "ibv_devinfo"
    if ! ${docker_exec} "ibv_devinfo" >/dev/null 2>&1; then
        echo "ibv_devinfo execution failed" >&2
        return 1
    else
        echo "ibv_devinfo execution successful"
    fi

    # install mooncake and upgrade sglang
    echo "=== Installing Mooncake and dependencies ==="
    echo "Executing pip installation commands:"
    IFS=';' read -ra COMMANDS <<< "$pip_cmd"
    for cmd in "${COMMANDS[@]}"; do
        echo "Command: $cmd"
    done
    if ! ${docker_exec} "${pip_cmd}"; then
        echo "ERROR: Failed to install Mooncake dependencies" >&2
        return 1
    fi
    
    return 0
}

clean_container(){
    local container_name=$1
    if [ -z "$container_name" ]; then
        echo "No container name provided"
        return 1
    fi
    
    # check if container exists
    if docker ps -a --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo "Stopping and removing existing container: ${container_name}"
        # stop container
        docker stop ${container_name} >/dev/null 2>&1
        # remove container
        docker rm ${container_name} >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            echo "Successfully removed container: ${container_name}"
        else
            echo "Failed to remove container: ${container_name}"
            return 1
        fi
    else
        echo "No existing container named: ${container_name}"
    fi

    return 0
}

append_str() {
    local original_str="$1"
    local append_value="$2"
    
    if [ -z "$original_str" ]; then
        echo "$append_value"
    else
        echo "${original_str}; ${append_value}"
    fi
}

check_server_ready() { 
    local server_log_path=$1
    local max_attempts=${2:-120}

    if [ -z "$server_log_path" ]; then
        echo "ERROR: Server log path not provided" >&2
        return 1
    fi

    echo "Waiting for server to be ready (checking: $server_log_path)..."
    for i in $(seq 1 $max_attempts); do
        if [ -f "$server_log_path" ]; then
            if grep -q 'The server is fired up and ready to roll!' "$server_log_path" 2>/dev/null; then
                echo "Server is ready!"
                return 0
            fi
            echo "Waiting... ($i/$max_attempts)"
            sleep 2
        fi
    done
    
    echo "ERROR: Server failed to start within timeout"
    return 1
}

# 6. 通用的服务就绪检查函数（支持自定义日志模式）
check_server_ready_with_pattern() {
    local server_log_path=$1
    local ready_pattern=$2
    local max_attempts=${3:-120}

    if [ -z "$server_log_path" ] || [ -z "$ready_pattern" ]; then
        echo "ERROR: Server log path or ready pattern not provided" >&2
        return 1
    fi

    echo "Waiting for server to be ready (pattern: '$ready_pattern')..."
    for i in $(seq 1 $max_attempts); do
        if [ -f "$server_log_path" ]; then
            if grep -q "$ready_pattern" "$server_log_path" 2>/dev/null; then
                echo "Server is ready!"
                return 0
            fi
            echo "Waiting... ($i/$max_attempts)"
            sleep 2
        fi
    done
    
    echo "ERROR: Server did not become ready in time" >&2
    return 1
}

get_whl(){
    whls_path="$1/whls"
    echo "whls_path: $whls_path and mkdir..."
    mkdir -p "$whls_path"

    echo "get whl file from github action"
    rm -f "$whls_path/mooncake.zip"
    rm -f "$whls_path/*.whl"

    local max_retries=5
    local base_delay=5 # seconds
    local success=false

    if [ -z "${GIT_REPO}" ] || [ -z "${ARTIFACT_ID}" ]; then
            echo "ERROR: GIT_REPO or ARTIFACT_ID is not set."
            return 1
    fi

    for attempt in $(seq 1 $max_retries); do
        echo "Attempt $attempt/$max_retries to download wheel file with gh..."

        if gh api  -H "Accept: application/vnd.github+json" \
            -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/${GIT_REPO}/actions/artifacts/$ARTIFACT_ID/zip \
            > $whls_path/mooncake.zip; then
            success=true
            break
        else
            echo "Failed to download wheel file from GitHub (attempt $attempt)"
            if [ $attempt -lt $max_retries ]; then
                wait_time=$((base_delay * attempt))
                echo "Retrying in $wait_time seconds..."
                sleep $wait_time
            fi
        fi
    done

    if [ "$success" = false ] || [ ! -f "$whls_path/mooncake.zip" ]; then
        echo "ERROR: Failed to download wheel file after $max_retries attempts"
        return 1
    fi

    unzip -o $whls_path/mooncake.zip -d $whls_path

    mooncake_whl_file=$(basename "$(find $whls_path -name "*.whl" -type f | head -n 1)")
    if [ -z "$mooncake_whl_file" ]; then
        echo "No wheel file found in the extracted archive"
        return 1
    fi
    echo "Found wheel file: $mooncake_whl_file"
    
    echo "Successfully downloaded and extracted wheel file to $whls_path"
    return 0
}

get_image(){
    # only support run in container
    local registry_addr=$1
    echo "Get image $registry_addr"

    echo "Pulling image ${registry_addr}..."
    docker pull $registry_addr
    if [ $? -ne 0 ]; then
        echo "Failed to pull image ${registry_addr}"
        return 1
    fi

    return 0
}

check_proxy_ready() { 
    local proxy_log_path=$1
    local max_attempts=${2:-60}
    local expected_workers=2

    if [ -z "$proxy_log_path" ]; then
        echo "ERROR: Proxy log path not provided" >&2
        return 1
    fi

    echo "Waiting for SGLang Router to be ready and $expected_workers workers to be activated..."
    echo "Checking log file: $proxy_log_path"
    
    for i in $(seq 1 $max_attempts); do
        if [ -f "$proxy_log_path" ]; then
            # "Activated 1 worker(s) (marked as healthy)"
            activated_count=$(grep -c "Activated 1 worker(s) (marked as healthy)" "$proxy_log_path" 2>/dev/null) || activated_count=0
            
            # "Successfully loaded tokenizer"
            tokenizer_ready=$(grep -c "Successfully loaded tokenizer" "$proxy_log_path" 2>/dev/null) || tokenizer_ready=0

            # "Starting server on 0.0.0.0:8000"
            server_started=$(grep -c "Starting server on 0.0.0.0" "$proxy_log_path" 2>/dev/null) || server_started=0
            
            if [ "$activated_count" -ge "$expected_workers" ] && [ "$tokenizer_ready" -gt 0 ]; then
                echo "Router is ready!"
                echo "  - Workers activated: $activated_count/$expected_workers"
                echo "  - Tokenizer: Loaded"
                if [ "$server_started" -gt 0 ]; then
                    echo "  - HTTP Server: Listening on port 8000"
                fi
                return 0
            fi
        fi
        
        if [ "$activated_count" -gt 0 ]; then
             echo "Waiting... ($i/$max_attempts) [Workers: $activated_count/$expected_workers, Tokenizer: $tokenizer_ready]"
        else
             echo "Waiting... ($i/$max_attempts) [Initializing...]"
        fi
        sleep 2
    done
    
    echo "ERROR: Router failed to start or workers failed to register within timeout"
    return 1
}


stop_container(){
    local container_name=${1:-$CONTAINER_NAME}
    local remote_host=$2
    local location="local"
    
    if [ -z "$container_name" ]; then
        echo "ERROR: No container name provided" >&2
        return 1
    fi
    
    if [ -n "$remote_host" ]; then
        location="remote"
    fi
    
    echo "Stopping ${location} Docker container: ${container_name}"
    
    if [ "$location" == "remote" ]; then
        ssh -o StrictHostKeyChecking=no $remote_host "docker stop ${container_name} >/dev/null 2>&1"
    else
        docker stop ${container_name} >/dev/null 2>&1
    fi
    
    if [ $? -eq 0 ]; then
        echo "Successfully stopped ${location} container: ${container_name}"
        return 0
    else
        echo "Failed to stop ${location} container: ${container_name} (may not exist)"
        return 1
    fi
}

save_test_result() {
    local test_case_name=$1
    local status=$2
    local result_dir=$3
    
    local result_json="${result_dir}/test_results.json"
    
    echo "{\"test_case\": \"$test_case_name\", \"status\": \"$status\", \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" > "$result_json"
    echo "Test results saved to: $result_json"
    echo "$test_case_name: $status"
}

cleanup_test_env() {
    local test_type=$1
    
    echo "===== Cleaning up $test_type machine environment ====="
    
    stop_container "${CONTAINER_NAME}"
    
    if [ "$test_type" = "double" ] && [ -n "$REMOTE_IP" ]; then
        stop_container "${CONTAINER_NAME}" "$REMOTE_IP"
    fi
    
    echo "Cleanup completed"
}

setup_node_env() {
    local registry_addr=$1
    echo "===== Setting up docker environment ====="
    
    if ! get_image "$registry_addr"; then
        echo "ERROR: Failed to get the required image"
        return 1
    fi

    if ! clean_container ${CONTAINER_NAME}; then
        echo "ERROR: Failed to clean up container"
        return 1
    fi

    local extra_args=""
    extra_args="$extra_args --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/uverbs1 --device=/dev/infiniband/rdma_cm "
    if [ "${USE_HUGGINGFACE_MIRROR}" = "true" ]; then
        extra_args="$extra_args -e HF_ENDPOINT=${HUGGINGFACE_MIRROR} -e HF_HUB_ENABLE_HF_TRANSFER=1"
    fi
    if [ "${USE_MODELSCOPE}" = "true" ]; then
        extra_args="$extra_args -e SGLANG_USE_MODELSCOPE=true"
    fi

    if ! docker_launch "$registry_addr" "$extra_args"; then
        echo "ERROR: Failed to launch docker container"
        return 1
    fi

    echo "Node environment setup completed"
    return 0
}

launch_and_track_process() {
    local full_cmd="$1"
    local grep_pattern="$2"
    local pid_file="$3"

    echo "Executing command..."
    echo "$full_cmd"
    eval "$full_cmd"

    echo "Waiting for process to initialize..."
    for i in {1..15}; do
        local container_main_pid=$(docker inspect --format '{{.State.Pid}}' "${CONTAINER_NAME}" 2>/dev/null)
        if [ -n "$container_main_pid" ] && [ "$container_main_pid" != "0" ]; then
            pid=$(ps -eo pid,ppid,cmd | awk -v root="$container_main_pid" -v pattern="$grep_pattern" '
                BEGIN { pids[root] = 1 }
                {
                    if ($2 in pids && $0 ~ pattern) {
                        print $1
                        exit
                    }
                }
            ')
        fi

        if [ -n "$pid" ]; then
            echo "$pid" > "$pid_file"
            echo "PID $pid (on host) saved to $pid_file"
            return 0
        fi
        
        echo "  Attempt $i/15..."
        sleep 2
    done

    echo "Process not found after 30 seconds"
    return 1
}

kill_process() {
    local pid_file=$1
    local service_name=$2

    if [ ! -f "$pid_file" ]; then
        echo "No PID file for $service_name."
        return 0
    fi

    local pid=$(cat "$pid_file")
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
        rm -f "$pid_file"
        return 0
    fi

    echo "Stopping $service_name (PID: $pid)..."
    
    kill -TERM "$pid" 2>/dev/null
    sleep 2
    if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null
    fi
    
    rm -f "$pid_file"
    echo "✓ $service_name stopped"
    return 0
}

check_vllm_server_ready(){
    local server_log_path=$1
    local max_attempts=${2:-120}

    if [ -z "$server_log_path" ]; then
        echo "ERROR: Server log path not provided" >&2
        return 1
    fi

    echo "Waiting for server to be ready (checking: $server_log_path)..."
    for i in $(seq 1 $max_attempts); do
        if [ -f "$server_log_path" ]; then
            if grep -q 'Application startup complete.' "$server_log_path" 2>/dev/null; then
                echo "Server is ready!"
                return 0
            fi
            echo "Waiting... ($i/$max_attempts)"
            sleep 2
        fi
    done
    
    echo "ERROR: Server failed to start within timeout"
    return 1
}

wait_for_server_ready() {
    local host=$1
    local port=$2
    local max_attempts=${4:-60}
    local endpoint=${3:-"/health"}

    if [ -z "$host" ] || [ -z "$port" ]; then
        echo "ERROR: Host and port must be provided" >&2
        return 1
    fi

    echo "Waiting for server at $host:$port to be ready (endpoint: $endpoint)..."

    for i in $(seq 1 $max_attempts); do
        local response_code
        response_code=$(curl -o /dev/null -s -w "%{http_code}" "http://$host:$port$endpoint" 2>/dev/null)
        
        if [ "$response_code" = "200" ]; then
            echo "Server is ready! Health check returned 200."
            return 0
        elif [ "$response_code" = "404" ] || [ "$response_code" = "405" ]; then
            # Some servers might not have a /health endpoint but are still starting up
            echo "Waiting... ($i/$max_attempts) - Got response code: $response_code"
        else
            echo "Waiting... ($i/$max_attempts) - Server not ready yet (response: $response_code)"
        fi
        
        sleep 2
    done
    
    echo "ERROR: Server failed to become ready within timeout (last response: $response_code)"
    return 1
}

detect_remote_mode() {
    if [ -z "${ISREMOTE}" ]; then
        if [ -n "${REMOTE_IP}" ] && [ -n "${REMOTE_TEST_DIR}" ] && [[ "$PWD" == "${REMOTE_TEST_DIR}"* ]]; then
            export ISREMOTE=1
        else
            export ISREMOTE=0
        fi
    fi
}

sanitize_model_name() {
    local model_name=$1
    echo "$model_name" | sed 's/\//__/g'
}

convert_container_path_to_host() {
    local container_path=$1
    echo "$container_path" | sed "s|/test_run/|$BASE_DIR/|"
}

setup_log_directory_dual() {
    local test_case_name=$1
    local model_name_clean=$2
    
    setup_log_directory "$TEST_RUN_DIR/logs/$test_case_name/$model_name_clean"
    
    if [ -n "$REMOTE_IP" ]; then
        ${SSH_CMD} $REMOTE_IP "source $REMOTE_TEST_DIR/run/.shrc; cd \$BASE_DIR/scripts && source ./common.sh && setup_log_directory \"\$TEST_RUN_DIR/logs/$test_case_name/$model_name_clean\""
    fi
}

cleanup_model_processes() {
    local pid_dir=$1
    local test_case_name=$2
    
    echo "===== Killing model processes ====="
    
    if [ -d "$pid_dir" ]; then
        echo "Cleaning up by PID files in $pid_dir..."
        for pid_file in "${pid_dir}"/*.pid; do
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

collect_remote_log_file() {
    local model_name_clean=$1
    local remote_log_filename=$2
    local test_case_name=$3
    
    local remote_log_dir="${REMOTE_TEST_DIR}/${TEST_CASE_RESULT_PATH}/${model_name_clean}"
    local local_log_dir="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${model_name_clean}"
    
    echo "  Copying remote ${remote_log_filename}..."
    scp ${REMOTE_IP}:${remote_log_dir}/${remote_log_filename} \
        ${local_log_dir}/ 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo "  ✓ Successfully copied ${remote_log_filename} for $model_name_clean"
        return 0
    else
        echo "  ✗ Failed to copy ${remote_log_filename} for $model_name_clean (file may not exist)"
        return 1
    fi
}

validate_json_response_error() {
    local response=$1
    local model_name=${2:-"unknown"}
    
    if echo "$response" | grep -q "\"object\":\"error\""; then
        local error_message=$(echo "$response" | grep -o '"message":"[^"]*"' | sed 's/"message":"//' | sed 's/"$//')
        echo "  ERROR: $error_message" >&2
        echo "  $model_name: Fail"
        return 1
    fi
    
    return 0
}

validate_http_status() {
    local status_code=$1
    local expected_code=${2:-200}
    
    if [ -z "$status_code" ]; then
        echo "ERROR: HTTP status code is empty" >&2
        return 1
    fi
    
    if ! [[ "$status_code" =~ ^[0-9]+$ ]]; then
        echo "ERROR: HTTP status code is not a valid number: '$status_code'" >&2
        return 1
    fi
    
    if [ "$status_code" -eq "$expected_code" ]; then
        return 0
    else
        echo "ERROR: HTTP request failed with status code $status_code (expected: $expected_code)" >&2
        return 1
    fi
}

validate_response_content() {
    local response=$1
    local json_path=$2
    local expected_pattern=${3:-""}
    
    local content=$(echo "$response" | jq -r "$json_path" 2>/dev/null)
    if [ -z "$content" ] || [ "$content" = "null" ]; then
        echo "ERROR: Failed to extract content from path: $json_path" >&2
        return 1
    fi
    
    if [ -n "$expected_pattern" ]; then
        if [[ "${content,,}" =~ ${expected_pattern,,} ]]; then
            echo "Content validation passed: found '$expected_pattern'"
            echo "Full content: $content"
            return 0
        else
            echo "ERROR: Content validation failed: '$expected_pattern' not found" >&2
            echo "Actual content: $content" >&2
            return 1
        fi
    fi
    
    echo "Content extracted successfully: $content"
    return 0
}

validate_api_response() {
    local response_body=$1
    local status_code=$2
    local json_path=${3:-""}
    local expected_pattern=${4:-""}
    
    if ! validate_http_status "$status_code" 200; then
        return 1
    fi
    
    if ! validate_json_response_error "$response_body"; then
        return 1
    fi
    
    if [ -n "$json_path" ]; then
        if ! validate_response_content "$response_body" "$json_path" "$expected_pattern"; then
            return 1
        fi
    else
        echo "Basic validation passed"
    fi
    
    return 0
}

validate_curl_response_from_log() {
    local log_file=$1
    local model_name=$2
    local expected_pattern=${3:-""}
    
    if [ ! -f "$log_file" ]; then
        echo "  ERROR: Curl response log not found at $log_file" >&2
        echo "  $model_name: Fail"
        return 1
    fi
    
    local curl_response=$(cat "$log_file")
    if [ -z "$curl_response" ]; then
        echo "  ERROR: Curl response log is empty" >&2
        echo "  $model_name: Fail"
        return 1
    fi
    
    if ! validate_json_response_error "$curl_response" "$model_name"; then
        return 1
    fi
    
    if [ -n "$expected_pattern" ]; then
        if echo "$curl_response" | grep -qEi "$expected_pattern"; then
            echo "  $model_name: Pass (pattern matched)"
        else
            echo "  ERROR: Expected pattern '$expected_pattern' not found in response" >&2
            echo "  $model_name: Fail"
            return 1
        fi
    else
        echo "  $model_name: Pass"
    fi
    
    return 0
}

collect_and_validate_model_results() {
    local models_array_name=$1[@]
    local models=("${!models_array_name}")
    local remote_log_filename=$2
    local test_case_name=$3
    local expected_pattern=${4:-""}
    
    local all_passed=true
    
    if [ -z "$REMOTE_IP" ]; then
        echo "ERROR: No REMOTE_IP specified, skipping result parsing" >&2
        return 1
    fi
    
    echo "Getting remote results from remote server..."
    
    for model in "${models[@]}"; do
        local model_name_clean=$(sanitize_model_name "$model")
        
        local remote_log_dir="${REMOTE_TEST_DIR}/${TEST_CASE_RESULT_PATH}/${model_name_clean}"
        local local_log_dir="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${model_name_clean}"
        
        echo "Processing model: $model_name_clean"
        echo "  Remote log dir: $remote_log_dir"
        echo "  Local log dir: $local_log_dir"
        
        collect_remote_log_file "$model_name_clean" "$remote_log_filename" "$test_case_name"
        
        local log_file="${local_log_dir}/curl_response.log"
        echo "  Checking results for model: $model"
        
        if ! validate_curl_response_from_log "$log_file" "$model" "$expected_pattern"; then
            all_passed=false
        fi
        
        echo ""
    done
    
    echo "Remote log collection completed"
    
    if [ "$all_passed" = true ]; then
        return 0
    else
        return 1
    fi
}

launch_sglang_server() {
    local model_path=$1
    local host=$2
    local port=$3
    local log_path=$4
    local pid_suffix=$5
    local extra_args=${6:-""}
    local ready_pattern=${7:-"The server is fired up and ready to roll!"}
    
    if [ -z "$model_path" ] || [ -z "$host" ] || [ -z "$port" ] || [ -z "$log_path" ] || [ -z "$pid_suffix" ]; then
        echo "ERROR: Missing required parameters for launch_sglang_server" >&2
        echo "Usage: launch_sglang_server <model_path> <host> <port> <log_path> <pid_suffix> [extra_args] [ready_pattern]" >&2
        return 1
    fi
    
    local sglang_cmd="${docker_exec} \"python -m sglang.launch_server --model-path ${model_path} --host ${host} --port ${port}"
    if [ -n "$extra_args" ]; then
        sglang_cmd="${sglang_cmd} ${extra_args}"
    fi
    

    sglang_cmd="${sglang_cmd} > ${log_path} 2>&1 &\""
    
    local pid_file="${PID_DIR}/server_${pid_suffix}.pid"
    local grep_pattern="python -m sglang.launch_server.*${model_path}"
    
    echo "Starting SGLang Server..."
    if ! launch_and_track_process "$sglang_cmd" "$grep_pattern" "$pid_file"; then
        return 1
    fi
    
    local host_log_path=$(convert_container_path_to_host "$log_path")
    if ! check_server_ready_with_pattern "$host_log_path" "$ready_pattern"; then
        return 1
    fi

    echo "Performing health check for ${pid_suffix}..."
    if ! wait_for_server_ready "$host" "$port" "/health"; then
        echo "ERROR: Health check failed for ${pid_suffix} at http://$host:$port/health"
        return 1
    fi
    echo "${pid_suffix} health check passed"
    
    return 0
}

launch_vllm_server() {
    local model_path=$1
    local host=$2
    local port=$3
    local log_path=$4
    local pid_suffix=$5
    local extra_args=${6:-""}
    local env_vars=${7:-""}
    
    if [ -z "$model_path" ] || [ -z "$host" ] || [ -z "$port" ] || [ -z "$log_path" ] || [ -z "$pid_suffix" ]; then
        echo "ERROR: Missing required parameters for launch_vllm_server" >&2
        echo "Usage: launch_vllm_server <model_path> <host> <port> <log_path> <pid_suffix> [extra_args] [env_vars]" >&2
        return 1
    fi
    
    local env_prefix=""
    if [ -n "$env_vars" ]; then
        env_prefix="${env_vars} "
    fi
    
    local vllm_cmd="${docker_exec} \"${env_prefix}python3 -m vllm.entrypoints.openai.api_server --model '${model_path}' --host '${host}' --port ${port}"
    
    if [ -n "$extra_args" ]; then
        vllm_cmd="${vllm_cmd} ${extra_args}"
    fi
    
    vllm_cmd="${vllm_cmd} > '${log_path}' 2>&1 &\""
    
    local pid_file="${PID_DIR}/server_${pid_suffix}.pid"
    local grep_pattern="python3 -m vllm.entrypoints.openai.api_server.*${model_path}"
    
    echo "Starting vLLM Server..."
    echo "Command: $vllm_cmd"
    if ! launch_and_track_process "$vllm_cmd" "$grep_pattern" "$pid_file"; then
        return 1
    fi
    
    local host_log_path=$(convert_container_path_to_host "$log_path")
    if ! check_vllm_server_ready "$host_log_path"; then
        return 1
    fi
    
    if ! wait_for_server_ready "$host" "$port" "/health"; then
        return 1
    fi
    
    return 0
}

launch_sglang_router() {
    local prefill_url=$1
    local decode_url=$2
    local host=$3
    local port=$4
    local log_path=$5
    local extra_args=${6:-""}
    
    if [ -z "$prefill_url" ] || [ -z "$decode_url" ] || [ -z "$host" ] || [ -z "$port" ] || [ -z "$log_path" ]; then
        echo "ERROR: Missing required parameters for launch_sglang_router" >&2
        echo "Usage: launch_sglang_router <prefill_url> <decode_url> <host> <port> <log_path> [extra_args]" >&2
        return 1
    fi
    
    echo "===== Starting SGLang Router ====="
    
    local router_cmd="${docker_exec} \"python3 -m sglang_router.launch_router --pd-disaggregation --prefill ${prefill_url} --decode ${decode_url} --host ${host} --port ${port}"
    if [ -n "$extra_args" ]; then
        router_cmd="${router_cmd} ${extra_args}"
    fi
    
    router_cmd="${router_cmd} > ${log_path} 2>&1 &\""
    
    local pid_file="${PID_DIR}/proxy.pid"
    local grep_pattern="sglang::router"
    
    echo "Load balancer starting..."
    echo "Command: $router_cmd"
    if ! launch_and_track_process "$router_cmd" "$grep_pattern" "$pid_file"; then
        return 1
    fi
    
    local host_log_path=$(convert_container_path_to_host "$log_path")
    if ! check_proxy_ready "$host_log_path"; then
        return 1
    fi
    
    return 0
}