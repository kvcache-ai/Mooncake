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