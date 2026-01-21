#!/bin/bash

docker_exec="docker exec ${CONTAINER_NAME} bash -c"
TEST_CASE_RESULT_PATH="run/$test_case_name"

setup_log_directory(){
    local test_name=$1
    local model_name=$2

    log_path="$BASE_DIR/$test_name/logs/$model_name"
    [ -d $log_path ] && rm -rf $log_path
    mkdir -p $log_path

    echo "Log directory set up at: $log_path"
}

docker_launch(){
    local extra_args=$1

    docker_run_cmd="docker run --name ${CONTAINER_NAME} \
    -d --ipc=host --cap-add=SYS_PTRACE --network=host --gpus all \
    --ulimit memlock=-1 --ulimit stack=67108864 --shm-size=128g \
    -v ${MODEL_CACHE}:/root/.cache $extra_args --privileged \
    -v $BASE_DIR:/test_run \
    -v /root/test.jsonl:/tmp/test.jsonl \
    --entrypoint bash \
    ${REGISTRY_ADDR} -c \"hostname;sleep 360000\""

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

    erdma_driver_cmd='wget -qO - http://mirrors.cloud.aliyuncs.com/erdma/GPGKEY | gpg --dearmour -o /etc/apt/trusted.gpg.d/erdma.gpg && \
    echo "deb [ ] http://mirrors.cloud.aliyuncs.com/erdma/apt/ubuntu '"${erdma_repo_codename}"'/erdma main" | tee /etc/apt/sources.list.d/erdma.list && \
    apt update && \
    apt install libibverbs1 ibverbs-providers ibverbs-utils librdmacm1 -y'
    mooncake_whl_file=$(ls $TEST_CASE_RESULT_DIR/whls/*.whl 2>/dev/null | xargs -n 1 basename | head -n 1)
    if [ -z "$mooncake_whl_file" ]; then
        echo "No wheel file found in $TEST_CASE_RESULT_DIR/whls/"
        return 1
    fi
    local relative_path=${TEST_CASE_RESULT_DIR#$BASE_DIR}
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
    local max_attempts=${2:-60}

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
    echo "Get image $REGISTRY_ADDR"

    if ! docker inspect $REGISTRY_ADDR >/dev/null 2>&1; then
        echo "Image ${REGISTRY_ADDR} not found, pulling..."
        docker pull $REGISTRY_ADDR
        if [ $? -ne 0 ]; then
            echo "Failed to pull image ${REGISTRY_ADDR}"
            return 1
        fi
    else
        echo "Image ${REGISTRY_ADDR} already exists, skipping pull"
    fi

    return 0
}

check_proxy_ready() { 
    local proxy_log_path=$1
    local max_attempts=${2:-60}

    if [ -z "$proxy_log_path" ]; then
        echo "ERROR: Proxy log path not provided" >&2
        return 1
    fi

    echo "Waiting for load balancer to be ready and workers to be activated..."
    echo "Checking log file: $proxy_log_path"
    for i in $(seq 1 $max_attempts); do
        if [ -f "$proxy_log_path" ]; then
            # Check if both workers are activated
            server_activated=$(grep -c "Activated worker http://${LOCAL_IP}:30001" "$proxy_log_path" 2>/dev/null) || server_activated=0
            client_activated=$(grep -c "Activated worker http://${REMOTE_IP}:30001" "$proxy_log_path" 2>/dev/null) || client_activated=0
            
            if [ "$server_activated" -gt 0 ] && [ "$client_activated" -gt 0 ]; then
                echo "Load balancer is ready with both workers activated!"
                echo "  - Server worker (http://${LOCAL_IP}:30001): $server_activated time(s)"
                echo "  - Client worker (http://${REMOTE_IP}:30001): $client_activated time(s)"
                return 0
            fi
        fi
        echo "Waiting... ($i/$max_attempts)"
        sleep 2
    done
    
    echo "ERROR: Server failed to start within timeout"
    return 1
}

setup_test_result_directory() {
    local test_case_result_path=$1
    
    if [ -d "$test_case_result_path" ]; then
        echo "Removing existing test result directory: $test_case_result_path"
        rm -rf "$test_case_result_path"
    fi
    
    echo "Creating test result directory: $test_case_result_path"
    if ! mkdir -p "$test_case_result_path"; then
        echo "ERROR: Failed to create test result directory: $test_case_result_path"
        return 1
    fi
    
    echo "Test result directory set up successfully"
    return 0
}

stop_container()
{
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