#!/bin/bash
# Platform-specific lifecycle; test helpers are shared.
source "$(dirname "${BASH_SOURCE[0]}")/common_shared.sh"

docker_launch(){
    local registry_addr=$1
    local extra_args=$2

    docker_run_cmd="docker run  --init --name ${CONTAINER_NAME} \
    -d --ipc=host --cap-add=SYS_PTRACE --network=host --gpus all \
    --ulimit memlock=-1 --ulimit stack=67108864 --shm-size=128g \
    -v ${MODEL_CACHE}:/root/.cache $extra_args --privileged \
    -v $BASE_DIR:/test_run ${SHARED_MOUNT_ARGS[*]} \
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
    pip_cmd=$(append_str "${pip_cmd}" "python3 -m pip install --force-reinstall /test_run/$cleaned_path/whls/$mooncake_whl_file")

    # Check if sglang-router is needed and missing
    if [[ "$registry_addr" == *"sglang"* ]]; then
        echo "=== Detected sglang image, checking sglang-router ==="
        if ! ${docker_exec} "python -c 'import sglang_router' 2>/dev/null"; then
            echo "sglang-router not found, will install it"
            pip_cmd=$(append_str "${pip_cmd}" \
                "pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/")
            pip_cmd=$(append_str "${pip_cmd}" \
                "pip install sglang-router")
        else
            echo "sglang-router already installed, skipping"
        fi
        # Reuse SGLang CI's single source of truth for the git-only evaluator
        # pin instead of duplicating the commit here.
        pip_cmd=$(append_str "${pip_cmd}" \
            'source /sgl-workspace/sglang/scripts/ci/utils/sgl_eval_ref.sh && pip install "$SGL_EVAL_SPEC"')
    fi

    echo "Installing ERDMA drivers"
    echo "Executing ERDMA driver installation command:"
    echo "${erdma_driver_cmd}"
    if ! ${docker_exec} "${erdma_driver_cmd}"; then
        echo "ERROR: Failed to install ERDMA drivers" >&2
        return 1
    fi

    echo "Checking RDMA devices"
    if ! ${docker_exec} "ibv_devinfo" >/dev/null 2>&1; then
        echo "ibv_devinfo execution failed" >&2
        return 1
    fi
    echo "ibv_devinfo execution successful"

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

stop_container(){
    local container_name=${1:-$CONTAINER_NAME}
    local remote_host=${2:-}
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
        local ssh_target=$remote_host
        ${SSH_CMD:-ssh -o StrictHostKeyChecking=no} "$ssh_target" \
            "docker stop ${container_name} >/dev/null 2>&1"
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

cleanup_test_env() {
    local test_type=$1
    local cleanup_failed=false

    echo "===== Cleaning up $test_type machine environment ====="

    if ! stop_container "${CONTAINER_NAME}"; then
        cleanup_failed=true
    fi

    if [ "$test_type" = "double" ] && [ -n "${REMOTE_IP:-}" ]; then
        if ! stop_container "${CONTAINER_NAME}" "$REMOTE_IP"; then
            cleanup_failed=true
        fi
    fi

    if $cleanup_failed; then
        echo "ERROR: Cleanup did not complete successfully" >&2
        return 1
    fi

    echo "Cleanup completed"
    return 0
}

# TONE retains its existing between-case lifecycle.
drain_gpu_between_tests() {
    return 0
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
    extra_args="$extra_args -e NCCL_GIN_TYPE=0 "
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
    local process_cmd=$1
    local log_path=$2
    local pid_file=$3
    local grep_pattern=${4:-}

    if [ -z "$grep_pattern" ]; then
        echo "ERROR: CUDA process tracking requires a grep pattern" >&2
        return 1
    fi

    local escaped_log launch_cmd
    printf -v escaped_log '%q' "$log_path"
    launch_cmd="${process_cmd} > ${escaped_log} 2>&1 &"
    echo "Executing command..."
    printf 'docker exec %q bash -c %q\n' "${CONTAINER_NAME}" "$launch_cmd"
    if ! docker exec "${CONTAINER_NAME}" bash -c "$launch_cmd"; then
        echo "ERROR: Failed to launch process in ${CONTAINER_NAME}" >&2
        return 1
    fi

    echo "Waiting for process to initialize..."
    local container_main_pid pid
    for i in {1..15}; do
        container_main_pid=$(docker inspect --format '{{.State.Pid}}' \
            "${CONTAINER_NAME}" 2>/dev/null)
        if [ -n "$container_main_pid" ] && [ "$container_main_pid" != "0" ]; then
            pid=$(ps -eo pid,ppid,cmd | awk \
                -v root="$container_main_pid" -v pattern="$grep_pattern" '
                BEGIN { pids[root] = 1 }
                {
                    if ($2 in pids && $0 ~ pattern) {
                        print $1
                        exit
                    }
                }
            ')
        fi

        if [ -n "${pid:-}" ]; then
            mkdir -p "$(dirname "$pid_file")"
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

    local pid
    pid=$(cat "$pid_file")
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

verify_model_processes_stopped() {
    return 0
}
