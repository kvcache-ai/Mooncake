#!/bin/bash

TEST_CASE_RESULT_PATH="run/logs/${test_case_name:-}"
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

    if [ "${CI_ACCELERATOR:-cuda}" = "rocm" ]; then
        local -a docker_args=(
            run --init --name "${CONTAINER_NAME}" -d
            --network=host
            --device=/dev/kfd
            --cpuset-cpus="${MOONCAKE_CPUSET_CPUS}"
            --cpuset-mems="${MOONCAKE_CPUSET_MEMS}"
            --cap-drop=ALL
            # apt/dpkg drops privileges to _apt while installing the standard
            # verbs userspace. Retain only the filesystem/identity capabilities
            # needed for that setup; serving never receives the default Docker
            # capability set.
            --cap-add=CHOWN
            --cap-add=DAC_OVERRIDE
            --cap-add=FOWNER
            --cap-add=IPC_LOCK
            --cap-add=SETGID
            --cap-add=SETUID
            # Mooncake queries page placement with move_pages(2) to select the
            # nearest RoCE rail. Docker's default seccomp profile rejects that
            # syscall with EPERM even for the container's own pages. Keep the
            # capability and device allowlists above as the security boundary.
            --security-opt=seccomp=unconfined
            --security-opt=no-new-privileges:true
            --pids-limit=32768
            --ulimit memlock=-1:-1
            --ulimit stack=67108864:67108864
            --shm-size=128g
            --stop-timeout=120
            -e CI_ACCELERATOR=rocm
            -e CI=true
            -e PYTHONDONTWRITEBYTECODE=1
            -e PYTHONFAULTHANDLER=1
            -e PYTHONUNBUFFERED=1
            -e "PYTEST_ADDOPTS=-p no:cacheprovider"
            -e NCCL_GIN_TYPE=0
            -e "NCCL_IB_HCA=${MOONCAKE_RDMA_DEVICES}"
            -e "NCCL_SOCKET_IFNAME=${MOONCAKE_RDMA_NETDEVS}"
            -e "MOONCAKE_DEVICE=${MOONCAKE_TRANSFER_DEVICE}"
            -e "MC_GID_INDEX=${MOONCAKE_GID_INDEX}"
            -e MC_FORCE_HCA=1
            # The MI35x image enables a host-wide SGLang affinity heuristic.
            # It ignores Docker's cpuset and assigns TP rank 1 to CPU96+, which
            # is outside this NUMA0 allocation. Docker already enforces the
            # correct affinity, so disable the conflicting inner policy.
            -e SGLANG_SET_CPU_AFFINITY=0
            -v "${MODEL_CACHE}:/root/.cache"
            -v "${BASE_DIR}:/test_run"
            --entrypoint bash
        )
        local host_libionic=""
        if command -v ldconfig >/dev/null 2>&1; then
            host_libionic=$(ldconfig -p 2>/dev/null | awk '/libionic[.]so[.]1/{print $NF; exit}')
        fi
        if [ -z "$host_libionic" ]; then
            local ionic_candidate
            for ionic_candidate in \
                /usr/lib/x86_64-linux-gnu/libionic.so.1 \
                /lib/x86_64-linux-gnu/libionic.so.1; do
                if [ -r "$ionic_candidate" ]; then
                    host_libionic=$ionic_candidate
                    break
                fi
            done
        fi
        if [ -n "$host_libionic" ]; then
            host_libionic=$(readlink -f "$host_libionic")
        fi
        if [ -n "$host_libionic" ] && [ -r "$host_libionic" ]; then
            echo "Using host-matched Ionic provider library: $host_libionic"
            docker_args+=(-v "${host_libionic}:/opt/mooncake-host-rdma/libionic.so.1:ro")
        else
            echo "WARNING: Host libionic.so.1 is unavailable; ROCm images must provide a compatible Ionic provider" >&2
        fi
        local -a render_nodes
        read -r -a render_nodes <<<"${MOONCAKE_RENDER_DEVICES:-}"
        if [ "${#render_nodes[@]}" -eq 0 ]; then
            echo "ERROR: ROCm profile must expose at least one render node" >&2
            return 1
        fi
        local device
        for device in "${render_nodes[@]}"; do
            if [[ ! "$device" =~ ^/dev/dri/render[D][0-9]+$ ]] || [ ! -c "$device" ]; then
                echo "ERROR: Invalid or missing ROCm render node: $device" >&2
                return 1
            fi
            docker_args+=(--device="$device")
        done
        if [ -z "${MOONCAKE_RDMA_DEVICES:-}" ]; then
            echo "ERROR: MOONCAKE_RDMA_DEVICES is required for ROCm" >&2
            return 1
        fi
        local rdma_device verbs_path uverbs_node uverbs_found
        local -A mounted_uverbs=()
        for rdma_device in ${MOONCAKE_RDMA_DEVICES//,/ }; do
            verbs_path="/sys/class/infiniband/${rdma_device}/device/infiniband_verbs"
            [ -d "$verbs_path" ] || {
                echo "ERROR: Missing verbs mapping for RDMA device $rdma_device" >&2
                return 1
            }
            uverbs_found=0
            for uverbs_node in "$verbs_path"/uverbs*; do
                [ -e "$uverbs_node" ] || continue
                uverbs_found=1
                device="/dev/infiniband/$(basename "$uverbs_node")"
                [ -c "$device" ] || {
                    echo "ERROR: Missing RDMA character device $device" >&2
                    return 1
                }
                if [ -z "${mounted_uverbs[$device]:-}" ]; then
                    docker_args+=(--device="$device")
                    mounted_uverbs[$device]=1
                fi
            done
            if [ "$uverbs_found" -eq 0 ]; then
                echo "ERROR: No userspace verbs device found for RDMA device $rdma_device" >&2
                return 1
            fi
        done
        if [ -c /dev/infiniband/rdma_cm ]; then
            docker_args+=(--device=/dev/infiniband/rdma_cm)
        fi
        if [ "${USE_HUGGINGFACE_MIRROR}" = "true" ]; then
            docker_args+=(-e "HF_ENDPOINT=${HUGGINGFACE_MIRROR}" -e HF_HUB_ENABLE_HF_TRANSFER=1)
        fi
        if [ "${USE_MODELSCOPE}" = "true" ]; then
            docker_args+=(-e SGLANG_USE_MODELSCOPE=true)
        fi
        if [ -n "${HF_TOKEN_FILE:-}" ] && [ -r "$HF_TOKEN_FILE" ]; then
            local hf_token
            hf_token=$(<"$HF_TOKEN_FILE")
            [ -n "$hf_token" ] || { echo "ERROR: $HF_TOKEN_FILE is empty" >&2; return 1; }
            export HF_TOKEN="$hf_token"
            docker_args+=(-e HF_TOKEN)
        fi
        printf 'Executing Docker run command:'
        printf ' %q' docker "${docker_args[@]}" "$registry_addr" -c 'hostname; sleep 360000'
        printf '\n'
        if ! docker "${docker_args[@]}" "$registry_addr" -c 'hostname; sleep 360000'; then
            echo "ERROR: Failed to launch ROCm container" >&2
            return 1
        fi
    else
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
    if [ "${CI_ACCELERATOR:-cuda}" = "rocm" ]; then
        # SGLang and vLLM images may already contain the CUDA distribution.
        # The CUDA and ROCm distributions share the same `mooncake` package,
        # so installing the ROCm wheel on top leaves a mixture of old and new
        # Python modules/native libraries and breaks the Store RPC ABI.
        pip_cmd=$(append_str "${pip_cmd}" \
            "python3 -m pip uninstall -y mooncake-transfer-engine mooncake-transfer-engine-rocm")
        pip_cmd=$(append_str "${pip_cmd}" \
            "python3 -c 'import shutil, site, sysconfig; from pathlib import Path; roots={Path(path).resolve() for path in (*site.getsitepackages(), site.getusersitepackages(), sysconfig.get_path(\"purelib\"), sysconfig.get_path(\"platlib\")) if path}; packages=sorted({root / \"mooncake\" for root in roots}); [(print(\"Removing orphaned Mooncake package:\", package), shutil.rmtree(package)) for package in packages if package.is_dir()]'")
    fi
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

    if [ "${CI_ACCELERATOR:-cuda}" = "rocm" ]; then
        local rocm_rdma_cmd="set -euo pipefail
rdma_ready=false
if command -v ibv_devinfo >/dev/null 2>&1 && ibv_devinfo >/tmp/mooncake-ibv-devinfo.log 2>&1; then
    rdma_ready=true
    echo 'ROCm RoCE userspace is functional'
else
    echo 'Initial ibv_devinfo failure:' >&2
    cat /tmp/mooncake-ibv-devinfo.log >&2 2>/dev/null || true
    echo 'Installing AMD Pensando AINIC userspace ${AINIC_VERSION}'
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        apt-transport-https ca-certificates curl gnupg
    install -d -m 0755 /etc/apt/keyrings
    curl -fsSL https://repo.radeon.com/rocm/rocm.gpg.key \
        | gpg --dearmor --yes --output /etc/apt/keyrings/amdainic.gpg
    echo 'deb [arch=amd64 signed-by=/etc/apt/keyrings/amdainic.gpg] https://repo.radeon.com/amdainic/pensando/ubuntu/${AINIC_VERSION} ${ubuntu_codename} main' \
        > /etc/apt/sources.list.d/amdainic.list
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ibverbs-utils ionic-common libionic-dev librdmacm1
    rm -rf /var/lib/apt/lists/*
fi

if [ \"\$rdma_ready\" != true ] && [ -r /opt/mooncake-host-rdma/libionic.so.1 ]; then
    echo 'Overlaying host-matched Ionic provider library'
    # Let the package installation update the loader cache before replacing
    # its ABI-incompatible provider. Running ldconfig afterwards would restore
    # libionic.so.1 to the newer container library.
    ldconfig
    install -m 0644 /opt/mooncake-host-rdma/libionic.so.1 \
        /usr/lib/x86_64-linux-gnu/libionic-host.so.1
    ln -sfn libionic-host.so.1 /usr/lib/x86_64-linux-gnu/libionic.so.1
    ln -sfn libionic-host.so.1 /usr/lib/x86_64-linux-gnu/libionic.so

    verbs_abi=\$(find /usr/lib/x86_64-linux-gnu/libibverbs -maxdepth 1 \
        \( -type f -o -type l \) 2>/dev/null \
        | sed -n 's/.*-rdmav\([0-9][0-9]*\)[.]so$/\1/p' | head -n 1)
    if [ -n \"\$verbs_abi\" ]; then
        ln -sfn ../libionic-host.so.1 \
            /usr/lib/x86_64-linux-gnu/libibverbs/libionic-rdmav\${verbs_abi}.so
        echo 'Ionic provider path:' \
            \"\$(readlink -f /usr/lib/x86_64-linux-gnu/libibverbs/libionic-rdmav\${verbs_abi}.so)\"
    else
        echo 'WARNING: Unable to determine the container libibverbs provider ABI' >&2
    fi
    echo 'Ionic provider checksums:'
    sha256sum /opt/mooncake-host-rdma/libionic.so.1 \
        /usr/lib/x86_64-linux-gnu/libionic-host.so.1
fi"
        echo "Checking ROCm RoCE userspace"
        if ! ${docker_exec} "${rocm_rdma_cmd}"; then
            echo "ERROR: Failed to install ROCm RoCE userspace" >&2
            return 1
        fi
    else
        echo "Installing ERDMA drivers"
        echo "Executing ERDMA driver installation command:"
        echo "${erdma_driver_cmd}"
        if ! ${docker_exec} "${erdma_driver_cmd}"; then
            echo "ERROR: Failed to install ERDMA drivers" >&2
            return 1
        fi
    fi

    if [ "${CI_ACCELERATOR:-cuda}" = "rocm" ]; then
        local rdma_device=${MOONCAKE_TRANSFER_DEVICE:-}
        if [ -z "$rdma_device" ]; then
            echo "ERROR: MOONCAKE_TRANSFER_DEVICE is required for ROCm" >&2
            return 1
        fi
        if ! [[ "$rdma_device" =~ ^[a-zA-Z0-9_.-]+$ ]]; then
            echo "ERROR: Invalid MOONCAKE_TRANSFER_DEVICE: $rdma_device" >&2
            return 1
        fi
        echo "Checking ROCm RDMA device ${rdma_device}"
        local rdma_preflight_cmd="set -e
echo '=== ibv_devinfo ==='
ibv_devinfo -d '${rdma_device}'
echo '=== RDMA link state ==='
if command -v rdma >/dev/null 2>&1; then rdma link show; fi
state=\$(cat '/sys/class/infiniband/${rdma_device}/ports/1/state')
echo '${rdma_device} port 1 state:' \"\$state\"
case \"\$state\" in *ACTIVE*) ;; *) echo 'RDMA port is not active' >&2; exit 1;; esac
	gid=\$(cat '/sys/class/infiniband/${rdma_device}/ports/1/gids/${MOONCAKE_GID_INDEX}')
	echo '${rdma_device} GID index ${MOONCAKE_GID_INDEX}:' \"\$gid\"
case \"\$gid\" in ''|'::'|'0:0:0:0:0:0:0:0') echo 'RDMA GID is empty' >&2; exit 1;; esac"
        if ! ${docker_exec} "${rdma_preflight_cmd}"; then
            echo "RDMA preflight failed for $rdma_device" >&2
            return 1
        fi
        echo "RDMA preflight successful"
    else
        echo "Checking RDMA devices"
        if ! ${docker_exec} "ibv_devinfo" >/dev/null 2>&1; then
            echo "ibv_devinfo execution failed" >&2
            return 1
        fi
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

    if [ "${CI_ACCELERATOR:-cuda}" = "rocm" ]; then
        local mooncake_install_check="python3 /test_run/python/verify_rocm_wheel.py && ! python3 -m pip show mooncake-transfer-engine >/dev/null 2>&1"
        if ! ${docker_exec} "${mooncake_install_check}"; then
            echo "ERROR: The ROCm wheel did not replace all image-provided Mooncake files" >&2
            return 1
        fi
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
            if grep -qE 'Fatal Python error|Segfault encountered|Subprocess .* crashed with exit code' \
                "$server_log_path" 2>/dev/null; then
                echo "ERROR: Server process crashed during startup; see $server_log_path" >&2
                tail -n 80 "$server_log_path" >&2
                return 1
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

    echo "get whl file from CI artifacts"
    rm -f "$whls_path/mooncake.zip"
    rm -f "$whls_path"/*.whl

    if [ -n "${WHEEL_DIR:-}" ]; then
        local local_wheel=""
        local local_wheel_count=0
        while IFS= read -r wheel; do
            local_wheel=$wheel
            local_wheel_count=$((local_wheel_count + 1))
        done < <(find "$WHEEL_DIR" -type f -name '*.whl' -print)
        if [ "$local_wheel_count" -ne 1 ]; then
            echo "ERROR: Expected exactly one wheel in $WHEEL_DIR, found $local_wheel_count" >&2
            return 1
        fi
        cp -L "$local_wheel" "$whls_path/"
        echo "Copied self-hosted Actions wheel: $(basename "$local_wheel")"
        return 0
    fi

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
        activated_count=0
        tokenizer_ready=0
        server_started=0
        if [ -f "$proxy_log_path" ]; then
            # "Activated 1 worker(s) (marked as healthy)"
            activated_count=$(grep -cF "Activated 1 worker(s) (marked as healthy)" "$proxy_log_path" 2>/dev/null) || activated_count=0

            # "Successfully loaded tokenizer"
            tokenizer_ready=$(grep -cE "Successfully (loaded|registered) tokenizer" "$proxy_log_path" 2>/dev/null) || tokenizer_ready=0

            # "Starting server on 0.0.0.0:8000"
            server_started=$(grep -cF "Starting server on 0.0.0.0" "$proxy_log_path" 2>/dev/null) || server_started=0

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
        if [ "${CI_ACCELERATOR:-cuda}" = "rocm" ]; then
            ssh_target=${REMOTE_SSH_TARGET:-$remote_host}
        fi
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

# Return the maximum used memory, in MiB, for only this CI allocation.
gpu_max_used_mb() {
    if [ "${CI_ACCELERATOR:-cuda}" = "rocm" ]; then
        command -v rocm-smi >/dev/null 2>&1 || return 1
        rocm-smi --showmeminfo vram --json 2>/dev/null | python3 -c '
import json, sys
indices = {f"card{i}" for i in sys.argv[1].split(",")}
data = json.load(sys.stdin)
used = []
for card, values in data.items():
    if card not in indices:
        continue
    for key, value in values.items():
        if "VRAM Total Used Memory" in key:
            used.append(int(value) // (1024 * 1024))
print(max(used) if used else -1)
' "${MOONCAKE_GPU_INDICES:-0,1,2,3}"
    else
        command -v nvidia-smi >/dev/null 2>&1 || return 1
        nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits 2>/dev/null \
            | sort -n | tail -n 1
    fi
}

# Wait until GPU memory on the allocated devices drains below a threshold.
# Returns 0 once drained, 1 if it times out.
wait_gpu_idle() {
    local max_seconds=${1:-90}
    local threshold_mb=${2:-1024}

    echo "Waiting for GPU memory to drain (threshold ${threshold_mb}MB, timeout ${max_seconds}s)..."
    local elapsed=0
    local max_used=0
    while [ $elapsed -lt $max_seconds ]; do
        max_used=$(gpu_max_used_mb)
        if ! [[ "$max_used" =~ ^[0-9]+$ ]]; then
            echo "ERROR: ${CI_ACCELERATOR:-CUDA} GPU memory query failed; cannot verify GPU drain" >&2
            return 1
        fi
        if [ "$max_used" -le "$threshold_mb" ]; then
            echo "GPU memory drained (max used ${max_used}MB)"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    echo "GPU memory not drained within ${max_seconds}s (max used ${max_used}MB)"
    return 1
}

# Kill only GPU processes whose cgroup still identifies them as belonging to
# the reused test container. Unknown or unrelated processes must be left alone;
# the caller will quarantine the environment if GPU memory remains occupied.
gpu_pid_belongs_to_container() {
    local pid=$1
    local container_id=$2
    [ -r "/proc/${pid}/cgroup" ] && grep -Fq "$container_id" "/proc/${pid}/cgroup"
}

force_kill_container_gpu_procs() {
    if [ "${CI_ACCELERATOR:-cuda}" = "rocm" ]; then
        echo "ROCm cleanup refuses host PID killing; quarantine the allocation if container restart does not drain it" >&2
        return 1
    fi
    command -v nvidia-smi >/dev/null 2>&1 || return 1

    local container_id
    container_id=$(docker inspect --format '{{.Id}}' "${CONTAINER_NAME}" 2>/dev/null) || {
        echo "ERROR: Cannot inspect container ${CONTAINER_NAME}; refusing to kill host GPU processes" >&2
        return 1
    }
    if [ -z "$container_id" ]; then
        echo "ERROR: Empty container ID for ${CONTAINER_NAME}; refusing to kill host GPU processes" >&2
        return 1
    fi

    local gpu_pids
    gpu_pids=$(nvidia-smi --query-compute-apps=pid --format=csv,noheader 2>/dev/null | tr -cd '0-9\n' | grep -E '^[0-9]+$' | sort -u)
    [ -z "$gpu_pids" ] && return 0

    local container_pids=""
    local pid
    for pid in $gpu_pids; do
        if gpu_pid_belongs_to_container "$pid" "$container_id"; then
            container_pids="${container_pids} ${pid}"
        else
            echo "WARNING: GPU PID ${pid} is not owned by ${CONTAINER_NAME}; leaving it untouched" >&2
        fi
    done

    if [ -z "$container_pids" ]; then
        echo "No container-owned GPU processes are safe to kill"
        return 0
    fi

    echo "Force-killing container-owned GPU PIDs:${container_pids}"
    for pid in $container_pids; do
        if ! kill -9 "$pid" 2>/dev/null; then
            echo "ERROR: Failed to kill container-owned GPU PID ${pid}" >&2
            return 1
        fi
    done
    sleep 3
}

# Fully clear GPU memory on the current node: restart the container first, then
# kill only container-owned GPU processes that survived the restart.
# Restart the reused container to reset all in-container state (processes, GPU
# memory, ERDMA queue-pairs / RDMA contexts). 'docker restart' keeps the
# writable layer, so the mooncake wheel and ERDMA drivers are NOT reinstalled.
drain_gpu_local() {
    echo "Restarting container ${CONTAINER_NAME} to reset GPU/ERDMA state..."
    if ! docker restart "${CONTAINER_NAME}" >/dev/null 2>&1; then
        echo "ERROR: Failed to restart container ${CONTAINER_NAME}; environment is unhealthy" >&2
        return 1
    fi

    if ! wait_gpu_idle 60; then
        echo "GPU memory remains occupied; checking for container-owned processes..."
        force_kill_container_gpu_procs || return 1
        if ! wait_gpu_idle 45; then
            echo "ERROR: GPU still occupied after bounded cleanup; environment is unhealthy" >&2
            return 1
        fi
    fi

    return 0
}

# Between test cases in run-all the container is reused; reset in-container
# state on both the local and (for double-machine runs) remote nodes via a
# lightweight container restart (no wheel / ERDMA driver reinstall).
drain_gpu_between_tests() {
    # The reset protocol is currently defined only for the dedicated ROCm
    # allocation. Preserve the existing CUDA/T-one lifecycle until an
    # accelerator-neutral reset contract is introduced and validated there.
    if [ "${CI_ACCELERATOR:-cuda}" != "rocm" ]; then
        return 0
    fi

    echo "===== Resetting environment between test cases ====="
    local reset_failed=false
    if ! drain_gpu_local; then
        echo "ERROR: Failed to reset the local test environment" >&2
        reset_failed=true
    fi

    if [ -n "$REMOTE_IP" ]; then
        echo "Resetting environment on remote node $REMOTE_IP..."
        if ! ${SSH_CMD} "${REMOTE_SSH_TARGET:-$REMOTE_IP}" "
            source ${REMOTE_TEST_DIR}/run/.shrc && \
            source ${REMOTE_TEST_DIR}/scripts/common.sh && \
            drain_gpu_local
        "; then
            echo "ERROR: Failed to reset the remote test environment on ${REMOTE_IP}" >&2
            reset_failed=true
        fi
    fi

    $reset_failed && return 1
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
    if [ "${CI_ACCELERATOR:-cuda}" != "rocm" ]; then
        extra_args="$extra_args --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/uverbs1 --device=/dev/infiniband/rdma_cm "
    fi
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
    local escaped_cmd escaped_log launch_cmd container_pid process_group

    printf -v escaped_cmd '%q' "$process_cmd"
    printf -v escaped_log '%q' "$log_path"
    launch_cmd="setsid bash -c ${escaped_cmd} > ${escaped_log} 2>&1 < /dev/null & echo \$!"

    echo "Executing command in a dedicated container process group..."
    echo "$process_cmd"
    container_pid=$(docker exec "${CONTAINER_NAME}" bash -c "$launch_cmd") || {
        echo "ERROR: Failed to launch process in ${CONTAINER_NAME}" >&2
        return 1
    }
    container_pid=$(printf '%s\n' "$container_pid" | tail -n 1 | tr -d '[:space:]')
    if ! [[ "$container_pid" =~ ^[0-9]+$ ]]; then
        echo "ERROR: Invalid container PID returned by launcher: $container_pid" >&2
        return 1
    fi

    for i in {1..15}; do
        process_group=$(docker exec "${CONTAINER_NAME}" \
            ps -o pgid= -p "$container_pid" 2>/dev/null | tr -d '[:space:]')
        if [[ "$process_group" =~ ^[0-9]+$ ]]; then
            mkdir -p "$(dirname "$pid_file")"
            echo "$process_group" > "$pid_file"
            echo "Container process group $process_group saved to $pid_file"
            return 0
        fi

        echo "  Waiting for process group... ($i/15)"
        sleep 2
    done

    echo "ERROR: Container process group not found after 30 seconds" >&2
    return 1
}

kill_process() {
    local pid_file=$1
    local service_name=$2

    if [ ! -f "$pid_file" ]; then
        echo "No PID file for $service_name."
        return 0
    fi

    local process_group
    process_group=$(tr -d '[:space:]' < "$pid_file")
    if ! [[ "$process_group" =~ ^[0-9]+$ ]]; then
        echo "ERROR: Invalid process group in $pid_file" >&2
        rm -f "$pid_file"
        return 1
    fi

    if ! docker exec "${CONTAINER_NAME}" bash -c \
        "kill -0 -- -${process_group} 2>/dev/null"; then
        rm -f "$pid_file"
        return 0
    fi

    echo "Stopping $service_name (container process group: $process_group)..."
    docker exec "${CONTAINER_NAME}" bash -c \
        "kill -TERM -- -${process_group} 2>/dev/null || true"
    local attempt
    for attempt in {1..15}; do
        if ! docker exec "${CONTAINER_NAME}" bash -c \
            "kill -0 -- -${process_group} 2>/dev/null"; then
            rm -f "$pid_file"
            echo "✓ $service_name stopped"
            return 0
        fi
        sleep 2
    done

    echo "Process group $process_group did not stop after SIGTERM; sending SIGKILL" >&2
    docker exec "${CONTAINER_NAME}" bash -c \
        "kill -KILL -- -${process_group} 2>/dev/null || true"
    sleep 2
    if docker exec "${CONTAINER_NAME}" bash -c \
        "kill -0 -- -${process_group} 2>/dev/null"; then
        echo "ERROR: $service_name process group $process_group survived SIGKILL" >&2
        return 1
    fi

    rm -f "$pid_file"
    echo "✓ $service_name stopped"
    return 0
}

verify_model_processes_stopped() {
    local process_pattern='sglang[.]launch_server|sglang_router[.]launch_router|sglang::router|vllm[.]entrypoints[.]openai[.]api_server|mooncake_connector_proxy[.]py|toy_proxy_server[.]py'
    local remaining
    remaining=$(docker exec "${CONTAINER_NAME}" bash -c \
        "ps -eo pid,ppid,pgid,stat,args | grep -E '${process_pattern}' | grep -v grep" 2>/dev/null || true)
    if [ -n "$remaining" ]; then
        echo "ERROR: Model processes remain in ${CONTAINER_NAME}:" >&2
        echo "$remaining" >&2
        return 1
    fi
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

check_vllm_proxy_ready(){
    local proxy_log_path=$1
    local ready_pattern=${2:-"All prefiller instances are ready."}
    local max_attempts=${3:-120}

    if [ -z "$proxy_log_path" ]; then
        echo "ERROR: Proxy log path not provided" >&2
        return 1
    fi

    echo "Waiting for proxy to be ready (checking: $proxy_log_path)..."
    echo "Looking for pattern: '$ready_pattern'"
    for i in $(seq 1 $max_attempts); do
        if [ -f "$proxy_log_path" ]; then
            if grep -q "$ready_pattern" "$proxy_log_path" 2>/dev/null; then
                echo "Proxy is ready!"
                return 0
            fi
            echo "Waiting... ($i/$max_attempts)"
            sleep 2
        fi
    done

    echo "ERROR: Proxy failed to start within timeout"
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
        ${SSH_CMD} "${REMOTE_SSH_TARGET:-$REMOTE_IP}" "source $REMOTE_TEST_DIR/run/.shrc; cd \$BASE_DIR/scripts && source ./common.sh && setup_log_directory \"\$TEST_RUN_DIR/logs/$test_case_name/$model_name_clean\""
    fi
}

cleanup_model_processes() {
    local pid_dir=$1
    local test_case_name=$2

    echo "===== Killing model processes ====="
    local cleanup_failed=false

    if [ -d "$pid_dir" ]; then
        echo "Cleaning up by PID files in $pid_dir..."
        for pid_file in "${pid_dir}"/*.pid; do
            if [ -f "$pid_file" ]; then
                local service_name=$(basename "$pid_file" .pid)
                kill_process "$pid_file" "$service_name" || cleanup_failed=true
            fi
        done
    fi

    verify_model_processes_stopped || cleanup_failed=true

    if [ "$ISREMOTE" == "0" ] && [ -n "$REMOTE_IP" ]; then
        echo "===== Killing model processes (remote: $REMOTE_IP) ====="
        if ! ${SSH_CMD} "${REMOTE_SSH_TARGET:-$REMOTE_IP}" \
            "source $REMOTE_TEST_DIR/run/.shrc; cd \$BASE_DIR/scripts && ./$test_case_name.sh stop_server"; then
            echo "ERROR: Remote model-process cleanup failed" >&2
            cleanup_failed=true
        fi
    fi

    echo "Process cleanup completed."
    $cleanup_failed && return 1
    return 0
}

collect_remote_log_file() {
    local model_name_clean=$1
    local remote_log_filename=$2
    local test_case_name=$3

    local remote_log_dir="${REMOTE_TEST_DIR}/${TEST_CASE_RESULT_PATH}/${model_name_clean}"
    local local_log_dir="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${model_name_clean}"

    echo "  Copying remote ${remote_log_filename}..."
    ${SCP_CMD:-scp} \
        "${REMOTE_SSH_TARGET:-$REMOTE_IP}:${remote_log_dir}/${remote_log_filename}" \
        "${local_log_dir}/" 2>/dev/null

    if [ $? -eq 0 ]; then
        echo "  ✓ Successfully copied ${remote_log_filename} for $model_name_clean"
        return 0
    else
        echo "  ✗ Failed to copy ${remote_log_filename} for $model_name_clean (file may not exist)"
        return 1
    fi
}

# Checks for API error responses containing "object":"error"
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

# Validates HTTP status codes (default expectation: 200)
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

# Extracts JSON values using jq paths and matches patterns
validate_response_content() {
    local response=$1
    local json_query=$2
    local expected_pattern=${3:-""}

    if [ -z "$json_query" ]; then
        return 0
    fi

    local content=$(echo "$response" | jq -r "$json_query" 2>/dev/null)
    if [ -z "$content" ] || [ "$content" = "null" ]; then
        echo "ERROR: Failed to extract content from JSON with query: $json_query" >&2
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
    local json_query=${3:-""}
    local expected_pattern=${4:-""}

    if ! validate_http_status "$status_code" 200; then
        return 1
    fi

    if ! validate_json_response_error "$response_body"; then
        return 1
    fi

    if [ -n "$json_query" ]; then
        if ! validate_response_content "$response_body" "$json_query" "$expected_pattern"; then
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

# Echo an offline env prefix only when a complete set of model weights exists.
# A config-only or interrupted snapshot must stay online so Hugging Face can
# resume it instead of failing later with "Cannot find any model weights".
hf_offline_prefix() {
    local model_name=$1
    [ -z "$model_name" ] && return 0
    local cache_dir="models--$(echo "$model_name" | sed 's#/#--#g')"
    if docker exec -i "${CONTAINER_NAME}" python3 - \
        "/root/.cache/huggingface/hub/${cache_dir}" <<'PY'
import glob
import json
import os
import re
import sys


def snapshot_complete(snapshot):
    if not os.path.isfile(os.path.join(snapshot, "config.json")):
        return False
    for index_name in ("model.safetensors.index.json", "pytorch_model.bin.index.json"):
        index_path = os.path.join(snapshot, index_name)
        if not os.path.isfile(index_path):
            continue
        try:
            with open(index_path, encoding="utf-8") as index_file:
                weights = set(json.load(index_file).get("weight_map", {}).values())
        except (OSError, ValueError):
            return False
        return bool(weights) and all(
            os.path.isfile(os.path.join(snapshot, weight))
            and os.path.getsize(os.path.join(snapshot, weight)) > 0
            for weight in weights
        )
    return any(
        os.path.isfile(path) and os.path.getsize(path) > 0
        for pattern in ("*.safetensors", "pytorch_model*.bin", "*.pt")
        for path in glob.glob(os.path.join(snapshot, pattern))
    )


repository = sys.argv[1]
ref_path = os.path.join(repository, "refs", "main")
try:
    with open(ref_path, encoding="utf-8") as ref_file:
        revision = ref_file.read().strip()
except OSError:
    sys.exit(1)

if re.fullmatch(r"[0-9a-fA-F]{40,64}", revision) is None:
    sys.exit(1)

snapshot = os.path.join(repository, "snapshots", revision)
sys.exit(0 if snapshot_complete(snapshot) else 1)
PY
    then
        echo "HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 "
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

    local offline_prefix=$(hf_offline_prefix "$model_path")
    local sglang_cmd="${offline_prefix}exec python -m sglang.launch_server --model-path ${model_path} --host ${host} --port ${port}"
    if [ -n "$extra_args" ]; then
        sglang_cmd="${sglang_cmd} ${extra_args}"
    fi
    if [ -n "${MOONCAKE_SGLANG_MEM_FRACTION_STATIC:-}" ] && \
        [[ " $extra_args " != *" --mem-fraction-static "* ]]; then
        sglang_cmd="${sglang_cmd} --mem-fraction-static ${MOONCAKE_SGLANG_MEM_FRACTION_STATIC}"
    fi


    local pid_file="${PID_DIR}/server_${pid_suffix}.pid"

    echo "Starting SGLang Server..."
    if ! launch_and_track_process "$sglang_cmd" "$log_path" "$pid_file"; then
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
    env_prefix="${env_prefix}$(hf_offline_prefix "$model_path")"

    local vllm_cmd="${env_prefix}exec python3 -m vllm.entrypoints.openai.api_server --model '${model_path}' --host '${host}' --port ${port}"

    if [ -n "$extra_args" ]; then
        vllm_cmd="${vllm_cmd} ${extra_args}"
    fi

    local pid_file="${PID_DIR}/server_${pid_suffix}.pid"

    echo "Starting vLLM Server..."
    echo "Command: $vllm_cmd"
    if ! launch_and_track_process "$vllm_cmd" "$log_path" "$pid_file"; then
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

    local router_cmd="exec python3 -m sglang_router.launch_router --pd-disaggregation --prefill ${prefill_url} --decode ${decode_url} --host ${host} --port ${port}"
    if [ -n "$extra_args" ]; then
        router_cmd="${router_cmd} ${extra_args}"
    fi

    local pid_file="${PID_DIR}/proxy.pid"

    echo "Load balancer starting..."
    echo "Command: $router_cmd"
    if ! launch_and_track_process "$router_cmd" "$log_path" "$pid_file"; then
        return 1
    fi

    local host_log_path=$(convert_container_path_to_host "$log_path")
    if ! check_proxy_ready "$host_log_path"; then
        return 1
    fi

    return 0
}
