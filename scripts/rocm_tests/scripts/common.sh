#!/bin/bash
# Platform-specific lifecycle; test helpers are shared.
source "$(dirname "${BASH_SOURCE[0]}")/common_shared.sh"

prepare_rocm_runtime_cache_args() {
    local registry_addr=$1
    local cache_root=${MOONCAKE_RUNTIME_CACHE:-}
    local cache_key image_cache_dir cache_dir

    if [ -z "$cache_root" ]; then
        echo "ERROR: MOONCAKE_RUNTIME_CACHE is required for ROCm" >&2
        return 1
    fi
    if ! [[ "$cache_root" =~ ^/[A-Za-z0-9._/-]+$ ]]; then
        echo "ERROR: MOONCAKE_RUNTIME_CACHE must be an absolute path without spaces: $cache_root" >&2
        return 1
    fi
    mkdir -p -- "$cache_root" || return 1
    cache_root=$(cd -P -- "$cache_root" && pwd) || return 1
    if [ "$cache_root" = "/" ]; then
        echo "ERROR: MOONCAKE_RUNTIME_CACHE must not resolve to /" >&2
        return 1
    fi

    cache_key=${registry_addr##*@sha256:}
    if [ "$cache_key" = "$registry_addr" ]; then
        cache_key=$(printf '%s' "$registry_addr" | cksum | awk '{print $1}')
    fi
    cache_key=${cache_key:0:16}
    image_cache_dir="${cache_root}/${cache_key}"
    for cache_dir in \
        aiter-jit pip tmp torch-extensions torchinductor triton xdg; do
        mkdir -p -- "${image_cache_dir}/${cache_dir}" || return 1
        [ -w "${image_cache_dir}/${cache_dir}" ] || {
            echo "ERROR: ROCm runtime cache is not writable: ${image_cache_dir}/${cache_dir}" >&2
            return 1
        }
    done

    ROCM_RUNTIME_CACHE_ARGS=(
        -v "${image_cache_dir}:/runtime-cache"
        -e AITER_JIT_DIR=/runtime-cache/aiter-jit
        -e PIP_CACHE_DIR=/runtime-cache/pip
        -e TMPDIR=/runtime-cache/tmp
        -e TORCH_EXTENSIONS_DIR=/runtime-cache/torch-extensions
        -e TORCHINDUCTOR_CACHE_DIR=/runtime-cache/torchinductor
        -e TRITON_CACHE_DIR=/runtime-cache/triton
        -e XDG_CACHE_HOME=/runtime-cache/xdg
    )
    echo "Using image-scoped ROCm runtime cache: $image_cache_dir"
}

docker_launch(){
    local registry_addr=$1
    local extra_args=$2

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
    docker_args+=("${SHARED_MOUNT_ARGS[@]}")
    prepare_rocm_runtime_cache_args "$registry_addr" || return 1
    docker_args+=("${ROCM_RUNTIME_CACHE_ARGS[@]}")
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

    pip_cmd=""

    # detect ubuntu codename and set appropriate ERDMA repository
    ubuntu_codename=$(${docker_exec} "cat /etc/os-release | grep UBUNTU_CODENAME | cut -d'=' -f2" 2>/dev/null | tr -d '"' || echo "")

    mooncake_whl_file=$(ls $TEST_RUN_DIR/whls/*.whl 2>/dev/null | xargs -n 1 basename | head -n 1)
    if [ -z "$mooncake_whl_file" ]; then
        echo "No wheel file found in $TEST_RUN_DIR/whls/"
        return 1
    fi
    local relative_path=${TEST_RUN_DIR#$BASE_DIR}
    local cleaned_path=${relative_path#/}
    # SGLang and vLLM images may already contain the CUDA distribution.
    # The CUDA and ROCm distributions share the same `mooncake` package,
    # so installing the ROCm wheel on top leaves a mixture of old and new
    # Python modules/native libraries and breaks the Store RPC ABI.
    pip_cmd=$(append_str "${pip_cmd}" \
        "python3 -m pip uninstall -y mooncake-transfer-engine mooncake-transfer-engine-rocm")
    pip_cmd=$(append_str "${pip_cmd}" \
        "python3 -c 'import shutil, site, sysconfig; from pathlib import Path; roots={Path(path).resolve() for path in (*site.getsitepackages(), site.getusersitepackages(), sysconfig.get_path(\"purelib\"), sysconfig.get_path(\"platlib\")) if path}; packages=sorted({root / \"mooncake\" for root in roots}); [(print(\"Removing orphaned Mooncake package:\", package), shutil.rmtree(package)) for package in packages if package.is_dir()]'")
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

    local mooncake_install_check="python3 /test_run/python/verify_rocm_wheel.py && ! python3 -m pip show mooncake-transfer-engine >/dev/null 2>&1"
    if ! ${docker_exec} "${mooncake_install_check}"; then
        echo "ERROR: The ROCm wheel did not replace all image-provided Mooncake files" >&2
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
        ssh_target=${REMOTE_SSH_TARGET:-$remote_host}
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

    if ! cleanup_rocm_node "local"; then
        cleanup_failed=true
    fi

    if [ "$test_type" = "double" ] && [ -n "${REMOTE_IP:-}" ]; then
        echo "===== Running ROCm postflight on remote node ${REMOTE_IP} ====="
        if ! ${SSH_CMD} "${REMOTE_SSH_TARGET:-$REMOTE_IP}" "
            source ${REMOTE_TEST_DIR}/run/.shrc && \
            source ${REMOTE_TEST_DIR}/scripts/common.sh && \
            cleanup_rocm_node remote
        "; then
            echo "ERROR: Remote ROCm cleanup/postflight failed on ${REMOTE_IP}" >&2
            cleanup_failed=true
        fi
    fi

    if $cleanup_failed; then
        echo "ERROR: Cleanup did not complete successfully" >&2
        return 1
    fi

    echo "Cleanup and postflight completed"
    return 0
}

# Return the maximum used memory, in MiB, for only this CI allocation.
gpu_max_used_mb() {
    command -v rocm-smi >/dev/null 2>&1 || return 1
    rocm-smi --showmeminfo vram --json 2>/dev/null | python3 -c '
import json, sys
indices = {f"card{i}" for i in sys.argv[1].split(",")}
data = json.load(sys.stdin)
used = {}
for card, values in data.items():
    if card not in indices:
        continue
    for key, value in values.items():
        if "VRAM Total Used Memory" in key:
            used[card] = int(value) // (1024 * 1024)
missing = sorted(indices - used.keys())
if missing:
    print("Missing ROCm memory data for: " + ", ".join(missing), file=sys.stderr)
    print(-1)
else:
    print(max(used.values()))
' "${MOONCAKE_GPU_INDICES:-0,1,2,3}"
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

# Fail closed if KFD still reports a process with a queue on one of this
# allocation's GPUs. Mapping the configured render nodes through KFD topology
# keeps this check scoped to the shared host's ROCm CI partition.
verify_no_allocated_gpu_processes() {

    local topology_root=${MOONCAKE_KFD_TOPOLOGY_ROOT:-/sys/class/kfd/kfd/topology/nodes}
    local process_root=${MOONCAKE_KFD_PROCESS_ROOT:-/sys/class/kfd/kfd/proc}
    if [ ! -d "$topology_root" ] || [ ! -d "$process_root" ]; then
        echo "ERROR: KFD topology or process sysfs is unavailable" >&2
        return 1
    fi

    local -a render_nodes
    read -r -a render_nodes <<<"${MOONCAKE_RENDER_DEVICES:-}"
    if [ "${#render_nodes[@]}" -eq 0 ]; then
        echo "ERROR: MOONCAKE_RENDER_DEVICES is empty; cannot check ROCm processes" >&2
        return 1
    fi

    local device render_minor node_dir node_render_minor gpu_id
    local allocation_gpu_ids=""
    for device in "${render_nodes[@]}"; do
        if [ ! -e "$device" ] || [[ ! "$device" =~ /render[D]([0-9]+)$ ]]; then
            echo "ERROR: Allocated ROCm render device is missing: $device" >&2
            return 1
        fi
        render_minor=${BASH_REMATCH[1]}

        gpu_id=""
        for node_dir in "$topology_root"/*; do
            [ -r "$node_dir/properties" ] && [ -r "$node_dir/gpu_id" ] || continue
            node_render_minor=$(awk '$1 == "drm_render_minor" { print $2; exit }' \
                "$node_dir/properties")
            if [ "$node_render_minor" = "$render_minor" ]; then
                gpu_id=$(tr -d '[:space:]' < "$node_dir/gpu_id")
                break
            fi
        done
        if ! [[ "$gpu_id" =~ ^[0-9]+$ ]] || [ "$gpu_id" = 0 ]; then
            echo "ERROR: Could not map $device to a KFD GPU ID" >&2
            return 1
        fi
        allocation_gpu_ids="${allocation_gpu_ids}${gpu_id}"$'\n'
    done

    local proc_dir queue_gpu_file queue_gpu_id pid
    local remaining_pids=""
    for proc_dir in "$process_root"/[0-9]*; do
        [ -d "$proc_dir" ] || continue
        pid=${proc_dir##*/}
        for queue_gpu_file in "$proc_dir"/queues/*/gpuid; do
            [ -r "$queue_gpu_file" ] || continue
            queue_gpu_id=$(tr -d '[:space:]' < "$queue_gpu_file")
            if printf '%s' "$allocation_gpu_ids" | grep -Fxq -- "$queue_gpu_id"; then
                remaining_pids="${remaining_pids}${pid}"$'\n'
                break
            fi
        done
    done

    remaining_pids=$(printf '%s' "$remaining_pids" | sort -nu)
    if [ -n "$remaining_pids" ]; then
        echo "ERROR: KFD processes remain on allocated ROCm GPUs:" >&2
        ps -o pid,ppid,stat,args -p \
            "$(printf '%s\n' "$remaining_pids" | paste -sd, -)" \
            >&2 2>/dev/null || true
        return 1
    fi

    echo "No KFD processes remain on the allocated ROCm GPUs"
    return 0
}

# Final ROCm teardown is intentionally stricter than the reusable between-test
# reset: remove the named container, then prove the allocated GPUs and device
# handles are clean before releasing the cluster lock.
cleanup_rocm_node() {
    local location=${1:-local}
    local cleanup_failed=false
    local container_names

    echo "Stopping and removing ${location} ROCm container: ${CONTAINER_NAME}"
    if ! container_names=$(docker ps -a --format '{{.Names}}'); then
        echo "ERROR: Failed to list ${location} Docker containers" >&2
        cleanup_failed=true
        container_names=""
    fi

    if printf '%s\n' "$container_names" | grep -Fxq -- "${CONTAINER_NAME}"; then
        if ! docker stop "${CONTAINER_NAME}" >/dev/null 2>&1; then
            echo "ERROR: Failed to stop ${location} container: ${CONTAINER_NAME}" >&2
            cleanup_failed=true
        fi
        if ! docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1; then
            echo "ERROR: Failed to remove ${location} container: ${CONTAINER_NAME}" >&2
            cleanup_failed=true
        fi
    else
        echo "No ${location} container named ${CONTAINER_NAME}"
    fi

    if ! container_names=$(docker ps -a --format '{{.Names}}'); then
        echo "ERROR: Failed to verify ${location} Docker container removal" >&2
        cleanup_failed=true
    elif printf '%s\n' "$container_names" | grep -Fxq -- "${CONTAINER_NAME}"; then
        echo "ERROR: ${location} container still exists: ${CONTAINER_NAME}" >&2
        cleanup_failed=true
    else
        echo "Verified ${location} container removal: ${CONTAINER_NAME}"
    fi

    if ! wait_gpu_idle "${MOONCAKE_CLEANUP_TIMEOUT_SECONDS:-90}" \
        "${MOONCAKE_GPU_IDLE_THRESHOLD_MB:-1024}"; then
        echo "ERROR: ${location} ROCm GPU allocation did not drain" >&2
        cleanup_failed=true
    fi
    if ! verify_no_allocated_gpu_processes; then
        echo "ERROR: ${location} ROCm process postflight failed" >&2
        cleanup_failed=true
    fi

    $cleanup_failed && return 1
    echo "ROCm cleanup/postflight passed on ${location} node"
    return 0
}

# Restart the container to reset GPU/RDMA state without reinstalling the wheel
# or userspace drivers. Never kill host PIDs on the shared ROCm allocation.
drain_gpu_local() {
    echo "Restarting container ${CONTAINER_NAME} to reset GPU/RDMA state..."
    if ! docker restart "${CONTAINER_NAME}" >/dev/null 2>&1; then
        echo "ERROR: Failed to restart container ${CONTAINER_NAME}; environment is unhealthy" >&2
        return 1
    fi

    if ! wait_gpu_idle 60; then
        echo "GPU memory remains occupied; refusing host process cleanup on a shared node."
        return 1
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
