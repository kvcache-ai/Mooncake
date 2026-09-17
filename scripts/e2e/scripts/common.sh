#!/bin/bash

TEST_CASE_RESULT_PATH="run/logs/${test_case_name:-}"
docker_exec="docker exec ${CONTAINER_NAME} bash -c"

# Local suites use links to shared cases. Workers receive regular files via
# rsync --copy-links, so they need no additional mount.
SHARED_MOUNT_ARGS=()
if [ -L "${BASH_SOURCE[0]}" ]; then
    shared_root=$(dirname "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")")
    SHARED_MOUNT_ARGS=(-v "${shared_root}:/e2e:ro")
fi

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

save_test_result() {
    local test_case_name=$1
    local status=$2
    local result_dir=$3

    local result_json="${result_dir}/test_results.json"

    echo "{\"test_case\": \"$test_case_name\", \"status\": \"$status\", \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" > "$result_json"
    echo "Test results saved to: $result_json"
    echo "$test_case_name: $status"
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
    local sglang_cmd="${offline_prefix}python -m sglang.launch_server --model-path ${model_path} --host ${host} --port ${port}"
    if [ -n "$extra_args" ]; then
        sglang_cmd="${sglang_cmd} ${extra_args}"
    fi
    if [ -n "${MOONCAKE_SGLANG_MEM_FRACTION_STATIC:-}" ] && \
        [[ " $extra_args " != *" --mem-fraction-static "* ]]; then
        sglang_cmd="${sglang_cmd} --mem-fraction-static ${MOONCAKE_SGLANG_MEM_FRACTION_STATIC}"
    fi


    local pid_file="${PID_DIR}/server_${pid_suffix}.pid"
    local grep_pattern="python -m sglang.launch_server.*${model_path}"

    echo "Starting SGLang Server..."
    if ! launch_and_track_process "$sglang_cmd" "$log_path" "$pid_file" "$grep_pattern"; then
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

    local vllm_cmd="${env_prefix}python3 -m vllm.entrypoints.openai.api_server --model '${model_path}' --host '${host}' --port ${port}"

    if [ -n "$extra_args" ]; then
        vllm_cmd="${vllm_cmd} ${extra_args}"
    fi

    local pid_file="${PID_DIR}/server_${pid_suffix}.pid"
    local grep_pattern="python3 -m vllm.entrypoints.openai.api_server.*${model_path}"

    echo "Starting vLLM Server..."
    echo "Command: $vllm_cmd"
    if ! launch_and_track_process "$vllm_cmd" "$log_path" "$pid_file" "$grep_pattern"; then
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

    local router_cmd="python3 -m sglang_router.launch_router --pd-disaggregation --prefill ${prefill_url} --decode ${decode_url} --host ${host} --port ${port}"
    if [ -n "$extra_args" ]; then
        router_cmd="${router_cmd} ${extra_args}"
    fi

    local pid_file="${PID_DIR}/proxy.pid"
    local grep_pattern="sglang::router"

    echo "Load balancer starting..."
    echo "Command: $router_cmd"
    if ! launch_and_track_process "$router_cmd" "$log_path" "$pid_file" "$grep_pattern"; then
        return 1
    fi

    local host_log_path=$(convert_container_path_to_host "$log_path")
    if ! check_proxy_ready "$host_log_path"; then
        return 1
    fi

    return 0
}
