#!/usr/bin/env bash

test_case_name="test_vllm_mooncake_store_rs"
TEST_TYPE="double"
SUPPORT_MODELS=("Qwen/Qwen3-8B")

PID_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}/run/pids/${test_case_name}
BASE_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}
# shellcheck source=common.sh
. "${BASE_DIR}/scripts/common.sh"

detect_remote_mode
mkdir -p "$PID_DIR"

start_server() {
    local model_name=$1
    local model_name_clean=$2
    local redis_port=$3
    local cache_prefix=$4
    local host port log_path config_path visible_devices

    if [[ "$ISREMOTE" == "0" ]]; then
        host=$LOCAL_IP
        port=8020
        log_path="/test_run/run/logs/${test_case_name}/${model_name_clean}/vllm_server_local.log"
        config_path="/tmp/mooncake-store-rs-vllm-local.json"
        visible_devices=${MOONCAKE_VLLM_VISIBLE_DEVICES:-6,7}
    else
        host=$REMOTE_IP
        port=8010
        log_path="/test_run/run/logs/${test_case_name}/${model_name_clean}/vllm_server_remote.log"
        config_path="/tmp/mooncake-store-rs-vllm-remote.json"
        visible_devices=${MOONCAKE_VLLM_VISIBLE_DEVICES:-6,7}
    fi

    local redis_url="redis://${LOCAL_IP}:${redis_port}/0"
    local device_name=${MOONCAKE_TRANSFER_DEVICE:-}
    local config_json
    printf -v config_json \
        '{"mode":"embedded","metadata_server":"%s","master_server_address":"%s","global_segment_size":"1GB","local_buffer_size":"1GB","protocol":"rdma","device_name":"%s"}' \
        "$redis_url" "$redis_url" "$device_name"
    printf '%s\n' "$config_json" | docker exec -i "$CONTAINER_NAME" \
        sh -c "cat > '${config_path}'"

    local kv_config
    printf -v kv_config \
        '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_both","kv_connector_extra_config":{"load_async":false,"lookup_async":false,"cache_prefix":"%s"}}' \
        "$cache_prefix"
    local extra_args="--tensor-parallel-size 2 --max-model-len 4096 --gpu-memory-utilization 0.75 --enable-prefix-caching --kv-transfer-config '${kv_config}'"
    local env_vars
    env_vars="CUDA_VISIBLE_DEVICES=${visible_devices} MOONCAKE_STORE_BACKEND=rs MOONCAKE_CONFIG_PATH=${config_path} MOONCAKE_REQUESTER_LOCAL_HOSTNAME=${host} MC_STORE_RS_METADATA_URL=${redis_url} MC_STORE_RS_TRANSPORT_METADATA_URL=${redis_url} MC_STORE_RS_TRANSPORT_BACKEND=${MC_STORE_RS_TRANSPORT_BACKEND:-classic-te} MC_STORE_RS_ROUTE_CONTROL=embedded-wrh MC_STORE_RS_REPLICA_COUNT=1 MC_STORE_RS_KEYSPACE=mc/store-rs/tone-vllm/${cache_prefix} MC_STORE_RS_STABLE_ID=vllm-${host}-${port}"

    launch_vllm_server \
        "$model_name" "$host" "$port" "$log_path" "store-rs-${port}" \
        "$extra_args" "$env_vars"
}

start_redis() {
    local redis_port=$1
    if ! ${docker_exec} "command -v redis-server >/dev/null && command -v redis-cli >/dev/null"; then
        echo "Installing Redis for Store-RS metadata..."
        if ! ${docker_exec} "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y redis-server redis-tools"; then
            echo "ERROR: failed to install Redis in the vLLM test image" >&2
            return 1
        fi
    fi
    # The remote T-One node must reach this ephemeral metadata service over the
    # private test network, so disable Redis protected mode for this process.
    ${docker_exec} "redis-server --bind 0.0.0.0 --protected-mode no --port ${redis_port} --save '' --appendonly no --daemonize yes"
    for _ in $(seq 1 50); do
        if ${docker_exec} "redis-cli -h 127.0.0.1 -p ${redis_port} PING" | grep -q PONG; then
            return 0
        fi
        sleep 0.2
    done
    echo "ERROR: Store-RS Redis metadata did not become ready" >&2
    return 1
}

stop_redis() {
    local redis_port=$1
    ${docker_exec} "redis-cli -h 127.0.0.1 -p ${redis_port} shutdown nosave" \
        >/dev/null 2>&1 || true
}

send_request() {
    local host=$1
    local port=$2
    local model_name=$3
    local response_file=$4
    local repeated_context=""
    local _
    for _ in $(seq 1 80); do
        repeated_context+="France is a country in Europe with a long cultural history. "
    done

    local payload response_body status_code
    payload=$(jq -n \
        --arg model "$model_name" \
        --arg prompt "${repeated_context}Based only on this context, what is the capital of France?" \
        '{model: $model, messages: [{role: "user", content: $prompt}], stream: false, max_tokens: 32, temperature: 0}')
    response_body=$(curl -sS -w $'\n%{http_code}' \
        -X POST "http://${host}:${port}/v1/chat/completions" \
        -H "Content-Type: application/json" \
        -d "$payload" --max-time 180)
    status_code=$(printf '%s\n' "$response_body" | tail -n 1)
    response_body=$(printf '%s\n' "$response_body" | sed '$d')
    printf '%s\n' "$response_body" >"$response_file"
    validate_api_response \
        "$response_body" "$status_code" ".choices[0].message.content" "paris"
}

store_metric_value() {
    local host=$1
    local port=$2
    local operation=$3
    local suffix=$4
    curl -fsS "http://${host}:${port}/metrics" | awk \
        -v operation="${operation}" \
        -v metric="vllm:mooncake_store_operation_${suffix}" '
            index($1, metric) == 1 && index($0, "operation=\"" operation "\"") {
                value += $NF
            }
            END { print value + 0 }
        '
}

wait_for_store_save() {
    local host=$1
    local port=$2
    local deadline=$((SECONDS + 60))
    local saved failed
    while (( SECONDS < deadline )); do
        saved=$(store_metric_value "$host" "$port" save_put keys_total) || saved=0
        failed=$(store_metric_value "$host" "$port" save_put failed_keys_total) || failed=0
        if (( failed > 0 )); then
            echo "ERROR: vLLM reported ${failed} failed Store-RS save keys" >&2
            return 1
        fi
        if (( saved > 0 )); then
            echo "vLLM saved ${saved} KV key(s) into Store-RS"
            return 0
        fi
        sleep 1
    done
    echo "ERROR: vLLM did not save any KV keys into Store-RS within 60 seconds" >&2
    return 1
}

assert_store_load() {
    local host=$1
    local port=$2
    local loaded failed
    loaded=$(store_metric_value "$host" "$port" load_get keys_total) || loaded=0
    failed=$(store_metric_value "$host" "$port" load_get failed_keys_total) || failed=0
    if (( failed > 0 )); then
        echo "ERROR: vLLM reported ${failed} failed Store-RS load keys" >&2
        return 1
    fi
    if (( loaded == 0 )); then
        echo "ERROR: the second vLLM instance loaded no KV keys from Store-RS" >&2
        return 1
    fi
    echo "vLLM loaded ${loaded} KV key(s) from Store-RS"
}

run_single_model() {
    local model_name=$1
    local model_name_clean
    model_name_clean=$(sanitize_model_name "$model_name")
    local redis_port cache_prefix status=0
    redis_port=$(python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("0.0.0.0", 0))
    print(sock.getsockname()[1])
PY
)
    cache_prefix="tone-${RANDOM}-${RANDOM}"
    setup_log_directory_dual "$test_case_name" "$model_name_clean"

    if ! start_redis "$redis_port"; then
        status=1
    elif ! start_server "$model_name" "$model_name_clean" "$redis_port" "$cache_prefix"; then
        status=1
    elif ! ${SSH_CMD} "${REMOTE_SSH_TARGET:-$REMOTE_IP}" \
        "source '$REMOTE_TEST_DIR/run/.shrc'; cd \"\$E2E_DIR/scripts\"; MC_STORE_RS_TRANSPORT_BACKEND='${MC_STORE_RS_TRANSPORT_BACKEND:-classic-te}' ./${test_case_name}.sh start_server '$model_name' '$model_name_clean' '$redis_port' '$cache_prefix'"; then
        status=1
    else
        local result_dir="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${model_name_clean}"
        send_request "$LOCAL_IP" 8020 "$model_name" "${result_dir}/local_response.json" || status=1
        # MooncakeStoreConnector publishes completed prompt blocks asynchronously;
        # wait for its connector metric instead of using an arbitrary sleep.
        wait_for_store_save "$LOCAL_IP" 8020 || status=1
        send_request "$REMOTE_IP" 8010 "$model_name" "${result_dir}/remote_response.json" || status=1
        assert_store_load "$REMOTE_IP" 8010 || status=1
    fi

    cleanup_model_processes "$PID_DIR" "$test_case_name" || status=1
    stop_redis "$redis_port"
    return "$status"
}

run_test() {
    if [[ -z "${REMOTE_IP:-}" || -z "${LOCAL_IP:-}" ]]; then
        echo "ERROR: Store-RS vLLM sharing requires local and remote T-One nodes" >&2
        return 1
    fi
    if ! ${docker_exec} "MOONCAKE_STORE_BACKEND=rs python3 -c 'import mooncake._store_rs; import mooncake.store as store; assert store._BACKEND == \"rs\"'"; then
        echo "ERROR: local vLLM image does not have the Store-RS backend" >&2
        return 1
    fi
    if ! ${SSH_CMD} "${REMOTE_SSH_TARGET:-$REMOTE_IP}" \
        "source '$REMOTE_TEST_DIR/run/.shrc'; MOONCAKE_STORE_BACKEND=rs docker exec '$CONTAINER_NAME' python3 -c 'import mooncake._store_rs; import mooncake.store as store; assert store._BACKEND == \"rs\"'"; then
        echo "ERROR: remote vLLM image does not have the Store-RS backend" >&2
        return 1
    fi
    run_single_model "${SUPPORT_MODELS[0]}"
}

parse() {
    local test_exit_code=$1
    if [[ "$test_exit_code" -eq 0 ]]; then
        save_test_result "$test_case_name" "Pass" "${BASE_DIR}/${TEST_CASE_RESULT_PATH}"
        return 0
    fi
    save_test_result "$test_case_name" "Fail" "${BASE_DIR}/${TEST_CASE_RESULT_PATH}"
    return 1
}

case "${1:-}" in
    start_server)
        shift
        start_server "$@"
        ;;
    stop_server)
        cleanup_model_processes "$PID_DIR" "$test_case_name"
        ;;
    "")
        exit_code=0
        run_test || exit_code=$?
        parse "$exit_code"
        ;;
    *)
        echo "unknown command: $1" >&2
        exit 2
        ;;
esac
