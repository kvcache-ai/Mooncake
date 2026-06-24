# the file provides is demo to test mooncake connector performance when work with vllm

set -ex

VLLM_SRC_PATH=${VLLM_SRC_PATH:-"vllm-src"}
MOONCAKE_CONFIG_PATH=${MOONCAKE_CONFIG_PATH:-"mooncake.json"}
MODEL=${MODEL:-"Qwen/Qwen2.5-7B-Instruct"}
DEMO_PATH=${DEMO_PATH:-"../proxy_demo.py"}
NUM_PREFILL=${NUM_PREFILL:-"4"}
NUM_DECODE=${NUM_DECODE:-"4"}
PREFIX_LEN=${PREFIX_LEN:-"50"}
NUM_FOLDS=${NUM_FOLDS:-"20"}
MASTER_PORT=${MASTER_PORT:-"10001"}
METADATA_PORT=${METADATA_PORT:-"2379"}
PREFILL_PORT_BASE=${PREFILL_PORT_BASE:-"8100"}
DECODE_PORT_BASE=${DECODE_PORT_BASE:-"8200"}
PROXY_PORT=${PROXY_PORT:-"8000"}
logs_root=${LOG_ROOT:-"logs"}
results_root=${RESULT_ROOT:-"results"}

PROXY_ID=0
CUDA_VISIBLE_ID=0

INPUT_LENS=(1024 4096)
OUTPUT_LENS=(6 256)

export VLLM_WORKER_MULTIPROC_METHOD=spawn
export VLLM_USE_V1=0

wait_for_server() {
  # wait for vllm server to start
  # return 1 if vllm server crashes
  local port=$1
  timeout 1200 bash -c "
    until curl -s http://localhost:${port}/v1/models > /dev/null; do
      sleep 1
    done" && return 0 || return 1
}

get_related_pids()
{
  local pid=${1}
  [ -z "$pid" ] && echo ""
  ps -ef | grep "$pid" | grep -v 'grep' | awk -F ' ' '{print $2}' | tr '\n' ' '
}

destroy_vllm_engine()
{
  local port=$1
  local main_pid=$(ps -ef | grep 'vllm.entrypoints.openai.api_server' | grep "port ${port}" | awk -F ' ' '{print $2}')
  if [ -n "${main_pid}" ]; then
    local related_pids=$(get_related_pids "${main_pid}" | sed 's/^[ \t]*//;s/[ \t]*$//')
    for pid in ${related_pids}
    do
      related_pids="${related_pids} $(get_related_pids $pid)"
    done
    if [ -n "$(echo "${related_pids}" | sed 's/^[ \t]*//;s/[ \t]*$//')" ];then
      kill -9 ${related_pids}
    fi
  fi
  sleep 5
}

kill_nodes() {
  # kill all processes by port
  lsof -t -i:$PROXY_PORT | xargs -r kill -9
  lsof -t -i:$METADATA_PORT | xargs -r kill -9
  for ((i=0; i<NUM_PREFILL; i++)); do
    destroy_vllm_engine $((${PREFILL_PORT_BASE} + i))
  done
  for ((i=0; i<NUM_DECODE; i++)); do
    destroy_vllm_engine $((${DECODE_PORT_BASE} + i))
  done
  lsof -t -i:$MASTER_PORT | xargs -r kill -9
  sleep 20
}

kill_process_by_pid() {
  local pid=$1
  while true; do
    if ! kill $pid 2>/dev/null; then
      echo "Process with PID $pid has been terminated or does not exist."
      break
    else
      echo "Sent termination signal to process with PID $pid, checking..."
      sleep 1
    fi
  done
}

launch_nodes() {
  etcd --listen-client-urls http://localhost:${METADATA_PORT} --advertise-client-urls http://localhost:${METADATA_PORT} > ${logs_root}/metadata.txt 2>&1 &
  nohup mooncake_master --port ${MASTER_PORT} > ${logs_root}/master.txt 2>&1 &
  # launch prefill instance
  for ((i=0; i<NUM_PREFILL; i++)); do
    # Construct the command with the specified port
    CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_ID \
    MOONCAKE_CONFIG_PATH=$MOONCAKE_CONFIG_PATH \
     python3 -m vllm.entrypoints.openai.api_server \
    --model $MODEL \
    --port $((${PREFILL_PORT_BASE} + i)) --max-model-len 10000 --gpu-memory-utilization 0.8 \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_producer"}' \
    > ${logs_root}/prefill-${i}.txt 2>&1 &
    echo "Launched node on port $PORT"
    CUDA_VISIBLE_ID=$((CUDA_VISIBLE_ID + 1))
  done
  # launch decode instance
  for ((i=0; i<NUM_DECODE; i++)); do
    # Construct the command with the specified port
    CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_ID \
    MOONCAKE_CONFIG_PATH=$MOONCAKE_CONFIG_PATH \
     python3 -m vllm.entrypoints.openai.api_server \
    --model $MODEL \
    --port $((${DECODE_PORT_BASE} + i))  --max-model-len 10000 --gpu-memory-utilization 0.8 \
    --kv-transfer-config '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_consumer"}' \
    > ${logs_root}/decode-${i}.txt 2>&1 &
    echo "Launched node on port $PORT"
    CUDA_VISIBLE_ID=$((CUDA_VISIBLE_ID + 1))
  done
  for ((i=0; i<NUM_PREFILL; i++)); do
    wait_for_server $((${PREFILL_PORT_BASE} + i))
    PORT=$((PORT + 1))
  done
    for ((i=0; i<NUM_DECODE; i++)); do
    wait_for_server $((${DECODE_PORT_BASE} + i)) 
    PORT=$((PORT + 1))
  done
  echo "All $NUM VLLM node have been launched."
}

launch_disagg_proxy() {
  if [ $# -ne 2 ]; then
      echo "Usage: launch_disagg_proxy <num_prefill> <num_decode>"
      return 1
  fi
  if [ "$PROXY_ID" -ne 0 ]; then
      kill_process_by_pid $PROXY_ID
  fi
  num_prefill=$1
  num_decode=$2
  prefill_ports=()
  for (( i=0; i<num_prefill; i++ )); do
    prefill_ports+=("localhost:$((PREFILL_PORT_BASE + i))")
  done    
  decode_ports=()
  for (( i=0; i<num_decode; i++ )); do
    decode_ports+=("localhost:$((DECODE_PORT_BASE + i))")
  done

  prefill_ports_str="${prefill_ports[@]}"
  decode_ports_str="${decode_ports[@]}"
  python3 $DEMO_PATH \
  --model $MODEL \
  --prefill $prefill_ports_str \
  --decode $decode_ports_str \
  --port $PROXY_PORT \
  2>&1 | tee ${logs_root}/proxy-${num_prefill}-${num_decode}.txt 2>&1 &
  PROXY_ID=$(ps -ef | grep $DEMO_PATH | grep "port $PROXY_PORT" | awk -F ' ' '{print $2}')
  echo "Launched disagg_proxy with PID: $PROXY_ID"
  sleep 1
}

benchmark() {
  dataset_name="random"
  prefix_len=50
  file_prefix=$1
  shift
  for max_concurrency in "$@"; do
    num_prompts=$(( max_concurrency * NUM_FOLDS ))
    for input_len in ${INPUT_LENS[@]}; do
      for output_len in ${OUTPUT_LENS[@]}; do
        input_len_name=$(printf %04d $input_len)
        output_len_name=$(printf %04d $output_len)
        max_concurrency_name=$(printf %03d $max_concurrency)
        python3 $VLLM_SRC_PATH/benchmarks/benchmark_serving.py \
              --backend vllm \
              --model ${MODEL} \
              --dataset-name random \
              --random-input-len $input_len \
              --random-output-len $output_len \
              --random-prefix-len ${PREFIX_LEN} \
              --num-prompts $num_prompts \
              --max-concurrency=${max_concurrency} \
              --trust-remote-code \
              --ignore_eos \
              --port ${PROXY_PORT} \
              --save-result \
              --percentile-metrics="ttft,tpot,itl,e2el" \
              --result-dir=${results_root} \
              --result-filename=${file_prefix}-input-${input_len_name}-output-${output_len_name}-concurrency-${max_concurrency_name}-serving.json \
              2>&1 | tee ${logs_root}/${file_prefix}-${input_len_name}-output-${output_len_name}-concurrency-${max_concurrency_name}-serving.txt
        sleep 2
      done
    done
  done
}

prepare_env(){
  (which wget && which curl) || (apt-get update && apt-get install -y wget curl)
  (which git) || (apt-get -y install git)
  (which socat) || (apt-get -y install socat)
  pip install vllm
  pip install quart httpx matplotlib aiohttp pandas datasets
  if ! [ -d $VLLM_SRC_PATH ]; then
    git clone https://github.com/vllm-project/vllm.git $VLLM_SRC_PATH
  fi
}

main() {
  prepare_env
  results_root=${results_root}-$(date "+%Y%m%d-%H:%M:%S")
  logs_root=${logs_root}-$(date "+%Y%m%d-%H:%M:%S")
  mkdir -p $results_root
  mkdir -p $logs_root
  echo "Results will be saved to $results_root"
  echo "Logs will be saved to $logs_root"

  export VLLM_HOST_IP=$(hostname -I | awk '{print $1}')
  kill_nodes
  ## launch instances.
  launch_nodes

  launch_disagg_proxy 1 1
  benchmark proxy-1-1 1 4 8 16

  launch_disagg_proxy 2 1
  benchmark proxy-2-1 4 8 16

  launch_disagg_proxy 2 2
  benchmark proxy-2-2 4 8 16

  launch_disagg_proxy 2 4
  benchmark proxy-2-4 4 8 16

  launch_disagg_proxy 4 4
  benchmark proxy-4-4 4 8 16

  kill_nodes

  python3 parse_results.py $results_root $results_root/result.xlsx
}

main "$@"