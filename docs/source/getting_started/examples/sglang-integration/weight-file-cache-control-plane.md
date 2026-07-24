# Mooncake File-level Model Weight Cache

This page describes the experimental file-level model weight cache control
plane for Mooncake Store.

The feature keeps model artifacts in their normal HuggingFace-style directory
layout. Operators import a local model directory into Mooncake Store, and
Mooncake records a file manifest for operational query, verification,
materialization, and deletion.

## Runtime Topology

Run the weight management CLI as a pure Mooncake Store client. A separate
standalone Store service owns the memory or SSD capacity:

```text
mooncake_master
  |
  v
python -m mooncake.mooncake_store_service
  # resource owner: global_segment_size > 0, local_buffer_size = 0

python -m mooncake.weight_store.cli
  # pure client: contributes no segment; client buffer is fixed internally
```

Start the master with the embedded HTTP metadata server:

```bash
mooncake_master \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080 \
  --rpc_address=127.0.0.1 \
  --rpc_port=50051
```

Create a Store service config:

```json
{
  "local_hostname": "storage-node:12345",
  "metadata_server": "http://127.0.0.1:8080/metadata",
  "global_segment_size": "300GB",
  "local_buffer_size": "0",
  "protocol": "tcp",
  "device_name": "",
  "master_server_address": "127.0.0.1:50051"
}
```

Start the standalone Store service:

```bash
python -m mooncake.mooncake_store_service \
  --config=/etc/mooncake/store-node.json \
  --port=8081
```

Do not use the weight management CLI as a temporary resource owner. The CLI is
always a pure client: it contributes no storage segment, so the model cache
lifetime is never tied to a short-lived CLI process. Scale the standalone Store
service to provide capacity instead.

## Import

When running from a source checkout without installing the wheel, use:

```bash
PYTHONPATH=mooncake-wheel python3 -m mooncake.weight_store.cli ...
```

After installing the wheel, use the console script directly:

```bash
mooncake_weight_store ...
```

Store connection settings can be written once:

```bash
mooncake_weight_store \
  --local-hostname cli-client:12346 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server-addr 127.0.0.1:50051 \
  --protocol tcp \
  config init
```

`config init` stores connection settings only. Use `--protocol rdma` together
with `--rdma-devices mlx5_0,mlx5_1` for RDMA transport; leave the default `tcp`
when RDMA hardware is not available.

By default this writes `~/.config/mooncake/weight_store.json`. Use
`--config /path/to/weight_store.json` or the `MOONCAKE_WEIGHT_STORE_CONFIG`
environment variable to select another file. Command-line flags still override
values loaded from the config file.

```bash
mooncake_weight_store model import \
  --checkpoint-id qwen2_5_32b_main \
  --model-id Qwen/Qwen2.5-32B-Instruct \
  --revision main \
  --source /models/qwen2.5-32b \
  --replica-num 1 \
  --file-chunk-size 64MB
```

`--replica-num` and `--file-chunk-size` are import-only options; other model
commands do not accept them.

The import command scans the source directory, validates
`model.safetensors.index.json` when present, streams files into fixed-size
chunk objects, writes weight chunks as `ObjectDataType.WEIGHT`, writes
config/tokenizer/index chunks as `ObjectDataType.METADATA`, and stores a final
manifest at:

```text
weight/models/{checkpoint_id}/manifest
```

Each manifest file record remains file-level and includes the chunk keys used
for storage:

```text
weight/models/{checkpoint_id}/files/{relative_path_sha256}/chunks/{chunk_index}
```

Import rejects an existing `checkpoint_id`. If object writes fail after import
has started, the tool writes a `FAILED` manifest so operators can inspect the
partial state.

## Operations

```bash
mooncake_weight_store model list
mooncake_weight_store model inspect qwen2_5_32b_main
mooncake_weight_store model verify qwen2_5_32b_main
mooncake_weight_store model materialize-file qwen2_5_32b_main \
  --path config.json \
  --output /tmp/config.json
mooncake_weight_store model delete qwen2_5_32b_main
```

`verify` checks that the checkpoint is `READY`, every manifest file object is
readable, each object's size and sha256 match the manifest, and the
`model.safetensors.index.json` references only files present in the manifest.

`delete` removes file objects, the manifest, and the index entries.

## Implementation Status

The current prototype includes:

- File-level `model import/list/inspect/verify/materialize-file/delete`.
- A config file for Store connection settings.
- Pure-client CLI operation that contributes no Store segment.
- File-level manifests backed by chunk-level Store objects.
- Safetensors index validation.
- `ObjectDataType.WEIGHT` and `ObjectDataType.METADATA` tagging.
- Import progress output with file count, imported bytes, percentage, average
  throughput, and ETA.
- Failed manifests for interrupted imports, with cleanup of the current file's
  partial chunks.

Validated scenarios:

- Unit tests for model cache and CLI behavior.
- Standalone Store service started with
  `python -m mooncake.mooncake_store_service`.
- MiniMax-M2.7 import and verify with a 300 GB Store segment.

## MiniMax Smoke Script

Run this script inside the test container. It does not include a `docker exec`
wrapper. It starts a fresh master, starts the standalone Store service with a
300 GB segment, configures the weight CLI as a pure client, imports MiniMax, and
then verifies the checkpoint.

```bash
#!/usr/bin/env bash
set -euo pipefail

export CODE=${CODE:-/tmp/mooncake-file-level}
export MODEL_DIR=${MODEL_DIR:-/data0/models/MiniMax-M2.7}
export CHECKPOINT_ID=${CHECKPOINT_ID:-MiniMax-M2.7}
export MODEL_ID=${MODEL_ID:-MiniMax-M2.7}
export STORE_SIZE=${STORE_SIZE:-300GB}
export CHUNK_SIZE=${CHUNK_SIZE:-64MB}

export PYTHONPATH=$CODE/mooncake-wheel
export LD_LIBRARY_PATH=/usr/local/lib/python3.12/dist-packages/mooncake_transfer_engine.libs:/usr/local/lib/python3.12/dist-packages/mooncake:$CODE/mooncake-wheel/mooncake

python3 - <<'PY'
import os
import signal
import subprocess

patterns = (
    "mooncake_master",
    "mooncake_http_metadata_server",
    "mooncake.mooncake_store_service",
    "/tmp/weight_store_node.py",
    "/tmp/storage_node.py",
)

for line in subprocess.check_output(["ps", "-ef"], text=True).splitlines():
    if any(pattern in line for pattern in patterns) and "python3 - <<'PY'" not in line:
        try:
            os.kill(int(line.split()[1]), signal.SIGTERM)
        except ProcessLookupError:
            pass
PY

sleep 2

nohup /usr/local/lib/python3.12/dist-packages/mooncake/mooncake_master \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080 \
  --rpc_address=127.0.0.1 \
  --rpc_port=50051 \
  --logtostderr=true \
  >/tmp/mooncake_master.log 2>&1 &

sleep 3

cat >/tmp/mooncake-store-node.json <<JSON
{
  "local_hostname": "storage-node:12345",
  "metadata_server": "http://127.0.0.1:8080/metadata",
  "global_segment_size": "$STORE_SIZE",
  "local_buffer_size": "0",
  "protocol": "tcp",
  "device_name": "",
  "master_server_address": "127.0.0.1:50051"
}
JSON

nohup python3 -m mooncake.mooncake_store_service \
  --config=/tmp/mooncake-store-node.json \
  --port=8081 \
  >/tmp/mooncake_store_service.log 2>&1 &

sleep 8
tail -80 /tmp/mooncake_store_service.log

python3 -m mooncake.weight_store.cli \
  --config /tmp/weight-store-test.json \
  config init \
  --local-hostname cli-client:12346 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server-addr 127.0.0.1:50051 \
  --protocol tcp

cat /tmp/weight-store-test.json

python3 -m mooncake.weight_store.cli \
  --config /tmp/weight-store-test.json \
  model delete "$CHECKPOINT_ID" || true

python3 -m mooncake.weight_store.cli \
  --config /tmp/weight-store-test.json \
  model import \
  --checkpoint-id "$CHECKPOINT_ID" \
  --model-id "$MODEL_ID" \
  --revision local \
  --source "$MODEL_DIR" \
  --replica-num 1 \
  --file-chunk-size "$CHUNK_SIZE"

python3 -m mooncake.weight_store.cli \
  --config /tmp/weight-store-test.json \
  model list

python3 -m mooncake.weight_store.cli \
  --config /tmp/weight-store-test.json \
  model verify "$CHECKPOINT_ID"
```

## SGLang Connector

This control plane only handles import/serve on the store side. Loading a
`mooncake://` checkpoint into SGLang requires the companion connector that ships
in the SGLang tree (`python/sglang/srt/connector/mooncake_store_file.py`); it is
not part of this repository. With that connector present, SGLang loads directly
from Mooncake Store through the standard remote loader:

```bash
python -m sglang.launch_server \
  --model mooncake://MiniMax-M2.7 \
  --served-model-name MiniMax-M2.7 \
  --load-format remote \
  --model-loader-extra-config '{
    "mooncake_weight_store_config": "/tmp/weight-store-test.json",
    "materialize_dir": "/tmp/mooncake-sglang-models"
  }'
```

`--served-model-name` is required because the `mooncake://` URL contains a colon,
which SGLang otherwise reserves for LoRA `model:adapter` syntax.

The connector reads Mooncake chunks sequentially, parses the safetensors header,
identifies tensor boundaries from `data_offsets`, and streams tensors to SGLang
without materializing complete weight files on local disk. Only small
metadata/tokenizer/config files are materialized under `materialize_dir`. Reads
coalesce each tensor batch into one range request against a reused, registered
RDMA buffer.

## Current Limitations

This first slice is file-level only. It does not create tensor-level
checkpoints, and does not yet implement serving leases or a production catalog
backend. The model index is stored as Mooncake objects for the prototype.
