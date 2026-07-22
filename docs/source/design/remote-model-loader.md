# Remote Model Loader

Mooncake can persist and reload sharded model weights through the Store using
the same key layout as SGLang's remote model loader
(`mooncake:///<model_name>`).

The implementation lives in `mooncake.model_loader` and is inspired by the
SGLang `RemoteModelLoader` + `MooncakeStoreConnector` design.

## Key layout

For URL `mooncake:///<model_name>` and tensor-parallel rank `r`:

- Weights: `{model_name}/keys/rank_{r}/{param_name}`
- Sidecar files: `{model_name}/files/{filename}` (`config.json`, tokenizer, …)
- Index sentinel for `list()`: `__index__{prefix}`

Load fills an already-constructed `nn.Module` **in place** via
`batch_get_into` after `register_buffer` (RDMA path) or byte copy (standalone
client path).

## Seed the Store

### From a local / remote safetensors checkpoint

```python
from mooncake.model_loader import seed_model_from_files

seed_model_from_files(
    "/data/ckpts/llama",
    "mooncake:///llama-tp1",
    tp_rank=0,
)
# Also supports s3:// via fsspec + storage_options
```

Requires `pip install "mooncake-transfer-engine[file-io]"` (and `s3fs` for S3).

### From an SGLang Engine (reference CLI)

Configure the Store, then:

```bash
export MOONCAKE_MASTER=<master>:50051
export MOONCAKE_TE_META_DATA_SERVER=http://<master>:8080/metadata
export MOONCAKE_PROTOCOL=rdma   # or tcp

python -m mooncake.model_loader.save_model_remote \
  --model Qwen/Qwen3-8B \
  --url mooncake:///qwen3-tp8 \
  --tp 8
```

This mirrors `python -m sglang.save_model_remote_loader` and requires an SGLang
build that exposes `Engine.save_remote_model`.

### From a Python `nn.Module`

```python
from mooncake.model_loader import save_model

save_model(model, "mooncake:///my-model", model_path="/path/to/hf", rank=0)
```

## Load with engines

### SGLang (upstream remote format)

```bash
python -m sglang.launch_server \
  --load-format remote \
  --model mooncake:///qwen3-tp8 \
  --tokenizer-path Qwen/Qwen3-8B \
  --tp 8
```

SGLang may use its in-tree connector; the key layout matches Mooncake's
first-party connector so the same Store entries work.

### SGLang (`load_format=mooncake` via Mooncake wrapper)

```bash
python -m mooncake.model_loader.sglang_launch \
  --load-format mooncake \
  --model mooncake:///qwen3-tp8 \
  --tokenizer-path Qwen/Qwen3-8B
```

### vLLM plugin

```bash
vllm serve ... --load-format mooncake \
  --model-loader-extra-config '{"url":"mooncake:///qwen3-tp8"}'
```

The `vllm.general_plugins` entry point
`mooncake.model_loader.vllm_loader:register` registers the loader when the
wheel is installed alongside vLLM.

## Configuration

Store connection settings come from the usual `MOONCAKE_*` variables (see
`MooncakeConfig`) or from `model_loader_extra_config` JSON keys such as
`master` / `master_server_address`, `protocol`, `device_name`,
`metadata_server`, `standalone_storage`, and `client_server_address`.

Optional:

- `MOONCAKE_STANDALONE_STORAGE=true` + `MOONCAKE_CLIENT=host:port` for the
  standalone client path (chunked put/get).
- `MOONCAKE_CONNECTOR_PREFERRED_SEGMENTS=seg0,seg1`

## Related

- {doc}`artifact-file-io` — fsspec tensor file I/O used by file seeding
- RFC [#2868](https://github.com/kvcache-ai/Mooncake/issues/2868) — unified
  artifact I/O roadmap
- SGLang PR [#21661](https://github.com/sgl-project/sglang/pull/21661) —
  upstream remote Mooncake weight load/save
