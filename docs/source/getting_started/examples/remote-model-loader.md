---
orphan: true
---

# Remote model loader (SGLang / vLLM)

See the design doc {doc}`../../design/remote-model-loader` for the full API.

## Quick recipes

Seed from safetensors into Mooncake Store:

```python
from mooncake.model_loader import seed_model_from_files

seed_model_from_files(
    "/data/ckpts/model",
    "mooncake:///demo-model",
    tp_rank=0,
)
```

Load with SGLang:

```bash
export MOONCAKE_MASTER=<master>:50051
export MOONCAKE_TE_META_DATA_SERVER=http://<master>:8080/metadata

python -m sglang.launch_server \
  --load-format remote \
  --model mooncake:///demo-model \
  --tokenizer-path /data/ckpts/model \
  --tp 1
```

Load with vLLM:

```bash
vllm serve ... --load-format mooncake \
  --model-loader-extra-config '{"url":"mooncake:///demo-model"}'
```
