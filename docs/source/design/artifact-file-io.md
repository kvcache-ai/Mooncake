# Artifact File I/O

Mooncake Store can persist tensors to external files and load them back into the
distributed store. This is useful for KV cache checkpoints, model-weight cold
start, and RL checkpoint handoff before RDMA distribution to peer nodes.

The Python implementation lives in `mooncake-wheel/mooncake/store_file_io.py` and
is enabled when the `mooncake` package is imported. It patches
`MooncakeDistributedStore` with fsspec-backed helpers; it does **not** start the
C++ engine by itself.

For the longer-term unified API (`save_artifact` / `load_artifact`, `hf://`,
tensor-parallel manifests), see [RFC #2868](https://github.com/kvcache-ai/Mooncake/issues/2868).

## Supported sources (current)

| Source | Status | Notes |
|--------|--------|-------|
| Local path | Supported | `/data/weights/model.safetensors` |
| `file://` URI | Supported | `file:///data/weights/model.safetensors` |
| `.safetensors` | Supported | Recommended format |
| `.pt` / `.pth` (`torch`) | Supported | `torch.load(..., weights_only=True)` by default |
| `s3://` | Supported via fsspec | Requires `s3fs` and credentials in `storage_options` |
| `oss://` | Planned | Needs an fsspec OSS backend (for example `ossfs`) |
| Hugging Face Hub (`hf://`) | Planned | Tracked in RFC #2868 |

Mooncake also ships C++ bindings for **local-only** safetensor export/import
(`load_tensor_from_safetensor` / `save_tensor_to_safetensor` in
`mooncake-integration/store/store_py.cpp`). When the Python patch is active,
those methods delegate to the fsspec layer instead.

## Dependencies

- **Runtime:** `fsspec` (declared in `mooncake-wheel/pyproject.toml`)
- **Optional:** install tensor codecs with the wheel extra:

```bash
pip install "mooncake-transfer-engine[file-io]"
# equivalent to: pip install safetensors torch
```

- **Remote backends:** install the matching fsspec implementation, for example
  `pip install s3fs` for `s3://` paths.

## Workflow

```text
External file (local / file:// / s3://)
  → load_tensor_from_file()  [fsspec open]
  → safetensors.torch.load / torch.load(weights_only=True)
  → put_tensor() into Mooncake Store
  → get_tensor() / RDMA transfer to peers
```

Save is the reverse: `get_tensor()` → encode → write through fsspec.

## Python API

All methods are attached to `MooncakeDistributedStore` after
`import mooncake` (or `from mooncake.store import MooncakeDistributedStore` once
the store module is loaded).

### Generic file I/O

```python
store.save_tensor_to_file(
    key,
    file_name="/data/out/tensor.safetensors",
    format="auto",              # auto | safetensors | torch
    filesystem="auto",          # auto | file | s3 | ...
    storage_options=None,       # passed to fsspec
    tensor_name=None,           # entry name inside safetensors file
) -> int

store.load_tensor_from_file(
    key=None,                   # store key; defaults to file_name when omitted
    file_name="s3://bucket/obj.safetensors",
    format="auto",
    filesystem="auto",
    storage_options={"key": "...", "secret": "..."},
    tensor_name=None,
    map_location=None,
    weights_only=True,          # torch format only
) -> torch.Tensor | None
```

### Safetensor helpers

`save_tensor_to_safetensor` and `load_tensor_from_safetensor` are thin wrappers
around the generic APIs with `format="safetensors"`. They accept the same
`filesystem` and `storage_options` parameters as `save_tensor_to_file` /
`load_tensor_from_file`.

### KV cache aliases

`save_kv_cache_to_file` and `load_kv_cache_from_file` are aliases of the generic
save/load methods for KV cache persistence call sites.

## Examples

### Local safetensors round-trip

```python
import torch
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
# ... setup() ...

tensor = torch.randn(128, 128)
store.put_tensor("session/kv", tensor)
store.save_tensor_to_safetensor("session/kv", "/tmp/kv.safetensors")

store.remove("session/kv")
loaded = store.load_tensor_from_safetensor("session/kv", "/tmp/kv.safetensors")
```

### S3 load (requires `s3fs`)

```python
tensor = store.load_tensor_from_file(
    "weights/shard0",
    "s3://my-bucket/checkpoints/shard0.safetensors",
    storage_options={
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
    },
)
```

### Torch checkpoint file

```python
store.save_tensor_to_file("weights/bias", "/tmp/bias.pt", format="torch")
bias = store.load_tensor_from_file(
    "weights/bias",
    "/tmp/bias.pt",
    format="torch",
    map_location="cpu",
)
```

## Error handling

| Return | Meaning |
|--------|---------|
| `0` | Save succeeded |
| Non-zero `int` | Save failed (`FILE_NOT_FOUND`, `INVALID_PARAMS`, `PERSISTENT_FAIL`, ...) |
| `None` | Load failed or tensor could not be stored |

Missing optional packages (`torch`, `safetensors`) raise `ValueError` with an
install hint; the patch catches these and maps them to error return codes on
save, or `None` on load.

## Testing

Unit tests live in `mooncake-wheel/tests/test_safetensor_functions.py`. They cover
local safetensors, torch `.pt` files, `file://` URIs, and KV cache aliases.
Running them requires a live Mooncake Store master and metadata server:

```bash
pip install safetensors torch fsspec
python -m unittest mooncake-wheel.tests.test_safetensor_functions
```

## Related documentation

- [Mooncake Store Python API](../python-api-reference/mooncake-store) — method
  reference
- [Mooncake Store design](mooncake-store) — in-cluster storage and eviction
- [P2P Store](p2p-store) — hot-path weight distribution after cold load
