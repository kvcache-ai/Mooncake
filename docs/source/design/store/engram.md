# EngramStore Backend

EngramStore reads immutable tables of opaque byte rows. Models own hashing,
quantization, scales, projections and distributed communication.

This implementation supports local CPU and CUDA lookup only. The table/row
contract is designed to accommodate future remote backends, but remote
Mooncake Store execution is not supported yet.

## Table API

The table API adds no public configuration or lookup-handle class. Import
`EngramStore` from `mooncake.engram`, or from `mooncake.store` with the C++ Store
backend.

```python
import numpy as np
from mooncake.engram import EngramStore

specs = {"layer1/head0": {"num_rows": 17, "row_bytes": 264}}
store = EngramStore(tables=specs, namespace="model-revision/layout-v1",
                    local_dir="/dev/shm/engram")
rows = np.zeros((17, 264), dtype=np.uint8)
store.populate("layer1/head0", rows)
ids = np.array([[0], [16]], dtype=np.int64)
output = np.empty((2, 1, 264), dtype=np.uint8)
store.lookup(["layer1/head0"], ids, output)
store.close()
```

- `EngramStore(*, tables, namespace, local_dir=None, store_client=None, persistent=True)`:
  use `local_dir` for local storage. `store_client` is reserved for a future
  remote backend and currently raises `NotImplementedError`. Each table has a
  nonnegative `num_rows` and positive `row_bytes`. The namespace identifies immutable
  weights and their physical layout, including any rank-specific sharding.
- `populate(table_id, rows)`: copies contiguous CPU `uint8 [N,R]`
  data. Local publication is create-only, locked across processes and atomic.
  The input can be released after return. Existing tables are never overwritten.
- `lookup(table_ids, row_ids, output, *, stream=None, offsets=None)`: reads IDs
  `[T,K]` into caller-owned `uint8 [T,K,R]` output. The K table names may repeat,
  and all selected rows have width R. CPU IDs are NumPy arrays or CPU tensors
  of int32/int64; CUDA IDs and output are CUDA tensors on the current device.
- `get_table_spec(table_id)`: returns a copy of the two-field layout.
- `close()`: waits for outstanding local CUDA work and releases mappings.
  Idempotent; also available through the context-manager protocol.
- `remove(table_id)`: explicitly removes a published table. Close all readers
  first, then remove through a separate instance that has not mapped the table.

`offsets` contains one nonnegative int64 offset per selected table. A valid ID
is converted to a physical row by subtracting that offset. ID -1 is handled
first and leaves the corresponding output bytes untouched. Every other ID must
lie in the selected table's range. CPU bounds errors are raised before writes;
CUDA bounds errors are reported asynchronously as device errors, and output is
invalid. Do not use invalid IDs to request zero padding: initialize output and
use -1 instead.

Row bytes must be contiguous. Nonoverlapping positive outer strides support
both token-major and head-major output; strided ID matrices are also accepted.
IDs and output must not overlap. Empty token batches are supported.

## Local CUDA execution

Build with `USE_CUDA=ON` (for wheel builds, pass
`-Ccmake.define.USE_CUDA=ON` to pip). The private CUDA extension has no PyTorch
build dependency. Local tables are memory mapped and registered as portable
CUDA host memory. The device gather kernel reads requested rows directly from
host memory and writes output on the supplied stream.

```bash
pip install '.[engram]' -Ccmake.define.USE_CUDA=ON
```

```python
import torch

ids = torch.tensor([[0], [16]], dtype=torch.int64, device="cuda")
output = torch.empty((2, 1, 264), dtype=torch.uint8, device="cuda")
store = EngramStore(tables=specs, namespace="model-revision/layout-v1",
                    local_dir="/dev/shm/engram")
stream = torch.cuda.current_stream()
store.lookup(["layer1/head0"], ids, output, stream=stream.cuda_stream)
# Subsequent work on this stream sees complete rows.
stream.synchronize()
store.close()
```

The CPU overload is synchronous. Supplying a CUDA stream enqueues the complete
lookup and returns `None`; consumers use ordinary stream ordering or CUDA
events. Keep IDs, output, and the Store alive until completion. Warm up each
(table names, offsets, device) combination before capturing a CUDA graph, and
destroy all graphs referring to a Store before closing it. Replays read current
IDs from the captured tensor addresses.

With the default `persistent=True`, local files survive close and can be reopened using the same namespace and
layout. Reopening checks the stored layout and file length. Host RAM must fit
all registered table pages, even when the directory is on disk. This initial
implementation pins entire selected tables and does not implement a page cache.

For worker-private tables, pass `persistent=False` with `local_dir`. Population
creates an anonymous file mapping on that filesystem; it has no named file to
leave behind after a worker is killed. These tables are isolated per Store
instance, cannot be reopened by namespace, and are released by `close()` or
process exit. `remove()` is not needed or supported in this mode.

## Future remote backends

`lookup(table_ids, row_ids, output, *, stream=None, offsets=None)` describes
requested rows and caller-owned output without exposing table addresses or a
transport. A future remote implementation can use the same contract, including
the requirement that consumers on the supplied CUDA stream observe complete
output. This does not promise CUDA graph support for every future backend.

Remote CPU and CUDA execution, transport selection and remote benchmarks are
not implemented here. Passing `store_client` fails explicitly rather than
falling back to local storage.

## Model coverage

The same interface represents N-gram/hash embeddings and direct token-indexed
per-layer embeddings. Models keep hashing, quantization interpretation, scales,
projections, request history and distributed communication in their own code.

| Model family | Stored rows | Model-side interpretation |
| --- | --- | --- |
| DeepSeek V4.1 | FP8 weights and E8M0 scales packed per row | Block dequantization and TP/DP/CP communication |
| Qwen4-Exp / Qwen3.8-Flash-Next | N-gram / PLE rows | Table scale, short convolution and request history |
| LongCat Ngram | Over-embedding rows | Hashing, projection and token history |
| Gemma3n / Gemma4 | Per-layer input embedding rows | Direct token indexing, scaling, projection and gating |

The table/row contract keeps model-side interpretation separate from storage.
The current local implementation supports CPU and CUDA lookup; remote lookup
is future work.

## Verification

Run `pytest -q scripts/test_engram_tables.py` for local table contracts and CUDA
graph replay, and verifies that remote backend selection is rejected.
`scripts/bench_engram_store_27b.py` benchmarks the same API against local storage.

The SGLang integration was tested end to end with real weights for all five
checkpoints in the four families above. Under matched controlled configurations,
all 30 request outputs matched the native implementation exactly, including
output token logprobs. CUDA graph decode was exercised on every checkpoint.
Gemma3n and Qwen used deterministic inference; DeepSeek, Qwen and LongCat used
fixed KV capacity and serialized requests. These are functional correctness
checks for local storage, not a performance benchmark or remote validation.
See [the companion SGLang PR](https://github.com/sgl-project/sglang/pull/44521)
for the complete test configuration.
