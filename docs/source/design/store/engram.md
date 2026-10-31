# EngramStore Backend

EngramStore reads immutable tables of opaque byte rows. Models own hashing,
quantization, scales, projections and distributed communication.

Local tables support CPU and CUDA lookup. Node-sharded CPU tables additionally
support CUDA lookup through DeepEP Engram RDMA READ, using the same byte-row
interface. Storage shard IDs identify nodes, independently of model ranks.

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

## Node-sharded DeepEP backend

Create the Store with the complete table specs, then call `initialize` with only
this node's tables. All tables in one remote Store must have the same row width.
The model supplies `table_owners`; for Engram, a table normally represents one
hash head. The storage API does not infer model/TP/DP/CP ownership.

```python
store = EngramStore(tables=all_head_specs, namespace="checkpoint-revision/layer1",
                    local_dir="/dev/shm/engram", persistent=False)
store.initialize(
    local_head_rows,                      # name -> CPU uint8 [rows, row_bytes]
    shard_id=node_id, num_shards=2,
    table_owners=head_to_node,
    master_addr="http://master-host:8080/metadata",
    session_id="unique-deployment-id",    # identical on every participating node
    caller_id=local_rank, num_callers=8,   # independent of storage shard ID
    max_lookup_rows=786432,               # maximum T*K, including masked entries
    rdma_device=f"mlx5_bond_{local_rank}",
    read_policy="auto",
)
store.lookup(head_names, gpu_ids, gpu_output,
             offsets=head_row_offsets, stream=stream.cuda_stream)
# Destroy graphs and coordinate every participant's shutdown before closing.
store.close()
```

`initialize` publishes immutable member descriptors through the Mooncake
Master's HTTP metadata endpoint, exchanges the NCCL bootstrap ID, creates GIN
connections and registers memory windows, and waits for ready acknowledgements.
It also checks that all callers on a node share the same physical allocation.
The existing `rpc_meta` create-only/idempotent metadata semantics reject duplicate
members. Use a new `session_id` after a restart; failed initialization records
are intentionally not silently overwritten. Set `MC_METADATA_HTTP_TOKEN` when
the metadata service requires authentication. The Master stores bootstrap and
layout information, while NCCL exchanges the underlying registered-window data
inside the initialization operation. Lookup never queries the Master.

One CUDA-VMM **CPU** allocation holds the node's packed head tables. Local caller
processes import the same allocation through an owner-only Unix socket and
POSIX FD transfer. There is no per-caller weight replica. Each caller has its own
GPU receive buffer, GIN context and cross-node rail group, with one caller from
each node. Equal logical address slots accommodate unequal head table lengths;
unused tail rows are never queried. The local share is volatile and released
when its final mapping/FD closes. `local_dir` identifies the local deployment;
remote weights are not persisted as ordinary mmap files. Initial model-side
weight loading can temporarily retain additional CPU copies until publication.

The GPU translates table-local IDs to owner/row addresses. It loads locally
owned rows directly from mapped CPU memory and issues remote GETs through the
unmodified DeepEP Engram READ kernel, then runs its completion kernel and
assembles caller-owned output. Byte widths are padded internally to 8-byte
alignment; 264-byte rows incur no padding. Output order, offsets, repeated table
names, outer strides and `-1` preservation retain their existing meaning.

`read_policy` accepts `native`, `flat`, or `auto`. Native submits `[T,K]` indices;
flat submits the same entries as `[T*K,1]`, distributing small reads across more
token tiles. Both use the same upstream CUDA kernel. Defaults are 15 active QPs
for native and 32 for flat. Auto selects flat up to `opt_max_rows=12288` submitted
rows and native above it. This is a configurable starting policy based on the
previous single-owner profile, not a claim that its crossover is optimal for
all shard counts or masked workloads. No GPU-to-CPU ID/count transfer occurs.
QP depth is sized during initialization; excessive capacity/QP combinations
fail explicitly. Source/destination bounds use signed 32-bit global row slots.

Remote CPU lookup, live membership changes and transparent recovery are not
implemented. Every node must run the same number of participating GPU callers,
with dense caller IDs `[0,num_callers)`. Owner nodes need CUDA/NCCL GIN support;
pure CPU-only owners are not supported by this integration. Initialize before
capture and warm up every shape/policy used in a CUDA graph. One Store owns one
lookup workspace: callers must serialize lookups and graph replays across
streams; consumers can use normal CUDA events. `close()` is coordinated across
all nodes in its rail group after outstanding work completes. `store_client`
remains unsupported; it is not the transport used by this backend.

### Optional build dependency

The local backend remains independent of Torch at native-build time. The
remote backend JIT-builds a small host bridge against these source revisions:

- DeepEP `f6aa38fe06cd4a0489e7730365d4e0114970a2f3`.
- Its DeepJIT submodule `cba802df081985141eeee36803685775c183a047`.

Set `MOONCAKE_DEEPEP_ROOT` to the DeepEP checkout (including `third-party/deep_jit`)
and `EP_NCCL_ROOT_DIR` to the matching NCCL distribution with GIN/GDAKI support.
The build requires CUDA, a C++20 compiler, Torch, Triton, Ninja and elfutils
headers. Set `CXX` and `CUDA_HOME` as appropriate for the host. Configure NCCL's
routable bootstrap interface and per-rank NIC selection before creating NCCL
communicators; avoid conflicting process-wide NCCL settings in the inference
launcher. No changes to the DeepEP CUDA READ/wait kernels are required.

### Remote validation

`scripts/test_engram_shard_layout.py` checks unequal table lengths and invalid
placement/address limits on CPU. `scripts/test_engram_deepep_distributed.py`
exercises two real nodes in both directions, including mixed local/remote
264-byte rows, masks, offsets, noncontiguous output and CUDA graph replay with
changed IDs. Run it concurrently on all callers with matching `--session`,
`--num-callers` and distinct `--device` values. This is a storage correctness
test, not a model accuracy or throughput benchmark.

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
Local lookup supports CPU and CUDA; the remote backend requires CUDA IDs/output.

## Verification

Run `pytest -q scripts/test_engram_tables.py` for local table contracts and CUDA
graph replay, including rejection of the unused `store_client` backend selector.
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
