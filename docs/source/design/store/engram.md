# EngramStore Backend

EngramStore reads immutable tables of opaque byte rows. Models own hashing,
quantization, scales, projections and distributed communication.

Local CPU/CUDA tables and node-sharded `deepep_rdma` tables share the same
byte-row lookup contract. The remote backend requires CUDA/NCCL GIN-capable
owners; CPU-only remote owners are not supported.

## Table API

The original local table interface remains available. Import
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

## Wheel installation

Both the PEP 517 build and `scripts/build_wheel.sh` include `mooncake.engram`,
the private remote packages, JIT C++ source and its license. Install the NumPy
extra with `pip install 'mooncake-transfer-engine[engram]'`. CUDA-enabled Store
builds also include `_engram_cuda`; CPU builds support the local CPU interface.
The `deepep_rdma` backend additionally needs PyTorch and the compatible
DeepEP/NCCL runtime described below; these are not bundled into the wheel.

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

## Configured backends and remote initialization

`EngramStoreConfig` lives in `mooncake.engram`. It is one flat, immutable
configuration, validated for its selected `backend`. Existing local constructor
calls remain supported; do not mix `config` with explicit legacy local options.

Under `mooncake-store/src/engram`, `local/` contains the native local CUDA
implementation. `remote/` contains shared placement and metadata rendezvous;
`remote/deepep_rdma/` owns DeepEP slot layout, CPU allocation, communication,
JIT compilation and lookup kernels. Future communication backends can add
sibling directories without changing the shared coordination code.

```python
from mooncake.engram import EngramStore, EngramStoreConfig

config = EngramStoreConfig(
    backend="deepep_rdma", assignment="auto", group_size=2,
    node_id=runtime_node_id, model_key=checkpoint_identity,
    caller_id=local_caller_id, num_callers=local_caller_count,
    max_lookup_rows=maximum_submitted_ids,
)
store = EngramStore(tables=all_head_specs, namespace=layer_namespace, config=config)
# Construction registers the node and waits for a complete group. Load only
# this node's heads, using store.shard_id and store.group_size.
store.populate_and_sync(local_tables, table_owners=head_owners)
store.lookup(head_names, ids, output, offsets=head_offsets, stream=stream)
```

Use `assignment="manual"`, `group_id` and `shard_id` for explicit placement.
Auto admission needs no total cluster size: compatible nodes form successive
fixed-size groups. Nodes on the same host share a final CPU allocation across
callers; each caller has its own communication rail and lookup workspace.

`MOONCAKE_MASTER`, or `master_server_address` in the existing Mooncake config,
selects the Master RPC endpoint. Engram registration, bootstrap records and
readiness all use Master RPC; no HTTP metadata endpoint or user session ID is
needed. This requires a Master and Python extension built with this change.
Lookup itself does not query Master. NIC selection reuses NCCL configuration,
including `NCCL_IB_HCA` when set; Engram does not override it. Set any NIC filter
before launching the runtime. The Config accepts an optional `timeout` override
(default: 300 seconds).

The runtime supplies stable node/model identities, dense local caller numbers,
matching caller counts, and lookup capacity. Capacity counts every submitted ID
in `[T,K]`, including masked IDs. These five fields are required for the remote
backend; local tables do not use them:

| Runtime field | Purpose | Why the runtime supplies it |
| --- | --- | --- |
| `node_id` | Stable identity of one storage node. | All GPU callers on that node share placement; restarted nodes can retain their shard. It is not a routable address. |
| `model_key` | Isolates placement pools by checkpoint identity. | Nodes serving different checkpoints must not join the same storage group. It need not be a weight-content hash. |
| `caller_id` | Dense caller index in `[0,num_callers)`. | The same index on each node forms one communication rail, with its own lookup workspace. It is independent of physical GPU and storage shard IDs. |
| `num_callers` | Number of GPU callers on each storage node. | Peers validate matching counts, and local callers wait until all have imported the shared CPU allocation. |
| `max_lookup_rows` | Maximum submitted IDs per lookup, `T * K`. | Communication buffers and queues are sized before lookup/CUDA Graph capture. Count masked IDs and graph padding as well as useful IDs. |

For four storage nodes with four GPU callers each, `group_size=4`,
`num_callers=4`, and each node has callers `0,1,2,3`. No field needs to be
entered in an inference runtime's user JSON if the runtime derives it, as
SGLang does. A single-caller deployment uses `caller_id=0, num_callers=1`;
these values must not be reused as defaults for a multi-caller deployment.

Tables must have equal byte-row widths.
Placement is independent of computational TP/DP/CP ownership. Initialize before
CUDA Graph capture, and order overlapping uses of one Store workspace.

Node leases distinguish active owners from replacement processes. A restart
retains placement, increments the group's bootstrap generation, and discards
old initialization records. All owners must rebuild their Stores/communicators
before lookup resumes. This is not transparent single-node recovery. Placement
is in-memory Master state; surviving a Master restart is outside this backend.
The compatibility `populate_shard_and_sync` entry point retains the previous
explicit HTTP-rendezvous path; configured stores use `populate_and_sync`.

`backend="local"` uses `local_dir` and `persistent`; remote allocations are
process-lifetime CPU memory and do not use these file settings. Other backend
names, including the planned MNNVL and UB paths, fail until implemented.

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
The local implementation supports CPU and CUDA lookup. The SGLang remote
adapter currently targets Qwen4-Exp and its Qwen3.8 checkpoint family.

## Verification

Run `pytest -q scripts/test_engram_tables.py` for local table contracts and CUDA
graph replay. Local regression checks cover Config validation, coordinator
admission, manual conflicts, restart placement, expired owners and generation
fencing.
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

The configured `deepep_rdma` path was also tested with Qwen3.8-Flash-Next-FP8
on two nodes, each running TP4/EP4 with PP1 and one model replica. Auto and manual
assignment matched local storage exactly for 40 remote requests / 1,280 generated
tokens, including output logprobs, with prefill and decode CUDA graphs enabled.
Eight-caller storage tests checked byte correctness, shared CPU allocations,
graph replay, whole-group restart and SIGKILL recovery with retained placement.
Real Master RPC tests additionally checked expired owners in partial groups.
These checks validate functionality under the tested configurations.
