# EngramStore Backend

EngramStore reads immutable tables of opaque byte rows. Models own hashing,
quantization, scales, projections and distributed communication.

This implementation keeps local CPU/CUDA lookup and adds an explicitly enabled
MNNVL EGM byte-table backend. The backend uses TENT to manage HOST_NUMA
allocations and FABRIC mappings, and GPU kernels fetch rows directly; it does
not use per-row KV get/put. A generic KV Store remote backend in the form of
`store_client` is still not implemented. Cross-node FABRIC and full-model
performance must be accepted on the target superpod and cannot be replaced by
local CUDA regression.

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

## MNNVL EGM Backend

```python
store = EngramStore(tables=all_specs, namespace="checkpoint/layout-v1",
                    local_dir="/tmp/engram", persistent=False)
store.populate_shard_and_sync(
    owned_rows, backend="mnnvl", shard_id=node_id, num_shards=num_nodes,
    table_owners=owners, master_addr="http://master:8080/metadata",
    session_id=deployment_id, caller_id=caller_id, num_callers=callers_per_node,
    max_lookup_rows=65536, max_egm_bytes_per_numa=128 << 30,
    device=current_device, host_id=physical_host_id,
)
store.lookup(names, gpu_ids, byte_output, offsets=offsets,
             stream=stream.cuda_stream)
# Call after every caller has stopped requests, destroyed dependent graphs,
# and completed its GPU work.
store.close()
```

The example above is a calling contract and must be replaced with real data and
node configuration. All callers pass the same specs, owners, namespace, session,
member count, and capacity limits; the shard identifies the physical node and a
caller identifies a participating GPU on that node. Only one writer per table
passes complete contiguous CPU uint8 data; other callers pass an empty
dictionary. Writers of different tables may share one NUMA, but the sum of their
arenas must not exceed that NUMA's budget; a single table does not span arenas.
The library does not guess the model semantics of table names, nor does it move
checkpoint parameters to re-shard compute splits.

- Initialization: membership/layout check → aligned allocation → copy →
  register and export → explicit mapping → GPU readback of first and last
  samples → READY from all members. Ordinary pinned/malloc memory is not used
  as an EGM fallback.
- `master_addr` points to a Master HTTP metadata service of the same version, or
  to the Python metadata server below. Both support conditional publication with
  `If-None-Match: *`; older services are rejected explicitly.
- `device` defaults to the current CUDA device; `numa_node` defaults to the host
  attachment of that GPU; `host_id` can be given explicitly, or comes from
  `MOONCAKE_HOST_ID`/hostname. Set it explicitly when the container hostname
  differs from the physical node. `rpc_hostname` may specify a local IP that is
  routable between nodes.
- `init_timeout_s=300` and `close_timeout_s=60`; use a new `session_id` for each
  redeployment. Membership is fixed: there is no transparent reconnection after
  a peer failure, no hot update, and no resharding.
- CUDA lookup keeps the byte, offset, -1, and stride contract; remote CPU lookup
  is rejected explicitly. `max_lookup_rows` bounds `T*K`. Warm up each table
  name/offset/device combination before capturing a graph.
- The caller orders multiple streams/graphs of one Store. A return does not mean
  CPU synchronization has completed; consumers wait through CUDA stream/event
  ordering. Lookup does not query the Master, download IDs, or submit TE READs.
- A mapping capsule holds a lease; the internal engine reuses it per
  process/caller/session. Close first drains, tears down table views and
  mappings, then publishes RELEASED; the provider releases memory only after it
  has collected every acknowledgement. A failed close can be retried; when
  safety cannot be confirmed, resources are retained until the whole session
  exits. The destructor is not a substitute for coordinated close.

### Build and Superpod-Only Test

First install the full C++ dependencies and submodules as described in the
repository dependency instructions, then build and install from the repository
root; the target CUDA must support the actual GPU. The GB200 example pins SM100
explicitly; for GB300 choose SM103 according to the actual device:

```bash
python3 -m pip install '.[engram]' \
  -Ccmake.define.USE_CUDA=ON -Ccmake.define.USE_TENT=ON \
  -Ccmake.define.WITH_TE=ON -Ccmake.define.WITH_STORE=ON \
  -Ccmake.define.CMAKE_CUDA_ARCHITECTURES=100
```

After installation, check that both `mooncake._engram_cuda` and
`mooncake._engram_mnnvl` can be imported. Set the same `MC_METADATA_HTTP_TOKEN`
on the server and on all callers (inject it through the environment; do not put
it in configuration files or public logs). The Python metadata server can run
standalone:

```bash
python3 -m mooncake.http_metadata_server --host 0.0.0.0 --port 8080
```

Set `MC_ENABLE_MNNVL=1` before starting any caller. There is no need to set
`MC_USE_TENT`: Engram uses its own native TENT session and does not switch the KV
transport on its own. `MC_FORCE_MNNVL` belongs to classic and cannot replace this
switch. Avoid overriding the strict EGM configuration with a global
`MC_TENT_CONF`; initialization fails when the effective capabilities or segment
identity do not match.

Run the same command on two real nodes: change `--node-id 0` to `1` on the
second node and fill in the same new session and a routable metadata URL; the
`master` placeholder in the following command must be replaced:

```bash
MC_ENABLE_MNNVL=1 python3 scripts/bench_engram_mnnvl.py \
  --master-addr http://master:8080/metadata --session-id test-run-001 \
  --nodes 2 --node-id 0 --devices 0,1 --graph --head-major
```

Drop `--graph` to compare against eager; `--backend local` is the local
full-table baseline for each caller. `--rows 17000000 --heads 2 --row-bytes 264
--budget-bytes 8589934592` can cover a single-table offset above 4 GiB, and you
must ensure memory yourself. Use a new session for every repeated run. The
microbenchmark reports bytewise checks, lookup latency, table capacity, and
initialization duration; it fails outright when hardware prerequisites are not
met and never fakes a pass with SKIP or ordinary host allocation.

## SGLang Plugin on the Mooncake Side

The plugin registers only through `sglang.srt.plugins` inside the Mooncake
package and does not modify engine source code. When
`MOONCAKE_ENGRAM_CONFIG` is unset, the plugin does not import the engine and
changes no behavior. The SGLang baseline that was checked:
`e8017bc63bbeb271a6b4729eb5ee7a7fd4584b9f`.

The plugin collects tables after native weight loading completes and before
kernel precompilation or graph capture. Native compute shards are exposed as
opaque tables; among replicas with the same range only one physical writer is
chosen, and weight and block scale select the same writer. Placement prefers
balancing nodes that **already hold the data**, and does not rewrite the loader
or change native TP/DP/ETP sharding just to satisfy whole-head placement. Across
compute nodes, capacity does not shrink further when there are no duplicate
tables. The startup phase must still hold both the natively loaded tables and the
new EGM; native backing is released after the copy completes, so only
steady-state deduplication is guaranteed here, not that startup peaks match a
version that pre-shards whole heads during load.

| Engine/model | Plugin row entry point | Formats and preserved behavior |
| --- | --- | --- |
| SGLang DeepSeek V4.1 | `EngramEmbedding._owned_rows` | FP8 or packed MXFP4 + E8M0; reuses native `engram_gather`, preserves DP/CP/masking |
| SGLang Qwen PLE | `Qwen4ExpPinnedHostEmbedding.gather` | BF16/FP8; keeps the original scalar scale, reduce, and prefetch |

Mooncake's custom `torch.library.custom_op` ships a fake implementation that
isolates byte lookup from model tracing and keeps metadata/initialization out of
the graph. Quantization formats follow the current native engine implementation
rather than duplicating model math.

Example configuration file for node 0 (not an extension of the existing engine's
`--engram-config`):

```json
{
  "engine": "sglang",
  "namespace": "actual-checkpoint-revision/layout-v1",
  "master_addr": "http://master:8080/metadata",
  "session_id": "new-deployment-001",
  "shard_id": 0,
  "num_shards": 2,
  "num_callers": 4,
  "max_lookup_rows": 1048576,
  "max_egm_bytes_per_numa": 137438953472,
  "host_id": "physical-node-0",
  "init_timeout_s": 600
}
```

Node 1 changes shard_id/host_id; every node has the same caller count. Each
worker's caller ID defaults to `distributed_rank % num_callers`, which only
applies to contiguous, node-aligned rank layouts; standalone workers can set it
explicitly with `MOONCAKE_ENGRAM_CALLER_ID`. The GPU device uses the ordinal
currently visible to the worker; do not use the global rank as a physical GPU ID.

```bash
export MC_ENABLE_MNNVL=1
export MOONCAKE_ENGRAM_CONFIG=/path/to/engram-node.json
# SGLang: if a plugin allowlist is already configured, append mooncake_engram.
export SGLANG_PLUGINS=mooncake_engram
```

Native model argument requirements:
- SGLang DeepSeek: `--engram-config '{"placement":"cpu","replication":"none"}'`;
  do not use `peer_read` or `per_node`, and pass
  `--startup-weight-load-mode serial`.
- SGLang Qwen: `--ple-offload-embedding --ple-offload-backend pinned`, with
  serial loading.
- The first phase of the plugin targets PP=1; it does not support hot update,
  weight-cache/custom loaders that bypass the load hook, native peer HBM or
  shared backing, or elastic membership changes. SGLang reports an error when
  the runner does not actually complete the attach; incompatible models or
  interfaces are not silently downgraded. Engine-specific acceleration modes
  require additional acceptance in the target environment.
- SGLang closes the Store when the scheduler releases host resources and cleans
  up best-effort on normal process exit; embedded use must stop requests and
  destroy graphs before calling `mooncake.engram_plugins.shutdown()`. If a
  process is killed or close finds missing members, stop the whole session and
  restart with a new ID.

The startup log should contain `Mooncake Engram plugin enabled`. Start the
native/local baseline and the plugin service separately, then compare
token/logprob with the same test script; it neither starts services nor injects
hooks:

```bash
python3 scripts/test_engram_sglang.py \
  --reference http://reference:30000 --candidate http://candidate:30000
```

`--logprob-atol` defaults to 0 and can be adjusted according to a precision
criterion fixed in advance; tokens must be identical. The script's
non-streaming latency is not TTFT/TPOT; throughput and tail latency still
require the engine's native streaming benchmark.

## MNNVL + EGM: Prerequisites and Acceptance

Mooncake backend acceptance is recorded separately from full-model acceptance
for SGLang. An implemented interface/plugin does not mean acceptance with a
real checkpoint, cross-node FABRIC, or performance is complete.

### Prerequisite Collection

The container must allow pinning host memory: run the server under
`prlimit --memlock=unlimited:unlimited` (or set an equivalent container limit).
Confirm that every visible GPU reports VMM/FABRIC support, that the target host
NUMA node has enough reclaimable memory for `max_egm_bytes_per_numa`, and that
the IMEX channel device is readable/writable. These are necessary but not
sufficient conditions; they do not prove cross-node EGM readback.

### Target Hardware Acceptance Gate

The following steps must be measured on at least two OS nodes in the same
NVLink/IMEX domain and cannot be replaced by same-machine A100 NVLink,
POSIX-FD/IPC, or prerequisite checks:

1. Verify actual GPU UUIDs, host NUMA, driver/Toolkit, IMEX domain, and container
   device permissions.
2. Create a HOST_NUMA VMM allocation on the correct NUMA and export a FABRIC
   handle.
3. Import and map it on another node, grant access to the actual caller GPUs,
   and check the bytes with a GPU kernel.
4. After the Engram backend is implemented, verify local/remote mixed rows,
   offsets above 2 GiB, stream/graph, and coordinated close; do not publish a
   usable conclusion before the data has been read back correctly.
5. Keep the source SHA, wheel/build configuration, checkpoint revision, and
   measured results; credentials and FABRIC handles in raw configuration or
   metadata must not be published with the report.

The source baseline for this phase is Mooncake
`98d8566a5a35f8ccaf58be0008d130fce4d3da85`. The A100 in the current development
environment reports `fabric=0`, and no default IMEX channel was found, so no EGM
allocation or cross-node readback was performed on the target superpod, which
does not constitute MNNVL Engram deployment validation.

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

The byte-table contract separates model interpretation from storage. The local
path supports CPU/CUDA and the MNNVL path supports CUDA; the table above
describes data expressiveness and does not mean every model has completed remote
deployment validation on this branch.

## Verification

`pytest -q scripts/test_engram_tables.py scripts/test_engram_mnnvl.py scripts/test_engram_plugins.py`
covers the local contract, the simulated multi-caller control plane, plugin
formats/hooks, and CUDA graph. The reserved `store_client` argument must still
be rejected. `scripts/bench_engram_store_27b.py` is the local benchmark;
`scripts/bench_engram_mnnvl.py` is used for real superpod byte validation and
benchmarking. Mock tests do not cover FABRIC drivers; the lightweight models in
the plugin tests do not replace acceptance with real engines/checkpoints.

Local checks on this branch include: standalone compilation of the local CUDA
helper, CPU/CUDA byte-table and plugin graph/trace regressions, simulated
distributed control plane and HTTP conditional write regressions, and standalone
C++ lease tests. The full-repository CMake configuration is blocked by the
`yaml-cpp` missing on the current development machine; the TENT changes
additionally received a syntax-only compile check (read-only use of third-party
headers already present in the workspace), which is not equivalent to linking the
real wheel. The target environment must still build the complete wheel and run
two-node FABRIC and real-model validation for SGLang. The documentation
HTML build lacks Sphinx on this machine, so render validation is also pending.

The following is the SGLang validation record from a historical local branch, not
the acceptance result for this MNNVL/plugin work:

The SGLang integration was tested end to end with real weights for all five
checkpoints in the four families above. Under matched controlled configurations,
all 30 request outputs matched the native implementation exactly, including
output token logprobs. CUDA graph decode was exercised on every checkpoint.
Gemma3n and Qwen used deterministic inference; DeepSeek, Qwen and LongCat used
fixed KV capacity and serialized requests. These are functional correctness
checks for local storage, not a performance benchmark or remote validation.
See [the companion SGLang PR](https://github.com/sgl-project/sglang/pull/44521)
for the complete test configuration.
