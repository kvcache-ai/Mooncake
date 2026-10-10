# HKVM DiffServer — Build, Run, Test

The DiffServer consumes Mooncake KV events from two masters over ZMQ,
bootstraps each master's key set from `GET /get_all_keys`, and reports the
symmetric difference as two lists: `only_in_master1` and `only_in_master2`.

See `plan.md` for the broader HKVM design and `include/hkvm/diff_server.h` for
the API.

---

## 1. Prerequisites

| Dependency | Why | Install |
|---|---|---|
| CMake ≥ 3.16 | build | system package |
| C++17 compiler | build | gcc/clang |
| **libzmq** | subscribe to master KV-event PUB sockets | `brew install zeromq` / `apt install libzmq3-dev` |
| **msgpack-c** (header-only) | decode event payloads | `brew install msgpack-cxx` / `apt install libmsgpack-dev` |

The DiffServer itself does **not** need PyTorch, CUDA, or the rest of
mooncake-store. Two build paths are provided (§2).

For the masters you point it at, mooncake-store must be built with
`-DENABLE_KV_EVENTS=ON` (which requires libzmq) so each master runs a
`KvEventPublisher`.

---

## 2. Build

### Option A — Standalone (recommended; no PyTorch/CUDA)

Builds only `libmooncake_hkvm_diff` + the example CLI, bypassing the
torch/CUDA requirements of the root `CMakeLists.txt`.

```sh
cd mooncake-hkvm
cmake -S standalone -B build-standalone -DCMAKE_BUILD_TYPE=Release
cmake --build build-standalone -j
```

Artifacts:
- `build-standalone/libmooncake_hkvm_diff.a`
- `build-standalone/mooncake_hkvm_diff_example`

### Option B — As part of the mooncake-hkvm root

Use this if you already configure the full root (requires PyTorch + CUDA).

```sh
cd mooncake-hkvm
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j --target mooncake_hkvm_diff mooncake_hkvm_diff_example
```

> If `find_package(Torch)` / `find_package(CUDAToolkit)` fails, use Option A.

---

## 3. Run

The example CLI takes `--key=value` flags. Defaults assume two local masters.

```sh
./mooncake_hkvm_diff_example \
  --master1_zmq=tcp://10.0.0.1:5557 --master1_host=10.0.0.1 --master1_port=9003 \
  --master2_zmq=tcp://10.0.0.2:5557 --master2_host=10.0.0.2 --master2_port=9003 \
  --interval=5
```

Flags:

| Flag | Default | Meaning |
|---|---|---|
| `master1_zmq` / `master2_zmq` | `tcp://127.0.0.1:5557` / `:5558` | master KvEventPublisher PUB endpoint |
| `master1_host` / `master2_host` | `127.0.0.1` | master metrics/admin HTTP host (for `/get_all_keys`) |
| `master1_port` / `master2_port` | `9003` / `9004` | master `metrics_port` (default 9003 on the master) |
| `settle_ms` | `300` | time ZMQ SUB settles before the snapshot fetch |
| `interval` | `5` | seconds between diff reports |
| `max_keys` | `20` | cap on diff keys printed per report |

Output (every `interval` s): `master1/master2` key counts, the two diff lists,
and per-master stats (`recv`, `stored`, `removed`, `malformed`, `seq_gaps`,
`snapshot`, `bootstrapped`). `Ctrl-C` stops cleanly.

### What the masters need

Start each master with KV events enabled, e.g.:

```sh
mooncake_master \
  --metrics_port=9003 \
  --enable_kv_events=true \
  --kv_events_bind_endpoint=tcp://0.0.0.0:5557 \
  --kv_events_backend_id=master1 \
  ...other flags...
```

(Second master: `--metrics_port=9004`, `--kv_events_bind_endpoint=tcp://0.0.0.0:5558`,
`--kv_events_backend_id=master2`.)

---

## 4. Test

### 4.1 Manual integration smoke test

This validates the end-to-end path: snapshot bootstrap + event stream + diff.

1. **Start two masters** (A on `5557`/`9003`, B on `5558`/`9004`) per §3.
2. **Start the diff server** pointing at both:
   ```sh
   ./mooncake_hkvm_diff_example \
     --master1_zmq=tcp://127.0.0.1:5557 --master1_host=127.0.0.1 --master1_port=9003 \
     --master2_zmq=tcp://127.0.0.1:5558 --master2_host=127.0.0.1 --master2_port=9004 \
     --interval=2
   ```
3. **Ground truth** — check each master's key set directly:
   ```sh
   curl -s http://127.0.0.1:9003/get_all_keys | sort  > keys_a.txt
   curl -s http://127.0.0.1:9004/get_all_keys | sort  > keys_b.txt
   comm -23 keys_a.txt keys_b.txt   # only_in_master1
   comm -13 keys_a.txt keys_b.txt   # only_in_master2
   ```
4. **Mutate and observe**:
   - Put a key into master A only → it appears in `only_in_master1` within one `interval`.
   - Put the same key into master B → it drops out of both diff lists.
   - Remove it from A → it appears in `only_in_master2`.
5. **Verify** the diff server's `only_in_master1` / `only_in_master2` match the
   `comm` output, and that `bootstrapped=true` with `snapshot` = the
   `/get_all_keys` line count for each master.

### 4.2 Sanity checks

- `seq_gaps > 0` ⇒ the publisher dropped events (queue overflow) — increase
  `--kv_events_queue_capacity` on the master.
- `malformed_events > 0` ⇒ unexpected payload shape — confirm master and diff
  server are built against compatible `KvEventPublisher` wire formats.
- With no mutations, the diff should be stable across intervals.

### 4.3 Programmatic use

```cpp
#include "hkvm/diff_server.h"

mooncake::hkvm::DiffServerConfig cfg;
mooncake::hkvm::DiffMasterConfig a, b;
a.id = "master1"; a.endpoint = "tcp://10.0.0.1:5557";
a.http_host = "10.0.0.1"; a.metrics_port = 9003;
b.id = "master2"; b.endpoint = "tcp://10.0.0.2:5557";
b.http_host = "10.0.0.2"; b.metrics_port = 9003;
cfg.masters = {a, b};

mooncake::hkvm::DiffServer server(std::move(cfg));
server.Start();
auto diff = server.GetDiff();   // diff.only_in_master1 / diff.only_in_master2
server.Stop();
```

Link against `mooncake_hkvm_diff` (and libzmq).

---

## 5. CopyEngine (reconcile two masters toward identity)

The CopyEngine consumes the DiffServer's two diff lists, copies each
diff-only key from the master that has it to the master that lacks it (via
`/query_key` for the source descriptor + `TransferEngine` relay + `PutStart`/
`PutEnd` on the destination), then calls `DiffServer::Rebootstrap()` to clear
the diff lists from ground truth.

Unlike the DiffServer, the CopyEngine **requires the full mooncake build** —
it links `mooncake_store` (MasterClient) and `transfer_engine`. It is therefore
not built by the standalone path; build it under the top-level mooncake tree
with `WITH_HKVM=ON`.

### 5.1 Build

From the top-level mooncake checkout (where `mooncake_store` and
`transfer_engine` targets exist):

```sh
cmake -S . -B build -DWITH_HKVM=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build -j --target mooncake_hkvm_copy mooncake_hkvm_copy_example
```

Artifacts: `libmooncake_hkvm_copy.a`, `mooncake_hkvm_copy_example`.

The `src/CMakeLists.txt` auto-detects the store/transfer-engine targets and
skips the CopyEngine otherwise (e.g. in the standalone diff-server build).

### 5.2 Run

The example runs DiffServer + CopyEngine together and reconciles on a timer:

```sh
./mooncake_hkvm_copy_example \
  --master1_zmq=tcp://10.0.0.1:5557 --master1_rpc=10.0.0.1:50051 \
  --master1_host=10.0.0.1 --master1_port=9003 \
  --master2_zmq=tcp://10.0.0.2:5557 --master2_rpc=10.0.0.2:50051 \
  --master2_host=10.0.0.2 --master2_port=9003 \
  --metadata=etcd://10.0.0.1:2379 --local_name=copy-engine-1 \
  --local_host=10.0.0.1 --transport=tcp --interval=10
```

Flags (in addition to the DiffServer flags in §3):

| Flag | Default | Meaning |
|---|---|---|
| `master1_rpc` / `master2_rpc` | `127.0.0.1:50051` / `:50052` | master RPC address for `MasterClient::Connect` |
| `metadata` | (required) | metadata server conn string, e.g. `etcd://host:2379` |
| `local_name` | `copy-engine` | unique TransferEngine instance name |
| `local_host` | `127.0.0.1` | advertised hostname/IP for the TransferEngine |
| `transfer_port` | `12345` | TransferEngine RPC port |
| `transport` | `tcp` | transport to install (must match masters' segments; `tcp`/`rdma`) |
| `interval` | `10` | seconds between reconcile passes |

Each pass prints `copied m1->m2 / m2->m1 / failed` and the diff size before vs.
after. `Ctrl-C` stops cleanly.

### 5.3 Test

Integration test (extends §4.1):

1. Start two masters (A `5557`/`9003`/RPC `50051`, B `5558`/`9004`/RPC `50052`)
   sharing one metadata server, both with KV events enabled.
2. Create divergence: put keys `k1,k2` only into A and `k3` only into B (using
   any mooncake client / the masters' RPC).
3. Start the copy example (§5.2).
4. Within one `--interval`, expect `copied m1->m2: 2, m2->m1: 1, failed: 0`,
   and the **after** diff sizes to be `0 / 0`.
5. Verify ground truth converged:
   ```sh
   diff <(curl -s http://127.0.0.1:9003/get_all_keys | sort) \
        <(curl -s http://127.0.0.1:9004/get_all_keys | sort)   # no output
   ```
6. Re-diverge (remove a key from one side) and confirm the next pass re-syncs.

Failure-mode checks:
- `failed > 0` with `after` diff non-zero ⇒ a copy failed; check the engine's
  transport matches the masters' segment `protocol_` and that the metadata
  server is reachable.
- `seq_gaps` rising on the DiffServer ⇒ master publisher overflow (raise
  `--kv_events_queue_capacity`).

### 5.4 Programmatic use

```cpp
#include "hkvm/copy_engine.h"
#include "hkvm/diff_server.h"

mooncake::hkvm::DiffServer diff_server(/*DiffServerConfig*/{...});
diff_server.Start();

mooncake::hkvm::CopyEngineConfig ccfg;
/* ...masters, metadata_conn_string, local_server_name, transports... */
mooncake::hkvm::CopyEngine copy(ccfg, diff_server);
copy.Start();
auto stats = copy.RunOnce();   // copies diff keys, then Rebootstraps
copy.Stop();
diff_server.Stop();
```

---

## 6. Notes & limitations

- **PUB/SUB slow-joiner**: a ZMQ subscriber that hasn't finished connecting when
  the snapshot is taken can miss events in that window. `settle_ms` mitigates
  this; for a hard guarantee the publisher would need to expose a snapshot
  version tied to event sequence numbers (not currently available).
- **Snapshot fetch failure is non-fatal**: if `/get_all_keys` is unreachable,
  the server logs a warning and falls back to event-only mode for that master
  (only keys observed via events are tracked). Check `bootstrapped` in stats.
- **Two masters only** for the two-list diff; `GetDiff()` compares
  `masters_[0]` vs `masters_[1]`.
- **CopyEngine relay transfer**: bytes are copied remote-source → local
  registered buffer → remote-destination (two hops), because the
  `TransferEngine` API transfers between a local and a remote buffer, not
  between two remote buffers. A per-key local buffer is allocated, registered,
  and freed per `CopyKey`.
- **CopyEngine transports**: `installTransport` is called with `nullptr` args,
  which works for `tcp`/`nvlink`. To copy from/to RDMA segments you must extend
  `Start()` to pass RDMA device args (as `transfer_engine_validator` does).
- **CopyEngine JSON parsing**: `/query_key` is parsed with a minimal targeted
  extractor matched to `HandleQueryKey`'s fixed schema (`size_`,
  `buffer_address_`, `protocol_`, `transport_endpoint_`). If that schema
  changes, update `ParseQueryKey` in `src/copy_engine.cpp`.
- **CopyEngine is full-build only**: it links `mooncake_store` +
  `transfer_engine`, so it is not produced by the standalone diff-server build
  (§2 Option A). Build it under the top-level mooncake tree with `WITH_HKVM=ON`.
