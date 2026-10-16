# Why TPU support needs a `TpuTransport` (and not just an `if (pjrt)` branch)

This note explains, for someone new to the TENT codebase, **why adding TPU
staging touches a transport class, a platform class, and the routing/capability
system** rather than being a one-line change inside `ProxyManager`. Everything
here is about *existing* TENT mechanics; TPU is just the motivating example.

All paths below are under `mooncake-transfer-engine/tent/`.

---

## 1. The three layers you need to know

TENT separates "how do I move bytes" into three cooperating layers:

| Layer | What it is | Example |
|-------|-----------|---------|
| **Platform** | The *local* memory primitives for one accelerator family: allocate, free, `copy`, classify a pointer's memory type, discover topology. One process picks exactly one Platform at build time. | `CpuPlatform`, `CudaPlatform`, `TpuPlatform` |
| **Transport** | A *data channel* that moves a transfer request's bytes. Each advertises **capabilities** (can it do dram→dram? gpu→dram? gpu→gpu?). | `RdmaTransport`, `ShmTransport`, `NvlinkTransport`, `TpuTransport` |
| **Staging (ProxyManager)** | When no single transport can do a hop directly, it splits the transfer into stages through host DRAM and chains transports. | `ProxyManager` |

Key headers: `include/tent/runtime/platform.h`, `include/tent/runtime/transport.h`
(the `Capabilities` struct is at the top of the latter).

---

## 2. The naive expectation

> "TPU HBM can't be reached by the NIC, so stage it through host DRAM. TENT
> already stages CUDA that way, so just find where `ProxyManager` does the
> device copy and add an `if (tpu) pjrt_copy() else cudaMemcpy()`."

That mental model is *almost* right about the data flow but wrong about the
mechanism. There is no `cudaMemcpy` inside `ProxyManager` to branch on.

---

## 3. What actually happens when you submit a transfer

Follow one transfer from the top:

1. **Submit** → `TransferEngineImpl::submitTransfer` → `prepareSubmit`
   (`src/runtime/transfer_engine_impl.cpp`).

2. **Should this be staged?** For each request, `prepareSubmit` calls
   [`findStagingPolicy`](../../runtime/transfer_engine_impl.cpp) (defined at
   `transfer_engine_impl.cpp:1278`). It returns a 3-element plan
   `[server, local_stage_location, remote_stage_location]`; empty entries mean
   "no staging on that side". Then:
   ```cpp
   owner.staging = !owner.staging_params.empty() && staging_proxy_;  // ~line 1401
   ```

3. **If staged**, the task is handed to the proxy instead of a transport:
   ```cpp
   staging_proxy_->submit(&task, batch, owner.staging_params);        // ~line 1477
   ```

4. **`ProxyManager` chops the transfer into chunks** and, per chunk, issues
   *sub-transfers* for each stage — it does **not** copy bytes itself. See
   `ProxyManager::transferEventLoop` (`src/runtime/proxy_manager.cpp:251`) and
   `submitLocalStage` (`:71`). The local stage is submitted as an ordinary
   transfer whose target is `LOCAL_SEGMENT_ID`:
   ```cpp
   // local_stage.source = device pointer, target_offset = host staging buffer
   impl_->submitStagingTransfer(batch, {local_stage});   // -> submitTransfer(...)
   ```

5. **That sub-transfer is routed like any other**, back through
   `getTransportType`, and the *selected transport's* `submitTransferTasks`
   runs. For the simple copy transports, that method is where the actual byte
   movement lives — and it calls **`Platform::copy`**, not any device API
   directly. Example, `ShmTransport::startTransfer`
   (`src/transport/shm/shm_transport.cpp:119`):
   ```cpp
   status = Platform::getLoader().copy(dst, src, length);  // :122 / :126
   ```

So the real device-copy seam is **`Platform::copy`**
(`CudaPlatform::copy` does `cudaMemcpyAsync`; `TpuPlatform::copy` will call the
PJRT adapter). That part *is* a clean override — good.

**But there is a gate before you ever reach step 5.**

---

## 4. The crux: routing is gated by transport *capabilities*

In step 5 the sub-transfer must be *routed to some transport*. Routing asks each
candidate transport: "can you do a copy from `local_memory_type` to
`remote_memory_type`?" via a capability check:

- Selector mode (**the default**): `TransportSelector::isTransportAvailable`
  (`src/runtime/transport_selector.cpp:311`).
- Legacy mode: `checkAvailability` (`src/runtime/transfer_engine_impl.cpp:889`).

Both map a `(local, remote)` memory-type pair to one capability bit:

```cpp
// GPU/device local side, CPU peer  -> needs caps.gpu_to_dram
// CPU local side, GPU/device peer  -> needs caps.dram_to_gpu
// GPU to GPU                        -> needs caps.gpu_to_gpu
// CPU to CPU                        -> needs caps.dram_to_dram
```

For the TPU **local stage**, the sub-transfer is `TPU device -> host DRAM`, so
routing looks for a transport advertising `gpu_to_dram`.

Now look at who advertises what:

| Transport | Advertises | Set where |
|-----------|-----------|-----------|
| `RdmaTransport` | `dram_to_dram`; adds `gpu_*` **only if `nvidia_peermem` is loaded** (CUDA GPUDirect) | `rdma_transport.cpp` `install` |
| `ShmTransport` | `dram_to_dram` **only** | `shm_transport.cpp` `install` |
| `NvlinkTransport` | `dram_to_gpu` / `gpu_to_dram` / `gpu_to_gpu` (CUDA) | `nvlink_transport.cpp` `install` |

On a TPU host there is **no transport that advertises `gpu_to_dram`**:
- `ShmTransport` = `dram_to_dram` only.
- `RdmaTransport` won't set `gpu_to_dram` (no `nvidia_peermem`), and it *couldn't*
  DMA out of HBM anyway.

**Therefore, without a new transport, the TPU local-stage sub-transfer has no
route, and it fails — even though `TpuPlatform::copy` exists and could do the
copy.** `findStagingPolicy` would also have nothing to hang the policy on.

This is the answer to "why not just one branch": the copy *mechanism* is a clean
`Platform::copy` override, but the *routing/capability system* has no way to
select it unless some `Transport` object sits in `transport_list_[...]` and
answers "yes, I can do `gpu_to_dram`."

For CUDA, that answering transport is `NvlinkTransport` — a full P2P transport
that TENT reuses. TPU has no equivalent to piggyback on, so we add a **thin one
whose only job is to advertise the device↔host capability and run
`Platform::copy` for `LOCAL_SEGMENT_ID` requests**: `TpuTransport`
(`src/transport/tpu/tpu_transport.cpp`). It never touches the network; the
host↔host hop is still RDMA/TCP.

---

## 5. What PR #1 therefore has to touch

| Concern | Change | File |
|---------|--------|------|
| A memory type for TPU HBM | `MTYPE_TPU` + `"tpu"` parsing | `platform.h`, `getTypeEnum` in `transfer_engine_impl.cpp` |
| Local device copy primitive | `TpuPlatform::copy` → PJRT adapter (the clean seam) | `src/platform/tpu/tpu_platform.cpp` |
| A transport that *advertises* `gpu_to_dram`/`dram_to_gpu` so routing can pick the copy | thin `TpuTransport` | `src/transport/tpu/tpu_transport.cpp` |
| Teach the capability checks that TPU is a device type | add `MTYPE_TPU` to `isGpuType` **and** the selector's `is_gpu` lambda | `transfer_engine_impl.cpp:880`, `transport_selector.cpp:337` |
| Decide to stage TPU transfers | `findStagingPolicy` TPU case | `transfer_engine_impl.cpp:1278` |

Note the **two** capability helpers (line 880 and 337): there are two routing
modes (selector = default, legacy), each with its own device-type predicate.
Both must learn about `MTYPE_TPU`, or TPU works in one mode and silently fails in
the other. This is exactly the kind of thing that isn't visible until you trace
both paths.

---

## 6. One-paragraph summary

TENT's `ProxyManager` already implements the staged-DRAM pipeline, and
`Platform::copy` is a clean place to plug in a PJRT device copy. But a staged
sub-transfer is still *routed*, and routing only selects a transport that
*advertises* the matching capability. No existing transport advertises
`device↔host` on a TPU host, so the copy would never be reached. `TpuTransport`
exists solely to answer that capability query and dispatch to `Platform::copy`;
it is the unavoidable "interface tax" of participating in TENT's transport
system, and it accounts for most of the non-test, non-doc line count in this
change.

See also: [`README.md`](README.md) in this directory for the component list and
the PJRT adapter ABI.
