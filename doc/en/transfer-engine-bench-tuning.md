# Transfer Engine Benchmarking & Tuning Guide
## Background

Mooncake’s data transfer backbone is the **Transfer Engine (Mooncake TE)**. Through the adoption of **SGLang**, Mooncake TE is being widely applied across different scenarios and vendors, and user feedback has been increasing.

To support validation and quick prototyping, we provide a **benchmark program** under `mooncake-transfer-engine/example`. This benchmark is intended to:

1. Demonstrate basic usage of the Mooncake TE API.
2. Verify whether Mooncake TE can run in a given environment.
3. Evaluate the performance impact of new patches.

⚠️ **Note:** This benchmark is closer to a *prototype micro-benchmark* than a production-grade tool. Compared with standardized benchmarking frameworks, it has limited functionality and rigor. Results should be interpreted accordingly.

## Usage

The benchmark requires **two processes**:  
- **Target** (must start first)  
- **Initiator** (must start after target)

### Example

- Target:
```bash
./transfer_engine_bench --mode=target --auto_discovery --metadata_server=P2PHANDSHAKE
````

* Initiator (`segment_id` is `target_ip:RPC_port`; RPC port is printed by target, range 15000–20000):
```
Transfer Engine RPC using XXX, listening on YYY:ZZZ
                                                ~~~
```

```bash
./transfer_engine_bench --mode=initiator --auto_discovery \
    --metadata_server=P2PHANDSHAKE --segment_id=node050:15428
```

The initiator will report bandwidth in GB/s.
By default, all NICs are used. To test a single NIC, replace `--auto_discovery` with `--device_name=mlx_XXXX`.

---

## Workflow

1. **Target** allocates 1GB DRAM per NUMA node (configurable via `--buffer_size`) and registers buffers. Then it waits indefinitely.
2. **Initiator** allocates and registers buffers similarly. Data transfers are performed via `submitTransfer`.
3. **Threads**: The initiator spawns multiple threads (default 12, configurable with `--threads`). Each thread:

   * Allocates a batch (`--batch_size`, default 128).
   * Issues read/write requests (`--block_size=65536`).
   * Submits requests with `submitTransfer`.
   * Polls `getTransferStatus` until all complete.
   * Releases batch and repeats.
4. After the specified duration (`--duration=10`), results are collected and printed.

---

## Advanced Parameters

Some advanced tuning parameters are passed via environment variables:

* **`MC_SLICE_SIZE`**: Request slicing granularity (default 65536).
  Larger values reduce CPU overhead but may limit multi-NIC aggregation. Smaller values increase parallelism but add software overhead.

Refer to the Mooncake TE API documentation for additional parameters.

---

## Performance Considerations

* Multiple frontend threads are required to fully utilize bandwidth due to CPU overhead in request preparation.
* `--batch_size`: Large values may cause higher latency if batch completion is gated by slow requests.
* `MC_SLICE_SIZE`: Requires workload-specific tuning for optimal performance.

The next-generation Mooncake TE reduces CPU overhead and achieves near full utilization with fewer threads.

---

## Alternative Benchmarking

Mooncake TE is also supported in **NIXL** as a backend plugin. **NIXL Bench** is recommended for standardized performance testing and comparisons.

Steps:

1. Build `libtransfer_engine.so` with `-DBUILD_SHARED_LIBS=ON` and install.
2. Build and install NIXL & NIXL Bench.
3. Verify `Mooncake.so` is available under `plugins/`.
4. Run benchmark:

```bash
numactl -m0 -N0 ./nixlbench --etcd-endpoints http://node050:2838 --backend UCX \
     [--start_batch_size=XXX] [--num_threads=YYY]
```
