# SGLang HiCache with Mooncake Backend

This guide walks through integrating SGLang HiCache with Mooncake as the L3 storage backend.

## Deployment

First, you need:
- Install [SGLang](https://docs.sglang.ai/get_started/install.html) with HiCache support on the machine that hosts your SGLang server.
- Install [Mooncake](https://kvcache-ai.github.io/Mooncake/getting_started/build.html) so it can serve as the hierarchical cache backend.

Both SGLang and Mooncake can be installed with a few commands, and you can always fall back to the official documentation for advanced configuration.

**Launch Mooncake `master service`:**

```bash
mooncake_master --enable_http_metadata_server=true
```

Then you can start SGLang with Mooncake L3:

```
MOONCAKE_MASTER=127.0.0.1:50051 python -m sglang.launch_server \
--model-path [model_path] \
--page-size 64 \
--enable-hierarchical-cache \
--hicache-storage-prefetch-policy timeout \
--hicache-storage-backend mooncake
```

**`--hicache-storage-prefetch-policy {best_effort,wait_complete,timeout}`**: Control when prefetching from storage should stop:
  - `best_effort`: Prefetch as much as possible without blocking
  - `wait_complete`: Wait for prefetch to complete before proceeding
  - `timeout`: Use timeout-based prefetching (recommended for Mooncake)

For more HiCache configuration details, see [SGLang HiCache](https://docs.sglang.ai/advanced_features/server_arguments.html#hierarchical-cache).

### Prefill Decode Disaggregation

For more details, you can refer to [SGLang PD Disaggregation](https://docs.sglang.ai/advanced_features/pd_disaggregation.html).

For the prefill worker:
```
MOONCAKE_MASTER=127.0.0.1:50051 python -m sglang.launch_server \
--model-path [model_path] \
--page-size 64 \
--enable-hierarchical-cache \
--hicache-storage-prefetch-policy timeout \
--hicache-storage-backend mooncake \
--disaggregation-mode prefill \
--disaggregation-ib-device "mlx5_1" \
--base-gpu-id 0 \
--port 30000
```

For the decode worker:
```
MOONCAKE_MASTER=127.0.0.1:50051 python -m sglang.launch_server \
--model-path [model_path] \
--page-size 64 \
--disaggregation-decode-enable-offload-kvcache \
--hicache-storage-prefetch-policy timeout \
--hicache-storage-backend mooncake \
--disaggregation-mode decode \
--disaggregation-ib-device "mlx5_1" \
--base-gpu-id 1 \
--port 30001
```

Then start the router:
```
python -m sglang_router.launch_router \
        --pd-disaggregation \
        --prefill "http://127.0.0.1:30000" \
        --decode "http://127.0.0.1:30001" \
        --host 0.0.0.0 \
        --port 8000
```


## Configurations

For the Mooncake deployment guide (master configurations, metrics, transfer tuning), you can check out [Mooncake Store Deployment & Operations Guide](https://kvcache-ai.github.io/Mooncake/deployment/mooncake-store-deployment-guide.html).

### Using env variables to configure Mooncake

```bash
MOONCAKE_TE_META_DATA_SERVER="http://127.0.0.1:8080/metadata" \
MOONCAKE_MASTER=127.0.0.1:50051 \
MOONCAKE_PROTOCOL="rdma" \
MOONCAKE_DEVICE="mlx5_0,mlx5_1" \
MOONCAKE_GLOBAL_SEGMENT_SIZE=4294967296 \
python -m sglang.launch_server \
    --enable-hierarchical-cache \
    --hicache-storage-backend mooncake \
    --model-path [model_path]
```

Parameter Explanation:

* `MOONCAKE_TE_META_DATA_SERVER`: The network address of the `metadata service`. The default port is 8080.
* `MOONCAKE_MASTER`: The network address of the `master service`. The default port is 50051.
* `MOONCAKE_PROTOCOL`: The protocol used by Mooncake. Supported values are `"rdma"` or `"tcp"`. For optimal performance, `"rdma"` is recommended.
* `MOONCAKE_DEVICE`: The RDMA devices used by Mooncake. You can leave this unset for both `"tcp"` and `"rdma"` protocols—Mooncake auto-detects RDMA devices when needed. Set it only if you need to override the detected devices. Available devices can be listed using the `ibv_devices` command.
* `MOONCAKE_GLOBAL_SEGMENT_SIZE`: The amount of memory (in bytes) contributed to the global memory pool. If at least one `store service` is launched, then this value could be set to `0`. In this case, the `SGLang server` will not contribute any memory to the system. Note that KV tensors cached in the contributed memory will be lost once this process terminates; however, this will not cause any system errors.

### Using extra-config of SGLang arguments to configure Mooncake

```bash
python -m sglang.launch_server \
    --enable-hierarchical-cache \
    --hicache-storage-backend mooncake \
    --model-path [model_path] \
    --hicache-storage-backend-extra-config '{"master_server_address": "127.0.0.1:50051", "local_hostname": "localhost", "metadata_server": "http://127.0.0.1:8080/metadata", "global_segment_size": 4294967296, "local_buffer_size": 16777216, "protocol": "rdma", "device_name": "mlx5_0,mlx5_1"}'
```

**Important: Understanding Global Segment Size**

`global_segment_size` for `store service` and `MOONCAKE_GLOBAL_SEGMENT_SIZE` for `SGLang service`: This parameter specifies the amount of memory each instance contributes to the distributed memory pool. The total memory available for KV cache storage across the cluster is the sum of the memory contributed by all instances.

Adjust this value according to the system's available memory and expected cache requirements.

### Launch Mooncake `store service` (Optional)

Launching the `store service` by itself provides dedicated storage capacity and decouples the storage lifecycle from the SGLang process.

First, create and save a configuration file in JSON format. For example:

```json
{
    "local_hostname": "localhost",
    "metadata_server": "http://localhost:8080/metadata",
    "master_server_address": "localhost:50051",
    "protocol": "rdma",
    "device_name": "mlx5_0,mlx5_1",
    "global_segment_size": 2684354560,
    "local_buffer_size": 0
}
```

Parameter Explanation:

* `local_hostname`: The hostname of the `store service`.
* `metadata_server`: The network address of the `metadata service`. The default port is 8080.
* `master_server_address`: The network address of the `master service`. The default port is 50051.
* `protocol`: The protocol used by Mooncake. Supported values are `"rdma"` or `"tcp"`. For optimal performance, `"rdma"` is recommended.
* `device_name`: The RDMA devices used by Mooncake. You can leave this unset for both `"tcp"` and `"rdma"`—Mooncake auto-detects RDMA devices when available. Set it only when you need to specify devices explicitly. Available devices can be listed using the `ibv_devices` command.
* `global_segment_size`: The amount of memory (in bytes) contributed to the global memory pool. A larger value allows Mooncake to cache more KV tensors.
* `local_buffer_size`: Local buffer is used to do request operations such as `Get` or `Put`. In this case, it is set to 0 because the instance functions solely as a storage server, contributing memory to the global pool without issuing any request operations.

Then start the `store service`:

```bash
python -m mooncake.mooncake_store_service --config=[config_path]
```



### Distributed Deployment

Distributed deployment of Mooncake is straightforward. Similar to the single-node setup, start one `metadata service` and one `master service` for this cluster. Then start a `store service` on each server.

Mooncake also supports high availability mode. This mode enhances fault tolerance by running the `master service` as a cluster of multiple master nodes coordinated through an `etcd` cluster. The master nodes use `etcd` to elect a leader, which is responsible for handling client requests. For more details about how to deploy in this mode, please refer to our [documents](https://kvcache-ai.github.io/Mooncake/).

## Troubleshooting

**RDMA Registration Failure**:

* In some environments, RDMA registration may require root privileges. In this case, try running the program as root.
* In certain environments (e.g., eRDMA), there is an upper limit on the total amount of RDMA memory that can be registered. Once this limit is exceeded, registration will fail. To resolve this, you can lower the value of `MOONCAKE_GLOBAL_SEGMENT_SIZE`, or reduce the host memory allocated to HiCache in the `SGLang server` (since this memory is fully registered with RDMA to enable zero-copy).

## Test Mooncake Store

This test is intended for developers to quickly verify that the MooncakeStore class interfaces are functioning correctly.

First, start the `metadata service` and `master service`. Then run the `test_mooncake_store.py`. A 16 MB global segment size is enough to run this test.

```bash
MOONCAKE_TE_META_DATA_SERVER="http://127.0.0.1:8080/metadata" \
MOONCAKE_MASTER=127.0.0.1:50051 \
MOONCAKE_PROTOCOL="rdma" \
MOONCAKE_DEVICE="mlx5_0,mlx5_1" \
MOONCAKE_GLOBAL_SEGMENT_SIZE=16777216 \
python3 [path of test_mooncake_store.py]
```

If all tests pass, the message "✅ All tests passed" will be printed at the end.
