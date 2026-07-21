# RBG Integration

This page covers the same Store + Transfer Engine P/D scenario as the [main guide](index), deployed with the [sgl-project/rbg](https://github.com/sgl-project/rbg) operator instead of the vanilla `Deployment` / `Service` manifests on the [Mooncake on Kubernetes](mooncake-on-kubernetes) page. Install the RBG operator before applying any `RoleBasedGroup`.

## RBG example

The upstream RBG repository ships ready-to-use examples for running Mooncake on RBG:

- [sgl-pd-disagg-with-mooncake-te.yaml](https://github.com/sgl-project/rbg/blob/main/examples/inference/ecosystem/mooncake/mooncake-transfer-engine/sgl-pd-disagg-with-mooncake-te.yaml) — SGLang P/D disaggregation using Mooncake's Transfer Engine for KV transfer.
- [vllm-pd-disagg-with-mooncake-te.yaml](https://github.com/sgl-project/rbg/blob/main/examples/inference/ecosystem/mooncake/mooncake-transfer-engine/vllm-pd-disagg-with-mooncake-te.yaml) — vLLM P/D disaggregation using Mooncake's Transfer Engine for KV transfer.
- [standalone-mooncake-store.yaml](https://github.com/sgl-project/rbg/blob/main/examples/inference/ecosystem/mooncake/mooncake-store/standalone-mooncake-store.yaml) — the shareable standalone Mooncake Store cluster (`mooncake-master` + `mooncake-store` roles).

For background on the integration, see the RBG [Mooncake integration KEP](https://github.com/sgl-project/rbg/blob/main/keps/74-mooncake-integration/README.md).

## Production Mooncake Cluster Example

A production Mooncake cluster on RBG (`workloads.x-k8s.io/v1alpha1`): a Mooncake **Store** (one `master` plus NUMA-split `store` pods) and SGLang **prefill/decode** engines, all over RDMA. The two RBGs below are the Mooncake-side backend; a router/gateway (out of scope for this page) fronts the prefill/decode endpoints to make the P/D deployment servable — see the [overview](index) and the [Prefill/Decode Disaggregation quick start](../../integrations/sglang/hicache-quick-start.md).

```{note}
The inline manifests below target `workloads.x-k8s.io/v1alpha1`. For the latest
API version `v1alpha2`, please refer to the upstream examples linked under
[RBG example](#rbg-example) above.
```

| Group | Roles | Purpose |
|---|---|---|
| Store (`qwen3-0`) | `master`, `store-1000gb` | Mooncake Store — the master coordinator plus NUMA-split store pods contributing the DRAM KV pool |
| Workers (`sglang-workers-0`) | `prefill`, `decode` | SGLang engines: `prefill` is a Store/HiCache client **and** P/D transfer; `decode` does P/D transfer only |

```{caution}
These manifests are sanitized **excerpts** that show the structure and the Mooncake wiring — they cannot be applied directly. The `decode` role and the second NUMA store container are abbreviated to comments, and image / paths / devices are `<…>` placeholders. Fill them in against your own cluster before applying.
```

### 1. Store group — master + NUMA store

The Store `RoleBasedGroup` has a `master` (the Store coordinator) and a `store-<size>` role whose pods each run two NUMA-pinned store processes. The RBG operator creates a Service `s-qwen3-0-master` for the master role, so clients reach it at `s-qwen3-0-master:50051` — no Service object of your own. Node scheduling uses custom `kvcache.ai/master` and `kvcache.ai/store` labels so a node belongs to exactly one role/size.

```yaml
apiVersion: workloads.x-k8s.io/v1alpha1
kind: RoleBasedGroup
metadata:
  name: qwen3-0
  namespace: default
  labels: { app.kubernetes.io/part-of: mooncake }
spec:
  roles:
  - name: master
    replicas: 1
    template:
      metadata:
        labels: { role: master, app.kubernetes.io/instance: qwen3-0 }
      spec:
        affinity:
          nodeAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
              nodeSelectorTerms:
              - matchExpressions:
                - { key: kvcache.ai/master, operator: In, values: [qwen3_0_master] }
        containers:
        - name: master
          image: <image>
          command:
          - sh
          - -c
          - |
            ulimit -n 1048576
            ulimit -l unlimited
            mooncake_master \
              --rpc_address=$(POD_IP) \
              --rpc_port=50051 \
              --eviction_high_watermark_ratio=0.9 \
              --default_kv_lease_ttl=10000
          env:
          - { name: POD_IP, valueFrom: { fieldRef: { fieldPath: status.podIP } } }
          - { name: NVIDIA_VISIBLE_DEVICES, value: "void" }   # master needs no GPU
          securityContext:
            # master is an RPC coordinator with no RDMA data path — it does not need
            # `privileged`. IPC_LOCK/SYS_RESOURCE cover the `ulimit -l unlimited` / mlock above.
            privileged: false
            capabilities: { add: ["IPC_LOCK", "SYS_RESOURCE"] }
          livenessProbe:
            exec: { command: ["/bin/sh", "-c", "pgrep -x mooncake_master >/dev/null"] }
            initialDelaySeconds: 20
            periodSeconds: 15
          ports:
          - { containerPort: 50051, name: http }
          - { containerPort: 9003, name: metrics }
    workload: { apiVersion: apps/v1, kind: StatefulSet }

  - name: store-1000gb
    replicas: 3
    template:
      metadata:
        labels:
          role: store-1000gb
          app.kubernetes.io/instance: qwen3-0
          app.kubernetes.io/part-of: mooncake
      spec:
        affinity:
          nodeAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
              nodeSelectorTerms:
              - matchExpressions:
                - { key: kvcache.ai/store, operator: In, values: [qwen3_0_store-1000gb] }
          podAntiAffinity:                    # at most one store pod per node across sizes
            requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchExpressions:
                - { key: app.kubernetes.io/part-of, operator: In, values: [mooncake] }
                - { key: app.kubernetes.io/instance, operator: In, values: [qwen3-0] }
              topologyKey: kubernetes.io/hostname
        hostNetwork: true                     # RDMA
        dnsPolicy: ClusterFirstWithHostNet
        containers:
        - name: store-numa0
          image: <image>
          command:
          - sh
          - -c
          - |
            ulimit -n 1048576
            ulimit -l unlimited
            exec numactl --cpunodebind=0 --membind=0 python3 -m mooncake.mooncake_store_service --port=8099
          env:
          - { name: MOONCAKE_LOCAL_HOSTNAME, valueFrom: { fieldRef: { fieldPath: status.podIP } } }
          - { name: MOONCAKE_MASTER, value: "s-qwen3-0-master:50051" }
          - { name: MOONCAKE_TE_META_DATA_SERVER, value: "P2PHANDSHAKE" }
          - { name: MOONCAKE_GLOBAL_SEGMENT_SIZE, value: "1000gb" }   # DRAM this store contributes
          - { name: MOONCAKE_LOCAL_BUFFER_SIZE, value: "67108864" }
          - { name: MOONCAKE_PROTOCOL, value: "rdma" }
          - { name: MOONCAKE_DEVICE, value: "<rdma-devices>" }
          - { name: MC_ENABLE_DEST_DEVICE_AFFINITY, value: "1" }
          # pin the client metrics HTTP server per container (numa0=9300 / numa1=9301);
          # under hostNetwork two processes cannot share 9300.
          - { name: MOONCAKE_ENABLE_CLIENT_HTTP_SERVER, value: "true" }
          - { name: MOONCAKE_CLIENT_HTTP_PORT, value: "9300" }
          ports: [{ containerPort: 8099 }]
          startupProbe:
            exec: { command: ["sh", "-c", "nc -z 127.0.0.1 8099"] }
            periodSeconds: 10
            failureThreshold: 90
          livenessProbe:
            exec: { command: ["sh", "-c", "nc -z 127.0.0.1 8099"] }
            initialDelaySeconds: 10
            periodSeconds: 10
          securityContext:
            # least-privilege RDMA: no `privileged` needed — IPC_LOCK/SYS_RESOURCE plus
            # the /dev/infiniband device mount below are enough for the Transfer Engine NICs.
            privileged: false
            capabilities: { add: ["IPC_LOCK", "SYS_RESOURCE"] }
          volumeMounts:
          - { mountPath: /dev/infiniband, name: ib }
        # store-numa1: identical, but `numactl --cpunodebind=1 --membind=1`, --port=8100,
        # containerPort 8100, and MOONCAKE_CLIENT_HTTP_PORT=9301.
        volumes:
        - { name: ib, hostPath: { path: /dev/infiniband, type: DirectoryOrCreate } }
    workload: { apiVersion: apps/v1, kind: StatefulSet }
```

### 2. Inference workers — prefill + decode

`sglang-workers-0` runs the SGLang PD engines, and the two roles are wired **differently**:

- **`prefill`** is the Store/HiCache client. It sets `MOONCAKE_MASTER=s-qwen3-0-master:50051` (the Store master), `MOONCAKE_TE_META_DATA_SERVER=P2PHANDSHAKE` (the Transfer Engine coordinates P/D directly, no metadata server), `MOONCAKE_PROTOCOL=rdma` + `MOONCAKE_DEVICE`, and `MOONCAKE_GLOBAL_SEGMENT_SIZE=0` (a **pure client** — it contributes no DRAM; the store pods do). It launches with `--enable-hierarchical-cache --hicache-storage-backend mooncake`. The prefill manifest is shown below.
- **`decode`** only participates in the P/D Transfer Engine — `--disaggregation-mode decode --disaggregation-ib-device`. It is **not** a Store/HiCache client: it sets no `MOONCAKE_MASTER` and enables no hierarchical cache.

```yaml
apiVersion: workloads.x-k8s.io/v1alpha1
kind: RoleBasedGroup
metadata:
  name: sglang-workers-0
  namespace: default
spec:
  roles:
  - name: prefill
    replicas: 4
    template:
      metadata:
        labels:
          app: sglang-worker
          rolebasedgroup.workloads.x-k8s.io/name: sglang-workers-0
          rolebasedgroup.workloads.x-k8s.io/role: prefill
      spec:
        hostNetwork: true                     # RDMA
        dnsPolicy: ClusterFirstWithHostNet
        nodeSelector: { deployment: sglang_0_prefill }
        containers:
        - name: sglang-prefill
          image: <image>
          command:
          - bash
          - -c
          - |
            set -e
            ulimit -n 1048576; ulimit -l unlimited
            python -m sglang.launch_server \
              --model ${MODEL_PATH} --served-model-name Qwen3-0.6B \
              --host 0.0.0.0 --port 8000 \
              --disaggregation-mode prefill \
              --disaggregation-ib-device $IB_DEVICE_LIST \
              --enable-hierarchical-cache --hicache-storage-backend mooncake \
              --tp 8 --page-size 64 --trust-remote-code \
              --enable-metrics --enable-cache-report
              # … model/hardware tuning flags omitted (context length, mem fraction,
              #    NSA backends, EAGLE speculative decoding, KV-cache dtype, etc.)
          env:
          # --- pod identity ---
          - { name: POD_NAME, valueFrom: { fieldRef: { fieldPath: metadata.name } } }
          - { name: POD_IP, valueFrom: { fieldRef: { fieldPath: status.podIP } } }
          - { name: MOONCAKE_LOCAL_HOSTNAME, valueFrom: { fieldRef: { fieldPath: status.podIP } } }
          - { name: SGLANG_HOST_IP, valueFrom: { fieldRef: { fieldPath: status.podIP } } }
          # --- model + fabric ---
          - { name: MODEL_PATH, value: /models/Qwen3-0.6B }
          - { name: IB_DEVICE_LIST, value: "<rdma-devices>" }
          # --- Mooncake wiring ---
          - { name: MOONCAKE_TE_META_DATA_SERVER, value: P2PHANDSHAKE }
          - { name: MOONCAKE_MASTER, value: "s-qwen3-0-master:50051" }   # the Store group's master
          - { name: MOONCAKE_PROTOCOL, value: rdma }
          - { name: MOONCAKE_DEVICE, value: "<rdma-devices>" }
          - { name: MOONCAKE_GLOBAL_SEGMENT_SIZE, value: "0" }   # pure client, contributes no DRAM
          - { name: MC_TE_METRIC, value: "true" }
          # … SGLANG_* / MC_* performance tuning omitted (heartbeat, timeouts,
          #   spec-decoding v2, auto-empty-cache, NCCL, JIT, CPU affinity, PRC port range) …
          ports:
          - { containerPort: 8000, name: http }
          - { containerPort: 8998, name: bootstrap }
          readinessProbe:
            tcpSocket: { port: 8000 }
            initialDelaySeconds: 30
            periodSeconds: 10
          resources:
            limits: { nvidia.com/gpu: "8" }
            requests: { nvidia.com/gpu: "8" }
          securityContext:
            # least-privilege RDMA: no `privileged` needed — IPC_LOCK/SYS_RESOURCE plus
            # the /dev/infiniband device mount below are enough for the NICs.
            privileged: false
            capabilities: { add: ["IPC_LOCK", "SYS_RESOURCE"] }
          volumeMounts:
          - { mountPath: /models, name: model }
          - { mountPath: /dev/shm, name: dshm }
          - { mountPath: /dev/infiniband, name: ib }
        volumes:
        - { name: model, hostPath: { path: <model-host-path>, type: DirectoryOrCreate } }
        - { name: dshm, emptyDir: { medium: Memory, sizeLimit: 1300Gi } }
        - { name: ib, hostPath: { path: /dev/infiniband, type: DirectoryOrCreate } }
    workload: { apiVersion: apps/v1, kind: StatefulSet }

  - name: decode
    replicas: 1
    # Abbreviated excerpt — a real decode PodSpec mirrors the prefill container EXCEPT:
    #   launch:  --disaggregation-mode decode --disaggregation-ib-device $IB_DEVICE_LIST
    #            (NO --enable-hierarchical-cache / --hicache-storage-backend — decode is
    #             not a Store client), plus DP/EP attention, low-latency deepep, etc.
    #   env:     NO MOONCAKE_MASTER / MOONCAKE_GLOBAL_SEGMENT_SIZE / hierarchical cache;
    #            keeps POD_NAME / POD_IP / MODEL_PATH / IB_DEVICE_LIST (P/D transfer only)
    #   sched:   nodeSelector deployment: sglang_0_decode ; dshm sizeLimit 15Gi
    template: { }   # fill in a real PodSpec to deploy
    workload: { apiVersion: apps/v1, kind: StatefulSet }
```

### Mooncake integration points (recap)

- **Store master** — `mooncake_master --rpc_address=$(POD_IP) --rpc_port=50051 …`; the RBG operator exposes it as `s-qwen3-0-master`, and its clients (the store pods and the **prefill** engine) set `MOONCAKE_MASTER=s-qwen3-0-master:50051`.
- **Only prefill is a Store client** — `prefill` enables HiCache (`--enable-hierarchical-cache --hicache-storage-backend mooncake`) and connects to the master; `decode` participates only in the P/D Transfer Engine and connects to no Store.
- **Transfer Engine uses P2P handshake** — `prefill` sets `MOONCAKE_TE_META_DATA_SERVER=P2PHANDSHAKE`; the TE side-channel coordinates prefill↔decode directly, so there is no HTTP metadata server here (unlike the [main guide](index)).
- **Store vs client segment size** — store pods set `MOONCAKE_GLOBAL_SEGMENT_SIZE=1000gb` (they own the DRAM pool); the prefill client sets `0` (contributes no DRAM, only `Get`/`Put`).
- **RDMA fabric** — `MOONCAKE_PROTOCOL=rdma` + `MOONCAKE_DEVICE=<rdma-devices>`, `hostNetwork: true`, `/dev/infiniband` mounted, and the `IPC_LOCK`/`SYS_RESOURCE` capabilities (no `privileged` needed — a `/dev/infiniband` hostPath mount plus those two capabilities is the least-privilege way to reach the NICs; an RDMA device plugin is an alternative).
- **NUMA split** — each store pod runs two `mooncake_store_service` processes, each `numactl`-pinned to one NUMA node with its own port and client-metrics port.

(placeholders)=
### Placeholders

The example values above (`qwen3-0`, `default`, `store-1000gb`, replica counts) are yours to change. The `<…>` placeholders are:

| Placeholder | Replace with |
|---|---|
| `<image>` | the container image (`registry/name:tag`) for this role |
| `<model-host-path>` | host directory holding the model weights |
| `<rdma-devices>` | your RDMA NIC list (e.g. `mlx5_0,mlx5_1,…`) |
