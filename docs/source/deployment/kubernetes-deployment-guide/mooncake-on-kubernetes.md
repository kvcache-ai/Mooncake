# Mooncake on Kubernetes

Deploy a Mooncake Store cluster — a `mooncake-master` plus replicated `mooncake-store` nodes — with plain Kubernetes objects (`Deployment` and `Service`).

Use it together with the [Mooncake Store Deployment & Tuning Guide](../mooncake-store-deployment-guide.md): that guide explains the components and tuning knobs; this page maps them to Kubernetes objects.

## Deploy the Mooncake Store cluster

A shareable Store cluster has one `mooncake-master` (the RPC coordinator) and a replicated set of stateless `mooncake-store` nodes that contribute DRAM to the pool. The nodes use Mooncake's P2P handshake (`P2PHANDSHAKE`) for Transfer Engine peer discovery, so there is no separate metadata service to run — each node stores its metadata locally and exchanges it with peers during connection setup. It needs no GPUs, and multiple inference deployments can point at the same cluster. The store nodes reach the master through the `mooncake-master` `Service`.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mooncake-master
  labels:
    app: mooncake-master
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mooncake-master
  template:
    metadata:
      labels:
        app: mooncake-master
    spec:
      containers:
        - name: mooncake-master
          image: lmsysorg/sglang:v0.5.5
          command: ["mooncake_master"]
          args:
            - --rpc_address
            - $(POD_IP)
            - --rpc_port
            - "50051"
            - --metrics_port
            - "9003"
          env:
            - name: POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
          ports:
            - name: rpc
              containerPort: 50051
            - name: metrics
              containerPort: 9003
          readinessProbe:
            tcpSocket:
              port: 50051
            initialDelaySeconds: 10
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: mooncake-master
  labels:
    app: mooncake-master
spec:
  type: ClusterIP
  selector:
    app: mooncake-master
  ports:
    - name: rpc
      port: 50051
      targetPort: 50051
    - name: metrics
      port: 9003
      targetPort: 9003
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mooncake-store
  labels:
    app: mooncake-store
spec:
  replicas: 3
  selector:
    matchLabels:
      app: mooncake-store
  template:
    metadata:
      labels:
        app: mooncake-store
    spec:
      containers:
        - name: mooncake-store
          image: lmsysorg/sglang:v0.5.5
          command: ["python3", "-m", "mooncake.mooncake_store_service"]
          args: ["--port", "8088"]
          env:
            - name: MOONCAKE_MASTER
              value: "mooncake-master:50051"
            - name: MOONCAKE_TE_META_DATA_SERVER
              value: "P2PHANDSHAKE"
            - name: MOONCAKE_GLOBAL_SEGMENT_SIZE
              value: "10gb"
            - name: MOONCAKE_LOCAL_BUFFER_SIZE
              value: "0"
            - name: MOONCAKE_PROTOCOL
              value: "rdma"
          resources:
            requests:
              memory: "16Gi"
            limits:
              memory: "16Gi"
```

See [Notes](#notes) for capacity, protocol (TCP/RDMA), and metadata guidance.

**Verify:**

```bash
kubectl get pods -l app=mooncake-master
kubectl get pods -l app=mooncake-store
# Master metrics summary:
kubectl port-forward svc/mooncake-master 9003:9003 &
curl -s http://localhost:9003/metrics/summary
```

## Notes

**Metadata (P2P handshake).** These manifests use Mooncake's P2P handshake (`MOONCAKE_TE_META_DATA_SERVER: P2PHANDSHAKE`): each node stores Transfer Engine metadata locally and exchanges it with peers during connection setup, so there is nothing extra to run and the `mooncake-master` needs no `--*http_metadata_server*` flags. This is the recommended starting point. For large or long-lived clusters, switch the store nodes to the master's embedded HTTP metadata server or an external etcd/Redis instead; see the store guide's [Deployment Scenarios](../mooncake-store-deployment-guide.md#deployment-scenarios).

**High availability.** A single `mooncake-master` is a single point of failure. For HA, see the store guide's [High Availability](../mooncake-store-deployment-guide.md#deployment-scenarios) section for etcd/Redis backends.

**TCP vs RDMA.** `MOONCAKE_PROTOCOL` selects the fabric. These manifests use `rdma`. Granting pods RDMA access is cluster-specific and not fully wired into the YAML above; the production reference (see [RBG Integration](rbg-integration)) does it with `hostNetwork: true`, a hostPath mount of `/dev/infiniband`, `privileged` + `IPC_LOCK`/`SYS_RESOURCE`, and an explicit NIC list via `MOONCAKE_DEVICE=<rdma-devices>`. (A device-plugin `rdma/hca` resource with `MC_MS_AUTO_DISC` / `MC_MS_FILTERS` auto-discovery is an alternative on clusters set up that way.) Switch `MOONCAKE_PROTOCOL` to `tcp` on clusters without an RDMA fabric.

**Capacity.** Keep `MOONCAKE_GLOBAL_SEGMENT_SIZE` within each pod's memory `limit`. A pure store node issues no `Get`/`Put` itself, so its `MOONCAKE_LOCAL_BUFFER_SIZE` is small; the production RDMA reference sets a modest non-zero buffer (`67108864` = 64 MiB) rather than `0`.

**Images.** The example uses `lmsysorg/sglang:v0.5.5`. This tag is **not** a reproducible pin — for production, replace it with a verified tag or digest.
