# Kubernetes High Availability Deployment

This guide covers deploying Mooncake Store with K8s-native leader election. The K8s Lease backend uses `coordination.k8s.io/v1` Lease objects for leader election, removing the need for an external etcd cluster when running on Kubernetes.

## Overview

In HA mode, multiple Mooncake Master replicas run as a StatefulSet. One master is elected leader and serves requests; the others remain on standby. If the leader fails, a standby acquires the Lease and becomes the new leader.

The K8s Lease backend leverages the standard `client-go` leader election library. Lease parameters are: duration 5s, renew deadline 3s, retry period 1s.

## Build

Enable the K8s Lease backend at build time:

```bash
cmake .. -DSTORE_USE_K8S_LEASE=ON
```

> **Note:** Do not enable `STORE_USE_K8S_LEASE` and `STORE_USE_ETCD` simultaneously. Both backends compile as Go shared libraries with separate Go runtimes, and loading two Go runtimes in a single process is unsupported.

## RBAC Requirements

The master pods need a ServiceAccount with permission to manage Lease objects in their namespace:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: mooncake
  namespace: <namespace>
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: mooncake-lease-manager
  namespace: <namespace>
rules:
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "create", "update", "list", "watch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: mooncake-lease-manager
  namespace: <namespace>
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: mooncake-lease-manager
subjects:
- kind: ServiceAccount
  name: mooncake
  namespace: <namespace>
```

## Master Configuration

Master startup flags for K8s HA:

- `--enable_ha` (bool, default `false`): Enable HA mode.
- `--ha_backend_type` (str, default `etcd`): HA backend. Set to `k8s` for K8s Lease.
- `--ha_backend_connstring` (str): Connection string for the HA backend. For K8s Lease, use the format `<namespace>/<lease-name>` (e.g., `mooncake-ha/mooncake-leader`).
- `--rpc_address` (str, default `0.0.0.0`): RPC bind address. The Lease `holderIdentity` is set to `<rpc_address>:<rpc_port>`, so it must be the pod IP, not `0.0.0.0`. Use the Downward API to inject `status.podIP` (see StatefulSet example below).
- `--enable_http_metadata_server` (bool, default `false`): Enable the embedded HTTP metadata server. Required for transfer engine metadata in HA mode.

Example:

```bash
mooncake_master \
  --enable_ha=true \
  --ha_backend_type=k8s \
  --ha_backend_connstring=mooncake-ha/mooncake-leader \
  --rpc_address=$(POD_IP) \
  --enable_http_metadata_server=true
```

```yaml
enable_ha: true
ha_backend_type: "k8s"
ha_backend_connstring: "mooncake-ha/mooncake-leader"
enable_http_metadata_server: true
```

## Kubernetes Manifests

A complete deployment consists of: Namespace, RBAC (above), headless Service, StatefulSet, and optionally a client pod for testing.

### Headless Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: mooncake-master
  namespace: <namespace>
spec:
  clusterIP: None
  selector:
    app: mooncake-master
  ports:
  - port: 50051
    name: rpc
  - port: 8080
    name: http
```

### StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mooncake-master
  namespace: <namespace>
spec:
  serviceName: mooncake-master
  replicas: 3
  selector:
    matchLabels:
      app: mooncake-master
  template:
    metadata:
      labels:
        app: mooncake-master
    spec:
      serviceAccountName: mooncake
      containers:
      - name: master
        image: <your-mooncake-image>
        command: ["mooncake_master"]
        args:
        - --enable_ha=true
        - --ha_backend_type=k8s
        - --ha_backend_connstring=<namespace>/mooncake-leader
        - --rpc_address=$(POD_IP)
        - --rpc_port=50051
        - --enable_http_metadata_server=true
        - --http_metadata_server_port=8080
        env:
        - name: POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: LD_LIBRARY_PATH
          value: /usr/local/lib
        ports:
        - containerPort: 50051
          name: rpc
        - containerPort: 8080
          name: http
```

See `k8s-test/manifests.yaml` for a complete working example.

## Client Discovery

Clients connect to the HA cluster using a `k8s://` connection string:

```python
import mooncake

client = mooncake.Client()
client.setup(
    metadata_server="http://<leader-ip>:8080/metadata",
    master_server_addr="k8s://<namespace>/<lease-name>",
)
```

The `master_server_addr` parameter accepts the format `k8s://<namespace>/<lease-name>`. The client reads the K8s Lease to discover the current leader's address for RPC operations.

> **Note:** The `metadata_server` parameter currently requires the leader's IP directly. On failover, the client's heartbeat thread automatically discovers the new leader via the Lease and reconnects. See [Known Limitations](#known-limitations) for details.

## Known Limitations

- **Metadata server routing**: The `metadata_server` parameter (`http://<leader-ip>:8080/metadata`) is currently bound to the leader's pod IP and does not automatically follow leader changes. On failover, the client's heartbeat thread discovers the new leader and reconnects, but the initial `metadata_server` URL must point to a reachable leader. A future enhancement will use K8s label-based routing (a Service selecting `mooncake-role: leader`) to provide a stable metadata endpoint that survives failover.
