# Mooncake Store HA Deployment

This guide describes how to deploy Mooncake Store in both single-master and
high-availability (HA) modes. It focuses on the deployment choices that affect
the Store master runtime, client failover, Transfer Engine metadata, and
snapshot restore.

For architecture and capability boundaries, see
[Mooncake Store High Availability](../design/mooncake-store-ha.md). For a full
flag reference and operational knobs, see
[Mooncake Store Deployment & Operations Guide](./mooncake-store-deployment-guide.md).

## Deployment Modes

Mooncake Store supports two deployment models:

- **Single-master mode**
  - One `mooncake_master` process serves all client RPCs.
  - Clients connect to the master directly through `host:port`.
  - Transfer Engine metadata may use `http`, `etcd`, or `redis`.
  - This is the simplest option for development, validation, and small
    clusters.
- **HA mode**
  - Multiple master processes share one HA backend.
  - Only one master serves Store RPCs at a time.
  - Clients resolve the current leader through a backend URI such as
    `etcd://...` or `redis://...`.
  - Snapshot restore may be combined with HA to shorten failover recovery.

The Store HA backend and the Transfer Engine metadata backend are configured
independently. They may use the same external service in deployment, but they
represent different control planes.

## Build Requirements

To build Mooncake Store without HA:

```bash
cmake -S . -B build \
  -DWITH_STORE=ON

cmake --build build -j
```

To build Mooncake Store with both supported HA backends and all Transfer Engine
metadata backends enabled:

```bash
cmake -S . -B build-ha \
  -DWITH_STORE=ON \
  -DSTORE_USE_ETCD=ON \
  -DSTORE_USE_REDIS=ON \
  -DUSE_ETCD=ON \
  -DUSE_REDIS=ON \
  -DUSE_HTTP=ON \
  -DBUILD_UNIT_TESTS=ON

cmake --build build-ha -j
```

Backend flags are split intentionally:

- `STORE_USE_ETCD` and `STORE_USE_REDIS` enable Store HA backends.
- `USE_ETCD`, `USE_REDIS`, and `USE_HTTP` enable Transfer Engine metadata
  backends.

If your deployment only needs one backend, enable only the matching
`STORE_USE_*` or `USE_*` flag set.

For Redis-backed builds, install the `hiredis` development package on the build
machine.

## Connection Model

Mooncake Store uses different connection strings for different planes:

| Plane | Configuration surface | Example |
| --- | --- | --- |
| Store leader discovery | `master_server_entry` in the API, `--master_server_address` in `mooncake_client` | `10.0.0.11:50051`, `etcd://10.0.0.1:2379;10.0.0.2:2379`, `redis://redis-prod.example.com:6379` |
| Store HA backend | `--ha-backend-type`, `--ha-backend-connstring` | `etcd` + `10.0.0.1:2379;10.0.0.2:2379`, `redis` + `redis-prod.example.com:6379` |
| Transfer Engine metadata | `metadata_server` or `--metadata_server` | `http://10.0.0.11:8080/metadata`, `10.0.0.1:2379`, `redis://redis-prod.example.com:6379` |
| Snapshot catalog store | `--snapshot_catalog_store_type`, `--snapshot_catalog_store_connstring` | `embedded`, `redis` + `redis-prod.example.com:6379` |
| Snapshot object store | `--snapshot_object_store_type` | `local`, `s3` |

Two format rules matter:

- The master flag `--ha-backend-connstring` always uses the backend-native
  connection string, such as `host:port` for Redis or semicolon-separated
  endpoints for etcd.
- Client-side failover uses a backend URI such as `etcd://...` or
  `redis://...` for the master entry.

If `--snapshot_catalog_store_type=redis` is selected and
`--snapshot_catalog_store_connstring` is omitted, the master falls back to
`--ha-backend-connstring`.

## Single-Master Deployment

Single-master mode is the fastest way to bring up Mooncake Store. The simplest
setup is:

- one `mooncake_master`
- one Transfer Engine metadata backend
- clients pointing directly at that master

### Option A: embedded HTTP metadata

This is the default choice for development and small clusters:

```bash
mooncake_master \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080 \
  --rpc-address=0.0.0.0 \
  --rpc-port=50051 \
  --metrics_port=9003
```

Example standalone client:

```bash
mooncake_client \
  --host=10.0.0.21 \
  --port=50052 \
  --metadata_server=http://10.0.0.11:8080/metadata \
  --master_server_address=10.0.0.11:50051 \
  --global_segment_size=4GB \
  --protocol=tcp
```

### Option B: external etcd or Redis metadata

If Transfer Engine metadata already runs on a shared backend, point the client
at that service instead:

- etcd metadata: `metadata_server=10.0.0.1:2379`
- Redis metadata: `metadata_server=redis://redis-prod.example.com:6379`

In single-master mode, the master entry remains a direct address such as
`10.0.0.11:50051`.

## HA Deployment

In HA mode, all master processes must share:

- the same `--ha-backend-type`
- the same `--ha-backend-connstring`
- the same `--cluster_id`

Each master process must still use its own:

- `--rpc-address`
- `--rpc-port`
- `--metrics_port`

Clients should use the backend URI as the master entry so failover is resolved
through the HA backend instead of a fixed leader address.

### etcd backend

Use etcd when you want the most complete HA capability set in the current code
base, including the existing etcd-based oplog path.

Example master:

```bash
mooncake_master \
  --enable-ha=true \
  --ha-backend-type=etcd \
  --ha-backend-connstring=10.0.0.1:2379;10.0.0.2:2379;10.0.0.3:2379 \
  --cluster_id=mooncake-store-prod \
  --rpc-address=10.0.0.11 \
  --rpc-port=50051 \
  --metrics_port=9003
```

Example client:

```bash
mooncake_client \
  --host=10.0.0.21 \
  --port=50052 \
  --metadata_server=http://10.0.0.11:8080/metadata \
  --master_server_address=etcd://10.0.0.1:2379;10.0.0.2:2379;10.0.0.3:2379 \
  --global_segment_size=4GB \
  --protocol=tcp
```

`--etcd_endpoints` is still accepted as a backward-compatible fallback when
`--ha-backend-connstring` is empty, but new deployments should prefer
`--ha-backend-connstring`.

### Redis backend

Use Redis when your environment already standardizes on Redis and you want
Redis-backed leadership, Redis-backed client failover, and an optional Redis
snapshot catalog.

Redis authentication is configured through environment variables:

```bash
export MC_REDIS_USERNAME='redis-user'
export MC_REDIS_PASSWORD='redis-password'
export MC_REDIS_DB_INDEX=1
```

Behavior:

- `MC_REDIS_PASSWORD` authenticates against the default Redis user when no
  username is provided.
- `MC_REDIS_USERNAME` plus `MC_REDIS_PASSWORD` enables ACL user
  authentication.
- `MC_REDIS_DB_INDEX` selects the Redis logical DB for the current process.

Example master:

```bash
export MC_REDIS_USERNAME='redis-user'
export MC_REDIS_PASSWORD='redis-password'

mooncake_master \
  --enable-ha=true \
  --ha-backend-type=redis \
  --ha-backend-connstring=redis-prod.example.com:6379 \
  --cluster_id=mooncake-store-prod \
  --rpc-address=10.0.0.11 \
  --rpc-port=50051 \
  --metrics_port=9003
```

Example client:

```bash
export MC_REDIS_USERNAME='redis-user'
export MC_REDIS_PASSWORD='redis-password'
export MC_STORE_CLUSTER_ID=mooncake-store-prod
export MC_METADATA_CLUSTER_ID=mooncake-te-prod

mooncake_client \
  --host=10.0.0.21 \
  --port=50052 \
  --metadata_server=redis://redis-prod.example.com:6379 \
  --master_server_address=redis://redis-prod.example.com:6379 \
  --global_segment_size=4GB \
  --protocol=tcp
```

When one Redis service is shared by Store HA and Transfer Engine metadata, use
different namespaces:

| Plane | Namespace control |
| --- | --- |
| Store HA on the master | `--cluster_id` |
| Store HA on the client | `MC_STORE_CLUSTER_ID` |
| Redis snapshot catalog | reuses `cluster_id` |
| Transfer Engine metadata | `MC_METADATA_CLUSTER_ID` |

Namespace mismatches commonly show up as:

- clients resolving an empty or stale master view
- standby restore failing to find the latest snapshot descriptor
- Transfer Engine metadata lookups returning missing segment or RPC data

## Snapshot Restore Options

Snapshot handling is split into two configurable backends:

- **Snapshot catalog store**
  - Tracks which snapshot descriptor is the latest recoverable one.
  - Supported values: `embedded`, `redis`
- **Snapshot object store**
  - Stores the actual snapshot payload files.
  - Supported values: `local`, `s3`

Typical combinations are:

- `embedded` catalog plus `local` object store
- `embedded` catalog plus `s3` object store
- `redis` catalog plus `local` object store
- `redis` catalog plus `s3` object store

Redis may store the snapshot catalog, but it does not store snapshot payload
objects. Payload data must still go to `local` or `s3`.

### Example: embedded catalog plus local object store

```bash
export MOONCAKE_SNAPSHOT_LOCAL_PATH=/data/mooncake-snapshots

mooncake_master \
  --enable-ha=true \
  --ha-backend-type=etcd \
  --ha-backend-connstring=10.0.0.1:2379;10.0.0.2:2379;10.0.0.3:2379 \
  --cluster_id=mooncake-store-prod \
  --enable_snapshot=true \
  --enable_snapshot_restore=true \
  --snapshot_object_store_type=local \
  --snapshot_catalog_store_type=embedded
```

### Example: Redis catalog plus S3 object store

```bash
export MC_REDIS_USERNAME='redis-user'
export MC_REDIS_PASSWORD='redis-password'

mooncake_master \
  --enable-ha=true \
  --ha-backend-type=redis \
  --ha-backend-connstring=redis-prod.example.com:6379 \
  --cluster_id=mooncake-store-prod \
  --enable_snapshot=true \
  --enable_snapshot_restore=true \
  --snapshot_object_store_type=s3 \
  --snapshot_catalog_store_type=redis \
  --snapshot_catalog_store_connstring=redis-prod.example.com:6379
```

### Restore cleanup behavior

By default, restore keeps complete objects even if their lease has expired.
Only incomplete replicas are removed automatically.

To also drop complete, lease-expired, non-soft-pinned objects during restore,
set either:

- `--cleanup_expired_on_restore=true`
- `MC_CLEANUP_EXPIRED_ON_RESTORE=1`

The environment variable overrides the final master setting.

## K8s Example

The most common K8s deployment today is a multi-master StatefulSet with one
external Redis service used for:

- Store HA leadership
- optional Redis snapshot catalogs
- optional Transfer Engine metadata

Use namespace separation rather than Redis DB sharing to isolate Store HA from
Transfer Engine metadata.

Example fragment:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mooncake-master
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
      containers:
        - name: master
          image: mooncake-store:latest
          env:
            - name: POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: MC_REDIS_USERNAME
              valueFrom:
                secretKeyRef:
                  name: mooncake-redis-auth
                  key: username
            - name: MC_REDIS_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: mooncake-redis-auth
                  key: password
            - name: MC_STORE_CLUSTER_ID
              value: mooncake-store-prod
            - name: MC_METADATA_CLUSTER_ID
              value: mooncake-te-prod
          command:
            - /usr/local/bin/mooncake_master
          args:
            - --enable-ha=true
            - --ha-backend-type=redis
            - --ha-backend-connstring=redis-prod.example.com:6379
            - --cluster_id=mooncake-store-prod
            - --rpc-address=$(POD_IP)
            - --rpc-port=50051
            - --metrics_port=9003
```

For an etcd-backed K8s deployment, keep the same master layout and replace only
the HA backend settings:

- `--ha-backend-type=etcd`
- `--ha-backend-connstring=<etcd service endpoints>`
- `--master_server_address=etcd://<etcd service endpoints>` on clients

If clients also use `redis://...` for Store leader discovery or Transfer Engine
metadata, mount the same Redis authentication secret in the client pods.

## Operational Notes

- Every master exposes its admin and metrics endpoints even when it is not the
  serving leader.
- Snapshot payload storage always uses `local` or `s3`; Redis only stores
  catalog metadata.
- Redis-backed HA currently provides leadership failover plus snapshot-backed
  standby bootstrap. Continuous oplog following is still etcd-only.
