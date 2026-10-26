# Object Storage Offload

## Overview

Object storage services, such as OSS and S3, provide key-based storage.
Mooncake Store integrates object storage through `ObjectStorageAdapter` in the
existing `FileStorage` offload path. As with local SSD and NVMe KV backends,
the master records `LOCAL_DISK` replicas owned by a real client; readers still
access the payload through that owner.

Two adapters are available:

| Adapter | `MOONCAKE_DISTRIBUTED_FS_TYPE` | Services |
|---------|--------------------------------|----------|
| OSS | `oss` | Alibaba Cloud OSS |
| S3-compatible | `s3` | AWS S3, SeaweedFS, MinIO, Ceph RGW and other services that accept AWS Signature V4 |

The two adapters are independent and are configured with separate variables,
described in the "OSS configuration" and "S3-compatible configuration" sections
below.

For implementation details, see [OSS Backend Design](../design/store/oss-backend.md).

## Prerequisites

- An existing bucket and an endpoint reachable from each offload owner. No
  filesystem mount of the object store is required.
- Credentials with permission to PUT, GET, HEAD, LIST, and DELETE within the
  chosen namespace. Temporary credentials (an OSS STS token or an S3 session
  token) are supported.
- A dedicated object-key prefix for each offload owner.
- An existing absolute, writable, non-symlink directory for
  `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH`, required by common `FileStorage`
  initialization. This does not enable a local SSD cache for object storage.
- libcurl and OpenSSL development libraries and headers.

## Build Support

The build enables both adapters when libcurl and OpenSSL are available. No
service SDK or additional build flag is required. CMake reports each adapter
on its own line (`Alibaba Cloud OSS adapter: Enabled`,
`S3-compatible adapter: Enabled`). Follow the
[build guide](../getting_started/build.md) to build and install Mooncake.

Batch I/O uses `curl_multi_wait`; libcurl 7.66.0 is not required. Upload-buffer
tuning is optional: with headers older than 7.62.0, the library default is used.

## Topology

```mermaid
flowchart TD
    App["Application or requesting Mooncake client"]
    Master["Mooncake master"]
    Owner["Offload-owning real client"]
    FileStorage["FileStorage"]
    Backend["DistributedStorageBackend"]
    Adapter["ObjectStorageAdapter (OSS or S3)"]
    Store["Bucket and owner prefix"]

    App <-->|"metadata query"| Master
    App <-->|"offload RPC and Transfer Engine"| Owner
    Owner <-->|"offload heartbeat and LOCAL_DISK updates"| Master
    Owner --> FileStorage --> Backend --> Adapter
    Adapter <-->|"HTTP requests"| Store
```

Only the offload owner needs object-storage credentials. The master and
requesting clients do not directly read or write objects in the bucket.

## Configuration

Set the backend variables and the variables of one adapter in each offload
owner's environment. Each `FileStorage` instance selects one backend: object
storage does not run alongside the local-file or NVMe KV backend within that
instance.

### Backend and namespace

These settings are the same for both adapters.

```bash
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=distributed_storage_backend
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/data/file_storage
export MOONCAKE_DISTRIBUTED_FS_TYPE=oss   # or s3
export MOONCAKE_DISTRIBUTED_ROOT_DIR=/mooncake/my-cluster/owner-1
```

| Environment variable | Setting | Description |
|----------------------|---------|-------------|
| `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR` | `distributed_storage_backend` | Select the backend that hosts the object-storage adapters. |
| `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | `/data/file_storage` by default | Existing local directory required by common initialization; object payloads go to the bucket. |
| `MOONCAKE_DISTRIBUTED_FS_TYPE` | `oss` or `s3` | Select object-storage mode, and which adapter, rather than a filesystem adapter. |
| `MOONCAKE_DISTRIBUTED_ROOT_DIR` | An owner-specific prefix | Use an absolute-style path; the adapter strips leading and trailing slashes. This is not a mount point. |
| `MOONCAKE_DISTRIBUTED_HEALTH_CHECK` | `false` | Write and read back a probe object during initialization, then best-effort delete it. |

Object-storage offload does not require Master DFS configuration and does not
require disabling a separately configured DFS tier. In the offload owner's
environment, `MOONCAKE_DFS_FS_ADAPTER` and `MOONCAKE_DFS_ROOT_DIR` override the
corresponding `MOONCAKE_DISTRIBUTED_*` values because they share a configuration
parser. Leave these overrides unset when using the example above.

Configuration is read at initialization; changing environment variables does
not reconfigure an active adapter or refresh its credentials.

### OSS configuration

```bash
export MOONCAKE_DISTRIBUTED_FS_TYPE=oss
export MOONCAKE_OSS_ENDPOINT=https://oss-cn-hangzhou.aliyuncs.com
export MOONCAKE_OSS_BUCKET=my-mooncake-bucket
export MOONCAKE_OSS_REGION=cn-hangzhou
# Supply MOONCAKE_OSS_ACCESS_KEY_ID and MOONCAKE_OSS_ACCESS_KEY_SECRET
# through your credential-management mechanism, not checked-in scripts.
```

Replace the example endpoint, bucket, region, and owner prefix for your
deployment.

| Environment variable | Default | Description |
|----------------------|---------|-------------|
| `MOONCAKE_OSS_ENDPOINT` | Required | Endpoint including `http://` or `https://`. Alias: `OSS_ENDPOINT`. |
| `MOONCAKE_OSS_BUCKET` | Required | Existing bucket. Alias: `OSS_BUCKET`. |
| `MOONCAKE_OSS_REGION` | Required | OSS signing region. Alias: `OSS_REGION`. |
| `MOONCAKE_OSS_ACCESS_KEY_ID` | Required unless anonymous | Access key ID. Alias: `OSS_ACCESS_KEY_ID`. |
| `MOONCAKE_OSS_ACCESS_KEY_SECRET` | Required unless anonymous | Access key secret. Alias: `OSS_ACCESS_KEY_SECRET`. |
| `MOONCAKE_OSS_SECURITY_TOKEN` | Empty | Optional STS token. Alias: `OSS_SESSION_TOKEN`. |
| `MOONCAKE_OSS_PATH_STYLE` | `false` | Use `endpoint/bucket/key` instead of virtual-hosted bucket addressing. |
| `MOONCAKE_OSS_ANONYMOUS` | `false` | Disable signing; only for test endpoints or suitably configured public access. |

Primary names take precedence over aliases, including explicitly empty values.

### S3-compatible configuration

The S3 adapter signs requests with AWS Signature V4.

```bash
export MOONCAKE_DISTRIBUTED_FS_TYPE=s3
export MOONCAKE_S3_ENDPOINT=http://seaweedfs-s3:8333
export MOONCAKE_S3_BUCKET=my-mooncake-bucket
export MOONCAKE_S3_PATH_STYLE=true
# Supply MOONCAKE_S3_ACCESS_KEY_ID and MOONCAKE_S3_SECRET_ACCESS_KEY
# through your credential-management mechanism, not checked-in scripts.
```

| Environment variable | Default | Description |
|----------------------|---------|-------------|
| `MOONCAKE_S3_ENDPOINT` | Required | Endpoint starting with `http://` or `https://`, without a path. Alias: `AWS_ENDPOINT_URL`. |
| `MOONCAKE_S3_BUCKET` | Required | Existing bucket. |
| `MOONCAKE_S3_REGION` | `us-east-1` | Signing region. Falls back to `AWS_REGION`, then `AWS_DEFAULT_REGION`; a warning is logged when none is set. |
| `MOONCAKE_S3_ACCESS_KEY_ID` | Required unless anonymous | Access key ID. Alias: `AWS_ACCESS_KEY_ID`. |
| `MOONCAKE_S3_SECRET_ACCESS_KEY` | Required unless anonymous | Secret access key. Alias: `AWS_SECRET_ACCESS_KEY`. |
| `MOONCAKE_S3_SESSION_TOKEN` | Empty | Optional session token. Alias: `AWS_SESSION_TOKEN`. |
| `MOONCAKE_S3_PATH_STYLE` | `false` | Use `endpoint/bucket/key`. Most self-hosted services (SeaweedFS, MinIO) need `true`; it is forced on for IP-address and `localhost` endpoints. |
| `MOONCAKE_S3_ANONYMOUS` | `false` | Disable signing; only for test endpoints or suitably configured public access. |

Credentials are read as one set: if `MOONCAKE_S3_ACCESS_KEY_ID` or
`MOONCAKE_S3_SECRET_ACCESS_KEY` is set, the key ID, secret and session token
all come from `MOONCAKE_S3_*`; otherwise all three come from `AWS_*`. An
`AWS_SESSION_TOKEN` left in the environment is therefore never sent with
`MOONCAKE_S3_*` keys. Other primary names take precedence over their `AWS_*`
aliases. The adapter does not create buckets. The payload is sent as
`UNSIGNED-PAYLOAD`; use `https://` endpoints outside trusted networks.

The S3 adapter retries transient failures (connection set-up errors, resets,
HTTP 429 and 5xx) up to three attempts with backoff. A request that was sent
and then timed out is not retried, and client errors such as 403 and 404 are
not retried.

### Concurrency and buffers

Each adapter has its own tuning variables with the same meaning and bounds.

| OSS variable | S3 variable | Default | Description |
|--------------|-------------|---------|-------------|
| `MOONCAKE_OSS_MAX_CONNECTIONS` | `MOONCAKE_S3_MAX_CONNECTIONS` | `64` | Maximum admitted requests and total/per-host connections per batch; minimum `1`. Not a process-wide limit. |
| `MOONCAKE_OSS_RECEIVE_BUFFER_SIZE` | `MOONCAKE_S3_RECEIVE_BUFFER_SIZE` | `1048576` (1 MiB) | libcurl receive-buffer suggestion for batch requests, clamped to 16 KiB–10 MiB. Single-request GETs keep the library default. |
| `MOONCAKE_OSS_UPLOAD_BUFFER_SIZE` | `MOONCAKE_S3_UPLOAD_BUFFER_SIZE` | `1048576` (1 MiB) | Upload-buffer suggestion for `PutV` / `PutBatch`, clamped to 16 KiB–2 MiB. Applied only with libcurl headers 7.62.0 or newer; otherwise the library default is used. |

Numeric tuning values are decimal integers. Invalid or out-of-range integers
use the default; the bounds above then apply. Buffer sizes are libcurl
suggestions, not TCP socket-buffer sizes or guaranteed throughput settings.

The common offload heartbeat defaults to 10 seconds and is configured through
`MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS`. Other common client settings are
described in [SSD Offload](ssd/ssd-offload.md).

## Start Mooncake

Start the master with offload enabled:

```bash
mooncake_master --rpc_port=50051 --enable_offload=true
```

After applying the backend settings above, create the required local directory
and start a real client. This example uses a local master and TCP transfers:

```bash
mkdir -p /data/file_storage
export MOONCAKE_MASTER=127.0.0.1:50051
export MOONCAKE_LOCAL_HOSTNAME=127.0.0.1
export MOONCAKE_PROTOCOL=tcp
export MOONCAKE_TE_META_DATA_SERVER=P2PHANDSHAKE
export MOONCAKE_OFFLOAD_ENABLED=true

python -m mooncake.mooncake_store_service
```

Use routable addresses for a multi-node deployment. This launcher example
assumes `MOONCAKE_CONFIG_PATH` is unset; a service configuration file otherwise
takes precedence.

Embedded real-client mode uses the same backend and adapter variables. Pass
`enable_ssd_offload=True` and `ssd_offload_path` to
`MooncakeDistributedStore.setup()` alongside the normal connection and memory
arguments. The [SSD Offload guide](ssd/ssd-offload.md) describes embedded and
standalone real-client deployment modes.

## Troubleshooting

### The adapter cannot initialize

Check build dependencies, required endpoint/region/credential settings, and the
local directory. Check for stale `MOONCAKE_DFS_*` overrides. The optional health
check exercises access to the bucket; it does not test the complete Store read
path.

### Requests fail with authentication or permission errors

Verify the endpoint, signing region, bucket permissions, and the lifetime of
any temporary credentials. The adapters do not refresh credentials
automatically. For S3-compatible services, also check `MOONCAKE_S3_PATH_STYLE`:
most self-hosted services need path-style addressing.

### An object is in the bucket but cannot be read through Store

A bucket object alone is not a readable Store replica. The master must have
the key's metadata and a reachable owner. Keep prefixes owner-specific; a shared
bucket does not make owners interchangeable.

### SSD capacity metrics do not match bucket usage

`MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` supplies a configured capacity value
(default: 2 TiB), not a queried bucket capacity. Master usage tracks registered
`LOCAL_DISK` replicas. There is no backend quota enforcement or automatic object
GC here, so these metrics are not physical bucket usage or a cloud-cost limit.
