# Multi-Tenant Deployment

## Configure the Master

### File Connector

Tenant quota admission is disabled by default. Enable strict multi-tenant mode on the master when you want memory writes admitted against connector-managed per-tenant quota:

```bash
mooncake_master \
  --enable_multi_tenants=true \
  --tenant_quota_connector_type=file \
  --tenant_quota_connector_uri=/etc/mooncake/tenant_quotas.yaml
```

### etcd Connector

You can also store the same YAML policy in etcd when Mooncake Store is built with `STORE_USE_ETCD=ON`:

```bash
mooncake_master \
  --enable_multi_tenants=true \
  --cluster_id=mooncake_cluster \
  --tenant_quota_connector_type=etcd \
  --tenant_quota_connector_uri=127.0.0.1:2379
```

The etcd connector stores the policy at `mooncake-store/<cluster_id>/tenant_quota_policy`. If the key does not exist, the master starts with an empty policy so the first tenant policy can be created through the admin API. It shares the process-wide store etcd client used by HA/oplog, so if HA or oplog also uses etcd, `tenant_quota_connector_uri` must match those etcd endpoints.

## Define the Tenant Policy

The policy must use schema version `1`; tenant names must be non-empty, unique, must not start with `_`, and must not contain NUL or control characters; quotas must be positive integers with optional `B`, `KB`, `MB`, `GB`, or `TB` units:

```yaml
version: 1

tenants:
  - name: tenant-a
    quota: 200GB

  - name: tenant-b
    quota: 500GB
```

When strict multi-tenant mode is enabled, write requests must include a registered tenant. The `default` tenant is not special unless it is explicitly registered in the connector policy.

## Manage Tenant Quotas

The same HTTP port used for metrics exposes the tenant quota admin API:

```bash
# List tenant quota snapshots
curl -s http://<master_host>:9003/api/v1/tenant_quotas

# Query one tenant
curl -s "http://<master_host>:9003/api/v1/tenant_quotas?tenant_id=tenant-a"

# Upsert an explicit policy. Explicit tenant policies must be positive.
curl -s -X PUT "http://<master_host>:9003/api/v1/tenant_quotas?tenant_id=tenant-a" \
  -H 'Content-Type: application/json' \
  -d '{"requested_quota_bytes":2147483648}'

# Delete an explicit policy. The tenant must not own objects or quota usage.
curl -s -X DELETE "http://<master_host>:9003/api/v1/tenant_quotas?tenant_id=tenant-a"
```

Each tenant quota snapshot returns:

```json
{
  "success": true,
  "data": {
    "tenant_id": "tenant-a",
    "requested_quota_bytes": 2147483648,
    "effective_quota_bytes": 2147483648,
    "used_bytes": 0,
    "reserved_bytes": 0,
    "committed_count": 0,
    "metadata_object_count": 0,
    "over_quota": false,
    "has_explicit_policy": true
  }
}
```

In HA mode, quota admin requests are served only by the active master service. Standby, candidate, or inactive services return HTTP 503. If strict multi-tenant mode is disabled, the quota admin API returns HTTP 409 with `UNAVAILABLE_IN_CURRENT_MODE`. Deleting a non-empty tenant returns HTTP 409 with `TENANT_NOT_EMPTY`.

## SGLang

When Mooncake is used as the HiCache storage backend, set `tenant_id` in the
Mooncake backend configuration:

```bash
--hicache-storage-backend mooncake \
--hicache-storage-backend-extra-config \
  '{"master_server_address":"127.0.0.1:50051","tenant_id":"tenant-a"}'
```

Alternatively, add `tenant_id` to the JSON file selected by
`SGLANG_HICACHE_MOONCAKE_CONFIG_PATH`, or use `MOONCAKE_TENANT_ID` when loading
the Mooncake configuration from environment variables. SGLang forwards the
resolved value to the Mooncake client.

All prefill, decode, and replica instances that should share KV cache entries
must use the same `tenant_id` and compatible model and release namespaces.

## vLLM

Add `tenant_id` to the Mooncake client JSON configuration:

```json
"tenant_id": "tenant-a"
```

Point `MOONCAKE_CONFIG_PATH` at that file and enable
`MooncakeStoreConnector` through `--kv-transfer-config`:

```bash
MOONCAKE_CONFIG_PATH=/path/to/mooncake_config.json \
vllm serve <model> \
  --kv-transfer-config \
  '{"kv_connector":"MooncakeStoreConnector","kv_role":"kv_both"}'
```

`MooncakeStoreConnector` reads the JSON during initialization and passes its
tenant ID to the Mooncake client.

All prefill, decode, and replica instances that should share KV cache entries
must use the same `tenant_id` and compatible model and release namespaces.
