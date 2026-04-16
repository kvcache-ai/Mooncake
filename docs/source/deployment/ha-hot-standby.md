# Mooncake Store HA Hot Standby

Mooncake Store supports a **Hot Standby** mode that keeps a passive replica of the master metadata in sync with the primary. When the primary fails, the standby can be promoted to take over with minimal downtime and no data loss.

## Architecture

```
  ┌─────────────────────┐          OpLog stream           ┌─────────────────────┐
  │   Primary Master    │ ──────────────────────────────▶ │   Standby Master    │
  │  (MasterService)    │                                  │ (HotStandbyService) │
  │                     │  ①  Snapshot bootstrap (once)   │                     │
  │  Oplog  ─────────── │ ──────────────────────────────▶ │  ─── apply oplogs   │
  │  (OpLogManager)     │  ②  Oplog replication (steady)  │  (OpLogApplier)     │
  └─────────────────────┘                                  └─────────────────────┘
           ▲                                                        │
           │           Leader Election (etcd / Redis)              │  promotion
           └────────────────────────────────────────────────────────┘
```

### Phase 1 — Snapshot Bootstrap

On startup, the standby optionally downloads the latest snapshot produced by the primary (see [Snapshot / Restore flags](mooncake-store-deployment-guide.md)). This baseline allows the standby to catch up quickly without replaying the entire oplog history.

Enable snapshot bootstrap via `HotStandbyConfig.enable_snapshot_bootstrap = true`.

### Phase 2 — Oplog Replication

After the snapshot is applied, the standby enters **steady-state replication**: it continuously polls the primary's `OpLogReplicator` for new oplog entries and applies them locally through `OpLogApplier`. The lag is bounded by `max_replication_lag_entries` (default 1000 entries).

### Leader Election

Mooncake uses either **etcd** or **Redis** as the distributed coordination backend for leader election:

- **etcd** (`STORE_USE_ETCD`): Build with `-DSTORE_USE_ETCD=ON`. Set `--etcd_endpoints` on the master.
- **Redis** (`STORE_USE_REDIS`): Build with `-DSTORE_USE_REDIS=ON`. Set connection details via environment variables (see [Redis HA Backend](mooncake-store-deployment-guide.md#redis-ha-backend)).

When the primary is unresponsive for longer than `--client_ttl` seconds, the standby acquires the leader lease and promotes itself.

### Promotion

Promotion is handled by `StandbyStateMachine`. On promotion the standby:
1. Stops polling the (now-dead) primary.
2. Registers itself in the metadata service under the primary's endpoint address.
3. Begins accepting client RPCs.

Clients that retry their connections will automatically reconnect to the promoted standby without any application-level change.

## Configuration Reference

### `HotStandbyConfig` Fields

| Field | Default | Description |
|-------|---------|-------------|
| `standby_id` | — | Unique identifier for this standby instance |
| `primary_address` | — | `ip:port` of the primary master's RPC endpoint |
| `replication_port` | 0 (auto) | Port for the oplog replication channel |
| `verification_interval_sec` | 30 | How often to verify sync status |
| `max_replication_lag_entries` | 1000 | Alert threshold for oplog lag |
| `enable_verification` | true | Enable periodic lag verification |
| `enable_snapshot_bootstrap` | false | Download latest snapshot before replicating |
| `enable_oplog_following` | true | Enable steady-state oplog following |
| `oplog_store_type` | default | Where to persist the local oplog copy |
| `oplog_store_root_dir` | default | Root directory for local oplog storage |
| `oplog_poll_interval_ms` | default | Polling interval for new oplog entries |

### Master Startup Flags (HA-related)

The following flags from [Mooncake Store Deployment Guide](mooncake-store-deployment-guide.md) apply to HA setups:

| Flag | Default | Description |
|------|---------|-------------|
| `--enable_ha` | `false` | Enable HA mode (requires etcd or Redis) |
| `--etcd_endpoints` | — | Semicolon-separated etcd endpoints |
| `--client_ttl` | `10` s | How long a client (or standby) has to re-ping before considered dead |
| `--cluster_id` | `mooncake_cluster` | Cluster ID used for persistence keys in HA mode |

## Deployment Example

### Step 1: Start the Primary Master (with HA enabled)

```bash
mooncake_master \
    --rpc_port=50051 \
    --enable_ha=true \
    --etcd_endpoints="http://etcd-0:2379;http://etcd-1:2379;http://etcd-2:2379" \
    --cluster_id=prod-cluster \
    --client_ttl=15 \
    --enable_snapshot=true \
    --snapshot_backend_type=local \
    --snapshot_interval_seconds=300
```

### Step 2: Start the Standby Master

The standby is a separate `mooncake_master` process with identical flags **plus** the standby-specific ones:

```bash
MOONCAKE_SNAPSHOT_LOCAL_PATH=/data/mooncake_snapshots \
mooncake_master \
    --rpc_port=50052 \
    --enable_ha=true \
    --etcd_endpoints="http://etcd-0:2379;http://etcd-1:2379;http://etcd-2:2379" \
    --cluster_id=prod-cluster \
    --client_ttl=15 \
    --enable_snapshot_restore=true \
    --snapshot_backend_type=local
```

The standby will:
1. Detect that another master holds the leader lease via etcd.
2. Start the `HotStandbyService` with snapshot bootstrap enabled.
3. Download the latest snapshot from the primary and apply it.
4. Begin following the oplog in steady state.

### Step 3: Verify Sync Status

The standby logs its sync status periodically. Look for lines like:

```
[HotStandbyService] applied_seq_id=12345 lag=0 entries
[HotStandbyService] verification OK: primary_seq_id=12345 standby_seq_id=12345
```

A non-zero lag that is growing indicates the standby cannot keep up with the primary write rate — consider reducing the primary write load or increasing `oplog_poll_interval_ms`.

### Step 4: Simulate a Failover

Kill the primary master process. Within `--client_ttl` seconds, the standby will:

1. Detect the primary is down (etcd lease expiry).
2. Acquire the leader lease.
3. Promote itself and start serving client RPCs on its own `--rpc_port`.

Point clients to the standby's address or use a DNS/load-balancer alias.

## Tuning Tips

- **`max_replication_lag_entries`**: Lower values trigger earlier alerts but may cause false positives during write bursts. The default of 1000 is conservative for most workloads.
- **`verification_interval_sec`**: Increase to reduce overhead if the standby's oplog applier is a bottleneck.
- **`enable_snapshot_bootstrap`**: Always enable in production to reduce time-to-sync after a standby restart.
- **`client_ttl`**: Set this equal to or slightly higher than your deployment's network heartbeat interval. A value too low causes spurious failovers; too high delays recovery.

## See Also

- [Mooncake Store Deployment Guide](mooncake-store-deployment-guide) — snapshot flags, S3 backend, Redis HA
- [Mooncake Store Design](../design/mooncake-store)
