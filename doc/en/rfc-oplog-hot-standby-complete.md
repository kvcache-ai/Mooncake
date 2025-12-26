# OpLog Hot-Standby Synchronization based on etcd - Complete RFC

## 1. Background

### 1.1 Current System Architecture

Mooncake Store is a high-performance distributed KV cache storage engine designed specifically for LLM inference scenarios. The system adopts a Master-Client architecture:

- **Master Service**: Manages object metadata, space allocation, node management, etc.
- **Client**: Acts as a storage server providing memory segments while also serving as a client to handle application requests

### 1.2 High Availability Requirements

The current system supports two deployment modes:

1. **Default Mode**: Single Master node, simple deployment but with single point of failure risk
2. **High Availability Mode (unstable)**: Multiple Master nodes coordinated through etcd for leader election

**Issues**:

- While HA mode implements leader election, Standby Masters do not perform any operations during the waiting period
- No data synchronization mechanism is implemented; metadata may be incomplete when Standby is promoted to Primary
- Lack of reliable primary-standby data synchronization solution

### 1.3 Business Scenarios

In LLM inference scenarios, Master Service requires:
- **High Availability**: Fast failover when Master fails, minimizing service interruption time
- **Data Consistency**: Standby must maintain data consistency with Primary
- **Fast Recovery**: Quick service recovery after failure without lengthy data reconstruction

### 1.4 Problems with Current Solution

1. **No Data Synchronization**: Standby Master does not perform any data synchronization operations during the election waiting period
2. **Metadata Loss Risk**: After Primary failure, metadata may be incomplete when Standby is promoted
3. **Long Recovery Time**: Need to re-collect metadata from Client nodes, resulting in long recovery time
4. **Data Inconsistency**: Cannot guarantee data consistency between Standby and Primary

## 2. Goals

### 2.1 Primary Goals

1. **Implement Reliable Primary-Standby Data Synchronization**
   - Synchronize all metadata change operations from Primary Master to Standby Master
   - Guarantee data consistency between Standby and Primary

2. **Fast Failure Recovery**
   - Standby can quickly promote to Primary after Primary failure
   - Complete metadata when promoted, no lengthy reconstruction required

3. **Minimize OpLog Size**
   - Only record critical state change operations (PUT, DELETE)
   - Do not record high-frequency but non-critical operations like lease renewals

4. **Integration with Existing System**
   - Integrate with existing snapshot mechanism
   - Integrate with existing leader election mechanism
   - Do not affect normal operation of existing features

### 2.2 Non-Functional Goals

1. **Performance**: OpLog synchronization should not significantly impact Primary performance
2. **Reliability**: Leverage etcd's strong consistency to guarantee data reliability
3. **Scalability**: Support multiple Standby Masters
4. **Maintainability**: Simple implementation, easy to understand and maintain

## 3. Proposal

### 3.1 Core Design Approach

**Use etcd as an intermediate reliability component to implement OpLog primary-standby synchronization**:

1. **OpLog Mechanism**: Primary Master records all state change operations to OpLog
2. **etcd Storage**: OpLog is written to etcd, leveraging etcd's strong consistency and persistence capabilities
3. **Watch Mechanism**: Standby Master receives OpLog in real-time through etcd Watch mechanism
4. **Ordering Guarantee**: Guarantee operation order through global sequence_id and key-level key_sequence_id

### 3.2 Architecture Design

#### 3.2.1 Overall Architecture

The overall architecture diagram shows the interaction relationships between Primary Master, etcd Cluster, and Standby Master:

![OpLog Hot-Standby Architecture](./diagrams/oplog-hot-standby-architecture.puml)

**Architecture Description**:
- **Primary Master**: Handles client requests, records OpLog and writes to etcd
- **etcd Cluster**: Acts as intermediate storage, providing strong consistency and Watch mechanism
- **Standby Master**: Receives OpLog in real-time by watching etcd and applies to local metadata store

#### 3.2.2 Data Flow Diagram

The data flow diagram shows the complete flow from Client request to Standby synchronization:

![OpLog Data Flow](./diagrams/oplog-data-flow.puml)

**Flow Description**:
1. Client sends `PutEnd` request to Primary Master
2. Primary Master records operation through `OpLogManager`, generating sequence_id
3. `EtcdOpLogStore` writes OpLog to etcd
4. etcd notifies Standby Master through Watch mechanism
5. `OpLogWatcher` receives events and passes to `OpLogApplier`
6. `OpLogApplier` checks order and applies to Standby's metadata store

#### 3.2.3 Failover Sequence

The failover sequence diagram shows the complete process from Primary failure to Standby promotion to Primary:

![OpLog Failover Sequence](./diagrams/oplog-failover-sequence.puml)

**Flow Description**:
1. **Normal Operation**: Primary maintains Lease, Standby continuously synchronizes OpLog through Watch
2. **Primary Failure**: Primary's Lease expires, etcd notifies Standby
3. **Standby Promotion**: Stop Standby service, initialize Lease, clean expired metadata, start leader election

### 3.3 Core Component Design

#### 3.3.1 OpLogManager (Primary Side)

**Responsibilities**:
- Record all state change operations (PUT_END, PUT_REVOKE, REMOVE)
- Generate global sequence_id and key-level key_sequence_id
- Maintain memory buffer (for fast queries)

**Key Methods**:
```cpp
class OpLogManager {
    uint64_t Append(OpType type, const std::string& key, 
                    const std::string& payload = "");
    std::vector<OpLogEntry> GetEntriesSince(uint64_t since_seq_id, 
                                            size_t limit = 1000) const;
    uint64_t GetLastSequenceId() const;
};
```

#### 3.3.2 EtcdOpLogStore (Primary Side)

**Responsibilities**:
- Write OpLog to etcd
- Update latest sequence_id
- Record snapshot corresponding sequence_id
- Clean up old OpLog

**etcd Key Design**:
- OpLog Entry: `mooncake-store/oplog/{cluster_id}/{sequence_id}`
- Latest Sequence ID: `mooncake-store/oplog/{cluster_id}/latest`
- Snapshot Sequence ID: `mooncake-store/oplog/{cluster_id}/snapshot/{snapshot_id}/sequence_id`

#### 3.3.3 OpLogWatcher (Standby Side)

**Responsibilities**:
- Watch etcd OpLog changes
- Read historical OpLog (for initial synchronization)
- Process Watch events and pass to OpLogApplier

**Key Methods**:
```cpp
class OpLogWatcher {
    void Start();
    void Stop();
    bool ReadOpLogSince(uint64_t start_seq_id, 
                        std::vector<OpLogEntry>& entries);
};
```

#### 3.3.4 OpLogApplier (Standby Side)

**Responsibilities**:
- Apply OpLog Entry to local metadata store
- Check global and key-level order
- Handle sequence number discontinuities and out-of-order cases
- Periodically clean up key_sequence_map_ (memory optimization)

**Key Methods**:
```cpp
class OpLogApplier {
    bool ApplyOpLogEntry(const OpLogEntry& entry);
    bool CheckSequenceOrder(const OpLogEntry& entry);
    void CleanupStaleKeySequences();
};
```

#### 3.3.5 HotStandbyService (Standby Side)

**Responsibilities**:
- Manage Standby mode lifecycle
- Coordinate OpLogWatcher and OpLogApplier
- Handle Standby promotion to Primary logic

**Key Methods**:
```cpp
class HotStandbyService {
    void StartStandby();
    void Stop();
    void Promote();
};
```

### 3.4 OpLog Entry Data Structure

```cpp
struct OpLogEntry {
    uint64_t sequence_id{0};        // Globally monotonically increasing sequence
    uint64_t timestamp_ms{0};        // Timestamp (milliseconds)
    OpType op_type{OpType::PUT_END}; // PUT_END, PUT_REVOKE, REMOVE
    std::string object_key;          // Object key
    std::string payload;             // Optional payload (carries replica info for PUT_END)
    uint32_t checksum{0};           // Checksum
    uint32_t prefix_hash{0};        // Key prefix hash
    uint64_t key_sequence_id{0};     // Per-key operation sequence (for ordering guarantee)
};
```

**JSON Serialization Format**:
```json
{
  "sequence_id": 12345,
  "timestamp": 1704110400123,
  "op_type": "PUT_END",
  "key": "object_key_123",
  "payload": "optional_payload",
  "checksum": 1234567890,
  "prefix_hash": 987654321,
  "key_sequence_id": 5
}
```

### 3.5 Ordering Guarantee Mechanism

#### 3.5.1 Global Sequence Number (sequence_id)

- **Purpose**: Guarantee global order of all OpLog events
- **Generation**: Generated globally incrementally by Primary's `OpLogManager`
- **Check**: Standby checks if sequence_id is continuous

#### 3.5.2 Key-Level Sequence Number (key_sequence_id)

- **Purpose**: Guarantee operation order for the same key
- **Generation**: Incremented separately for each key on Primary side
- **Check**: Standby checks if key_sequence_id is increasing

#### 3.5.3 Out-of-Order Handling

When key_sequence_id out-of-order is detected:
1. **Rollback**: Delete all state of the key from metadata_store
2. **Replay**: Re-read all OpLog from etcd starting from the key's first sequence_id
3. **Rewrite**: Re-apply all OpLog in correct order to rebuild metadata

For detailed design, please refer to: `doc/en/rfc-oplog-rollback-replay-on-sequence-violation.md`

### 3.6 Snapshot Integration

#### 3.6.1 Record Sequence ID During Snapshot

- When snapshot is generated, record current OpLog sequence_id
- Write snapshot info to etcd: `mooncake-store/oplog/{cluster_id}/snapshot/{snapshot_id}/sequence_id`

#### 3.6.2 OpLog Cleanup

- After snapshot generation, OpLog before snapshot can be cleaned up
- Cleanup strategy: Query minimum existing sequence_id from etcd, use DeleteRange to delete

For detailed design, please refer to: `doc/en/rfc-oplog-cleanup-start-sequence-id.md`

### 3.7 Standby Service Integration

#### 3.7.1 Problem

In existing code, Standby only blocks and waits during leader election, without running Standby service to synchronize OpLog.

#### 3.7.2 Solution

In `MasterServiceSupervisor::Start()`:
1. Check if there is currently a leader
2. If there is a leader and it's not self → Start Standby service (watch OpLog and apply)
3. After successful election → Stop Standby service and promote to Primary

For detailed design, please refer to: `doc/en/rfc-standby-service-integration.md`

### 3.8 Lease Initialization When Standby Promotes to Primary

#### 3.8.1 Problem

Objects on Standby all have lease = 0 (because OpLog only contains PUT_END, not renewal information), and all objects will expire immediately after promotion to Primary.

#### 3.8.2 Solution

In `HotStandbyService::Promote()`:
1. Stop Standby service
2. Iterate through all metadata
3. For objects with lease_timeout = 0, grant default lease time (`default_kv_lease_ttl`)

For detailed design, please refer to: `doc/en/rfc-standby-promotion-lease-initialization.md`

### 3.9 Memory Optimization: key_sequence_map_ Cleanup

#### 3.9.1 Problem

`key_sequence_map_` on Standby side is used to track `key_sequence_id` for each key. After metadata is deleted, these entries are still retained, which may cause memory leaks during long-term operation.

#### 3.9.2 Solution

Implement periodic cleanup mechanism:
- **Cleanup Condition**: Last operation is `REMOVE` and more than 1 hour has passed
- **Cleanup Frequency**: Scan once per hour
- **Retention Strategy**: Keys with `PUT_END` and `PUT_REVOKE` operations are not cleaned

For detailed design, please refer to: `doc/en/rfc-oplog-key-sequence-map-cleanup.md`

## 4. Key Design Points Summary

1. **etcd as Intermediate Storage**: Leverage etcd's strong consistency and Watch mechanism
2. **Record Only Critical Operations**: PUT_END, PUT_REVOKE, REMOVE, do not record LEASE_RENEW
3. **Dual Sequence Number Guarantee**: Global sequence_id + key-level key_sequence_id
4. **Snapshot Integration**: Integrate with existing snapshot mechanism, support OpLog cleanup
5. **Standby Service Runs in Parallel**: Continuously synchronize data during election waiting period
6. **Memory Optimization**: Periodically clean up expired entries in key_sequence_map_

