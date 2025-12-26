# OpLog 清理时如何获取 start_sequence_id

## 问题

使用 `DeleteRange` 清理 etcd 中某个 `sequence_id` 之前的所有 OpLog 时，需要确定 `start_sequence_id`（范围的起始点）。

## 方案选择

### 方案对比

| 方案 | 可靠性 | 实现复杂度 | 性能 | 推荐度 |
|------|--------|-----------|------|--------|
| 方案1：维护"已清理到"记录 | 低（Primary切换会丢失） | 中 | 高 | ❌ |
| 方案2：从快照记录获取 | 中 | 中 | 高 | ⚠️ |
| **方案3：从etcd查询最小sequence_id** | **高** | **中** | **中** | **✅** |
| 方案4：固定从1开始 | 高 | 低 | 低 | ❌ |

### 推荐方案：方案3（从etcd查询最小sequence_id）

**选择理由**：
1. **可靠性高**：信息存储在 etcd 中，Primary 切换不会丢失
2. **自动适应**：自动获取实际存在的最小 sequence_id
3. **容错性好**：可以结合快照记录作为 fallback
4. **无需维护额外状态**：不需要"已清理到"的 key

## 方案3详细设计

### 核心思路

1. **从 etcd 查询当前最小的 OpLog sequence_id**
   - 使用 `Get` with `WithPrefix` + `WithLimit(1)` + `WithSort`
   - 获取第一个（最小的）OpLog key

2. **Fallback 机制**
   - 如果查询不到 OpLog，使用快照记录作为 fallback
   - 如果快照记录也没有，使用保守策略（从 1 开始）

3. **执行 DeleteRange**
   - 从查询到的最小 sequence_id 开始删除
   - 到目标 sequence_id（不包含）结束

### 架构设计

```
┌─────────────────────────────────────────────────────────┐
│         CleanupOpLogBefore(target_sequence_id)         │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  1. GetMinSequenceId()                                  │
│     ┌──────────────────────────────────────┐           │
│     │ GetFirstKeyWithPrefix(prefix)        │           │
│     │ - WithPrefix                         │           │
│     │ - WithLimit(1)                       │           │
│     │ - WithSort(SortByKey, SortAscend)   │           │
│     └──────────────────────────────────────┘           │
│         │                                               │
│         ├─ 成功 → 解析 sequence_id                      │
│         │                                               │
│         └─ 失败 → Fallback                               │
│             │                                           │
│             ├─ GetLastSnapshotSequenceId()              │
│             │                                           │
│             └─ 都没有 → 使用 1（保守策略）              │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  2. DeleteRange(start_seq_id, target_sequence_id)     │
│     - start_key = BuildOpLogKey(start_seq_id)          │
│     - end_key = BuildOpLogKey(target_sequence_id)      │
│     - 执行 DeleteRange                                 │
└─────────────────────────────────────────────────────────┘
```

## 实现细节

### 1. etcd Wrapper：GetFirstKeyWithPrefix

**在 `etcd_wrapper.go` 中添加**：

```go
//export EtcdStoreGetFirstKeyWithPrefixWrapper
func EtcdStoreGetFirstKeyWithPrefixWrapper(prefix *C.char, prefixSize C.int,
                                           firstKey **C.char, firstKeySize *C.int,
                                           firstValue **C.char, firstValueSize *C.int,
                                           errMsg **C.char) int {
    if storeClient == nil {
        *errMsg = C.CString("etcd client not initialized")
        return -1
    }
    
    prefixStr := C.GoStringN(prefix, prefixSize)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    // 使用 Get with prefix，Limit=1，Sort=ASC 获取第一个 key
    resp, err := storeClient.Get(ctx, prefixStr, 
                                 clientv3.WithPrefix(),
                                 clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
                                 clientv3.WithLimit(1))
    if err != nil {
        *errMsg = C.CString(err.Error())
        return -1
    }
    
    if len(resp.Kvs) == 0 {
        // 没有找到，返回 -2 表示不存在
        *errMsg = C.CString("no key found with prefix")
        return -2
    }
    
    // 返回第一个 key 和 value
    kv := resp.Kvs[0]
    *firstKey = C.CString(string(kv.Key))
    *firstKeySize = C.int(len(kv.Key))
    *firstValue = C.CString(string(kv.Value))
    *firstValueSize = C.int(len(kv.Value))
    
    return 0
}
```

### 2. C++ EtcdHelper：GetFirstKeyWithPrefix

**在 `etcd_helper.h` 中添加**：

```cpp
/**
 * @brief Get the first key with a given prefix (sorted by key, ascending)
 * @param prefix Key prefix
 * @param prefix_size Size of prefix
 * @param first_key Output: first key found
 * @param first_value Output: value of first key
 * @return ErrorCode::OK on success, ErrorCode::ETCD_KEY_NOT_EXIST if not found
 */
static ErrorCode GetFirstKeyWithPrefix(const char* prefix, size_t prefix_size,
                                      std::string& first_key, std::string& first_value);
```

**在 `etcd_helper.cpp` 中实现**：

```cpp
ErrorCode EtcdHelper::GetFirstKeyWithPrefix(const char* prefix, size_t prefix_size,
                                            std::string& first_key, std::string& first_value) {
    char* err_msg = nullptr;
    char* key_ptr = nullptr;
    int key_size = 0;
    char* value_ptr = nullptr;
    int value_size = 0;
    
    int ret = EtcdStoreGetFirstKeyWithPrefixWrapper(
        (char*)prefix, (int)prefix_size,
        &key_ptr, &key_size,
        &value_ptr, &value_size,
        &err_msg);
    
    if (ret == -2) {
        // 没有找到
        free(err_msg);
        return ErrorCode::ETCD_KEY_NOT_EXIST;
    }
    
    if (ret != 0) {
        LOG(ERROR) << "Failed to get first key with prefix: " << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    
    first_key = std::string(key_ptr, key_size);
    first_value = std::string(value_ptr, value_size);
    
    free(key_ptr);
    free(value_ptr);
    free(err_msg);
    
    return ErrorCode::OK;
}
```

### 3. EtcdOpLogStore：GetMinSequenceId

**实现**：

```cpp
uint64_t EtcdOpLogStore::GetMinSequenceId() const {
    // 构建 OpLog 的 prefix
    std::string prefix = etcd_prefix_ + "/" + cluster_id_ + "/";
    
    // 查询第一个 OpLog key（最小的 sequence_id）
    std::string first_key, first_value;
    auto err = EtcdHelper::GetFirstKeyWithPrefix(
        prefix.c_str(), prefix.size(),
        first_key, first_value);
    
    if (err == ErrorCode::OK) {
        // 成功获取，从 key 中提取 sequence_id
        uint64_t min_seq_id = ExtractSequenceIdFromKey(first_key);
        if (min_seq_id > 0) {
            LOG(INFO) << "Found min sequence_id in etcd: " << min_seq_id;
            return min_seq_id;
        }
    }
    
    // Fallback：尝试从快照记录获取
    uint64_t last_snapshot_seq_id = GetLastSnapshotSequenceId();
    if (last_snapshot_seq_id > 0) {
        LOG(INFO) << "Using last snapshot sequence_id as fallback: " 
                 << last_snapshot_seq_id;
        return last_snapshot_seq_id;
    }
    
    // 保守策略：从 1 开始
    // 注意：如果所有 OpLog 都被清理了，DeleteRange 会安全处理不存在的 key
    LOG(INFO) << "No OpLog or snapshot found, using conservative start: 1";
    return 1;
}

uint64_t EtcdOpLogStore::ExtractSequenceIdFromKey(const std::string& key) const {
    // key 格式：mooncake-store/oplog/{cluster_id}/{sequence_id}
    // 例如：mooncake-store/oplog/mooncake_cluster/12345
    
    size_t last_slash = key.find_last_of('/');
    if (last_slash == std::string::npos) {
        LOG(ERROR) << "Invalid OpLog key format: " << key;
        return 0;
    }
    
    std::string seq_id_str = key.substr(last_slash + 1);
    try {
        uint64_t sequence_id = std::stoull(seq_id_str);
        return sequence_id;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse sequence_id from key: " << key
                   << ", error: " << e.what();
        return 0;
    }
}
```

### 4. EtcdOpLogStore：CleanupOpLogBefore

**实现**：

```cpp
bool EtcdOpLogStore::CleanupOpLogBefore(uint64_t target_sequence_id) {
    if (target_sequence_id <= 1) {
        LOG(INFO) << "No OpLog to cleanup: target_sequence_id=" << target_sequence_id;
        return true;  // 没有需要清理的
    }
    
    // 1. 从 etcd 查询最小的 sequence_id
    uint64_t min_seq_id = GetMinSequenceId();
    
    // 2. 如果 min_seq_id >= target_sequence_id，无需清理
    if (min_seq_id >= target_sequence_id) {
        LOG(INFO) << "No OpLog to cleanup: min_seq_id=" << min_seq_id
                 << " >= target_sequence_id=" << target_sequence_id;
        return true;
    }
    
    // 3. 执行 DeleteRange
    std::string start_key = BuildOpLogKey(min_seq_id);
    std::string end_key = BuildOpLogKey(target_sequence_id);
    
    LOG(INFO) << "Cleaning up OpLog from " << min_seq_id 
             << " to " << target_sequence_id;
    
    int64_t deleted_count = 0;
    auto err = EtcdHelper::DeleteRange(
        start_key.c_str(), start_key.size(),
        end_key.c_str(), end_key.size(),
        deleted_count);
    
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to cleanup OpLog from " << min_seq_id
                  << " to " << target_sequence_id;
        return false;
    }
    
    LOG(INFO) << "Successfully cleaned up " << deleted_count 
              << " OpLog entries from " << min_seq_id 
              << " to " << target_sequence_id;
    return true;
}
```

### 5. EtcdHelper：DeleteRange

**在 `etcd_helper.h` 中添加**：

```cpp
/**
 * @brief Delete a range of keys
 * @param start_key Start key (inclusive)
 * @param start_key_size Size of start_key
 * @param end_key End key (exclusive)
 * @param end_key_size Size of end_key
 * @param deleted_count Output: number of keys deleted
 * @return ErrorCode::OK on success
 */
static ErrorCode DeleteRange(const char* start_key, size_t start_key_size,
                            const char* end_key, size_t end_key_size,
                            int64_t& deleted_count);
```

**在 `etcd_wrapper.go` 中添加**：

```go
//export EtcdStoreDeleteRangeWrapper
func EtcdStoreDeleteRangeWrapper(startKey *C.char, startKeySize C.int,
                                 endKey *C.char, endKeySize C.int,
                                 deletedCount *C.int64, errMsg **C.char) int {
    if storeClient == nil {
        *errMsg = C.CString("etcd client not initialized")
        return -1
    }
    
    start := C.GoStringN(startKey, startKeySize)
    end := C.GoStringN(endKey, endKeySize)
    
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    // 使用 WithRange 删除指定范围内的 key
    resp, err := storeClient.Delete(ctx, start, clientv3.WithRange(end))
    if err != nil {
        *errMsg = C.CString(err.Error())
        return -1
    }
    
    *deletedCount = C.int64(resp.Deleted)
    return 0
}
```

**在 `etcd_helper.cpp` 中实现**：

```cpp
ErrorCode EtcdHelper::DeleteRange(const char* start_key, size_t start_key_size,
                                  const char* end_key, size_t end_key_size,
                                  int64_t& deleted_count) {
    char* err_msg = nullptr;
    int64_t deleted = 0;
    int ret = EtcdStoreDeleteRangeWrapper(
        (char*)start_key, (int)start_key_size,
        (char*)end_key, (int)end_key_size,
        &deleted, &err_msg);
    
    if (ret != 0) {
        LOG(ERROR) << "Failed to delete range: " << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    
    deleted_count = deleted;
    free(err_msg);
    return ErrorCode::OK;
}
```

## 使用场景示例

### 场景 1：正常清理

```
当前状态：
- etcd 中 OpLog: sequence_id = 1000, 1001, 1002, ..., 5000
- 快照时 sequence_id = 5000
- 需要清理 sequence_id < 5000 的 OpLog

执行流程：
1. GetMinSequenceId() → 返回 1000
2. DeleteRange(1000, 5000) → 删除 1000-4999
3. 结果：etcd 中只剩下 sequence_id >= 5000 的 OpLog
```

### 场景 2：所有 OpLog 都被清理了

```
当前状态：
- etcd 中没有 OpLog（都被清理了）
- 快照时 sequence_id = 10000
- 需要清理 sequence_id < 10000 的 OpLog

执行流程：
1. GetMinSequenceId() → 查询不到 OpLog
2. Fallback 到快照记录 → 返回 10000
3. DeleteRange(10000, 10000) → 无需删除（范围为空）
4. 结果：安全处理，不会出错
```

### 场景 3：Primary 切换后清理

```
场景：
- 原 Primary 清理了 sequence_id < 5000 的 OpLog
- 原 Primary 崩溃，Standby 提升为新的 Primary
- 新 Primary 需要清理 sequence_id < 10000 的 OpLog

执行流程：
1. GetMinSequenceId() → 从 etcd 查询，返回 5000（实际存在的最小值）
2. DeleteRange(5000, 10000) → 删除 5000-9999
3. 结果：正确清理，不会重复删除已清理的 key
```

## 性能考虑

### 查询性能

- **GetFirstKeyWithPrefix**：使用 `WithLimit(1)`，只获取第一个 key
- **性能开销**：O(log n)，n 为 OpLog key 数量
- **频率**：只在清理时执行（10 分钟一次），开销可接受

### 删除性能

- **DeleteRange**：etcd 原生支持，性能高效
- **批量删除**：一次操作删除整个范围
- **如果范围很大**：可以考虑分批删除（但通常不需要）

## 容错机制

### 1. 查询失败处理

```cpp
if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
    // 没有 OpLog，使用 fallback
    return GetLastSnapshotSequenceId();
}
```

### 2. 解析失败处理

```cpp
try {
    uint64_t sequence_id = std::stoull(seq_id_str);
    return sequence_id;
} catch (const std::exception& e) {
    // 解析失败，使用 fallback
    return GetLastSnapshotSequenceId();
}
```

### 3. DeleteRange 失败处理

```cpp
if (err != ErrorCode::OK) {
    LOG(ERROR) << "Failed to cleanup OpLog";
    // 可以重试，或者记录错误，下次再试
    return false;
}
```

## 与快照集成

### 快照时清理

```cpp
class SnapshotManager {
public:
    MetadataSnapshot CreateSnapshot() {
        MetadataSnapshot snapshot;
        
        // 1. 导出 metadata
        snapshot.metadata = ExportMetadata();
        
        // 2. 记录当前的 OpLog sequence_id
        snapshot.last_oplog_sequence_id = oplog_manager_->GetLastSequenceId();
        
        // 3. 将快照信息写入 etcd
        std::string snapshot_id = GenerateSnapshotId();
        etcd_oplog_store_->RecordSnapshotSequenceId(
            snapshot_id, snapshot.last_oplog_sequence_id);
        
        // 4. 清理旧的 OpLog（使用方案3）
        etcd_oplog_store_->CleanupOpLogBefore(
            snapshot.last_oplog_sequence_id);
        
        return snapshot;
    }
};
```

## 总结

### 方案3的优势

1. **可靠性高**：信息存储在 etcd 中，Primary 切换不会丢失
2. **自动适应**：自动获取实际存在的最小 sequence_id
3. **容错性好**：结合快照记录作为 fallback
4. **无需维护额外状态**：不需要"已清理到"的 key
5. **性能可接受**：查询只在清理时执行，频率低

### 关键实现点

1. **GetFirstKeyWithPrefix**：使用 etcd 的 `WithPrefix` + `WithLimit(1)` + `WithSort`
2. **ExtractSequenceIdFromKey**：从 key 中解析 sequence_id
3. **Fallback 机制**：快照记录 → 保守策略（从1开始）
4. **DeleteRange**：使用 etcd 的 `WithRange` 删除范围

### 注意事项

1. **Key 格式**：必须固定格式，便于解析 sequence_id
2. **错误处理**：完善的 fallback 机制
3. **日志记录**：记录清理过程，便于排查问题

