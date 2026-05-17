# store.put 调用的是 Put 还是 BatchPut

## 结论
**store.put 调用的是 C++ 的 `Client::Put` 方法**，而不是 `Client::BatchPut` 方法。

## 详细分析

### Python 绑定中的 put 方法
在 Python 绑定代码中，有多个 put 相关的方法，但它们对应不同的 C++ 实现：

| Python 方法 | 对应的 C++ 方法 | 功能 |
|-------------|----------------|------|
| `store.put` | `Client::Put` | 单个键值对的存储操作 |
| `store.put_batch` | `Client::BatchPut` | 批量键值对的存储操作 |
| `store.put_parts` | `Client::Put` | 多部分数据的单个键存储 |
| `store.put_from` | `Client::Put` | 从缓冲区直接存储单个键值对 |
| `store.put_from_with_metadata` | `Client::Put` | 带元数据的单个键值对存储 |

### 关键代码分析

#### 1. store.put 的实现
```cpp
// PyClient::put 方法（对应 Python 的 store.put）
tl::expected<void, ErrorCode> PyClient::put_internal(
    const std::string &key, std::span<const char> value,
    const ReplicateConfig &config) {
    // ...（参数验证和准备）
    
    // 调用 C++ 的 Client::Put 方法
    auto put_result = client_->Put(key, slices, config);
    if (!put_result) {
        LOG(ERROR) << "Put operation failed for key '" << key
                   << "' with error: " << toString(put_result.error());
        return tl::unexpected(put_result.error());
    }
    return {};
}
```

#### 2. store.put_batch 的实现
```cpp
// PyClient::put_batch 方法（对应 Python 的 store.put_batch）
tl::expected<void, ErrorCode> PyClient::put_batch_internal(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values,
    const ReplicateConfig &config) {
    // ...（参数验证和准备）
    
    // 调用 C++ 的 Client::BatchPut 方法
    auto results = client_->BatchPut(keys, ordered_batched_slices, config);
    
    // ...（结果处理）
    return {};
}
```

## 调用链总结

### store.put 调用链
```
Python: store.put(key, value)
└── C++: PyClient::put_internal(key, value, config)
    └── C++: Client::Put(key, slices, config)
        ├── 分配副本
        ├── 数据传输
        └── 完成操作
```

### store.put_batch 调用链
```
Python: store.put_batch(keys, values)
└── C++: PyClient::put_batch_internal(keys, values, config)
    └── C++: Client::BatchPut(keys, batched_slices, config)
        ├── 批量分配副本
        ├── 批量提交传输任务
        ├── 等待传输完成
        └── 批量完成或撤销操作
```

## 功能差异

| 特性 | Client::Put | Client::BatchPut |
|------|-------------|------------------|
| 操作范围 | 单个键值对 | 批量键值对 |
| 执行模式 | 同步执行 | 异步批量处理 |
| 效率 | 适合单个操作 | 批量操作效率更高 |
| API 复杂度 | 简单易用 | 相对复杂 |

## 代码位置参考

| 代码项 | 文件位置 |
|--------|----------|
| PyClient::put_internal | `/home/models/src/Mooncake/mooncake-store/src/pybind_client.cpp:226` |
| PyClient::put_batch_internal | `/home/models/src/Mooncake/mooncake-store/src/pybind_client.cpp:259` |
| Client::Put | `/home/models/src/Mooncake/mooncake-store/src/client.cpp:595` |
| Client::BatchPut | `/home/models/src/Mooncake/mooncake-store/src/client.cpp:1130` |