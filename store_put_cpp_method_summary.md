# store.put 调用的 C++ 方法分析

## 核心结论
**store.put 调用的是 C++ 的 `Client::Put` 方法**，而不是 `Client::BatchPut` 方法。

## 关键代码证据

在 Python 绑定代码中，`store.put` 对应 `PyClient::put_internal` 方法，该方法直接调用了 C++ 的 `Client::Put`：

```cpp
// PyClient::put_internal (对应 Python 的 store.put)
tl::expected<void, ErrorCode> PyClient::put_internal(
    const std::string &key, std::span<const char> value,
    const ReplicateConfig &config) {
    // ... (参数验证和准备)
    
    // 直接调用 C++ 的 Client::Put 方法
    auto put_result = client_->Put(key, slices, config);
    
    // ... (错误处理)
    return {};
}
```

而 `store.put_batch` 则对应 `PyClient::put_batch_internal` 方法，该方法调用了 C++ 的 `Client::BatchPut`：

```cpp
// PyClient::put_batch_internal (对应 Python 的 store.put_batch)
tl::expected<void, ErrorCode> PyClient::put_batch_internal(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values,
    const ReplicateConfig &config) {
    // ... (参数验证和准备)
    
    // 调用 C++ 的 Client::BatchPut 方法
    auto results = client_->BatchPut(keys, ordered_batched_slices, config);
    
    // ... (结果处理)
    return {};
}
```

## 功能差异

| 特性 | Client::Put | Client::BatchPut |
|------|-------------|------------------|
| 操作范围 | 单个键值对 | 批量键值对 |
| 执行模式 | 同步执行 | 异步批量处理 |
| 效率 | 适合单个操作 | 批量操作效率更高 |
| API 复杂度 | 简单易用 | 相对复杂 |

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