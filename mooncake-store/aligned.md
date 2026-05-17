# store.setup 参数中 local buffer 的对齐方式分析

## 结论
store.setup 参数中的 local buffer **不是4k对齐**的，而是**64字节对齐**的。

## 代码分析

在 `/home/models/src/Mooncake/mooncake-store/src/client_buffer.cpp` 文件中，`ClientBufferAllocator` 类的构造函数明确显示了 local buffer 的对齐方式：

```cpp
ClientBufferAllocator::ClientBufferAllocator(size_t size) : buffer_size_(size) {
    // Align to 64 bytes(cache line size) for better cache performance
    constexpr size_t alignment = 64;
    buffer_ = std::aligned_alloc(alignment, size);
    if (!buffer_) {
        throw std::bad_alloc();
    }
    // ...
}
```

## 对齐原因
代码注释清楚地说明了采用64字节对齐的原因：**为了获得更好的缓存性能**（Align to 64 bytes(cache line size) for better cache performance）。

## 其他分配器的对齐要求
虽然项目中存在其他分配器（如 `CachelibBufferAllocator`）有4MB对齐的要求，但这是用于不同场景的分配器，不是 store.setup 中 local buffer 所使用的分配器。