# Add allocator benchmark executable
add_executable(allocator_bench allocator_bench.cpp)
target_link_libraries(allocator_bench PRIVATE cachelib_memory_allocator mooncake_store)

# Add master benchmark executable
add_executable(master_bench master_bench.cpp)
target_link_libraries(master_bench PRIVATE cachelib_memory_allocator mooncake_store)

# Add storage backend benchmark executable
# This benchmark tests all storage backends (OffsetAllocator, Bucket, FilePerKey)
# with realistic KV cache workloads for LLM inference scenarios
add_executable(storage_backend_bench storage_backend_bench.cpp)
target_link_libraries(storage_backend_bench PRIVATE
    mooncake_store
    cachelib_memory_allocator
    gflags::gflags
    glog::glog
    pthread
)

# Add allocation strategy benchmark executable
# This benchmark tests AllocationStrategy performance with configurable
# segment counts, allocation sizes, replica counts, and workload patterns
add_executable(allocation_strategy_bench allocation_strategy_bench.cpp)
target_link_libraries(allocation_strategy_bench PRIVATE
    mooncake_store
    cachelib_memory_allocator
    gflags::gflags
    spdlog::spdlog
    pthread
)
