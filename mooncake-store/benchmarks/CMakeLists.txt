# Add allocator benchmark executable
add_executable(allocator_bench allocator_bench.cpp)
target_link_libraries(allocator_bench PRIVATE cachelib_memory_allocator mooncake_store)

# The benchmark exercises the centralized Master RPC API.
if(MOONCAKE_STORE_CLIENT_ARCH STREQUAL "centralized")
add_executable(master_bench
    master_bench.cpp
    ../src/master_client.cpp
)
target_link_libraries(master_bench PRIVATE cachelib_memory_allocator mooncake_master_core)
endif()

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
