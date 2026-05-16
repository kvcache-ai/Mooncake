# Add allocator benchmark executable
add_executable(allocator_bench allocator_bench.cpp)
target_link_libraries(allocator_bench PRIVATE cachelib_memory_allocator
                                              mooncake_store)

# Add master benchmark executable
add_executable(master_bench master_bench.cpp)
target_link_libraries(master_bench PRIVATE cachelib_memory_allocator
                                           mooncake_store)

# Add file interface benchmark executable
add_executable(file_interface_bench file_interface_bench.cpp)
target_link_libraries(file_interface_bench PRIVATE mooncake_store glog::glog)

# Add storage backend benchmark executable
# This benchmark tests all storage backends (OffsetAllocator, Bucket, FilePerKey)
# with realistic KV cache workloads for LLM inference scenarios
add_executable(storage_backend_bench storage_backend_bench.cpp)
target_link_libraries(
  storage_backend_bench PRIVATE mooncake_store cachelib_memory_allocator
                                gflags::gflags glog::glog pthread)

# Add allocation strategy benchmark executable
# This benchmark tests AllocationStrategy performance with configurable 
# segment counts, allocation sizes, replica counts, and workload patterns
add_executable(allocation_strategy_bench allocation_strategy_bench.cpp)
target_link_libraries(
  allocation_strategy_bench PRIVATE mooncake_store cachelib_memory_allocator
                                    gflags::gflags glog::glog pthread)

# Cost-aware routing benchmark (Forge RL design 02)
add_executable(query_cost_bench query_cost_bench.cpp)
target_link_libraries(query_cost_bench
    PRIVATE mooncake_store
            cachelib_memory_allocator
            glog::glog
            pthread)

# Smoke-run the bench so accidental ABI breaks in master_service surface in
# CI without anybody having to remember to manually run benchmarks/.
add_test(NAME query_cost_bench_smoke
         COMMAND query_cost_bench --candidates=1 --workers=1 --seconds=1
         WORKING_DIRECTORY $<TARGET_FILE_DIR:query_cost_bench>)
set_tests_properties(query_cost_bench_smoke PROPERTIES
                     LABELS "smoke;benchmark"
                     TIMEOUT 30)
