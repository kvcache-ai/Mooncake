# Add allocator benchmark executable
add_executable(allocator_bench allocator_bench.cpp)
target_link_libraries(allocator_bench PRIVATE cachelib_memory_allocator
                                              mooncake_store)

# Add focused OffsetAllocator concurrency benchmark
add_executable(offset_allocator_concurrency_bench
               offset_allocator_concurrency_bench.cpp)
target_link_libraries(offset_allocator_concurrency_bench
                      PRIVATE mooncake_store gflags::gflags pthread)

# Add master benchmark executable
add_executable(master_bench master_bench.cpp)
target_link_libraries(master_bench PRIVATE cachelib_memory_allocator
                                           mooncake_store)

# Add BatchGetReplicaList latency benchmark executable
add_executable(batch_get_replica_bench batch_get_replica_bench.cpp)
target_link_libraries(batch_get_replica_bench PRIVATE cachelib_memory_allocator
                                                      mooncake_store)

# Offline, dependency-aware replay of master RPC operation traces.
add_library(master_rpc_trace STATIC master_rpc_trace/trace.cpp)
target_link_libraries(master_rpc_trace PUBLIC JsonCpp::JsonCpp pthread)
target_include_directories(master_rpc_trace PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
add_executable(master_rpc_trace_bench master_rpc_trace_bench.cpp)
target_link_libraries(master_rpc_trace_bench
                      PRIVATE master_rpc_trace mooncake_store gflags::gflags)
if(BUILD_UNIT_TESTS)
  add_executable(master_rpc_trace_test master_rpc_trace/tests/trace_test.cpp)
  target_include_directories(master_rpc_trace_test PRIVATE master_rpc_trace)
  target_link_libraries(master_rpc_trace_test PRIVATE master_rpc_trace
                                                      gtest_main)
  add_test(NAME master_rpc_trace_test COMMAND master_rpc_trace_test)
endif()

# Add file interface benchmark executable
add_executable(file_interface_bench file_interface_bench.cpp)
target_link_libraries(file_interface_bench PRIVATE mooncake_store glog::glog)

# Add storage backend benchmark executable This benchmark tests all storage
# backends (OffsetAllocator, Bucket, FilePerKey) with realistic KV cache
# workloads for LLM inference scenarios
add_executable(storage_backend_bench storage_backend_bench.cpp)
target_link_libraries(
  storage_backend_bench PRIVATE mooncake_store cachelib_memory_allocator
                                gflags::gflags glog::glog pthread)

# Add allocation strategy benchmark executable This benchmark tests
# AllocationStrategy performance with configurable segment counts, allocation
# sizes, replica counts, and workload patterns
add_executable(allocation_strategy_bench allocation_strategy_bench.cpp)
target_link_libraries(
  allocation_strategy_bench PRIVATE mooncake_store cachelib_memory_allocator
                                    gflags::gflags glog::glog pthread)

# Add NoF worker pool benchmark executable
if(USE_NOF)
  add_executable(nof_worker_pool_bench nof_worker_pool_bench.cpp)
  target_link_libraries(nof_worker_pool_bench PRIVATE mooncake_store glog::glog
                                                      gflags::gflags)
endif()

# Add BatchEvict benchmark executable
add_executable(batch_evict_bench batch_evict_bench.cpp)
target_link_libraries(
  batch_evict_bench PRIVATE master_service_test_peer cachelib_memory_allocator
                            gflags::gflags glog::glog pthread)

add_executable(batch_oplog_promotion_bench batch_oplog_promotion_bench.cpp)
target_link_libraries(batch_oplog_promotion_bench PRIVATE mooncake_store
                                                          gflags::gflags)

if(STORE_USE_ETCD)
  add_executable(oplog_batch_bench oplog_batch_bench.cpp)
  target_link_libraries(
    oplog_batch_bench PRIVATE mooncake_store glog::glog gflags::gflags
                              JsonCpp::JsonCpp)
endif()

add_executable(uring_batch_read_bench uring_batch_read_bench.cpp)
target_link_libraries(uring_batch_read_bench PRIVATE mooncake_store)
