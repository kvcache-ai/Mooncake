add_executable(allocator_bench_p2p
    "${MOONCAKE_STORE_SOURCE_ROOT}/benchmarks/allocator_bench.cpp")
target_link_libraries(allocator_bench_p2p PRIVATE cachelib_memory_allocator mooncake_store_p2p)
