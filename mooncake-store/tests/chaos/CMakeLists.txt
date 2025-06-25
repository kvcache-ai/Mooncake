add_executable(chaosctl chaosctl.cpp)
target_link_libraries(chaosctl PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    pthread
)

add_executable(chaos_client chaos_client.cpp)
target_link_libraries(chaos_client PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    pthread
)

add_executable(chaos_test chaos_test.cpp)
target_link_libraries(chaos_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)

add_executable(chaos_rand_test chaos_rand_test.cpp)
target_link_libraries(chaos_rand_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)