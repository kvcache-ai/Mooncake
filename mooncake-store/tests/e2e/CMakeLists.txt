add_executable(clientctl clientctl.cpp)
target_link_libraries(clientctl PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    pthread
)

add_executable(e2e_rand_test e2e_rand_test.cpp)
target_link_libraries(e2e_rand_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    pthread
)