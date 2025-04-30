add_executable(buffer_allocator_test buffer_allocator_test.cpp)
target_link_libraries(buffer_allocator_test PUBLIC mooncake_store cachelib_memory_allocator gtest gtest_main pthread)
add_test(NAME buffer_allocator_test COMMAND buffer_allocator_test)

add_executable(master_service_test master_service_test.cpp)
target_link_libraries(master_service_test PUBLIC mooncake_store cachelib_memory_allocator glog gtest gtest_main pthread)
add_test(NAME master_service_test COMMAND master_service_test)

add_executable(client_integration_test client_integration_test.cpp)
target_link_libraries(client_integration_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)
add_test(NAME client_integration_test COMMAND client_integration_test)

add_executable(stress_workload_test stress_workload_test.cpp)
target_link_libraries(stress_workload_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)

add_executable(local_file_test local_file_test.cpp)
target_link_libraries(local_file_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)
add_test(NAME local_file_test COMMAND local_file_test)

add_executable(thread_pool_test thread_pool_test.cpp)
target_link_libraries(thread_pool_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)
add_test(NAME thread_pool_test COMMAND thread_pool_test)