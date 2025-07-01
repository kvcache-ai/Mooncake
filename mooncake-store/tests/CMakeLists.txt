add_executable(buffer_allocator_test buffer_allocator_test.cpp)
target_link_libraries(buffer_allocator_test PUBLIC mooncake_store cachelib_memory_allocator gtest gtest_main pthread)
add_test(NAME buffer_allocator_test COMMAND buffer_allocator_test)

add_executable(allocation_strategy_test allocation_strategy_test.cpp)
target_link_libraries(allocation_strategy_test PUBLIC mooncake_store cachelib_memory_allocator glog gtest gtest_main pthread)
add_test(NAME allocation_strategy_test COMMAND allocation_strategy_test)

add_executable(eviction_strategy_test eviction_strategy_test.cpp)
target_link_libraries(eviction_strategy_test PUBLIC mooncake_store cachelib_memory_allocator glog gtest gtest_main pthread)
add_test(NAME eviction_strategy_test COMMAND eviction_strategy_test)

add_executable(master_service_test master_service_test.cpp)
target_link_libraries(master_service_test PUBLIC mooncake_store cachelib_memory_allocator ${ETCD_WRAPPER_LIB} glog gtest gtest_main pthread)
add_test(NAME master_service_test COMMAND master_service_test)

add_executable(client_integration_test client_integration_test.cpp)
target_link_libraries(client_integration_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    ${ETCD_WRAPPER_LIB}
    glog
    gtest
    gtest_main
    pthread
)
add_test(NAME client_integration_test COMMAND client_integration_test)

add_executable(master_metrics_test master_metrics_test.cpp)
target_link_libraries(master_metrics_test PUBLIC mooncake_store cachelib_memory_allocator ${ETCD_WRAPPER_LIB} glog gtest gtest_main pthread)
add_test(NAME master_metrics_test COMMAND master_metrics_test)

add_executable(high_availability_test high_availability_test.cpp)
target_link_libraries(high_availability_test PUBLIC mooncake_store cachelib_memory_allocator ${ETCD_WRAPPER_LIB} glog gtest gtest_main pthread)
if (STORE_USE_ETCD)
    add_test(NAME high_availability_test COMMAND high_availability_test)
endif()

add_executable(stress_workload_test stress_workload_test.cpp)
target_link_libraries(stress_workload_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    ${ETCD_WRAPPER_LIB}
    glog
    gtest
    gtest_main
    pthread
)

add_executable(transfer_task_test transfer_task_test.cpp)
target_link_libraries(transfer_task_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)
add_test(NAME transfer_task_test COMMAND transfer_task_test)

add_executable(segment_test segment_test.cpp)
target_link_libraries(segment_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)
add_test(NAME segment_test COMMAND segment_test)

add_subdirectory(e2e)