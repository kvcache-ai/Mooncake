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

add_executable(master_service_ssd_test master_service_ssd_test.cpp)
target_link_libraries(master_service_ssd_test PUBLIC mooncake_store cachelib_memory_allocator ${ETCD_WRAPPER_LIB} glog gtest gtest_main pthread)
add_test(NAME master_service_ssd_test COMMAND master_service_ssd_test)

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
    mooncake_common
    glog
    gflags
    pthread
)

add_executable(posix_file_test posix_file_test.cpp)
target_link_libraries(posix_file_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)
add_test(NAME posix_file_test COMMAND posix_file_test)

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

add_executable(offset_allocator_test offset_allocator_test.cpp)
target_link_libraries(offset_allocator_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)
add_test(NAME offset_allocator_test COMMAND offset_allocator_test)

add_executable(utils_test utils_test.cpp)
target_link_libraries(utils_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)
add_test(NAME utils_test COMMAND utils_test)

add_executable(client_buffer_test client_buffer_test.cpp)
target_link_libraries(client_buffer_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)
add_test(NAME client_buffer_test COMMAND client_buffer_test)

add_executable(pybind_client_test pybind_client_test.cpp)
target_link_libraries(pybind_client_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    ${ETCD_WRAPPER_LIB}
    glog
    gtest
    gtest_main
    pthread
)
add_test(NAME pybind_client_test COMMAND pybind_client_test)

add_executable(client_metrics_test client_metrics_test.cpp)
target_link_libraries(client_metrics_test PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    gtest
    gtest_main
    pthread
)
add_test(NAME client_metrics_test COMMAND client_metrics_test)

add_subdirectory(e2e)
