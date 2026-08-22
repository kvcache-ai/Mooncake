function(add_store_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PUBLIC
        ${MOONCAKE_STORE_LIBS}
        transfer_engine
        cachelib_memory_allocator
        ${ETCD_WRAPPER_LIB}
        glog
        ibverbs
        gtest
        gtest_main
        pthread
    )
    add_test(NAME ${name} COMMAND ${name})
endfunction()

add_store_test(buffer_allocator_test buffer_allocator_test.cpp)
add_store_test(allocation_strategy_test allocation_strategy_test.cpp)
add_store_test(eviction_strategy_test eviction_strategy_test.cpp)
add_store_test(master_service_test master_service_test.cpp)
add_store_test(master_service_ssd_test master_service_ssd_test.cpp)
add_store_test(client_integration_test client_integration_test.cpp)
add_store_test(cxl_client_integration_test cxl_client_integration_test.cpp)
add_store_test(master_metrics_test master_metrics_test.cpp)
add_store_test(posix_file_test posix_file_test.cpp)
add_store_test(thread_pool_test thread_pool_test.cpp)
add_store_test(transfer_task_test transfer_task_test.cpp)
add_store_test(centralized_segment_manager_test centralized_segment_manager_test.cpp)
add_store_test(offset_allocator_test offset_allocator_test.cpp)
add_store_test(utils_test utils_test.cpp)
add_store_test(client_buffer_test client_buffer_test.cpp)
add_store_test(client_config_builder_test client_config_builder_test.cpp)
add_store_test(pybind_client_test pybind_client_test.cpp)
add_store_test(ipv6_client_test ipv6_client_test.cpp)
add_store_test(client_metrics_test client_metrics_test.cpp)
add_store_test(serializer_test serializer_test.cpp)
add_store_test(non_ha_reconnect_test non_ha_reconnect_test.cpp)
add_store_test(heartbeat_dedicated_port_test heartbeat_dedicated_port_test.cpp)
add_store_test(real_client_remount_test real_client_remount_test.cpp)
add_store_test(shm_helper_test shm_helper_test.cpp)
add_store_test(storage_backend_test storage_backend_test.cpp)
add_store_test(mutex_test mutex_test.cpp)
add_store_test(file_storage_test file_storage_test.cpp)
add_store_test(centralized_client_meta_test centralized_client_meta_test.cpp)
add_store_test(centralized_client_manager_test centralized_client_manager_test.cpp)
add_store_test(task_manager_test task_manager_test.cpp)
add_subdirectory(p2p)
add_subdirectory(e2e)

add_executable(high_availability_test high_availability_test.cpp)
target_link_libraries(high_availability_test PUBLIC ${MOONCAKE_STORE_LIBS} transfer_engine cachelib_memory_allocator ${ETCD_WRAPPER_LIB} glog gtest gtest_main pthread)
if (STORE_USE_ETCD)
    add_test(NAME high_availability_test COMMAND high_availability_test)
endif()


add_executable(stress_workload_test stress_workload_test.cpp)
target_link_libraries(stress_workload_test PUBLIC
    ${MOONCAKE_STORE_LIBS}
    transfer_engine
    cachelib_memory_allocator
    ${ETCD_WRAPPER_LIB}
    mooncake_common
    glog
    gflags
    pthread
)
