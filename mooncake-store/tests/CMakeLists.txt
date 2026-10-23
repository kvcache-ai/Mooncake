function(add_store_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PUBLIC
        mooncake_store
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
add_store_test(posix_file_test posix_file_test.cpp)
add_store_test(thread_pool_test thread_pool_test.cpp)
add_store_test(inflight_tracker_test inflight_tracker_test.cpp)
add_store_test(offset_allocator_test offset_allocator_test.cpp)
add_store_test(utils_test utils_test.cpp)
add_store_test(client_buffer_test client_buffer_test.cpp)
add_store_test(ipv6_parsing_test ipv6_parsing_test.cpp)
add_store_test(serializer_test serializer_test.cpp)
add_store_test(shm_helper_test shm_helper_test.cpp)
add_store_test(storage_backend_test storage_backend_test.cpp)
add_store_test(mutex_test mutex_test.cpp)
add_store_test(real_client_deployment_test real_client_deployment_test.cpp)
target_link_libraries(real_client_deployment_test PRIVATE mooncake_master_core)
set_tests_properties(real_client_deployment_test
    PROPERTIES LABELS "${MOONCAKE_STORE_CLIENT_ARCH};client;deployment" TIMEOUT 180)

if(MOONCAKE_STORE_CLIENT_ARCH STREQUAL "centralized")
    add_store_test(allocation_strategy_test allocation_strategy_test.cpp)
    add_store_test(eviction_strategy_test eviction_strategy_test.cpp)
    add_store_test(master_service_test master_service_test.cpp)
    add_store_test(master_service_ssd_test master_service_ssd_test.cpp)
    add_store_test(master_metrics_test master_metrics_test.cpp)
    add_store_test(segment_test segment_test.cpp)
    add_store_test(task_manager_test task_manager_test.cpp)
    add_store_test(client_integration_test client_integration_test.cpp)
    if(USE_CXL)
        add_store_test(cxl_client_integration_test cxl_client_integration_test.cpp)
    endif()
    add_store_test(transfer_task_test transfer_task_test.cpp)
    add_store_test(client_config_builder_test client_config_builder_test.cpp)
    add_store_test(centralized_real_client_test centralized_real_client_test.cpp)
    add_store_test(ipv6_client_test ipv6_client_test.cpp)
    add_store_test(client_metrics_test client_metrics_test.cpp)
    add_store_test(non_ha_reconnect_test non_ha_reconnect_test.cpp)
    add_store_test(real_client_remount_test real_client_remount_test.cpp)
    add_store_test(file_storage_test file_storage_test.cpp)

    add_executable(high_availability_test high_availability_test.cpp)
    target_link_libraries(high_availability_test PUBLIC mooncake_store transfer_engine cachelib_memory_allocator ${ETCD_WRAPPER_LIB} glog gtest gtest_main pthread)
    if(STORE_USE_ETCD)
        add_test(NAME high_availability_test COMMAND high_availability_test)
    endif()

    add_executable(stress_workload_test stress_workload_test.cpp)
    target_link_libraries(stress_workload_test PUBLIC
        mooncake_store transfer_engine cachelib_memory_allocator
        ${ETCD_WRAPPER_LIB} mooncake_common glog gflags pthread)
else()
    add_subdirectory(p2p)
endif()

add_subdirectory(e2e)
