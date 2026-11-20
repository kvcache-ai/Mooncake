set(source_dir "${STORE_TEST_SOURCE_DIR}/e2e")
add_store_clientctl(clientctl_p2p mooncake_store_p2p p2p_client_wrapper.cpp)

if(STORE_USE_REDIS)
    add_executable(redis_chaos_test
        "${source_dir}/redis_chaos_test.cpp"
        "${source_dir}/process_handler.cpp"
        "${source_dir}/p2p_client_wrapper.cpp"
    )
    target_link_libraries(redis_chaos_test PRIVATE ${HIREDIS_LIB})
    target_link_libraries(redis_chaos_test PUBLIC
        mooncake_store_p2p
        transfer_engine
        cachelib_memory_allocator
        glog
        gtest
        pthread
        ${ETCD_WRAPPER_LIB}
    )
    add_test(NAME redis_chaos_test COMMAND redis_chaos_test
        --master_path=$<TARGET_FILE:mooncake_p2p_master>
        --clientctl_path=$<TARGET_FILE:clientctl_p2p>
        --out_dir=${CMAKE_CURRENT_BINARY_DIR}/redis_chaos_output)
    set_tests_properties(redis_chaos_test PROPERTIES
        LABELS "p2p;ha;redis_ha" RUN_SERIAL TRUE TIMEOUT 360)
endif()
