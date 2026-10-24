set(MOONCAKE_E2E_TEST_SOURCES
    process_handler.cpp
)
if(MOONCAKE_STORE_CLIENT_ARCH STREQUAL "p2p")
    list(APPEND MOONCAKE_E2E_TEST_SOURCES p2p_client_wrapper.cpp)
else()
    list(APPEND MOONCAKE_E2E_TEST_SOURCES client_wrapper.cpp)
endif()

add_executable(clientctl clientctl.cpp ${MOONCAKE_E2E_TEST_SOURCES})
target_link_libraries(clientctl PUBLIC
    mooncake_store
    transfer_engine
    cachelib_memory_allocator
    glog
    pthread
    ${ETCD_WRAPPER_LIB}
)

if(MOONCAKE_STORE_CLIENT_ARCH STREQUAL "centralized")
    add_executable(e2e_rand_test e2e_rand_test.cpp ${MOONCAKE_E2E_TEST_SOURCES})
    target_link_libraries(e2e_rand_test PUBLIC
        mooncake_store
        transfer_engine
        cachelib_memory_allocator
        glog
        gtest
        pthread
        ${ETCD_WRAPPER_LIB}
    )

    add_executable(chaosctl chaosctl.cpp ${MOONCAKE_E2E_TEST_SOURCES})
    target_link_libraries(chaosctl PUBLIC
        mooncake_store
        transfer_engine
        cachelib_memory_allocator
        glog
        pthread
        ${ETCD_WRAPPER_LIB}
    )

    add_executable(client_runner client_runner.cpp ${MOONCAKE_E2E_TEST_SOURCES})
    target_link_libraries(client_runner PUBLIC
        mooncake_store
        transfer_engine
        cachelib_memory_allocator
        glog
        pthread
        ${ETCD_WRAPPER_LIB}
    )

    add_executable(chaos_test chaos_test.cpp ${MOONCAKE_E2E_TEST_SOURCES})
    target_link_libraries(chaos_test PUBLIC
        mooncake_store
        transfer_engine
        cachelib_memory_allocator
        glog
        gtest
        pthread
        ${ETCD_WRAPPER_LIB}
    )

    add_executable(chaos_rand_test chaos_rand_test.cpp ${MOONCAKE_E2E_TEST_SOURCES})
    target_link_libraries(chaos_rand_test PUBLIC
        mooncake_store
        transfer_engine
        cachelib_memory_allocator
        glog
        gtest
        pthread
        ${ETCD_WRAPPER_LIB}
    )
else()
    if(STORE_USE_REDIS)
        add_executable(redis_chaos_test redis_chaos_test.cpp ${MOONCAKE_E2E_TEST_SOURCES})
        target_link_libraries(redis_chaos_test PUBLIC
            mooncake_store
            transfer_engine
            cachelib_memory_allocator
            glog
            gtest
            pthread
            ${ETCD_WRAPPER_LIB}
        )
        add_test(NAME redis_chaos_test COMMAND redis_chaos_test
            --master_path=$<TARGET_FILE:mooncake_master>
            --clientctl_path=$<TARGET_FILE:clientctl>
            --out_dir=${CMAKE_CURRENT_BINARY_DIR}/redis_chaos_output)
        set_tests_properties(redis_chaos_test PROPERTIES
            LABELS "p2p;ha;redis_ha" RUN_SERIAL TRUE TIMEOUT 180)
    endif()
endif()
