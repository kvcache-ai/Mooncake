set(MOONCAKE_E2E_TEST_SOURCES
    process_handler.cpp
    client_wrapper.cpp
)

add_executable(clientctl clientctl.cpp ${MOONCAKE_E2E_TEST_SOURCES})
target_link_libraries(clientctl PUBLIC
    mooncake_store
    transfer_engine
    cachelib_memory_allocator
    glog
    pthread
    ${ETCD_WRAPPER_LIB}
)

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