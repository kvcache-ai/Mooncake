set(STORE_TEST_SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}")

function(add_store_test name)
    set(test_target "${name}${STORE_TEST_SUFFIX}")
    set(sources ${ARGN})
    list(TRANSFORM sources PREPEND "${STORE_TEST_SOURCE_DIR}/")
    add_executable(${test_target} ${sources})
    target_link_libraries(${test_target} PUBLIC
        ${STORE_LIBRARY}
        transfer_engine
        cachelib_memory_allocator
        ${ETCD_WRAPPER_LIB}
        glog
        ibverbs
        gtest
        gtest_main
        pthread
    )
    add_test(NAME ${test_target} COMMAND ${test_target})
endfunction()

function(add_common_store_tests backend_label)
    add_store_test(buffer_allocator_test buffer_allocator_test.cpp)
    add_store_test(posix_file_test posix_file_test.cpp)
    add_store_test(thread_pool_test thread_pool_test.cpp)
    add_store_test(offset_allocator_test offset_allocator_test.cpp)
    add_store_test(utils_test utils_test.cpp)
    add_store_test(byte_size_test byte_size_test.cpp)
    add_store_test(client_auto_port_config_test client_auto_port_config_test.cpp)
    add_store_test(client_buffer_test client_buffer_test.cpp)
    add_store_test(ipv6_parsing_test ipv6_parsing_test.cpp)
    add_store_test(serializer_test serializer_test.cpp)
    add_store_test(shm_helper_test shm_helper_test.cpp)
    add_store_test(mutex_test mutex_test.cpp)
    # TODO(UPG-T0084, replace-in=UPG2.3): receiver=UPG2.3 (Management 4).
    # Source: Management 3 UPG1.1,
    # 0d67311c4e44044f8628a00a64f4dc0148535204:mooncake-store/src/real_client.cpp:310.
    # Owner: UPG2.3 shared deployment. Keep both backend fixtures, CTest names,
    # labels, and timeout. Verify RPC drain and SHM cleanup with both binaries.
    add_store_test(real_client_deployment_test real_client_deployment_test.cpp)
    set_tests_properties(real_client_deployment_test${STORE_TEST_SUFFIX}
        PROPERTIES LABELS "${backend_label};client;deployment;shm;ping" TIMEOUT 180)
endfunction()

function(add_store_clientctl target store_library wrapper)
    set(source_dir "${STORE_TEST_SOURCE_DIR}/e2e")
    add_executable(${target}
        "${source_dir}/clientctl.cpp"
        "${source_dir}/process_handler.cpp"
        "${source_dir}/${wrapper}"
    )
    target_link_libraries(${target} PUBLIC
        ${store_library}
        transfer_engine
        cachelib_memory_allocator
        glog
        pthread
        ${ETCD_WRAPPER_LIB}
    )
endfunction()
