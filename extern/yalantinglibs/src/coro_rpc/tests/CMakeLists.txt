set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests)
set(TEST_SRCS
        test_coro_rpc_server.cpp
        test_coro_rpc_client.cpp
        test_register_handler.cpp
        test_router.cpp
        test_connection.cpp
        test_function_name.cpp
        test_variadic.cpp
        test_parallel.cpp
        test_client_filter.cpp
        test_abi_compatible.cpp
        )
set(TEST_COMMON
        rpc_api.cpp
        main.cpp
        )
add_executable(coro_rpc_test
        ${TEST_SRCS}
        ${TEST_COMMON}
        )
add_custom_command(
        TARGET coro_rpc_test POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/openssl_files
        ${CMAKE_BINARY_DIR}/src/coro_rpc/openssl_files)
add_custom_command(
        TARGET coro_rpc_test POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/openssl_files
        ${CMAKE_BINARY_DIR}/output/openssl_files)
target_compile_definitions(coro_rpc_test PRIVATE UNIT_TEST_INJECT)
target_compile_definitions(coro_rpc_test PRIVATE STRUCT_PACK_ENABLE_UNPORTABLE_TYPE)

add_test(NAME coro_rpc_test COMMAND coro_rpc_test)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests/coro_rpc)

add_executable(coro_rpc_regist_test_1 rpc_api.cpp test_register_duplication_1.cpp)
add_executable(coro_rpc_regist_test_2 rpc_api.cpp test_register_duplication_2.cpp)
add_executable(coro_rpc_regist_test_3 rpc_api.cpp test_register_duplication_3.cpp)

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_SYSTEM_NAME MATCHES "Windows") # mingw-w64
  target_link_libraries(coro_rpc_test PRIVATE ws2_32 mswsock)
  target_link_libraries(coro_rpc_regist_test_1 PRIVATE ws2_32 mswsock)
  target_link_libraries(coro_rpc_regist_test_2 PRIVATE ws2_32 mswsock)
  target_link_libraries(coro_rpc_regist_test_3 PRIVATE ws2_32 mswsock)
endif()
