set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests)
add_executable(coro_io_test
        test_corofile.cpp
        test_load_balancer.cpp
        test_client_pool.cpp
        test_rate_limiter.cpp
        test_coro_channel.cpp
        test_cancel.cpp
        main.cpp
        )
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_SYSTEM_NAME MATCHES "Windows") # mingw-w64
    target_link_libraries(coro_io_test PRIVATE ws2_32 mswsock)
endif()
add_test(NAME coro_io_test COMMAND coro_io_test)

add_subdirectory(ibverbs)


