set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/benchmark)

add_executable(coro_rpc_benchmark_server server.cpp)
add_executable(coro_rpc_benchmark_client client.cpp)

add_executable(bench bench.cpp)

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_SYSTEM_NAME MATCHES "Windows") # mingw-w64
    target_link_libraries(coro_rpc_benchmark_server wsock32 ws2_32)
    target_link_libraries(coro_rpc_benchmark_client wsock32 ws2_32)
endif()

if (GENERATE_BENCHMARK_DATA)
    add_executable(coro_rpc_benchmark_data_gen data_gen.cpp)
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_SYSTEM_NAME MATCHES "Windows") # mingw-w64
        target_link_libraries(coro_rpc_benchmark_data_gen wsock32 ws2_32)
    endif()
    target_compile_definitions(coro_rpc_benchmark_data_gen PRIVATE GENERATE_BENCHMARK_DATA)
    add_custom_command(
            TARGET coro_rpc_benchmark_data_gen POST_BUILD
            COMMAND ./coro_rpc_benchmark_data_gen
            COMMENT "generate benchmark data"
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/output/benchmark
            VERBATIM
    )
endif ()
