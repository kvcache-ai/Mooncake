set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests)
add_executable(metric_test
        test_metric.cpp
        parallel_test.cpp
        parallel_test2.cpp
        )
check_asan(HAS_ASAN)
if (has_asan) 
        target_compile_options(metric_test PRIVATE -fsanitize=address)
        target_link_options(metric_test PRIVATE -fsanitize=address)
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_SYSTEM_NAME MATCHES "Windows") # mingw-w64
    target_link_libraries(metric_test PRIVATE ws2_32 mswsock)
endif()

add_test(NAME metric_test COMMAND metric_test)
