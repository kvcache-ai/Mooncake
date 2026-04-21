set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests/util)

add_executable(util_meta_string_test test_meta_string.cpp)
add_executable(util_time_test test_time_util.cpp)

add_test(NAME util_meta_string_test COMMAND util_meta_string_test)
add_test(NAME util_time_test COMMAND util_time_test)

add_executable(ff_ptr_test main.cpp test_ff_ptr.cpp)