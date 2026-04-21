set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests/util)

add_executable(ff_ptr_bench ff_ptr_bench_main.cpp ff_ptr_bench_impl.cpp)