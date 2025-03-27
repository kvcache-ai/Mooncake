add_executable(ngxl_bench transfer_engine_bench.cpp)
target_link_libraries(ngxl_bench PUBLIC ngxl)

add_executable(ngxl_memory_pool memory_pool.cpp)
target_link_libraries(ngxl_memory_pool PUBLIC ngxl)
