cmake_minimum_required(VERSION 3.22)
project(MooncakeStore)

execute_process(
    COMMAND make -f ${CMAKE_CURRENT_SOURCE_DIR}/Makefile
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

# Add include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/fake_include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/
    ${CMAKE_CURRENT_SOURCE_DIR}/include/mooncake-store/proto/
    ${CMAKE_CURRENT_SOURCE_DIR}/include/
    ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-transfer-engine/include
)

# Add subdirectories
add_subdirectory(src)
add_subdirectory(tests)
