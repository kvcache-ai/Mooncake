project(MooncakeStore VERSION 2.0.0)

# Extract version components for C++ usage
set(MOONCAKE_STORE_VERSION ${PROJECT_VERSION})

configure_file(include/version.h.in include/version.h @ONLY)

if (STORE_USE_ETCD)
    set(ETCD_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/)
    set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/libetcd_wrapper.so)
endif()

if (STORE_USE_REDIS)
    find_path(MOONCAKE_STORE_HIREDIS_INCLUDE_DIR hiredis/hiredis.h REQUIRED)
    find_library(MOONCAKE_STORE_HIREDIS_LIBRARY hiredis REQUIRED)
    message(STATUS "Redis HA backend: Enabled")
endif()

if (NOT STORE_USE_ETCD AND NOT STORE_USE_REDIS)
    message(STATUS "Store HA backends are disabled")
endif()

# Add include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/fake_include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/
    ${CMAKE_CURRENT_SOURCE_DIR}/include/mooncake-store/proto/
    ${CMAKE_CURRENT_SOURCE_DIR}/include/
    ${CMAKE_CURRENT_BINARY_DIR}/include/
    ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-transfer-engine/include
    ${ETCD_WRAPPER_INCLUDE}
)

# Add subdirectories
add_subdirectory(src)

if (BUILD_UNIT_TESTS)
    add_subdirectory(tests)
    add_subdirectory(benchmarks)
endif()
