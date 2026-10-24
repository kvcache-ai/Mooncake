project(MooncakeStore VERSION 2.0.0)

# Extract version components for C++ usage
set(MOONCAKE_STORE_VERSION ${PROJECT_VERSION})

if (STORE_USE_ETCD)
    set(ETCD_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/)
    set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/libetcd_wrapper.so)
else()
    message(STATUS "STORE_USE_ETCD=OFF, high availability of Store is disabled")
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

set(MOONCAKE_STORE_CLIENT_ARCH "centralized" CACHE STRING
    "Store architecture for master, client, tests and benchmarks: centralized or p2p")
set_property(CACHE MOONCAKE_STORE_CLIENT_ARCH PROPERTY STRINGS centralized p2p)
if(NOT MOONCAKE_STORE_CLIENT_ARCH STREQUAL "centralized" AND
   NOT MOONCAKE_STORE_CLIENT_ARCH STREQUAL "p2p")
    message(FATAL_ERROR "Invalid MOONCAKE_STORE_CLIENT_ARCH: ${MOONCAKE_STORE_CLIENT_ARCH}")
endif()

if(DEFINED MOONCAKE_CONFIGURED_STORE_ARCH AND
   NOT MOONCAKE_CONFIGURED_STORE_ARCH STREQUAL MOONCAKE_STORE_CLIENT_ARCH)
    message(FATAL_ERROR
        "Changing MOONCAKE_STORE_CLIENT_ARCH requires a new build directory")
endif()
set(MOONCAKE_CONFIGURED_STORE_ARCH "${MOONCAKE_STORE_CLIENT_ARCH}" CACHE INTERNAL
    "Store architecture selected when this build directory was configured")

configure_file(include/version.h.in include/version.h @ONLY)

# Add subdirectories
add_subdirectory(src)

if (BUILD_UNIT_TESTS)
    add_subdirectory(tests)
    add_subdirectory(benchmarks)
endif()
