project(MooncakeStore)

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
    ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-transfer-engine/include
    ${ETCD_WRAPPER_INCLUDE}
)

# Add subdirectories
add_subdirectory(src)

if (BUILD_UNIT_TESTS)
    add_subdirectory(tests)
    add_subdirectory(benchmarks)
endif()