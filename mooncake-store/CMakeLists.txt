project(MooncakeStore)

set(ETCD_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/)
set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/libetcd_wrapper.so)

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
add_subdirectory(tests)