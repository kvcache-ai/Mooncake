project(MooncakeStore)

if (USE_ETCD)
    if (USE_ETCD_LEGACY)
        message(WARNING "Mooncake-store does not support etcd-cpp-api-v3")
    else()
        add_compile_definitions(MOONCAKE_STORE_USE_ETCD)
    endif()
endif()

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