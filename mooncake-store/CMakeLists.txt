project(MooncakeStore VERSION 2.0.0)

# Extract version components for C++ usage
set(MOONCAKE_STORE_VERSION ${PROJECT_VERSION})

configure_file(include/version.h.in include/version.h @ONLY)

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

# Until base-class inheritance is severed (Phase 2) and centralized code is
# restored (Phase 3), the two static archives have circular symbol references;
# RESCAN lets the linker iterate the group until everything resolves.
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.24)
    set(MOONCAKE_STORE_LIBS
        $<LINK_GROUP:RESCAN,mooncake_store,mooncake_store_p2p>)
elseif(UNIX AND NOT APPLE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    # LINK_GROUP was added in CMake 3.24. Keep compatibility with the project's
    # 3.16 minimum and older Linux CI images by spelling out the GNU ld group.
    set(MOONCAKE_STORE_LIBS
        -Wl,--start-group
        mooncake_store
        mooncake_store_p2p
        -Wl,--end-group)
else()
    message(FATAL_ERROR
        "Circular mooncake_store libraries require CMake 3.24 or a GNU-compatible linker")
endif()

# mooncake-integration is a sibling directory and needs the same link group.
set(MOONCAKE_STORE_LIBS ${MOONCAKE_STORE_LIBS} PARENT_SCOPE)

# Add subdirectories
add_subdirectory(src)

if (BUILD_UNIT_TESTS)
    add_subdirectory(tests)
    add_subdirectory(benchmarks)
endif()
