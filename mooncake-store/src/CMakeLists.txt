# Find Python package
add_subdirectory(cachelib_memory_allocator)

set(MOONCAKE_MASTER_SUPPORT_SOURCES
    common_types.cpp
    utils.cpp
    etcd_helper.cpp
    allocator.cpp
    offset_allocator.cpp
)

set(MOONCAKE_CENTRALIZED_MASTER_SOURCES
    master_service.cpp
    master_metric_manager.cpp
    ha_helper.cpp
    segment.cpp
    rpc_service.cpp
    http_metadata_server.cpp
    task_manager.cpp
)

set(MOONCAKE_STORE_SOURCES
    ${MOONCAKE_MASTER_SUPPORT_SOURCES}
    client_resources.cpp
    thread_pool.cpp
    posix_file.cpp
    client_buffer.cpp
    real_client.cpp
    dummy_client.cpp
)

set(EXTRA_LIBS "")

if(USE_3FS)
  add_subdirectory(hf3fs)
  list(APPEND MOONCAKE_STORE_SOURCES  ${HF3FS_SOURCES})
  find_library(HF3FS_API_LIB hf3fs_api_shared PATHS /usr/lib NO_DEFAULT_PATH)
  if(NOT HF3FS_API_LIB)
    message(FATAL_ERROR "hf3fs_api_shared library not found in /usr/lib")
  endif()
  set(EXTRA_LIBS ${HF3FS_API_LIB})
endif()

# TODO(P2P-010.5): Verify production, UT and benchmark artifacts per architecture.
# The cache_allocator library
include_directories(${Python3_INCLUDE_DIRS})
add_library(mooncake_store ${MOONCAKE_STORE_SOURCES})
# Note: transfer_engine is PRIVATE to avoid propagating its dependencies (e.g., vendor-specific hardware)
# to targets that don't need it (e.g., mooncake_master).
# Targets that need transfer_engine should link it explicitly.
target_link_libraries(mooncake_store
    PUBLIC
        cachelib_memory_allocator
        ${ETCD_WRAPPER_LIB}
        glog::glog
        gflags::gflags
        JsonCpp::JsonCpp
        ${EXTRA_LIBS}
        asio_shared
    PRIVATE
        transfer_engine
)

if (STORE_USE_ETCD)
    add_dependencies(mooncake_store build_etcd_wrapper)
endif()

if (USE_ASCEND_DIRECT)
    set(ACL_RUNTIME_HEADER_PATH "${ASCEND_INCLUDE_DIR}/acl/acl_rt.h")
    if(EXISTS "${ACL_RUNTIME_HEADER_PATH}")
        file(READ "${ACL_RUNTIME_HEADER_PATH}" ACL_RUNTIME_CONTENT)
        if("${ACL_RUNTIME_CONTENT}" MATCHES "ACL_MEM_P2P_HUGE1G")
            message(STATUS "ACL_MEM_P2P_HUGE1G exist.")
            add_compile_definitions(ASCEND_SUPPORT_FABRIC_MEM)
        else()
            message(STATUS "ACL_MEM_P2P_HUGE1G is not exist.")
        endif()
    else()
        message(WARNING "Acl runtime header file is not exist")
    endif()
endif()

if (BUILD_SHARED_LIBS)
  install(TARGETS mooncake_store DESTINATION lib)
endif()

# Master executables never link the client aggregate. Explicit STATIC libraries
# keep this boundary intact even when the installed Store libraries are shared.
add_library(mooncake_master_support STATIC ${MOONCAKE_MASTER_SUPPORT_SOURCES})
target_link_libraries(mooncake_master_support PUBLIC
    cachelib_memory_allocator
    glog::glog
    gflags::gflags
    JsonCpp::JsonCpp
    ${ETCD_WRAPPER_LIB}
    mooncake_common
    pthread
    ibverbs
    asio_shared
)
# utils.cpp uses the Ascend allocation primitives only in direct mode.
if(USE_ASCEND_DIRECT)
    target_link_libraries(mooncake_master_support PRIVATE transfer_engine)
endif()
if(STORE_USE_ETCD)
    add_dependencies(mooncake_master_support build_etcd_wrapper)
endif()

add_library(mooncake_master_core STATIC)
target_link_libraries(mooncake_master_core PUBLIC mooncake_master_support)
if(STORE_USE_ETCD)
    add_dependencies(mooncake_master_core build_etcd_wrapper)
endif()

# Master binary
add_executable(mooncake_master)

set(MASTER_EXTRA_INCS)
set(MASTER_EXTRA_LIBS)

if (STORE_USE_JEMALLOC)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(JEMALLOC REQUIRED jemalloc)
    list(APPEND MASTER_EXTRA_INCS ${JEMALLOC_INCLUDE_DIRS})
    list(APPEND MASTER_EXTRA_LIBS ${JEMALLOC_STATIC_LIBRARIES})
endif()

target_include_directories(mooncake_master PRIVATE ${MASTER_EXTRA_INCS})
target_link_libraries(mooncake_master PRIVATE
    mooncake_master_core
    ${MASTER_EXTRA_LIBS}
)

if (STORE_USE_ETCD)
    add_dependencies(mooncake_master build_etcd_wrapper)
endif()

target_compile_options(mooncake_master PRIVATE -Os)
target_link_options(mooncake_master PRIVATE -Os -s)

# Each build ships only the selected Dummy-facing client daemon.
add_executable(mooncake_client)
target_link_libraries(mooncake_client PRIVATE
    mooncake_store transfer_engine asio_shared)
target_compile_options(mooncake_client PRIVATE -Os)
target_link_options(mooncake_client PRIVATE -Os -s)

# Architecture-specific sources extend the standard targets.
# TODO: Move centralized sources and build logic into their own directory.
if(MOONCAKE_STORE_CLIENT_ARCH STREQUAL "p2p")
    add_subdirectory(p2p)
else()
    target_sources(mooncake_store PRIVATE
        ${MOONCAKE_CENTRALIZED_MASTER_SOURCES}
        client_service.cpp
        centralized_real_client.cpp
        client_metric.cpp
        master_client.cpp
        transfer_task.cpp
        file_storage.cpp
        storage_backend.cpp
    )
    target_sources(mooncake_master_core PRIVATE ${MOONCAKE_CENTRALIZED_MASTER_SOURCES})
    target_sources(mooncake_master PRIVATE master.cpp)
    target_sources(mooncake_client PRIVATE real_client_main.cpp)
endif()

install(TARGETS mooncake_master mooncake_client
        DESTINATION bin)
