add_subdirectory(cachelib_memory_allocator)

# Sources used by either architecture, grouped by their Store role.
set(MOONCAKE_STORE_COMMON_SHARED_SOURCES
    common_types.cpp
    utils.cpp
    common/file_util.cpp
    common/network.cpp
    etcd_helper.cpp
    thread_pool.cpp
    allocator.cpp
    offset_allocator.cpp
)

set(MOONCAKE_STORE_COMMON_CLIENT_SOURCES
    common/client_buffer_allocation.cpp
    common/mmap_buffer.cpp
    client_resources.cpp
    posix_file.cpp
    client_buffer.cpp
    real_client.cpp
    dummy_client.cpp
    shm_helper.cpp
)

set(EXTRA_LIBS "")

if(USE_3FS)
  # TODO(F10-S38-T001, replace-in=UPG1-F10-S38): Reconcile this client-side
  # ThreeFSFile wiring with U's shared HF3FS resource manager and DFS adapter
  # when the approved descriptor-based DFS consumers are implemented.
  # Owner: F10-S38. Keep USE_3FS source/header/library conditions aligned;
  # this existing wiring does not provide descriptor-based DFS support.
  # Remove this marker after the actual migration or approved exclusion.
  add_subdirectory(hf3fs)
  list(APPEND MOONCAKE_STORE_COMMON_CLIENT_SOURCES ${HF3FS_SOURCES})
  find_library(HF3FS_API_LIB hf3fs_api_shared PATHS /usr/lib NO_DEFAULT_PATH)
  if(NOT HF3FS_API_LIB)
    message(FATAL_ERROR "hf3fs_api_shared library not found in /usr/lib")
  endif()
  set(EXTRA_LIBS ${HF3FS_API_LIB})
endif()

# TODO(P2P-010.5): Verify production, UT and benchmark artifacts per architecture.
include_directories(${Python3_INCLUDE_DIRS})
add_library(mooncake_store
    ${MOONCAKE_STORE_COMMON_SHARED_SOURCES}
    ${MOONCAKE_STORE_COMMON_CLIENT_SOURCES}
)
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
# The existing support target consumes shared sources until the OBJECT and
# aggregate target migration in F01-2.2/F01-2.3.
add_library(mooncake_master_support STATIC
    ${MOONCAKE_STORE_COMMON_SHARED_SOURCES}
)
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

# Only the selected architecture extends the shared, master and client groups.
# Architecture validation and the build-directory cache guard run in the parent.
if(MOONCAKE_STORE_CLIENT_ARCH STREQUAL "p2p")
    add_subdirectory(p2p)
else()
    set(MOONCAKE_STORE_CENTRALIZED_MASTER_SOURCES
        master_service.cpp
        master_metric_manager.cpp
        ha_helper.cpp
        segment.cpp
        rpc_service.cpp
        http_metadata_server.cpp
        task_manager.cpp
    )
    set(MOONCAKE_STORE_CENTRALIZED_CLIENT_SOURCES
        client_service.cpp
        centralized_real_client.cpp
        client_metric.cpp
        master_client.cpp
        transfer_task.cpp
        file_storage.cpp
        storage_backend.cpp
    )
    target_sources(mooncake_store PRIVATE
        ${MOONCAKE_STORE_CENTRALIZED_MASTER_SOURCES}
        ${MOONCAKE_STORE_CENTRALIZED_CLIENT_SOURCES}
    )
    target_sources(mooncake_master_core PRIVATE
        ${MOONCAKE_STORE_CENTRALIZED_MASTER_SOURCES}
    )
    target_sources(mooncake_master PRIVATE master.cpp)
    target_sources(mooncake_client PRIVATE real_client_main.cpp)
endif()

install(TARGETS mooncake_master mooncake_client
        DESTINATION bin)
