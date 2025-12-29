# Find Python package
add_subdirectory(cachelib_memory_allocator)

set(MOONCAKE_STORE_SOURCES
    allocator.cpp
    master_service.cpp
    client_service.cpp
    client_metric.cpp
    types.cpp
    master_client.cpp
    utils.cpp
    master_metric_manager.cpp
    storage_backend.cpp
    thread_pool.cpp
    etcd_helper.cpp
    ha_helper.cpp
    segment.cpp
    transfer_task.cpp
    rpc_service.cpp
    offset_allocator.cpp
    posix_file.cpp
    client_buffer.cpp
    real_client.cpp
    dummy_client.cpp
    http_metadata_server.cpp
    file_storage.cpp
    oplog_manager.cpp
    etcd_oplog_store.cpp
    oplog_watcher.cpp
    oplog_applier.cpp
    hot_standby_service.cpp
    # replication_service.cpp removed - using etcd-based OpLog sync instead
)

set(EXTRA_LIBS "")

# Find xxHash (required for ComputeChecksum)
find_path(XXHASH_INCLUDE_DIR NAMES xxhash.h PATHS /usr/include /usr/local/include)
find_library(XXHASH_LIBRARY NAMES xxhash libxxhash PATHS /usr/lib /usr/local/lib /usr/lib64)
if (XXHASH_INCLUDE_DIR AND XXHASH_LIBRARY)
  message(STATUS "Found xxHash: include=${XXHASH_INCLUDE_DIR} lib=${XXHASH_LIBRARY}")
  list(APPEND MASTER_EXTRA_INCS ${XXHASH_INCLUDE_DIR})
else()
  message(FATAL_ERROR "xxHash library/header not found. Please install xxhash (development headers) and try again.")
endif()

if(USE_3FS)
  add_subdirectory(hf3fs)
  list(APPEND MOONCAKE_STORE_SOURCES  ${HF3FS_SOURCES})
  find_library(HF3FS_API_LIB hf3fs_api_shared PATHS /usr/lib NO_DEFAULT_PATH)
  if(NOT HF3FS_API_LIB)
    message(FATAL_ERROR "hf3fs_api_shared library not found in /usr/lib")
  endif()
  set(EXTRA_LIBS ${HF3FS_API_LIB})
endif()

# The cache_allocator library
include_directories(${Python3_INCLUDE_DIRS})
add_library(mooncake_store ${MOONCAKE_STORE_SOURCES})
target_include_directories(mooncake_store PUBLIC ${XXHASH_INCLUDE_DIR})
target_link_libraries(mooncake_store PUBLIC ${XXHASH_LIBRARY})
target_link_libraries(mooncake_store PUBLIC transfer_engine cachelib_memory_allocator ${ETCD_WRAPPER_LIB} glog::glog gflags::gflags
    ${EXTRA_LIBS}
)
if (STORE_USE_ETCD)
    add_dependencies(mooncake_store build_etcd_wrapper)
endif()

if (BUILD_SHARED_LIBS)
  install(TARGETS mooncake_store DESTINATION lib)
endif()

# Master binary
add_executable(mooncake_master master.cpp)

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
    mooncake_store
    cachelib_memory_allocator
    pthread
    ibverbs
    mooncake_common
    ${ETCD_WRAPPER_LIB}
    ${MASTER_EXTRA_LIBS}
)

if (STORE_USE_ETCD)
    add_dependencies(mooncake_master build_etcd_wrapper)
endif()

target_compile_options(mooncake_master PRIVATE -Os)
target_link_options(mooncake_master PRIVATE -Os -s)

# Client server binary
add_executable(mooncake_client real_client_main.cpp)
target_link_libraries(mooncake_client PRIVATE mooncake_store)
target_compile_options(mooncake_master PRIVATE -Os)
target_link_options(mooncake_client PRIVATE -Os -s)

install(TARGETS mooncake_master mooncake_client DESTINATION bin)
