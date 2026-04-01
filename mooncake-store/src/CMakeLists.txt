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
    segment.cpp
    transfer_task.cpp
    rpc_service.cpp
    offset_allocator.cpp
    posix_file.cpp
    client_buffer.cpp
    aligned_client_buffer.cpp
    real_client.cpp
    dummy_client.cpp
    shm_helper.cpp
    http_metadata_server.cpp
    file_storage.cpp
    serialize/serializer.cpp
    ha/leadership/leader_coordinator_factory.cpp
    ha/leadership/backends/etcd/etcd_leader_coordinator.cpp
    ha/common/redis/redis_connection.cpp
    ha/leadership/backends/redis/redis_leader_coordinator.cpp
    ha/leadership/master_service_supervisor.cpp
    ha/snapshot/object/snapshot_object_store.cpp
    ha/snapshot/object/backends/local/local_file_snapshot_object_store.cpp
    ha/snapshot/object/backends/s3/s3_snapshot_object_store.cpp
    ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store.cpp
    ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.cpp
    utils/type_util.cpp
    utils/file_util.cpp
    task_manager.cpp
    local_hot_cache.cpp
    oplog_manager.cpp
    etcd_oplog_store.cpp
    oplog_watcher.cpp
    oplog_applier.cpp
    hot_standby_service.cpp
    standby_state_machine.cpp
    ha_metric_manager.cpp)

set(EXTRA_LIBS "")

# Find AWS SDK
find_package(AWSSDK QUIET COMPONENTS s3)
if(AWSSDK_FOUND)
  message(STATUS "AWS SDK found: ${AWSSDK_VERSION}")
  set(HAVE_AWS_SDK TRUE)
  # Add S3 related source files
  list(APPEND MOONCAKE_STORE_SOURCES utils/s3_helper.cpp)
else()
  message(STATUS "AWS SDK not found, S3 functionality will be disabled")
  set(HAVE_AWS_SDK FALSE)
endif()

# Find zstd library
find_library(ZSTD_LIBRARY NAMES zstd)
if(NOT ZSTD_LIBRARY)
  message(FATAL_ERROR "zstd library not found")
endif()

set(EXTRA_LIBS ${ZSTD_LIBRARY})

# If AWS SDK is found, add it to EXTRA_LIBS
if(HAVE_AWS_SDK)
  list(APPEND EXTRA_LIBS ${AWSSDK_LINK_LIBRARIES})
  add_definitions(-DHAVE_AWS_SDK)
  if(AWS_S3_LIB AND AWS_CORE_LIB)
    list(APPEND EXTRA_LIBS ${AWS_S3_LIB} ${AWS_CORE_LIB})
    add_definitions(-DHAVE_AWS_SDK)
  endif()
endif()

# Find xxHash (required for ComputeChecksum)
find_path(
  XXHASH_INCLUDE_DIR
  NAMES xxhash.h
  PATHS /usr/include /usr/local/include)
find_library(
  XXHASH_LIBRARY
  NAMES xxhash libxxhash
  PATHS /usr/lib /usr/local/lib /usr/lib64)
if(XXHASH_INCLUDE_DIR AND XXHASH_LIBRARY)
  message(
    STATUS "Found xxHash: include=${XXHASH_INCLUDE_DIR} lib=${XXHASH_LIBRARY}")
  list(APPEND MASTER_EXTRA_INCS ${XXHASH_INCLUDE_DIR})
else()
  message(
    FATAL_ERROR
      "xxHash library/header not found. Please install xxhash (development headers) and try again."
  )
endif()

if(USE_3FS)
  add_subdirectory(hf3fs)
  list(APPEND MOONCAKE_STORE_SOURCES ${HF3FS_SOURCES})
  find_library(
    HF3FS_API_LIB hf3fs_api_shared
    PATHS /usr/lib
    NO_DEFAULT_PATH)
  if(NOT HF3FS_API_LIB)
    message(FATAL_ERROR "hf3fs_api_shared library not found in /usr/lib")
  endif()
  set(EXTRA_LIBS ${HF3FS_API_LIB})
endif()

# io_uring support (auto-detected)
find_library(URING_LIB uring PATHS /usr/lib /usr/lib64 /usr/local/lib
                                   /usr/local/lib64)
find_path(URING_INCLUDE liburing.h PATHS /usr/include /usr/local/include)
if(URING_LIB AND URING_INCLUDE)
  message(STATUS "io_uring: Enabled for mooncake_store")
  list(APPEND MOONCAKE_STORE_SOURCES uring_file.cpp)
  list(APPEND EXTRA_LIBS ${URING_LIB})
else()
  message(STATUS "io_uring: Disabled (liburing not found)")
endif()

if(STORE_USE_REDIS)
  list(APPEND EXTRA_LIBS ${MOONCAKE_STORE_HIREDIS_LIBRARY})
endif()

# The cache_allocator library
include_directories(${Python3_INCLUDE_DIRS})
add_library(mooncake_store ${MOONCAKE_STORE_SOURCES})
target_include_directories(mooncake_store PUBLIC ${XXHASH_INCLUDE_DIR})
target_link_libraries(mooncake_store PUBLIC ${XXHASH_LIBRARY})
if(STORE_USE_REDIS)
    target_include_directories(mooncake_store PRIVATE
        ${MOONCAKE_STORE_HIREDIS_INCLUDE_DIR})
endif()
# Note: transfer_engine is PRIVATE to avoid propagating its dependencies (e.g., vendor-specific hardware)
# to targets that don't need it (e.g., mooncake_master).
# Targets that need transfer_engine should link it explicitly.
target_link_libraries(mooncake_store
    PUBLIC
        cachelib_memory_allocator
        ${ETCD_WRAPPER_LIB}
        glog::glog
        gflags::gflags
        ${EXTRA_LIBS}
        asio_shared
    PRIVATE
        transfer_engine
)
if(STORE_USE_ETCD)
    add_dependencies(mooncake_store build_etcd_wrapper)
endif()

if(URING_LIB AND URING_INCLUDE)
  target_compile_definitions(mooncake_store PUBLIC USE_URING)
  target_include_directories(mooncake_store PRIVATE ${URING_INCLUDE})
endif()

if(BUILD_SHARED_LIBS)
  install(TARGETS mooncake_store DESTINATION lib)
endif()

# Master binary
add_executable(mooncake_master master.cpp)

set(MASTER_EXTRA_INCS)
set(MASTER_EXTRA_LIBS)

if(STORE_USE_JEMALLOC)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(JEMALLOC REQUIRED jemalloc)
  list(APPEND MASTER_EXTRA_INCS ${JEMALLOC_INCLUDE_DIRS})
  list(APPEND MASTER_EXTRA_LIBS ${JEMALLOC_STATIC_LIBRARIES})
endif()

target_include_directories(mooncake_master PRIVATE ${MASTER_EXTRA_INCS})
target_link_libraries(
  mooncake_master
  PRIVATE mooncake_store
          cachelib_memory_allocator
          pthread
          ibverbs
          mooncake_common
          ${ETCD_WRAPPER_LIB}
          ${MASTER_EXTRA_LIBS}
          asio_shared)

if(STORE_USE_ETCD)
  add_dependencies(mooncake_master build_etcd_wrapper)
endif()

target_compile_options(mooncake_master PRIVATE -Os)
target_link_options(mooncake_master PRIVATE -Os -s)

# Client server binary
add_executable(mooncake_client real_client_main.cpp)
# Client needs transfer_engine for data transfer operations
target_link_libraries(mooncake_client PRIVATE mooncake_store transfer_engine
                                              asio_shared)
target_compile_options(mooncake_client PRIVATE -Os)
target_link_options(mooncake_client PRIVATE -Os -s)

install(TARGETS mooncake_master mooncake_client DESTINATION bin)
