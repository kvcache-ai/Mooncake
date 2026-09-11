# Find Python package
add_subdirectory(cachelib_memory_allocator)

set(MOONCAKE_MASTER_SUPPORT_SOURCES
    types.cpp
    utils.cpp
    etcd_helper.cpp
)

set(MOONCAKE_CENTRALIZED_MASTER_SOURCES
    replica.cpp
    allocator.cpp
    master_service.cpp
    master_metric_manager.cpp
    ha_helper.cpp
    segment.cpp
    rpc_service.cpp
    offset_allocator.cpp
    http_metadata_server.cpp
    task_manager.cpp
)

set(MOONCAKE_P2P_MASTER_SOURCES
    p2p/master/p2p_route_table.cpp
    p2p/master/p2p_master_service.cpp
    p2p/master/p2p_master_metric_manager.cpp
    p2p/master/p2p_segment_manager.cpp
    p2p/master/p2p_rpc_service.cpp
    p2p/master/p2p_master.cpp
    p2p/master/p2p_client_meta.cpp
    p2p/master/p2p_client_manager.cpp
    p2p/client/client_metrics_aggregator.cpp
    p2p/util/metric_util.cpp
    p2p/ha/oplog/oplog_manager.cpp
    p2p/ha/oplog/oplog_serializer.cpp
    p2p/ha/oplog/localfs_oplog_store.cpp
    p2p/ha/oplog/polling_oplog_change_notifier.cpp
    p2p/ha/oplog/oplog_store_factory.cpp
    p2p/ha/oplog/oplog_replicator.cpp
    p2p/ha/oplog/p2p_oplog_types.cpp
    p2p/ha/oplog/p2p_oplog_applier.cpp
    p2p/ha/oplog/p2p_standby_metadata_store.cpp
    p2p/ha/oplog/p2p_hot_standby_service.cpp
    p2p/ha/oplog/p2p_standby_snapshot_service.cpp
    p2p/ha/standby_state_machine.cpp
    p2p/ha/p2p_master_view.cpp
    p2p/ha/ha_metric_manager.cpp
)

set(MOONCAKE_STORE_SOURCES
    ${MOONCAKE_MASTER_SUPPORT_SOURCES}
    ${MOONCAKE_CENTRALIZED_MASTER_SOURCES}
    client_service.cpp
    centralized_client_service.cpp
    client_metric.cpp
    master_client.cpp
    storage_backend.cpp
    thread_pool.cpp
    transfer_task.cpp
    posix_file.cpp
    client_buffer.cpp
    real_client.cpp
    dummy_client.cpp
    file_storage.cpp
)

set(EXTRA_LIBS "")

# Find hiredis for Redis election backend
if(STORE_USE_REDIS)
    find_path(HIREDIS_INCLUDE_DIR NAMES hiredis/hiredis.h REQUIRED)
    find_library(HIREDIS_LIB NAMES hiredis REQUIRED)
    message(STATUS "Found hiredis: include=${HIREDIS_INCLUDE_DIR} lib=${HIREDIS_LIB}")
    list(APPEND MOONCAKE_P2P_MASTER_SOURCES p2p/ha/redis_util.cpp)
    list(APPEND MOONCAKE_P2P_MASTER_SOURCES p2p/ha/redis_election_helper.cpp)
    list(APPEND MOONCAKE_P2P_MASTER_SOURCES p2p/ha/oplog/redis_oplog_store.cpp)
    list(APPEND EXTRA_LIBS ${HIREDIS_LIB})
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
  list(APPEND MOONCAKE_STORE_SOURCES  ${HF3FS_SOURCES})
  find_library(HF3FS_API_LIB hf3fs_api_shared PATHS /usr/lib NO_DEFAULT_PATH)
  if(NOT HF3FS_API_LIB)
    message(FATAL_ERROR "hf3fs_api_shared library not found in /usr/lib")
  endif()
  set(EXTRA_LIBS ${HF3FS_API_LIB})
endif()

# Ascend Cache Tier support
if(USE_ASCEND_CACHE_TIER)
  list(APPEND MOONCAKE_STORE_P2P_SOURCES p2p/client/tiered_cache/tiers/ascend_tier.cpp)
  message(STATUS "Ascend Cache Tier enabled")
endif()

# Ascend DRAM Tier support (aclrtMallocHost-based DRAM allocation)
if(USE_ASCEND_DRAM_TIER)
  message(STATUS "Ascend DRAM Tier enabled")
endif()

# The cache_allocator library
include_directories(${Python3_INCLUDE_DIRS})
add_library(mooncake_store ${MOONCAKE_STORE_SOURCES})
# Note: transfer_engine is PRIVATE to avoid propagating its dependencies (e.g., vendor-specific hardware)
# to targets that don't need it (e.g., mooncake_master).
# Targets that need transfer_engine should link it explicitly.
target_include_directories(mooncake_store PUBLIC ${XXHASH_INCLUDE_DIR})
if(STORE_USE_REDIS)
    target_include_directories(mooncake_store PUBLIC ${HIREDIS_INCLUDE_DIR})
    target_compile_definitions(mooncake_store PUBLIC STORE_USE_REDIS)
endif()
target_link_libraries(mooncake_store
    PUBLIC
        cachelib_memory_allocator
        ${ETCD_WRAPPER_LIB}
        glog::glog
        gflags::gflags
        JsonCpp::JsonCpp
        ${XXHASH_LIBRARY}
        ${EXTRA_LIBS}
        asio_shared
    PRIVATE
        transfer_engine
)

# The client aggregate still includes both architectures until C4 splits the
# public client facade. Master executables use the isolated targets below.
list(APPEND MOONCAKE_STORE_P2P_SOURCES
    ${MOONCAKE_P2P_MASTER_SOURCES}
    p2p/master/p2p_master_client.cpp
    p2p/client/p2p_client_service.cpp
    p2p/client/p2p_client_metric.cpp
    p2p/client/peer_client.cpp
    p2p/client/client_rpc_service.cpp
    p2p/client/data_manager_types.cpp
    p2p/client/v1/data_manager_v1.cpp
    p2p/client/route_cache.cpp
    p2p/client/async_memcpy_executor.cpp
    p2p/client/async_metadata_notifier.cpp
    p2p/client/runtime_config_store.cpp
    p2p/client/tiered_cache/copier_registry.cpp
    p2p/client/tiered_cache/data_copier.cpp
    p2p/client/tiered_cache/tiered_backend.cpp
    p2p/client/tiered_cache/tiers/dram_tier.cpp
    p2p/client/tiered_cache/tiers/storage_tier.cpp
    p2p/client/tiered_cache/scheduler/simple_policy.cpp
    p2p/client/tiered_cache/scheduler/lru_policy.cpp
    p2p/client/tiered_cache/scheduler/client_scheduler.cpp
    p2p/client/tiered_cache/scheduler/scheduler_factory.cpp
    p2p/client/tiered_cache/event_driven_scheduler/multi_lru_stats_collector.cpp
    p2p/client/tiered_cache/event_driven_scheduler/multi_lru_policy.cpp
    p2p/client/tiered_cache/event_driven_scheduler/event_driven_client_scheduler.cpp
    p2p/ha/ha_recovery_manager.cpp
)

add_library(mooncake_store_p2p_objects OBJECT ${MOONCAKE_STORE_P2P_SOURCES})
target_link_libraries(mooncake_store_p2p_objects PUBLIC mooncake_store)

# Keep the standalone P2P library artifact for installation. Internal
# consumers use the object target above to avoid circular static-library
# ordering constraints during the transitional split phases.
add_library(mooncake_store_p2p
    $<TARGET_OBJECTS:mooncake_store_p2p_objects>)
target_link_libraries(mooncake_store_p2p PUBLIC mooncake_store)

# Link Ascend ACL library if USE_ASCEND_CACHE_TIER is enabled
if(USE_ASCEND_CACHE_TIER)
  # Try to find Ascend ACL library
  set(ASCEND_SEARCH_PATHS
      $ENV{ASCEND_HOME}/lib64
      /usr/local/Ascend/ascend-toolkit/latest/lib64
      /usr/local/Ascend/nnrt/latest/lib64
  )
  find_library(ASCENDCL_LIB ascendcl PATHS ${ASCEND_SEARCH_PATHS} NO_DEFAULT_PATH)

  if(NOT ASCENDCL_LIB)
    message(WARNING "ascendcl library not found. Ascend cache tier will use fallback mode (no ACL).")
  else()
    message(STATUS "Found ascendcl library: ${ASCENDCL_LIB}")
    target_link_libraries(mooncake_store PUBLIC ${ASCENDCL_LIB})

    # Add include paths for Ascend headers
    set(ASCEND_INCLUDE_PATHS
        $ENV{ASCEND_HOME}/include
        /usr/local/Ascend/ascend-toolkit/latest/include
        /usr/local/Ascend/nnrt/latest/include
    )
    target_include_directories(mooncake_store PUBLIC ${ASCEND_INCLUDE_PATHS})

    # Only define USE_ASCEND_CACHE_TIER when library is found
    target_compile_definitions(mooncake_store PUBLIC USE_ASCEND_CACHE_TIER)
  endif()
endif()

# Link Ascend ACL library if USE_ASCEND_DRAM_TIER is enabled
if(USE_ASCEND_DRAM_TIER)
  set(NEED_LINK_ASCEND FALSE)
  if(NOT ASCENDCL_LIB)
    set(NEED_LINK_ASCEND TRUE)
    set(ASCEND_SEARCH_PATHS
        $ENV{ASCEND_HOME}/lib64
        /usr/local/Ascend/ascend-toolkit/latest/lib64
        /usr/local/Ascend/nnrt/latest/lib64
    )
    find_library(ASCENDCL_LIB ascendcl PATHS ${ASCEND_SEARCH_PATHS} NO_DEFAULT_PATH)
  endif()

  if(NOT ASCENDCL_LIB)
    message(FATAL_ERROR "ascendcl library not found, USE_ASCEND_DRAM_TIER requires it")
  else()
    message(STATUS "Found ascendcl library for DRAM tier: ${ASCENDCL_LIB}")
    if(NEED_LINK_ASCEND)
      target_link_libraries(mooncake_store PUBLIC ${ASCENDCL_LIB})
      set(ASCEND_INCLUDE_PATHS
          $ENV{ASCEND_HOME}/include
          /usr/local/Ascend/ascend-toolkit/latest/include
          /usr/local/Ascend/nnrt/latest/include
      )
      target_include_directories(mooncake_store PUBLIC ${ASCEND_INCLUDE_PATHS})
    endif()
    target_compile_definitions(mooncake_store PUBLIC USE_ASCEND_DRAM_TIER)
  endif()
endif()
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
  install(TARGETS mooncake_store mooncake_store_p2p DESTINATION lib)
endif()

# Master executables never link the client aggregate. Explicit STATIC libraries
# keep this boundary intact even when the installed Store libraries are shared.
add_library(mooncake_master_support STATIC ${MOONCAKE_MASTER_SUPPORT_SOURCES})
target_include_directories(mooncake_master_support PUBLIC ${XXHASH_INCLUDE_DIR})
target_link_libraries(mooncake_master_support PUBLIC
    cachelib_memory_allocator
    glog::glog
    gflags::gflags
    JsonCpp::JsonCpp
    ${XXHASH_LIBRARY}
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

add_library(mooncake_master_core STATIC ${MOONCAKE_CENTRALIZED_MASTER_SOURCES})
target_link_libraries(mooncake_master_core PUBLIC mooncake_master_support)

add_library(mooncake_p2p_master_core STATIC ${MOONCAKE_P2P_MASTER_SOURCES})
target_link_libraries(mooncake_p2p_master_core PUBLIC mooncake_master_support)
if(STORE_USE_ETCD)
    add_dependencies(mooncake_master_core build_etcd_wrapper)
    add_dependencies(mooncake_p2p_master_core build_etcd_wrapper)
endif()
if(STORE_USE_REDIS)
    target_include_directories(mooncake_p2p_master_core PUBLIC ${HIREDIS_INCLUDE_DIR})
    target_link_libraries(mooncake_p2p_master_core PUBLIC ${HIREDIS_LIB})
endif()

# Master binary
add_executable(mooncake_master master.cpp)
add_executable(mooncake_master_p2p p2p/master/p2p_master_main.cpp)

set(MASTER_EXTRA_INCS)
set(MASTER_EXTRA_LIBS)

if (STORE_USE_JEMALLOC)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(JEMALLOC REQUIRED jemalloc)
    list(APPEND MASTER_EXTRA_INCS ${JEMALLOC_INCLUDE_DIRS})
    list(APPEND MASTER_EXTRA_LIBS ${JEMALLOC_STATIC_LIBRARIES})
endif()

target_include_directories(mooncake_master PRIVATE ${MASTER_EXTRA_INCS})
target_include_directories(mooncake_master_p2p PRIVATE ${MASTER_EXTRA_INCS})
target_link_libraries(mooncake_master PRIVATE
    mooncake_master_core
    ${MASTER_EXTRA_LIBS}
)
target_link_libraries(mooncake_master_p2p PRIVATE
    mooncake_p2p_master_core
    ${MASTER_EXTRA_LIBS}
)

if (STORE_USE_ETCD)
    add_dependencies(mooncake_master build_etcd_wrapper)
    add_dependencies(mooncake_master_p2p build_etcd_wrapper)
endif()

target_compile_options(mooncake_master PRIVATE -Os)
target_link_options(mooncake_master PRIVATE -Os -s)
target_compile_options(mooncake_master_p2p PRIVATE -Os)
target_link_options(mooncake_master_p2p PRIVATE -Os -s)

# Client server binary
add_executable(mooncake_client real_client_main.cpp)
# Client needs transfer_engine for data transfer operations
target_link_libraries(mooncake_client PRIVATE ${MOONCAKE_STORE_LIBS} transfer_engine asio_shared)
target_compile_options(mooncake_client PRIVATE -Os)
target_link_options(mooncake_client PRIVATE -Os -s)

install(TARGETS mooncake_master mooncake_master_p2p mooncake_client
        DESTINATION bin)
