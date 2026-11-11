# Sources, entry points and usage requirements of the selected P2P implementation.
option(USE_ASCEND_CACHE_TIER "Enable Ascend NPU cache tier support" OFF)
option(USE_ASCEND_DRAM_TIER "Enable Ascend ACL Host memory for DRAM tier (aclrtMallocHost)" OFF)

set(MOONCAKE_STORE_P2P_SHARED_SOURCES
    util/metric_util.cpp
    ha/p2p_master_view.cpp
    ha/ha_metric_manager.cpp
)

set(MOONCAKE_STORE_P2P_MASTER_SOURCES
    master/p2p_route_table.cpp
    master/p2p_master_service.cpp
    master/p2p_master_metric_manager.cpp
    master/p2p_segment_manager.cpp
    master/p2p_rpc_service.cpp
    master/p2p_master.cpp
    master/p2p_client_meta.cpp
    master/p2p_client_manager.cpp
    master/client_metrics_aggregator.cpp
    ha/oplog/oplog_manager.cpp
    ha/oplog/oplog_serializer.cpp
    ha/oplog/localfs_oplog_store.cpp
    ha/oplog/polling_oplog_change_notifier.cpp
    ha/oplog/oplog_store_factory.cpp
    ha/oplog/oplog_replicator.cpp
    ha/oplog/p2p_oplog_types.cpp
    ha/oplog/p2p_oplog_applier.cpp
    ha/oplog/p2p_standby_metadata_store.cpp
    ha/oplog/p2p_hot_standby_service.cpp
    ha/oplog/p2p_standby_snapshot_service.cpp
    ha/standby_state_machine.cpp
)

set(MOONCAKE_STORE_P2P_CLIENT_SOURCES
    master/p2p_master_client.cpp
    client/p2p_client_service.cpp
    client/p2p_real_client.cpp
    client/p2p_client_metric.cpp
    client/peer_client.cpp
    client/client_rpc_service.cpp
    client/data_manager_types.cpp
    client/data_manager_factory.cpp
    client/data_manager/v1/data_manager_v1.cpp
    client/data_manager/v2/v2_common.cpp
    client/data_manager/v2/movement.cpp
    client/data_manager/v2/copier.cpp
    client/data_manager/v2/eviction_policy.cpp
    client/data_manager/v2/movement_tracker.cpp
    client/data_manager/v2/tier_graph.cpp
    client/data_manager/v2/block.cpp
    client/data_manager/v2/block_registry.cpp
    client/data_manager/v2/block_pool.cpp
    client/data_manager/v2/block_index.cpp
    client/data_manager/v2/event_center.cpp
    client/data_manager/v2/tiler_manager.cpp
    client/data_manager/v2/frequency_tracker.cpp
    client/data_manager/v2/lease_manager.cpp
    client/data_manager/v2/movement_consumers.cpp
    client/data_manager/v2/migration_engine.cpp
    client/data_manager/v2/evict_engine.cpp
    client/data_manager/v2/local_copy_engine.cpp
    client/data_manager/v2/transfer_coordinator.cpp
    client/data_manager/v2/data_manager_v2.cpp
    client/route_cache.cpp
    client/async_memcpy_executor.cpp
    client/async_metadata_notifier.cpp
    client/runtime_config_store.cpp
    client/tiered_cache/copier_registry.cpp
    client/tiered_cache/data_copier.cpp
    client/tiered_cache/tiered_backend.cpp
    client/tiered_cache/tiers/dram_tier.cpp
    client/tiered_cache/tiers/storage_tier.cpp
    client/tiered_cache/tiers/bucket_storage_backend.cpp
    client/tiered_cache/scheduler/simple_policy.cpp
    client/tiered_cache/scheduler/lru_policy.cpp
    client/tiered_cache/scheduler/client_scheduler.cpp
    client/tiered_cache/scheduler/scheduler_factory.cpp
    client/tiered_cache/event_driven_scheduler/multi_lru_stats_collector.cpp
    client/tiered_cache/event_driven_scheduler/multi_lru_policy.cpp
    client/tiered_cache/event_driven_scheduler/event_driven_client_scheduler.cpp
    ha/metadata_recovery_worker.cpp
)

if(STORE_USE_REDIS)
    list(APPEND MOONCAKE_STORE_P2P_SHARED_SOURCES
        ha/redis_util.cpp
        ha/redis_election_helper.cpp)
    list(APPEND MOONCAKE_STORE_P2P_MASTER_SOURCES
        ha/oplog/redis_oplog_store.cpp
        ha/oplog/redis_oplog_trim_worker.cpp)
endif()

if(USE_ASCEND_CACHE_TIER)
    list(APPEND MOONCAKE_STORE_P2P_CLIENT_SOURCES client/tiered_cache/tiers/ascend_tier.cpp)
endif()

# Resolve P2P paths before attaching sources to the common OBJECT targets.
foreach(role SHARED MASTER CLIENT)
    list(TRANSFORM MOONCAKE_STORE_P2P_${role}_SOURCES
         PREPEND "${CMAKE_CURRENT_LIST_DIR}/")
endforeach()

target_sources(mooncake_store_shared_objects PRIVATE
    ${MOONCAKE_STORE_P2P_SHARED_SOURCES})
target_sources(mooncake_store_master_objects PRIVATE
    ${MOONCAKE_STORE_P2P_MASTER_SOURCES})
target_sources(mooncake_store_client_objects PRIVATE
    ${MOONCAKE_STORE_P2P_CLIENT_SOURCES})
target_sources(mooncake_master PRIVATE ${CMAKE_CURRENT_LIST_DIR}/master/p2p_master_main.cpp)
target_sources(mooncake_client PRIVATE ${CMAKE_CURRENT_LIST_DIR}/client/p2p_real_client_main.cpp)

foreach(target mooncake_store_shared_objects mooncake_store_master_objects
               mooncake_store_client_objects)
    target_compile_definitions(${target} PRIVATE MOONCAKE_STORE_CLIENT_P2P)
endforeach()
target_compile_definitions(mooncake_store_master PUBLIC MOONCAKE_STORE_CLIENT_P2P)
target_compile_definitions(mooncake_store PUBLIC MOONCAKE_STORE_CLIENT_P2P)

# Shared P2P discovery headers expose hiredis when Redis is enabled.
if(STORE_USE_REDIS)
    find_path(HIREDIS_INCLUDE_DIR NAMES hiredis/hiredis.h REQUIRED)
    find_library(HIREDIS_LIB NAMES hiredis REQUIRED)
    message(STATUS "Found hiredis: include=${HIREDIS_INCLUDE_DIR} lib=${HIREDIS_LIB}")
    foreach(target mooncake_store_shared_objects mooncake_store_master_objects
                   mooncake_store_client_objects)
        target_include_directories(${target} PRIVATE ${HIREDIS_INCLUDE_DIR})
    endforeach()
    target_link_libraries(mooncake_store_shared_objects PRIVATE JsonCpp::JsonCpp)
    foreach(target mooncake_store_master mooncake_store)
        target_include_directories(${target} PUBLIC ${HIREDIS_INCLUDE_DIR})
        target_link_libraries(${target} PRIVATE ${HIREDIS_LIB})
    endforeach()
endif()

if(USE_ASCEND_CACHE_TIER)
    message(STATUS "Ascend Cache Tier enabled")
endif()
if(USE_ASCEND_DRAM_TIER)
    message(STATUS "Ascend DRAM Tier enabled")
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
else()
  message(
    FATAL_ERROR
      "xxHash library/header not found. Please install xxhash (development headers) and try again."
  )
endif()

# OpLog is compiled by the master object target and used by both aggregates.
target_include_directories(mooncake_store_master_objects PRIVATE ${XXHASH_INCLUDE_DIR})
foreach(target mooncake_store_master mooncake_store)
    target_link_libraries(${target} PRIVATE ${XXHASH_LIBRARY})
endforeach()

# Cache tier can compile without ACL; DRAM tier requires host allocation APIs.
if(USE_ASCEND_CACHE_TIER OR USE_ASCEND_DRAM_TIER)
    set(ASCEND_SEARCH_PATHS
        $ENV{ASCEND_HOME}/lib64
        /usr/local/Ascend/ascend-toolkit/latest/lib64
        /usr/local/Ascend/nnrt/latest/lib64
    )
    find_library(ASCENDCL_LIB ascendcl PATHS ${ASCEND_SEARCH_PATHS} NO_DEFAULT_PATH)
    if(NOT ASCENDCL_LIB)
        if(USE_ASCEND_DRAM_TIER)
            message(FATAL_ERROR "ascendcl library not found, USE_ASCEND_DRAM_TIER requires it")
        else()
            message(WARNING "ascendcl library not found. Ascend cache tier will use fallback mode (no ACL).")
        endif()
    else()
        message(STATUS "Found ascendcl library: ${ASCENDCL_LIB}")
        # Apply usage requirements even when the library path comes from cache.
        target_link_libraries(mooncake_store PUBLIC ${ASCENDCL_LIB})
        set(ASCEND_INCLUDE_PATHS
            $ENV{ASCEND_HOME}/include
            /usr/local/Ascend/ascend-toolkit/latest/include
            /usr/local/Ascend/nnrt/latest/include
        )
        target_include_directories(mooncake_store_client_objects PRIVATE ${ASCEND_INCLUDE_PATHS})
        target_include_directories(mooncake_store PUBLIC ${ASCEND_INCLUDE_PATHS})
        if(USE_ASCEND_CACHE_TIER)
            target_compile_definitions(mooncake_store_client_objects PRIVATE USE_ASCEND_CACHE_TIER)
            target_compile_definitions(mooncake_store PUBLIC USE_ASCEND_CACHE_TIER)
        endif()
        if(USE_ASCEND_DRAM_TIER)
            target_compile_definitions(mooncake_store_client_objects PRIVATE USE_ASCEND_DRAM_TIER)
            target_compile_definitions(mooncake_store PUBLIC USE_ASCEND_DRAM_TIER)
        endif()
    endif()
endif()
