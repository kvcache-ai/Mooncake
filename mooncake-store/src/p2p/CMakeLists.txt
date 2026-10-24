# Extend the standard Store targets only for the selected P2P architecture.
option(USE_ASCEND_CACHE_TIER "Enable Ascend NPU cache tier support" OFF)
option(USE_ASCEND_DRAM_TIER "Enable Ascend ACL Host memory for DRAM tier (aclrtMallocHost)" OFF)

set(MOONCAKE_P2P_MASTER_SOURCES
    master/p2p_route_table.cpp
    master/p2p_master_service.cpp
    master/p2p_master_metric_manager.cpp
    master/p2p_segment_manager.cpp
    master/p2p_rpc_service.cpp
    master/p2p_master.cpp
    master/p2p_client_meta.cpp
    master/p2p_client_manager.cpp
    client/client_metrics_aggregator.cpp
    util/metric_util.cpp
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
    ha/p2p_master_view.cpp
    ha/ha_metric_manager.cpp
)

# Find hiredis for Redis election backend
if(STORE_USE_REDIS)
    find_path(HIREDIS_INCLUDE_DIR NAMES hiredis/hiredis.h REQUIRED)
    find_library(HIREDIS_LIB NAMES hiredis REQUIRED)
    message(STATUS "Found hiredis: include=${HIREDIS_INCLUDE_DIR} lib=${HIREDIS_LIB}")
    list(APPEND MOONCAKE_P2P_MASTER_SOURCES ha/redis_util.cpp)
    list(APPEND MOONCAKE_P2P_MASTER_SOURCES ha/redis_election_helper.cpp)
    list(APPEND MOONCAKE_P2P_MASTER_SOURCES ha/oplog/redis_oplog_store.cpp)
    foreach(target mooncake_store mooncake_master_core)
        target_include_directories(${target} PUBLIC ${HIREDIS_INCLUDE_DIR})
        target_link_libraries(${target} PUBLIC ${HIREDIS_LIB})
    endforeach()
    target_compile_definitions(mooncake_store PUBLIC STORE_USE_REDIS)
endif()

set(MOONCAKE_STORE_P2P_SOURCES
    ${MOONCAKE_P2P_MASTER_SOURCES}
    master/p2p_master_client.cpp
    client/p2p_client_service.cpp
    client/p2p_client_metric.cpp
    client/peer_client.cpp
    client/client_rpc_service.cpp
    client/data_manager_types.cpp
    client/data_manager_factory.cpp
    client/v1/data_manager_v1.cpp
    client/v2/v2_common.cpp
    client/v2/movement.cpp
    client/v2/copier.cpp
    client/v2/eviction_index.cpp
    client/v2/eviction_policy.cpp
    client/v2/movement_tracker.cpp
    client/v2/tier_graph.cpp
    client/v2/block.cpp
    client/v2/block_registry.cpp
    client/v2/block_pool.cpp
    client/v2/block_index.cpp
    client/v2/event_center.cpp
    client/v2/tiler_manager.cpp
    client/v2/frequency_tracker.cpp
    client/v2/lease_manager.cpp
    client/v2/movement_consumers.cpp
    client/v2/migration_engine.cpp
    client/v2/evict_engine.cpp
    client/v2/local_copy_engine.cpp
    client/v2/transfer_coordinator.cpp
    client/v2/data_manager_v2.cpp
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

# Ascend Cache Tier support
if(USE_ASCEND_CACHE_TIER)
  list(APPEND MOONCAKE_STORE_P2P_SOURCES client/tiered_cache/tiers/ascend_tier.cpp)
  message(STATUS "Ascend Cache Tier enabled")
endif()

# Ascend DRAM Tier support (aclrtMallocHost-based DRAM allocation)
if(USE_ASCEND_DRAM_TIER)
  message(STATUS "Ascend DRAM Tier enabled")
endif()

target_sources(mooncake_store PRIVATE
    ${MOONCAKE_STORE_P2P_SOURCES}
    client/p2p_real_client.cpp
)
target_sources(mooncake_master_core PRIVATE ${MOONCAKE_P2P_MASTER_SOURCES})
target_sources(mooncake_master PRIVATE master/p2p_master_main.cpp)
target_sources(mooncake_client PRIVATE client/p2p_real_client_main.cpp)

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

# Both targets compile OpLog; neutral master support does not consume xxHash.
# PUBLIC usage requirements also reach the P2P test consumers.
foreach(target mooncake_store mooncake_master_core)
    target_compile_definitions(${target} PUBLIC MOONCAKE_STORE_CLIENT_P2P)
    target_include_directories(${target} PUBLIC ${XXHASH_INCLUDE_DIR})
    target_link_libraries(${target} PUBLIC ${XXHASH_LIBRARY})
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
        target_include_directories(mooncake_store PUBLIC ${ASCEND_INCLUDE_PATHS})
        if(USE_ASCEND_CACHE_TIER)
            target_compile_definitions(mooncake_store PUBLIC USE_ASCEND_CACHE_TIER)
        endif()
        if(USE_ASCEND_DRAM_TIER)
            target_compile_definitions(mooncake_store PUBLIC USE_ASCEND_DRAM_TIER)
        endif()
    endif()
endif()
