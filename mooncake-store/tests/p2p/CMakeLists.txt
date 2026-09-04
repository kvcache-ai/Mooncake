# Tests for the P2P architecture (P2P split, Phase 1). Mirrors add_store_test
# from tests/CMakeLists.txt; assertions are unchanged, only location moved.
function(add_p2p_store_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PUBLIC
        ${MOONCAKE_STORE_LIBS}
        transfer_engine
        cachelib_memory_allocator
        ${ETCD_WRAPPER_LIB}
        glog
        ibverbs
        gtest
        gtest_main
        pthread
    )
    add_test(NAME ${name} COMMAND ${name})
endfunction()

# master side
add_p2p_store_test(p2p_master_service_test p2p_master_service_test.cpp)
add_p2p_store_test(p2p_master_http_endpoints_test p2p_master_http_endpoints_test.cpp)
add_p2p_store_test(p2p_master_metrics_test p2p_master_metrics_test.cpp)
add_p2p_store_test(p2p_segment_manager_test p2p_segment_manager_test.cpp)

# client side
add_p2p_store_test(p2p_client_integration_test p2p_client_integration_test.cpp)
add_p2p_store_test(p2p_client_ha_test p2p_client_ha_test.cpp)
add_p2p_store_test(p2p_client_http_endpoints_test p2p_client_http_endpoints_test.cpp)
add_p2p_store_test(p2p_client_manager_test p2p_client_manager_test.cpp)
add_p2p_store_test(p2p_client_meta_test p2p_client_meta_test.cpp)
add_p2p_store_test(p2p_register_concurrency_test p2p_register_concurrency_test.cpp)
add_p2p_store_test(peer_client_test peer_client_test.cpp)
add_p2p_store_test(peer_client_perf_test peer_client_perf_test.cpp)
add_p2p_store_test(client_rpc_service_test client_rpc_service_test.cpp)
add_p2p_store_test(data_manager_v1_test data_manager_v1_test.cpp)
add_p2p_store_test(data_manager_contract_test data_manager_contract_test.cpp)
add_p2p_store_test(data_manager_differential_test data_manager_differential_test.cpp)

# V2 component tests
add_p2p_store_test(v2_block_registry_test v2/block_registry_test.cpp)
add_p2p_store_test(v2_block_index_test v2/block_index_test.cpp)
add_p2p_store_test(v2_block_pool_test v2/block_pool_test.cpp)
add_p2p_store_test(v2_local_copy_engine_test v2/local_copy_engine_test.cpp)
add_p2p_store_test(v2_eviction_index_test v2/eviction_index_test.cpp)
add_p2p_store_test(v2_eviction_policy_test v2/eviction_policy_test.cpp)
add_p2p_store_test(v2_movement_tracker_test v2/movement_tracker_test.cpp)
add_p2p_store_test(v2_tier_graph_test v2/tier_graph_test.cpp)
add_p2p_store_test(v2_frequency_tracker_test v2/frequency_tracker_test.cpp)
add_p2p_store_test(v2_lease_manager_test v2/lease_manager_test.cpp)
add_p2p_store_test(v2_tiler_manager_test v2/tiler_manager_test.cpp)
add_p2p_store_test(v2_config_test v2/config_test.cpp)
add_p2p_store_test(v2_metrics_test v2/metrics_test.cpp)
add_p2p_store_test(v2_event_center_test v2/event_center_test.cpp)
add_p2p_store_test(v2_movement_consumers_test v2/movement_consumers_test.cpp)
add_p2p_store_test(v2_migration_engine_test v2/migration_engine_test.cpp)
add_p2p_store_test(v2_evict_engine_test v2/evict_engine_test.cpp)
add_p2p_store_test(route_cache_test route_cache_test.cpp)
add_p2p_store_test(inflight_tracker_test inflight_tracker_test.cpp)
add_p2p_store_test(async_metadata_notifier_test async_metadata_notifier_test.cpp)
add_p2p_store_test(runtime_config_store_test runtime_config_store_test.cpp)
add_p2p_store_test(client_metrics_aggregator_test client_metrics_aggregator_test.cpp)
add_p2p_store_test(client_http_metrics_test client_http_metrics_test.cpp)

# tiered cache
add_p2p_store_test(tiered_backend_test tiered_backend_test.cpp)
add_p2p_store_test(storage_tier_test storage_tier_test.cpp)
add_p2p_store_test(scheduler_integration_test scheduler_integration_test.cpp)
add_p2p_store_test(multi_lru_test multi_lru_test.cpp)
add_p2p_store_test(event_driven_policy_test event_driven_policy_test.cpp)
add_p2p_store_test(event_driven_scheduler_test event_driven_scheduler_test.cpp)
add_p2p_store_test(bounded_dedup_queue_test bounded_dedup_queue_test.cpp)
add_p2p_store_test(tinylfu_sketch_test tinylfu_sketch_test.cpp)
add_p2p_store_test(tier_role_resolver_test tier_role_resolver_test.cpp)
if(USE_ASCEND_CACHE_TIER)
    add_p2p_store_test(ascend_tier_test ascend_tier_test.cpp)
endif()

# HA
add_p2p_store_test(ha_integration_test ha_integration_test.cpp)
add_p2p_store_test(ha_recovery_manager_test ha_recovery_manager_test.cpp)
add_p2p_store_test(oplog_manager_test ha/oplog/oplog_manager_test.cpp)
add_p2p_store_test(oplog_serializer_test ha/oplog/oplog_serializer_test.cpp)
add_p2p_store_test(localfs_oplog_store_test ha/oplog/localfs_oplog_store_test.cpp)
add_p2p_store_test(standby_state_machine_test ha/oplog/standby_state_machine_test.cpp)
add_p2p_store_test(ha_metric_manager_test ha/oplog/ha_metric_manager_test.cpp)
add_p2p_store_test(oplog_applier_test ha/oplog/oplog_applier_test.cpp)
add_p2p_store_test(oplog_replicator_test ha/oplog/oplog_replicator_test.cpp)
add_p2p_store_test(p2p_oplog_test
    ha/oplog/p2p_oplog_test.cpp
    ha/oplog/p2p_oplog_applier_test.cpp
    ha/oplog/p2p_record_oplog_test.cpp
    ha/oplog/p2p_standby_metadata_store_test.cpp
    ha/oplog/p2p_hot_standby_service_test.cpp
)

# redis-dependent entries (registered only when STORE_USE_REDIS, as before)
add_executable(redis_election_helper_test redis_election_helper_test.cpp)
target_link_libraries(redis_election_helper_test PUBLIC ${MOONCAKE_STORE_LIBS} transfer_engine cachelib_memory_allocator ${ETCD_WRAPPER_LIB} glog gtest gtest_main pthread)
if(STORE_USE_REDIS)
    add_test(NAME redis_election_helper_test COMMAND redis_election_helper_test)
    add_p2p_store_test(redis_oplog_store_test ha/oplog/redis_oplog_store_test.cpp)
endif()
