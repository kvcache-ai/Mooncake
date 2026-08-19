# P2P tests need access to tests/utils/ helper files
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../ha/oplog)
find_path(JSONCPP_INCLUDE_DIR json/json.h)
if(JSONCPP_INCLUDE_DIR)
    include_directories(${JSONCPP_INCLUDE_DIR})
endif()

# P2P Master tests
add_store_test(p2p_master_service_test master/p2p_master_service_test.cpp)
add_store_test(p2p_segment_manager_test master/p2p_segment_manager_test.cpp)
add_store_test(p2p_client_manager_test master/p2p_client_manager_test.cpp)
add_store_test(p2p_client_meta_test master/p2p_client_meta_test.cpp)
add_store_test(p2p_register_concurrency_test master/p2p_register_concurrency_test.cpp)
add_store_test(p2p_master_http_endpoints_test master/p2p_master_http_endpoints_test.cpp)

# P2P Client tests
add_store_test(route_cache_test client/route_cache_test.cpp)
add_store_test(client_config_builder_test client/client_config_builder_test.cpp)
add_store_test(peer_client_test client/peer_client_test.cpp)
add_store_test(client_rpc_service_test client/client_rpc_service_test.cpp)
add_store_test(inflight_tracker_test client/inflight_tracker_test.cpp)
add_store_test(p2p_client_http_endpoints_test client/p2p_client_http_endpoints_test.cpp)
add_store_test(async_metadata_notifier_test client/async_metadata_notifier_test.cpp)
add_store_test(ha_recovery_manager_test client/ha_recovery_manager_test.cpp)
add_store_test(client_metrics_aggregator_test client/client_metrics_aggregator_test.cpp)

# P2P Tiered Cache tests
add_store_test(tiered_backend_test client/tiered_backend_test.cpp)
add_store_test(storage_tier_test client/storage_tier_test.cpp)
add_store_test(bounded_dedup_queue_test client/bounded_dedup_queue_test.cpp)
add_store_test(multi_lru_test client/multi_lru_test.cpp)
add_store_test(tinylfu_sketch_test client/tinylfu_sketch_test.cpp)
add_store_test(event_driven_policy_test client/event_driven_policy_test.cpp)
add_store_test(tier_role_resolver_test client/tier_role_resolver_test.cpp)

# P2P HA Oplog tests
add_store_test(p2p_oplog_applier_test ha/oplog/p2p_oplog_applier_test.cpp)
add_store_test(p2p_oplog_test ha/oplog/p2p_oplog_test.cpp)
add_store_test(p2p_record_oplog_test ha/oplog/p2p_record_oplog_test.cpp)
add_store_test(p2p_standby_metadata_store_test ha/oplog/p2p_standby_metadata_store_test.cpp)
add_store_test(p2p_hot_standby_service_test ha/oplog/p2p_hot_standby_service_test.cpp)
add_store_test(p2p_ha_metric_manager_test ha/oplog/ha_metric_manager_test.cpp)
add_store_test(p2p_standby_state_machine_test ha/oplog/standby_state_machine_test.cpp)
add_store_test(p2p_oplog_manager_test ha/oplog/oplog_manager_test.cpp)
add_store_test(p2p_oplog_replicator_test ha/oplog/oplog_replicator_test.cpp)
add_store_test(p2p_oplog_serializer_test ha/oplog/oplog_serializer_test.cpp)
add_store_test(p2p_localfs_oplog_store_test ha/oplog/localfs_oplog_store_test.cpp)
# add_store_test(p2p_redis_oplog_store_test ha/oplog/redis_oplog_store_test.cpp)  # requires Redis