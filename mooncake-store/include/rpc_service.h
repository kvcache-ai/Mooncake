#pragma once
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/reflection/user_reflect_macro.hpp>

#include "master_metric_manager.h"
#include "master_service.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

struct ExistKeyResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(ExistKeyResponse, error_code)

struct GetReplicaListResponse {
    std::vector<Replica::Descriptor> replica_list;
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(GetReplicaListResponse, replica_list, error_code)

struct BatchGetReplicaListResponse {
    std::unordered_map<std::string, std::vector<Replica::Descriptor>>
        batch_replica_list;
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(BatchGetReplicaListResponse, batch_replica_list, error_code)

struct PutStartResponse {
    std::vector<Replica::Descriptor> replica_list;
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(PutStartResponse, replica_list, error_code)

struct PutEndResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(PutEndResponse, error_code)
struct PutRevokeResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(PutRevokeResponse, error_code)
struct BatchPutStartResponse {
    std::unordered_map<std::string, std::vector<Replica::Descriptor>>
        batch_replica_list;
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(BatchPutStartResponse, batch_replica_list, error_code)

struct BatchPutEndResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(BatchPutEndResponse, error_code)

struct BatchPutRevokeResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(BatchPutRevokeResponse, error_code)

struct RemoveResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(RemoveResponse, error_code)
struct RemoveAllResponse {
    long removed_count = 0;
};
YLT_REFL(RemoveAllResponse, removed_count)
struct MountSegmentResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(MountSegmentResponse, error_code)

struct ReMountSegmentResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(ReMountSegmentResponse, error_code)

struct UnmountSegmentResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(UnmountSegmentResponse, error_code)

struct PingResponse {
    ViewVersionId view_version = 0;
    ClientStatus client_status = ClientStatus::UNDEFINED;
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(PingResponse, view_version, error_code)

struct BatchExistResponse {
    std::vector<ErrorCode> exist_responses;
};
YLT_REFL(BatchExistResponse, exist_responses)

constexpr uint64_t kMetricReportIntervalSeconds = 10;

class WrappedMasterService {
   public:
    WrappedMasterService(
        bool enable_gc, uint64_t default_kv_lease_ttl,
        bool enable_metric_reporting = true, uint16_t http_port = 9003,
        double eviction_ratio = DEFAULT_EVICTION_RATIO,
        double eviction_high_watermark_ratio =
            DEFAULT_EVICTION_HIGH_WATERMARK_RATIO,
        ViewVersionId view_version = 0,
        int64_t client_live_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC,
        bool enable_ha = false)
        : master_service_(enable_gc, default_kv_lease_ttl, eviction_ratio,
                          eviction_high_watermark_ratio, view_version,
                          client_live_ttl_sec, enable_ha),
          http_server_(4, http_port),
          metric_report_running_(enable_metric_reporting),
          view_version_(view_version) {
        // Initialize HTTP server for metrics
        init_http_server();

        // Set the config for metric reporting
        MasterMetricManager::instance().set_enable_ha(enable_ha);

        // Start metric reporting thread if enabled
        if (enable_metric_reporting) {
            metric_report_thread_ = std::thread([this]() {
                while (metric_report_running_) {
                    std::string metrics_summary =
                        MasterMetricManager::instance().get_summary_string();
                    LOG(INFO) << "Master Metrics: " << metrics_summary;
                    std::this_thread::sleep_for(
                        std::chrono::seconds(kMetricReportIntervalSeconds));
                }
            });
        }
    }

    ~WrappedMasterService() {
        metric_report_running_ = false;
        if (metric_report_thread_.joinable()) {
            metric_report_thread_.join();
        }
        // Stop HTTP server
        http_server_.stop();
    }

    // Initialize and start the HTTP server
    void init_http_server() {
        using namespace coro_http;

        // Endpoint for Prometheus metrics
        http_server_.set_http_handler<GET>(
            "/metrics", [](coro_http_request& req, coro_http_response& resp) {
                std::string metrics =
                    MasterMetricManager::instance().serialize_metrics();
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                resp.set_status_and_content(status_type::ok, metrics);
            });

        // Endpoint for human-readable metrics summary
        http_server_.set_http_handler<GET>(
            "/metrics/summary",
            [](coro_http_request& req, coro_http_response& resp) {
                std::string summary =
                    MasterMetricManager::instance().get_summary_string();
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                resp.set_status_and_content(status_type::ok, summary);
            });

        // Endpoint for query a key's location
        http_server_.set_http_handler<GET>(
            "/query_key",
            [&](coro_http_request& req, coro_http_response& resp) {
                auto key = req.get_query_value("key");
                GetReplicaListResponse response;
                response = GetReplicaList(std::string(key));
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                std::string ss = "";
                for (size_t i = 0; i < response.replica_list.size(); i++) {
                    for (const auto& handle :
                         response.replica_list[i].buffer_descriptors) {
                        std::string tmp = "";
                        struct_json::to_json(handle, tmp);
                        ss += tmp;
                        ss += "\n";
                    }
                }
                resp.set_status_and_content(status_type::ok, ss);
            });

        // Endpoint for query all keys
        http_server_.set_http_handler<GET>(
            "/get_all_keys",
            [&](coro_http_request& req, coro_http_response& resp) {
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                std::string ss = "";
                std::vector<std::string> all_keys;
                master_service_.GetAllKeys(all_keys);
                for (const auto& key : all_keys) {
                    ss += key;
                    ss += "\n";
                }
                resp.set_status_and_content(status_type::ok, ss);
            });

        // Endpoint for query all segments
        http_server_.set_http_handler<GET>(
            "/get_all_segments",
            [&](coro_http_request& req, coro_http_response& resp) {
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                std::string ss = "";
                std::vector<std::string> all_segments;
                master_service_.GetAllSegments(all_segments);
                for (const auto& segment : all_segments) {
                    ss += segment;
                    ss += "\n";
                }
                resp.set_status_and_content(status_type::ok, ss);
            });

        // Endpoint for query segment details
        http_server_.set_http_handler<GET>(
            "/query_segment",
            [&](coro_http_request& req, coro_http_response& resp) {
                auto segment = req.get_query_value("segment");
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                std::string ss = "";
                size_t used = 0, capacity = 0;
                if (master_service_.QuerySegments(std::string(segment), used,
                                                  capacity) == ErrorCode::OK) {
                    ss += segment;
                    ss += "\n";
                    ss += "Used(bytes): ";
                    ss += std::to_string(used);
                    ss += "\nCapacity(bytes) : ";
                    ss += std::to_string(capacity);
                    ss += "\n";
                    resp.set_status_and_content(status_type::ok, ss);
                } else {
                    resp.set_status_and_content(status_type::not_found, ss);
                }
            });

        // Health check endpoint
        http_server_.set_http_handler<GET>(
            "/health", [](coro_http_request& req, coro_http_response& resp) {
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                resp.set_status_and_content(status_type::ok, "OK");
            });

        // Start the HTTP server asynchronously
        http_server_.async_start();
        LOG(INFO) << "HTTP metrics server started on port "
                  << http_server_.port();
    }

    ExistKeyResponse ExistKey(const std::string& key) {
        ScopedVLogTimer timer(1, "ExistKey");
        timer.LogRequest("key=", key);

        // Increment request metric
        MasterMetricManager::instance().inc_exist_key_requests();

        ExistKeyResponse response;
        response.error_code = master_service_.ExistKey(key);

        // Track failures if needed
        if (response.error_code != ErrorCode::OK) {
            MasterMetricManager::instance().inc_exist_key_failures();
        }

        timer.LogResponseJson(response);
        return response;
    }

    BatchExistResponse BatchExistKey(const std::vector<std::string>& keys) {
        ScopedVLogTimer timer(1, "BatchExistKey");
        timer.LogRequest("keys_count=", keys.size());

        BatchExistResponse response{master_service_.BatchExistKey(keys)};
        timer.LogResponseJson(response);
        return response;
    }

    GetReplicaListResponse GetReplicaList(const std::string& key) {
        ScopedVLogTimer timer(1, "GetReplicaList");
        timer.LogRequest("key=", key);

        // Increment request metric
        MasterMetricManager::instance().inc_get_replica_list_requests();

        GetReplicaListResponse response;
        response.error_code =
            master_service_.GetReplicaList(key, response.replica_list);

        // Track failures if needed
        if (response.error_code != ErrorCode::OK) {
            MasterMetricManager::instance().inc_get_replica_list_failures();
        }

        timer.LogResponseJson(response);
        return response;
    }

    BatchGetReplicaListResponse BatchGetReplicaList(
        const std::vector<std::string>& keys) {
        ScopedVLogTimer timer(1, "BatchGetReplicaList");
        timer.LogRequest("action=get_batch_replica_list");

        BatchGetReplicaListResponse response;
        response.error_code = master_service_.BatchGetReplicaList(
            keys, response.batch_replica_list);

        timer.LogResponseJson(response);
        return response;
    }

    PutStartResponse PutStart(const std::string& key, uint64_t value_length,
                              const std::vector<uint64_t>& slice_lengths,
                              const ReplicateConfig& config) {
        ScopedVLogTimer timer(1, "PutStart");
        timer.LogRequest("key=", key, ", value_length=", value_length,
                         ", slice_lengths=", slice_lengths.size());

        // Increment request metric
        MasterMetricManager::instance().inc_put_start_requests();

        // Track value size in histogram
        MasterMetricManager::instance().observe_value_size(value_length);

        PutStartResponse response;
        response.error_code = master_service_.PutStart(
            key, value_length, slice_lengths, config, response.replica_list);

        // Track failures if needed
        if (response.error_code != ErrorCode::OK) {
            MasterMetricManager::instance().inc_put_start_failures();
        } else {
            // Increment key count on successful put start
            MasterMetricManager::instance().inc_key_count();
        }

        timer.LogResponseJson(response);
        return response;
    }

    PutEndResponse PutEnd(const std::string& key) {
        ScopedVLogTimer timer(1, "PutEnd");
        timer.LogRequest("key=", key);

        // Increment request metric
        MasterMetricManager::instance().inc_put_end_requests();

        PutEndResponse response;
        response.error_code = master_service_.PutEnd(key);

        // Track failures if needed
        if (response.error_code != ErrorCode::OK) {
            MasterMetricManager::instance().inc_put_end_failures();
        }

        timer.LogResponseJson(response);
        return response;
    }

    PutRevokeResponse PutRevoke(const std::string& key) {
        ScopedVLogTimer timer(1, "PutRevoke");
        timer.LogRequest("key=", key);

        // Increment request metric
        MasterMetricManager::instance().inc_put_revoke_requests();

        PutRevokeResponse response;
        response.error_code = master_service_.PutRevoke(key);

        // Track failures if needed
        if (response.error_code != ErrorCode::OK) {
            MasterMetricManager::instance().inc_put_revoke_failures();
        } else {
            // Decrement key count on successful revoke
            MasterMetricManager::instance().dec_key_count();
        }

        timer.LogResponseJson(response);
        return response;
    }

    BatchPutStartResponse BatchPutStart(
        const std::vector<std::string>& keys,
        const std::unordered_map<std::string, uint64_t>& value_lengths,
        const std::unordered_map<std::string, std::vector<uint64_t>>&
            slice_lengths,
        const ReplicateConfig& config) {
        ScopedVLogTimer timer(1, "BatchPutStart");
        timer.LogRequest("xrrkeys_count=", keys.size());

        BatchPutStartResponse response;
        response.error_code =
            master_service_.BatchPutStart(keys, value_lengths, slice_lengths,
                                          config, response.batch_replica_list);

        // Track failures if needed
        if (response.error_code == ErrorCode::OK) {
            MasterMetricManager::instance().inc_key_count(keys.size());
        }
        timer.LogResponseJson(response);
        return response;
    }

    BatchPutEndResponse BatchPutEnd(const std::vector<std::string>& keys) {
        ScopedVLogTimer timer(1, "BatchPutEnd");
        timer.LogRequest("keys_count=", keys.size());

        BatchPutEndResponse response;
        response.error_code = master_service_.BatchPutEnd(keys);
        timer.LogResponseJson(response);
        return response;
    }

    BatchPutRevokeResponse BatchPutRevoke(
        const std::vector<std::string>& keys) {
        ScopedVLogTimer timer(1, "BatchPutRevoke");
        timer.LogRequest("keys_count=", keys.size());

        BatchPutRevokeResponse response;
        response.error_code = master_service_.BatchPutRevoke(keys);
        // Track failures if needed
        if (response.error_code == ErrorCode::OK) {
            MasterMetricManager::instance().dec_key_count(keys.size());
        }
        timer.LogResponseJson(response);
        return response;
    }

    RemoveResponse Remove(const std::string& key) {
        ScopedVLogTimer timer(1, "Remove");
        timer.LogRequest("key=", key);

        // Increment request metric
        MasterMetricManager::instance().inc_remove_requests();

        RemoveResponse response;
        response.error_code = master_service_.Remove(key);

        // Track failures if needed
        if (response.error_code != ErrorCode::OK) {
            MasterMetricManager::instance().inc_remove_failures();
        } else {
            // Decrement key count on successful remove
            MasterMetricManager::instance().dec_key_count();
        }

        timer.LogResponseJson(response);
        return response;
    }

    RemoveAllResponse RemoveAll() {
        ScopedVLogTimer timer(1, "RemoveAll");
        timer.LogRequest("action=remove_all_objects");

        // Increment request metric
        MasterMetricManager::instance().inc_remove_all_requests();

        RemoveAllResponse response;
        const long removed_count = master_service_.RemoveAll();

        assert(removed_count >= 0);
        response.removed_count = removed_count;
        timer.LogResponseJson(response);
        return response;
    }

    MountSegmentResponse MountSegment(const Segment& segment,
                                      const UUID& client_id) {
        ScopedVLogTimer timer(1, "MountSegment");
        timer.LogRequest("base=", segment.base, ", size=", segment.size,
                         ", segment_name=", segment.name, ", id=", segment.id);

        // Increment request metric
        MasterMetricManager::instance().inc_mount_segment_requests();

        MountSegmentResponse response;
        response.error_code = master_service_.MountSegment(segment, client_id);

        // Track failures if needed
        if (response.error_code != ErrorCode::OK) {
            MasterMetricManager::instance().inc_mount_segment_failures();
        }

        timer.LogResponseJson(response);
        return response;
    }

    ReMountSegmentResponse ReMountSegment(const std::vector<Segment>& segments,
                                          const UUID& client_id) {
        ScopedVLogTimer timer(1, "ReMountSegment");
        timer.LogRequest("segments_count=", segments.size(),
                         ", client_id=", client_id);

        // Increment request metric
        MasterMetricManager::instance().inc_remount_segment_requests();

        ReMountSegmentResponse response;
        response.error_code =
            master_service_.ReMountSegment(segments, client_id);

        // Track failures if needed
        if (response.error_code != ErrorCode::OK) {
            MasterMetricManager::instance().inc_remount_segment_failures();
        }

        timer.LogResponseJson(response);
        return response;
    }

    UnmountSegmentResponse UnmountSegment(const UUID& segment_id,
                                          const UUID& client_id) {
        ScopedVLogTimer timer(1, "UnmountSegment");
        timer.LogRequest("segment_id=", segment_id);

        // Increment request metric
        MasterMetricManager::instance().inc_unmount_segment_requests();

        UnmountSegmentResponse response;
        response.error_code =
            master_service_.UnmountSegment(segment_id, client_id);

        // Track failures if needed
        if (response.error_code != ErrorCode::OK) {
            MasterMetricManager::instance().inc_unmount_segment_failures();
        }

        timer.LogResponseJson(response);
        return response;
    }

    PingResponse Ping(const UUID& client_id) {
        ScopedVLogTimer timer(1, "Ping");
        timer.LogRequest("client_id=", client_id);

        MasterMetricManager::instance().inc_ping_requests();

        PingResponse response;
        response.error_code = master_service_.Ping(
            client_id, response.view_version, response.client_status);

        if (response.error_code != ErrorCode::OK) {
            MasterMetricManager::instance().inc_ping_failures();
        }

        timer.LogResponseJson(response);
        return response;
    }

   private:
    MasterService master_service_;
    std::thread metric_report_thread_;
    coro_http::coro_http_server http_server_;
    std::atomic<bool> metric_report_running_;
    ViewVersionId view_version_;
};

inline void RegisterRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedMasterService& wrapped_master_service) {
    server.register_handler<&mooncake::WrappedMasterService::ExistKey>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::GetReplicaList>(
        &wrapped_master_service);
    server
        .register_handler<&mooncake::WrappedMasterService::BatchGetReplicaList>(
            &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::PutStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::PutEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::PutRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchPutStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchPutEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchPutRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::Remove>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::RemoveAll>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::MountSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::UnmountSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::Ping>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchExistKey>(
        &wrapped_master_service);
}

}  // namespace mooncake