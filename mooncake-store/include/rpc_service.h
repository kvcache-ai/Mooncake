#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/coro_http/coro_http_server.hpp>
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

struct UnmountSegmentResponse {
    ErrorCode error_code = ErrorCode::OK;
};
YLT_REFL(UnmountSegmentResponse, error_code)

constexpr uint64_t kMetricReportIntervalSeconds = 10;

class WrappedMasterService {
   public:
    WrappedMasterService(bool enable_gc, uint64_t default_kv_lease_ttl,
                         bool enable_metric_reporting = true,
                         uint16_t http_port = 9003,
                         double eviction_ratio = DEFAULT_EVICTION_RATIO,
                         double eviction_low_watermark_ratio = DEFAULT_EVICTION_HIGH_WATERMARK_RATIO)
        : master_service_(enable_gc, default_kv_lease_ttl, eviction_ratio, eviction_low_watermark_ratio),
          http_server_(4, http_port),
          metric_report_running_(enable_metric_reporting) {
        // Initialize HTTP server for metrics
        init_http_server();

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

    MountSegmentResponse MountSegment(uint64_t buffer, uint64_t size,
                                      const std::string& segment_name) {
        ScopedVLogTimer timer(1, "MountSegment");
        timer.LogRequest("buffer=", buffer, ", size=", size,
                         ", segment_name=", segment_name);

        // Increment request metric
        MasterMetricManager::instance().inc_mount_segment_requests();

        MountSegmentResponse response;
        response.error_code =
            master_service_.MountSegment(buffer, size, segment_name);

        // Track failures if needed
        if (response.error_code != ErrorCode::OK) {
            MasterMetricManager::instance().inc_mount_segment_failures();
        } else {
            // Update total capacity on successful mount
            MasterMetricManager::instance().inc_total_capacity(size);
        }

        timer.LogResponseJson(response);
        return response;
    }

    UnmountSegmentResponse UnmountSegment(const std::string& segment_name) {
        ScopedVLogTimer timer(1, "UnmountSegment");
        timer.LogRequest("segment_name=", segment_name);

        // Increment request metric
        MasterMetricManager::instance().inc_unmount_segment_requests();

        UnmountSegmentResponse response;
        response.error_code = master_service_.UnmountSegment(segment_name);

        // Track failures if needed
        if (response.error_code != ErrorCode::OK) {
            MasterMetricManager::instance().inc_unmount_segment_failures();
        }

        timer.LogResponseJson(response);
        return response;
    }

   private:
    MasterService master_service_;
    std::thread metric_report_thread_;
    coro_http::coro_http_server http_server_;
    std::atomic<bool> metric_report_running_;
};

}  // namespace mooncake
