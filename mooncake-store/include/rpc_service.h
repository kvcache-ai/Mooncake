#pragma once

#include <atomic>
#include <cstdint>
#include <thread>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_service.h"
#include "types.h"

namespace mooncake {

extern const uint64_t kMetricReportIntervalSeconds;

class WrappedMasterService {
   public:
    WrappedMasterService(
        bool enable_gc, uint64_t default_kv_lease_ttl,
        uint64_t default_kv_soft_pin_ttl = DEFAULT_KV_SOFT_PIN_TTL_MS,
        bool allow_evict_soft_pinned_objects =
            DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS,
        bool enable_metric_reporting = true, uint16_t http_port = 9003,
        double eviction_ratio = DEFAULT_EVICTION_RATIO,
        double eviction_high_watermark_ratio =
            DEFAULT_EVICTION_HIGH_WATERMARK_RATIO,
        ViewVersionId view_version = 0,
        int64_t client_live_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC,
        bool enable_ha = false,
        const std::string& cluster_id = DEFAULT_CLUSTER_ID,
        const std::string& root_fs_dir = DEFAULT_ROOT_FS_DIR,
        BufferAllocatorType memory_allocator = BufferAllocatorType::CACHELIB);

    ~WrappedMasterService();

    void init_http_server();

    tl::expected<bool, ErrorCode> ExistKey(const std::string& key);

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys);

    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str);

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> GetReplicaList(
        const std::string& key);

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& keys);

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> PutStart(
        const std::string& key, const std::vector<uint64_t>& slice_lengths,
        const ReplicateConfig& config);

    tl::expected<void, ErrorCode> PutEnd(const std::string& key,
                                         ReplicaType replica_type);

    tl::expected<void, ErrorCode> PutRevoke(const std::string& key,
                                            ReplicaType replica_type);

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchPutStart(const std::vector<std::string>& keys,
                  const std::vector<std::vector<uint64_t>>& slice_lengths,
                  const ReplicateConfig& config);

    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const std::vector<std::string>& keys);

    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const std::vector<std::string>& keys);

    tl::expected<void, ErrorCode> Remove(const std::string& key);

    tl::expected<long, ErrorCode> RemoveByRegex(const std::string& str);

    long RemoveAll();

    tl::expected<void, ErrorCode> MountSegment(const Segment& segment,
                                               const UUID& client_id);

    tl::expected<void, ErrorCode> ReMountSegment(
        const std::vector<Segment>& segments, const UUID& client_id);

    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id,
                                                 const UUID& client_id);

    tl::expected<std::string, ErrorCode> GetFsdir();

    tl::expected<PingResponse, ErrorCode> Ping(const UUID& client_id);

   private:
    MasterService master_service_;
    std::thread metric_report_thread_;
    coro_http::coro_http_server http_server_;
    std::atomic<bool> metric_report_running_;
};

void RegisterRpcService(coro_rpc::coro_rpc_server& server,
                        mooncake::WrappedMasterService& wrapped_master_service);

}  // namespace mooncake
