#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <cstdint>
#include <thread>
#include <utility>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_service.h"
#include "metadata_store.h"
#include "types.h"
#include "rpc_types.h"
#include "master_config.h"

// Forward declaration
namespace mooncake {
// ReplicationService forward declaration removed - using etcd-based OpLog sync instead
}

namespace mooncake {

extern const uint64_t kMetricReportIntervalSeconds;

class WrappedMasterService {
   public:
    WrappedMasterService(const WrappedMasterServiceConfig& config);

    ~WrappedMasterService();

    void init_http_server();

    // Restore metadata and OpLog sequence from a promoted Standby (fast failover).
    void RestoreFromStandby(
        const std::vector<std::pair<std::string, StandbyObjectMetadata>>& snapshot,
        uint64_t initial_oplog_sequence_id);

    tl::expected<bool, ErrorCode> ExistKey(const std::string& key);

    tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats();

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys);

    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    tl::expected<std::vector<std::string>, ErrorCode> BatchReplicaClear(
        const std::vector<std::string>& object_keys, const UUID& client_id,
        const std::string& segment_name);

    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str);

    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        const std::string& key);

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& keys);

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> PutStart(
        const UUID& client_id, const std::string& key,
        const uint64_t slice_length, const ReplicateConfig& config);

    tl::expected<void, ErrorCode> PutEnd(const UUID& client_id,
                                         const std::string& key,
                                         ReplicaType replica_type);

    tl::expected<void, ErrorCode> PutRevoke(const UUID& client_id,
                                            const std::string& key,
                                            ReplicaType replica_type);

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchPutStart(const UUID& client_id, const std::vector<std::string>& keys,
                  const std::vector<uint64_t>& slice_lengths,
                  const ReplicateConfig& config);

    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const UUID& client_id, const std::vector<std::string>& keys);

    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const UUID& client_id, const std::vector<std::string>& keys);

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

    tl::expected<GetStorageConfigResponse, ErrorCode> GetStorageConfig();

    tl::expected<PingResponse, ErrorCode> Ping(const UUID& client_id);

    tl::expected<std::string, ErrorCode> ServiceReady();

    tl::expected<void, ErrorCode> MountLocalDiskSegment(const UUID& client_id,
                                                        bool enable_offloading);

    tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>
    OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading);

    tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas);

    // GetReplicationService removed - using etcd-based OpLog sync instead

   private:
    MasterService master_service_;
    std::thread metric_report_thread_;
    coro_http::coro_http_server http_server_;
    std::atomic<bool> metric_report_running_;
    
    // ReplicationService removed - using etcd-based OpLog sync instead
};

void RegisterRpcService(coro_rpc::coro_rpc_server& server,
                        mooncake::WrappedMasterService& wrapped_master_service);

}  // namespace mooncake
