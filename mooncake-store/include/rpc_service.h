#pragma once

#include <csignal>
#include <atomic>
#include <boost/functional/hash.hpp>
#include <cstdint>
#include <thread>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_service.h"
#include "types.h"
#include "rpc_types.h"
#include "master_config.h"

namespace mooncake {

extern const uint64_t kMetricReportIntervalSeconds;

class WrappedMasterService {
   public:
    WrappedMasterService(const WrappedMasterServiceConfig& config);

    ~WrappedMasterService();

    void init_http_server();

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

    tl::expected<void, ErrorCode> Remove(const std::string& key,
                                         bool force = false);

    tl::expected<long, ErrorCode> RemoveByRegex(const std::string& str,
                                                bool force = false);

    long RemoveAll(bool force = false);

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
    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::vector<std::string>& targets);

    tl::expected<UUID, ErrorCode> CreateMoveTask(const std::string& key,
                                                 const std::string& source,
                                                 const std::string& target);

    tl::expected<QueryTaskResponse, ErrorCode> QueryTask(const UUID& task_id);

    tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        const UUID& client_id, size_t batch_size);

    tl::expected<void, ErrorCode> MarkTaskToComplete(
        const UUID& client_id, const TaskCompleteRequest& request);

    tl::expected<CopyStartResponse, ErrorCode> CopyStart(
        const UUID& client_id, const std::string& key,
        const std::string& src_segment,
        const std::vector<std::string>& tgt_segments);

    tl::expected<void, ErrorCode> CopyEnd(const UUID& client_id,
                                          const std::string& key);

    tl::expected<void, ErrorCode> CopyRevoke(const UUID& client_id,
                                             const std::string& key);

    tl::expected<Replica::Descriptor, ErrorCode> CacheLocalStart(
        const UUID& client_id, const std::string& key,
        const std::string& tgt_segment);

    tl::expected<void, ErrorCode> CacheLocalEnd(const UUID& client_id,
                                                const std::string& key,
                                                ReplicaID replica_id);

    tl::expected<void, ErrorCode> CacheLocalRevoke(const UUID& client_id,
                                                   const std::string& key,
                                                   ReplicaID replica_id);

    tl::expected<MoveStartResponse, ErrorCode> MoveStart(
        const UUID& client_id, const std::string& key,
        const std::string& src_segment, const std::string& tgt_segment);

    tl::expected<void, ErrorCode> MoveEnd(const UUID& client_id,
                                          const std::string& key);

    tl::expected<void, ErrorCode> MoveRevoke(const UUID& client_id,
                                             const std::string& key);

   private:
    MasterService master_service_;
    std::thread metric_report_thread_;
    coro_http::coro_http_server http_server_;
    std::atomic<bool> metric_report_running_;
};

void RegisterRpcService(coro_rpc::coro_rpc_server& server,
                        mooncake::WrappedMasterService& wrapped_master_service);

}  // namespace mooncake
