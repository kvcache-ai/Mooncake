#pragma once

#include <csignal>
#include <atomic>
#include <boost/functional/hash.hpp>
#include <cstdint>
#include <thread>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_rpc/coro_rpc_context.hpp>
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

    uint16_t GetHttpPort() const { return http_server_.port(); }

    tl::expected<bool, ErrorCode> ExistKeyInternal(std::string_view key);

    // Bypass (out-of-band attachment) RPC handler for single-key exist. Log the
    // per-request request_id read from the coro_rpc attachment (set client-side
    // via invoke_rpc), delegate to the value-returning ExistKeyInternal (shared
    // with in-process tests), and reply via ctx.response_msg.
    void ExistKey(coro_rpc::context<tl::expected<bool, ErrorCode>> ctx,
                  std::string_view key);

    tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats();

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKeyInternal(
        const std::vector<std::string_view>& keys);

    // Bypass (out-of-band attachment) RPC handler for the batch-exist route.
    // Mirrors GetReplicaList/BatchGetReplicaList: log the per-request
    // request_id read from the coro_rpc attachment, delegate to the
    // value-returning BatchExistKeyInternal (also used in-process by tests),
    // and reply via ctx.response_msg. The client invokes this context-handler
    // entry; the value body stays untouched for in-process callers.
    void BatchExistKey(
        coro_rpc::context<std::vector<tl::expected<bool, ErrorCode>>> ctx,
        const std::vector<std::string_view>& keys);

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

    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaListInternal(
        std::string_view key, const GetReplicaListRequestConfig& config =
                                  GetReplicaListRequestConfig());

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaListInternal(const std::vector<std::string_view>& keys,
                                const GetReplicaListRequestConfig& config =
                                    GetReplicaListRequestConfig());

    // Bypass (out-of-band attachment) RPC handlers for the read route. They
    // delegate to the value-returning GetReplicaListInternal /
    // BatchGetReplicaListInternal (also used in-process by tests and HTTP
    // /batch_query_keys), so the read, logging and metrics logic lives in one
    // place. These handlers only: log the per-request request_id (read from the
    // coro_rpc out-of-band attachment set client-side), invoke the shared body,
    // and reply via ctx.response_msg. The read request struct carries no
    // request_id field.
    void GetReplicaList(
        coro_rpc::context<tl::expected<GetReplicaListResponse, ErrorCode>> ctx,
        std::string_view key,
        const GetReplicaListRequestConfig& config =
            GetReplicaListRequestConfig());

    void BatchGetReplicaList(
        coro_rpc::context<
            std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>>
            ctx,
        const std::vector<std::string_view>& keys,
        const GetReplicaListRequestConfig& config =
            GetReplicaListRequestConfig());

    tl::expected<void, ErrorCode> RemoveInternal(std::string_view key,
                                                 bool force = false);

    // Bypass (out-of-band attachment) RPC handler for single-key remove. Log
    // the per-request request_id read from the coro_rpc attachment, delegate to
    // the value-returning RemoveInternal (shared with in-process tests), and
    // reply via ctx.response_msg.
    void Remove(coro_rpc::context<tl::expected<void, ErrorCode>> ctx,
                std::string_view key, bool force = false);

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
