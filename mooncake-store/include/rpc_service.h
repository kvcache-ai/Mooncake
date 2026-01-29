#pragma once

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

static const uint64_t kMetricReportIntervalSeconds = 10;

class WrappedMasterService {
   public:
    WrappedMasterService(const WrappedMasterServiceConfig& config);

    virtual ~WrappedMasterService();

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

    tl::expected<void, ErrorCode> Remove(const std::string& key);

    tl::expected<long, ErrorCode> RemoveByRegex(const std::string& str);

    long RemoveAll();

    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id,
                                                 const UUID& client_id);

    tl::expected<PingResponse, ErrorCode> Ping(const UUID& client_id);

    tl::expected<std::string, ErrorCode> ServiceReady();

   protected:
    virtual MasterService& GetMasterService() = 0;

   protected:
    std::thread metric_report_thread_;
    coro_http::coro_http_server http_server_;
    std::atomic<bool> metric_report_running_;
};

void RegisterRpcService(coro_rpc::coro_rpc_server& server,
                        mooncake::WrappedMasterService& wrapped_master_service);

}  // namespace mooncake
