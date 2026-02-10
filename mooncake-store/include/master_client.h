#pragma once

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <glog/logging.h>

#include <memory>
#include <string>
#include <vector>
#include <cstdlib>
#include <boost/functional/hash.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_io/client_pool.hpp>

#include "client_metric.h"
#include "types.h"
#include "rpc_types.h"

namespace mooncake {

template <auto Method>
struct RpcNameTraits;

static const std::string kDefaultMasterAddress = "localhost:50051";

/**
 * @brief Client for interacting with the mooncake master service
 */
class MasterClient {
   public:
    virtual ~MasterClient() = default;

    MasterClient(const MasterClient&) = delete;
    MasterClient& operator=(const MasterClient&) = delete;

    /**
     * @brief Connects to the master service
     * @param master_addr Master service address (IP:Port)
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] ErrorCode Connect(
        const std::string& master_addr = kDefaultMasterAddress);

    /**
     * @brief Checks if an object exists
     * @param object_key Key to query
     * @return tl::expected<bool, ErrorCode> indicating exist or not
     */
    [[nodiscard]] tl::expected<bool, ErrorCode> ExistKey(
        const std::string& object_key);

    /**
     * @brief Checks if multiple objects exist
     * @param object_keys Vector of keys to query
     * @return Vector containing existence status for each key
     */
    [[nodiscard]] std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& object_keys);
    /**
     * @brief Gets replica list for an object
     * @param object_key Key to query
     * @param config Filter configuration for getting replica list
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] tl::expected<GetReplicaListResponse, ErrorCode>
    GetReplicaList(const std::string& key,
                   const GetReplicaListRequestConfig& config =
                       GetReplicaListRequestConfig());

    /**
     * @brief Batch query read routes
     */
    [[nodiscard]] std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& keys,
                        const GetReplicaListRequestConfig& config =
                            GetReplicaListRequestConfig());

    /**
     * @brief Calculate cache hit rate metrics
     * @param object_keys None
     * @return Map containing metrics
     */
    [[nodiscard]] tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats();

    /**
     * @brief Batch query IP addresses for multiple client IDs.
     * @param client_ids Vector of client UUIDs to query.
     * @return An expected object containing a map from client_id to their IP
     * address lists on success, or an ErrorCode on failure.
     */
    [[nodiscard]] tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    /**
     * @brief Retrieves replica lists for object keys that match a regex
     * pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing a map from object keys to their
     * replica descriptors on success, or an ErrorCode on failure.
     */
    [[nodiscard]] tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str);

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> Remove(const std::string& key);

    /**
     * @brief Removes objects from the master whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    [[nodiscard]] tl::expected<long, ErrorCode> RemoveByRegex(
        const std::string& str);

    /**
     * @brief Removes all objects and all its replicas
     * @return tl::expected<long, ErrorCode> number of removed objects or error
     */
    [[nodiscard]] tl::expected<long, ErrorCode> RemoveAll();

    /**
     * @brief Unregisters a memory segment from master
     * @param segment_id ID of the segment to unmount
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> UnmountSegment(
        const UUID& segment_id);

    /**
     * @brief Sends heartbeat to master to maintain client liveness
     * @return tl::expected<HeartbeatResponse, ErrorCode>
     * containing view version and client status
     */
    [[nodiscard]] tl::expected<HeartbeatResponse, ErrorCode> Heartbeat(
        const HeartbeatRequest& req);

    /**
     * @brief Registers a segment to master for allocation
     * @param segment Segment to register
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MountSegment(
        const Segment& segment);

    /**
     * @brief Register client with the master on startup.
     * Client calls this to register its UUID and local segments.
     * @param segments Segments to register
     * @return tl::expected<RegisterClientResponse, ErrorCode>
     */
    [[nodiscard]] tl::expected<RegisterClientResponse, ErrorCode>
    RegisterClient(const std::vector<Segment>& segments);

   protected:
    MasterClient(const UUID& client_id, MasterClientMetric* metrics = nullptr)
        : client_id_(client_id), metrics_(metrics) {
        coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config
            pool_conf{};
        const char* value = std::getenv("MC_RPC_PROTOCOL");
        if (value && std::string_view(value) == "rdma") {
            pool_conf.client_config.socket_config =
                coro_io::ib_socket_t::config_t{};
        }
        client_pools_ =
            std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
                pool_conf);
    }
    /**
     * @brief Generic RPC invocation helper for single-result operations
     * @tparam ServiceMethod Pointer to WrappedMasterService member function
     * @tparam ReturnType The expected return type of the RPC call
     * @tparam Args Parameter types for the RPC call
     * @param args Arguments to pass to the RPC call
     * @return The result of the RPC call
     */
    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] tl::expected<ReturnType, ErrorCode> invoke_rpc(
        Args&&... args) {
        auto pool = client_accessor_.GetClientPool();

        // Increment RPC counter
        if (metrics_) {
            metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
        }

        auto start_time = std::chrono::steady_clock::now();
        return async_simple::coro::syncAwait(
            [&]() -> async_simple::coro::Lazy<
                      tl::expected<ReturnType, ErrorCode>> {
                auto ret = co_await pool->send_request(
                    [&](coro_io::client_reuse_hint,
                        coro_rpc::coro_rpc_client& client) {
                        return client.send_request<ServiceMethod>(
                            std::forward<Args>(args)...);
                    });
                if (!ret.has_value()) {
                    LOG(ERROR) << "Client not available";
                    co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
                }
                auto result = co_await std::move(ret.value());
                if (!result) {
                    LOG(ERROR) << "RPC call failed: " << result.error().msg;
                    co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
                }
                if (metrics_) {
                    auto end_time = std::chrono::steady_clock::now();
                    auto latency =
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            end_time - start_time);
                    metrics_->rpc_latency.observe(
                        {RpcNameTraits<ServiceMethod>::value}, latency.count());
                }
                co_return result->result();
            }());
    }

    /**
     * @brief Generic RPC invocation helper for batch operations
     * @tparam ServiceMethod Pointer to WrappedMasterService member function
     * @tparam ResultType The expected return type of the RPC call
     * @tparam Args Parameter types for the RPC call
     * @param input_size Size of input batch for error handling
     * @param args Arguments to pass to the RPC call
     * @return Vector of results from the batch RPC call
     */
    template <auto ServiceMethod, typename ResultType, typename... Args>
    [[nodiscard]] std::vector<tl::expected<ResultType, ErrorCode>>
    invoke_batch_rpc(size_t input_size, Args&&... args) {
        auto pool = client_accessor_.GetClientPool();

        // Increment RPC counter
        if (metrics_) {
            metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
        }

        auto start_time = std::chrono::steady_clock::now();
        return async_simple::coro::syncAwait(
            [&]() -> async_simple::coro::Lazy<
                      std::vector<tl::expected<ResultType, ErrorCode>>> {
                auto ret = co_await pool->send_request(
                    [&](coro_io::client_reuse_hint,
                        coro_rpc::coro_rpc_client& client) {
                        return client.send_request<ServiceMethod>(
                            std::forward<Args>(args)...);
                    });
                if (!ret.has_value()) {
                    LOG(ERROR) << "Client not available";
                    co_return std::vector<tl::expected<ResultType, ErrorCode>>(
                        input_size, tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                auto result = co_await std::move(ret.value());
                if (!result) {
                    LOG(ERROR)
                        << "Batch RPC call failed: " << result.error().msg;
                    std::vector<tl::expected<ResultType, ErrorCode>>
                        error_results;
                    error_results.reserve(input_size);
                    for (size_t i = 0; i < input_size; ++i) {
                        error_results.emplace_back(
                            tl::make_unexpected(ErrorCode::RPC_FAIL));
                    }
                    co_return error_results;
                }
                if (metrics_) {
                    auto end_time = std::chrono::steady_clock::now();
                    auto latency =
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            end_time - start_time);
                    metrics_->rpc_latency.observe(
                        {RpcNameTraits<ServiceMethod>::value}, latency.count());
                }
                co_return result->result();
            }());
    }

    /**
     * @brief Accessor for the coro_rpc_client pool. Since coro_rpc_client pool
     * cannot reconnect to a different address, a new coro_rpc_client pool is
     * created if the address is different from the current one.
     */
   protected:
    class RpcClientAccessor {
       public:
        void SetClientPool(
            std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
                client_pool) {
            std::lock_guard<std::shared_mutex> lock(client_mutex_);
            client_pool_ = client_pool;
        }

        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
        GetClientPool() {
            std::shared_lock<std::shared_mutex> lock(client_mutex_);
            return client_pool_;
        }

       private:
        mutable std::shared_mutex client_mutex_;
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
            client_pool_;
    };

   protected:
    RpcClientAccessor client_accessor_;

    // The client identification.
    const UUID client_id_;

    // Metrics for tracking RPC operations
    MasterClientMetric* metrics_;
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;

    // Mutex to insure the Connect function is atomic.
    mutable Mutex connect_mutex_;
    // The address which is passed to the coro_rpc_client
    std::string client_addr_param_ GUARDED_BY(connect_mutex_);
};

}  // namespace mooncake
