#ifndef MOONCAKE_HA_HELPER_H_
#define MOONCAKE_HA_HELPER_H_

#include <glog/logging.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "etcd_helper.h"
#include "rpc_service.h"
#include "config.h"
#include "types.h"

namespace mooncake {

// The key to store the master view in etcd
inline const char* const MASTER_VIEW_KEY = "mooncake-store/master_view";

/*
 * @brief A helper class for maintain and monitor the master view change.
 *        The cluster is assumed to have multiple master servers, but only
 *        one master can be elected as leader to serve client requests.
 *        Each master view is associated with a unique version id, which
 *        is incremented monotonically each time the master view is changed.
 */
class MasterViewHelper {
   public:
    MasterViewHelper(const MasterViewHelper&) = delete;
    MasterViewHelper& operator=(const MasterViewHelper&) = delete;
    MasterViewHelper() = default;

    /*
     * @brief Connect to the etcd cluster. This function should be called at
     * first
     * @param etcd_endpoints: The endpoints of the etcd store client.
     *        Multiple endpoints are separated by semicolons.
     * @return: Error code.
     */
    ErrorCode ConnectToEtcd(const std::string& etcd_endpoints);

    /*
     * @brief Elect the master to be the leader. This is a blocking function.
     * @param master_address: The ip:port address of the master to be elected.
     * @param version: Output param, the version of the new master view.
     * @param lease_id: Output param, the lease id of the leader.
     */
    void ElectLeader(const std::string& master_address, ViewVersionId& version,
                     EtcdLeaseId& lease_id);

    /*
     * @brief Keep the master to be the leader. This function blocks until the
     * master is no longer the leader.
     * @param lease_id: The lease id of the leader.
     */
    void KeepLeader(EtcdLeaseId lease_id);

    /*
     * @brief Get the current master view.
     * @param master: Output param, the ip:port address of the master.
     * @param version: Output param, the version of the master view.
     * @return: Error code.
     */
    ErrorCode GetMasterView(std::string& master_address,
                            ViewVersionId& version);
};

/*
 * @brief A supervisor class for the master service, only used in HA mode.
 *        This class will continuously do the following procedures after start:
 *        1. Elect local master to be the leader.
 *        2. Start the master service when it is elected as leader.
 *        3. Stop the master service when it is no longer the leader.
 */
class MasterServiceSupervisor {
   public:
    MasterServiceSupervisor(
        int rpc_port, size_t rpc_thread_num, bool enable_gc,
        bool enable_metric_reporting, int metrics_port,
        int64_t default_kv_lease_ttl, int64_t default_kv_soft_pin_ttl,
        bool allow_evict_soft_pinned_objects,
        double eviction_ratio, double eviction_high_watermark_ratio,
        int64_t client_live_ttl_sec,
        const std::string& etcd_endpoints = "0.0.0.0:2379",
        const std::string& local_hostname = "0.0.0.0:50051",
        const std::string& rpc_address = "0.0.0.0",
        std::chrono::steady_clock::duration rpc_conn_timeout =
            std::chrono::seconds(
                0),  // Client connection timeout. 0 = no timeout (infinite)
        bool rpc_enable_tcp_no_delay = true,
        const std::string& cluster_id = DEFAULT_CLUSTER_ID,
        BufferAllocatorType memory_allocator = BufferAllocatorType::CACHELIB);

    MasterServiceSupervisor(const MasterConfig &master_config)
        : enable_gc_(master_config.enable_gc),
          enable_metric_reporting_(master_config.enable_metric_reporting),
          metrics_port_(master_config.metrics_port),
          default_kv_lease_ttl_(master_config.default_kv_lease_ttl),
          default_kv_soft_pin_ttl_(master_config.default_kv_soft_pin_ttl),
          allow_evict_soft_pinned_objects_(
              master_config.allow_evict_soft_pinned_objects),
          eviction_ratio_(master_config.eviction_ratio),
          eviction_high_watermark_ratio_(
              master_config.eviction_high_watermark_ratio),
          client_live_ttl_sec_(master_config.client_live_ttl_sec),
          rpc_port_(master_config.rpc_port),
          rpc_thread_num_(master_config.rpc_thread_num),
          rpc_address_(master_config.rpc_address),
          rpc_conn_timeout_(std::chrono::seconds(master_config.rpc_conn_timeout_seconds)),
          rpc_enable_tcp_no_delay_(master_config.rpc_enable_tcp_no_delay),
          etcd_endpoints_(master_config.etcd_endpoints),
          local_hostname_(master_config.rpc_address + ":" +
                          std::to_string(master_config.rpc_port)),
          cluster_id_(master_config.cluster_id) {
            if (master_config.memory_allocator == "cachelib") {
                memory_allocator_ = BufferAllocatorType::CACHELIB;
            } else {
                memory_allocator_ = BufferAllocatorType::OFFSET;
            }
          };
    int Start();
    ~MasterServiceSupervisor();

   private:
    // Master service parameters
    bool enable_gc_;
    bool enable_metric_reporting_;
    int metrics_port_;
    int64_t default_kv_lease_ttl_;
    int64_t default_kv_soft_pin_ttl_;
    bool allow_evict_soft_pinned_objects_;
    double eviction_ratio_;
    double eviction_high_watermark_ratio_;
    int64_t client_live_ttl_sec_;

    // RPC server configuration parameters
    const int rpc_port_;
    const size_t rpc_thread_num_;
    const std::string rpc_address_;
    const std::chrono::steady_clock::duration rpc_conn_timeout_;
    const bool rpc_enable_tcp_no_delay_;

    // coro_rpc server thread
    std::thread server_thread_;

    // ETCD parameters
    std::string etcd_endpoints_;

    // Local hostname for leader election
    std::string local_hostname_;

    std::string cluster_id_;
    BufferAllocatorType memory_allocator_;
};

}  // namespace mooncake

#endif  // MOONCAKE_HA_HELPER_H_