#ifndef MOONCAKE_HA_HELPER_H_
#define MOONCAKE_HA_HELPER_H_

#include <glog/logging.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "types.h"
#include "config_helper.h"
#include "rpc_service.h"

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

class MasterServiceSupervisorConfig {
   public:
    // no default values (required parameters) - using RequiredParam
    RequiredParam<bool> enable_gc;
    RequiredParam<bool> enable_metric_reporting;
    RequiredParam<int> metrics_port;
    RequiredParam<int64_t> default_kv_lease_ttl;
    RequiredParam<int64_t> default_kv_soft_pin_ttl;
    RequiredParam<bool> allow_evict_soft_pinned_objects;
    RequiredParam<double> eviction_ratio;
    RequiredParam<double> eviction_high_watermark_ratio;
    RequiredParam<int64_t> client_live_ttl_sec;
    RequiredParam<int> rpc_port;
    RequiredParam<size_t> rpc_thread_num;

    // Parameters with default values (optional parameters)
    std::string rpc_address = "0.0.0.0";
    std::chrono::steady_clock::duration rpc_conn_timeout = std::chrono::seconds(
        0);  // Client connection timeout. 0 = no timeout (infinite)
    bool rpc_enable_tcp_no_delay = true;
    std::string etcd_endpoints = "0.0.0.0:2379";
    std::string local_hostname = "0.0.0.0:50051";
    std::string cluster_id = DEFAULT_CLUSTER_ID;
    std::string root_fs_dir = DEFAULT_ROOT_FS_DIR;
    BufferAllocatorType memory_allocator = BufferAllocatorType::OFFSET;

    WrappedMasterServiceConfig createWrappedMasterServiceConfig(
        ViewVersionId view_version) const;
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
    MasterServiceSupervisor(const MasterServiceSupervisorConfig& config);
    int Start();
    ~MasterServiceSupervisor();

   private:
    // coro_rpc server thread
    std::thread server_thread_;

    MasterServiceSupervisorConfig config_;
};

}  // namespace mooncake

#endif  // MOONCAKE_HA_HELPER_H_