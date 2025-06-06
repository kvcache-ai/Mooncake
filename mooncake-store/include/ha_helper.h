#ifndef MOONCAKE_HA_HELPER_H_
#define MOONCAKE_HA_HELPER_H_

#include <glog/logging.h>
#include <string>
#include <thread>
#include <chrono>
#include <cstdint>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "rpc_service.h"
#include "etcd_helper.h"
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
     * @brief Connect to the etcd cluster. This function should be called at first
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
    void ElectLeader(const std::string& master_address, ViewVersionId& version, EtcdLeaseId& lease_id);

    /*
     * @brief Keep the master to be the leader. This function blocks until the master is
     *        no longer the leader.
     * @param lease_id: The lease id of the leader.
     */
    void KeepLeader(EtcdLeaseId lease_id);

    /*
     * @brief Get the current master view.
     * @param master: Output param, the ip:port address of the master.
     * @param version: Output param, the version of the master view.
     * @return: Error code.
     */
    ErrorCode GetMasterView(std::string& master_address, ViewVersionId& version);
};

/*
 * @brief A supervisor class for the master service, only used in HA mode.
 *        This class will continuously do the following procedures:
 *        1. Elect local master to be the leader.
 *        2. Start the master service when it is elected as leader.
 *        3. Stop the master service when it is no longer the leader.
*/
class MasterServiceSupervisor {
public:
    MasterServiceSupervisor(int port, int server_thread_num, bool enable_gc,
                          bool enable_metric_reporting, int metrics_port,
                          int64_t default_kv_lease_ttl, double eviction_ratio,
                          double eviction_high_watermark_ratio,
                          const std::string& etcd_endpoints = "0.0.0.0:2379",
                          const std::string& local_hostname = "0.0.0.0:50051");
    int Start();
    ~MasterServiceSupervisor();

private:
    // Master service parameters
    int port_;
    int server_thread_num_;
    bool enable_gc_;
    bool enable_metric_reporting_;
    int metrics_port_;
    int64_t default_kv_lease_ttl_;
    double eviction_ratio_;
    double eviction_high_watermark_ratio_;

    // coro_rpc server thread
    std::thread server_thread_;

    // ETCD parameters
    std::string etcd_endpoints_;

    // Local hostname for leader election
    std::string local_hostname_;
};

}  // namespace mooncake

#endif  // MOONCAKE_HA_HELPER_H_