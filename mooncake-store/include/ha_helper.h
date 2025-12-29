#ifndef MOONCAKE_HA_HELPER_H_
#define MOONCAKE_HA_HELPER_H_

#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "hot_standby_service.h"
#include "master_config.h"
#include "types.h"

namespace mooncake {

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
    MasterViewHelper();

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

   private:
    std::string master_view_key_;
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
    /**
     * @brief Start HotStandbyService when there is an existing leader
     * @param mv_helper MasterViewHelper instance
     * @param current_leader Current leader address
     */
    void StartStandbyService(MasterViewHelper& mv_helper,
                             const std::string& current_leader);

    /**
     * @brief Stop HotStandbyService
     */
    void StopStandbyService();

    // coro_rpc server thread
    std::thread server_thread_;

    MasterServiceSupervisorConfig config_;

    // HotStandbyService for standby mode
    std::unique_ptr<HotStandbyService> standby_service_;
    std::atomic<bool> standby_running_{false};
};

}  // namespace mooncake

#endif  // MOONCAKE_HA_HELPER_H_