#ifndef MOONCAKE_HA_HELPER_H_
#define MOONCAKE_HA_HELPER_H_

#include <csignal>
#include <glog/logging.h>

#include <memory>
#include <string>
#include <thread>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "types.h"
#include "master_config.h"

namespace mooncake {

/*
 * @brief A helper class for maintain and monitor the master view change.
 *        The cluster is assumed to have multiple master servers, but only
 *        one master can be elected as leader to serve client requests.
 *        Each master view is associated with a unique version id, which
 *        is incremented monotonically each time the master view is changed.
 *
 *        This base class implements the etcd-based election. Redis-based
 *        election is provided by the RedisMasterViewHelper subclass.
 */
class MasterViewHelper {
   public:
    MasterViewHelper(const MasterViewHelper&) = delete;
    MasterViewHelper& operator=(const MasterViewHelper&) = delete;
    MasterViewHelper();
    virtual ~MasterViewHelper();

    /*
     * @brief Connect to the etcd cluster. This function should be called first
     *        when using the etcd election backend.
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
    virtual void ElectLeader(const std::string& master_address,
                             ViewVersionId& version, EtcdLeaseId& lease_id);

    /*
     * @brief Keep the master to be the leader. This function blocks until the
     * master is no longer the leader.
     * @param lease_id: The lease id of the leader.
     */
    virtual void KeepLeader(EtcdLeaseId lease_id);

    /*
     * @brief Cancel the keep-alive loop (for graceful shutdown).
     * @param lease_id: The lease id of the leader.
     */
    virtual void CancelKeepAlive(EtcdLeaseId lease_id);

    /*
     * @brief Get the current master view.
     * @param master: Output param, the ip:port address of the master.
     * @param version: Output param, the version of the master view.
     * @return: Error code.
     */
    virtual ErrorCode GetMasterView(std::string& master_address,
                                    ViewVersionId& version);

   private:
    std::string master_view_key_;
};

/*
 * @brief Create the appropriate MasterViewHelper based on election backend
 *        configuration. Returns an etcd-based helper by default, or a
 *        Redis-based helper when election_backend is REDIS.
 * @param config: The supervisor configuration.
 * @return: A unique_ptr to the created helper, or nullptr on error.
 */
std::unique_ptr<MasterViewHelper> CreateMasterViewHelper(
    const MasterServiceSupervisorConfig& config);

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
