#ifndef MOONCAKE_HA_HELPER_H_
#define MOONCAKE_HA_HELPER_H_

#include <csignal>
#include <glog/logging.h>

#include <string>
#include <thread>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "types.h"
#include "master_config.h"

namespace mooncake {

/*
 * @brief An abstract interface class for maintain and monitor the master view
 * change. The cluster is assumed to have multiple master servers, but only one
 * master can be elected as leader to serve client requests. Each master view is
 * associated with a unique version id, which is incremented monotonically each
 * time the master view is changed.
 */
class MasterViewHelper {
   public:
    virtual ~MasterViewHelper() = default;

    MasterViewHelper(const MasterViewHelper&) = delete;
    MasterViewHelper& operator=(const MasterViewHelper&) = delete;

    /**
     * @brief Create and init a MasterViewHelper for master to elect and keep
     * leader.
     * @param coordinator_type can be "etcd" or "k8s"
     * @param coordinator_endpoints etcd endpoints for "etcd", empty for "k8s"
     */
    static std::unique_ptr<MasterViewHelper> CreateForMaster(
        const std::string& coordinator_type,
        const std::string& coordinator_endpoints);

    /**
     * @brief Create and init a MasterViewHelper for client to get master view.
     * @param coordinator_type can be "etcd" or "k8s"
     * @param coordinator_endpoints etcd endpoints for "etcd", lease namespace
     * for "k8s"
     */
    static std::unique_ptr<MasterViewHelper> CreateForClient(
        const std::string& coordinator_type,
        const std::string& coordinator_endpoints);

    /*
     * @brief Elect the master to be the leader. This is a blocking function.
     * @param master_address: The ip:port address of the master to be elected.
     * @param version: Output param, the version of the new master view.
     */
    virtual ErrorCode ElectLeader(const std::string& master_address,
                                  ViewVersionId& version) = 0;

    /*
     * @brief Keep the master to be the leader. This function blocks until the
     * master is no longer the leader.
     */
    virtual void KeepLeader() = 0;

    /**
     * @brief Stop keeping leader.
     */
    virtual ErrorCode CancelKeepLeader() = 0;

    /*
     * @brief Get the current master view.
     * @param master: Output param, the ip:port address of the master.
     * @param version: Output param, the version of the master view.
     * @return: Error code.
     */
    virtual ErrorCode GetMasterView(std::string& master_address,
                                    ViewVersionId& version) = 0;

   protected:
    MasterViewHelper() = default;

    ViewVersionId view_version_ = 0;
};

/*
 * @brief A helper class for maintain and monitor the master view change using
 * etcd. The cluster is assumed to have multiple master servers, but only one
 * master can be elected as leader to serve client requests. Each master view is
 * associated with a unique version id, which is incremented monotonically each
 * time the master view is changed.
 */
class EtcdMasterViewHelper : public MasterViewHelper {
   public:
    EtcdMasterViewHelper(const EtcdMasterViewHelper&) = delete;
    EtcdMasterViewHelper& operator=(const EtcdMasterViewHelper&) = delete;
    EtcdMasterViewHelper(const std::string& etcd_endpoints);

    /*
     * @brief Connect to the etcd cluster. This function should be called at
     * first
     * @return: Error code.
     */
    ErrorCode ConnectToCoordinator();

    /*
     * @brief Elect the master to be the leader. This is a blocking function.
     * @param master_address: The ip:port address of the master to be elected.
     * @param version: Output param, the version of the new master view.
     */
    ErrorCode ElectLeader(const std::string& master_address,
                          ViewVersionId& version) override;

    /*
     * @brief Keep the master to be the leader. This function blocks until the
     * master is no longer the leader.
     */
    void KeepLeader() override;

    /*
     * @brief Get the current master view.
     * @param master: Output param, the ip:port address of the master.
     * @param version: Output param, the version of the master view.
     * @return: Error code.
     */
    ErrorCode GetMasterView(std::string& master_address,
                            ViewVersionId& version) override;

    /*
     * @brief Stop keeping leader.
     * @return: Error code.
     */
    ErrorCode CancelKeepLeader() override;

   private:
    std::string etcd_endpoints_;
    std::string master_view_key_;
    EtcdLeaseId lease_id_;
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