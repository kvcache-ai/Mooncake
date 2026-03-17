#ifndef MOONCAKE_K8S_HA_HELPER_H_
#define MOONCAKE_K8S_HA_HELPER_H_

#include <glog/logging.h>

#include <string>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "types.h"
#include "k8s_helper.h"
#include "ha_helper.h"

namespace mooncake {

/*
 * @brief A helper class for maintain and monitor the master view change using
 * k8s leader election. The cluster is assumed to have multiple master servers,
 * but only one master can be elected as leader to serve client requests.
 */
class K8sMasterViewHelper : public MasterViewHelper {
   public:
    K8sMasterViewHelper(const K8sMasterViewHelper&) = delete;
    K8sMasterViewHelper& operator=(const K8sMasterViewHelper&) = delete;
    K8sMasterViewHelper();
    K8sMasterViewHelper(const std::string& lease_namespace);

    /*
     * @brief Elect the master to be the leader. This is a blocking function.
     * @param master_address: The ip:port address of the master to be elected.
     * @param version: Output param, the version of the new master view.
     * @return: Error code.
     */
    ErrorCode ElectLeader(const std::string& master_address,
                          ViewVersionId& version) override;

    /*
     * @brief Keep the master to be the leader. This function blocks until the
     * master is no longer the leader.
     */
    void KeepLeader() override;

    /*
     * @brief Stop keeping leader.
     * @return: Error code.
     */
    ErrorCode CancelKeepLeader() override;

    /*
     * @brief Get the current master view.
     * @param master: Output param, the ip:port address of the master.
     * @param version: Output param, the version of the master view.
     * @return: Error code.
     */
    ErrorCode GetMasterView(std::string& master_address,
                            ViewVersionId& version) override;

   private:
    std::string lease_namespace_;
    std::string lease_name_;
    K8sHelper k8s_helper_;

    /*
     * @brief Initialize the lease name based on cluster ID.
     */
    void initLeaseName();
};

}  // namespace mooncake

#endif  // MOONCAKE_K8S_HA_HELPER_H_