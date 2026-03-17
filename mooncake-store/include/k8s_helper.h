#pragma once

#include <glog/logging.h>

#include "types.h"

namespace mooncake {

/*
 * @brief A helper class for kubernetes leader election operations.
 *        This class is used to handle the leader election requests to the
 * kubernetes cluster. All methods of this class are thread-safe.
 */
class K8sHelper {
   public:
    /*
     * @brief Initialize the leader elector with lease name and identity.
     * @param lease_name: The name of the lease to use for leader election.
     * @param identity: The identity of this instance participating in leader
     * election.
     * @return: Error code.
     */
    ErrorCode InitLeaderElector(const std::string& lease_name,
                                const std::string& identity);

    /*
     * @brief Start the leader election process. This is a blocking function.
     * @return: Error code.
     */
    ErrorCode ElectLeader();

    /*
     * @brief Keep the leader role. This is a blocking function.
     * @return: Error code.
     */
    ErrorCode KeepAlive();

    /*
     * @brief Stop the leader elector.
     * @return: Error code.
     */
    ErrorCode StopLeaderElector();

    /*
     * @brief Get the current master address.
     * @param address: Output param, the address of the current master.
     * @return: Error code.
     */
    ErrorCode GetMasterAddress(const std::string& lease_namespace,
                               const std::string& lease_name,
                               std::string& address);

   private:
    std::mutex k8s_mutex_;
    bool k8s_initialized_;
};

}  // namespace mooncake