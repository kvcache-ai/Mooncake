#include "k8s_ha_helper.h"
#include "k8s_helper.h"
#include "rpc_service.h"

namespace mooncake {

K8sMasterViewHelper::K8sMasterViewHelper() { initLeaseName(); }

K8sMasterViewHelper::K8sMasterViewHelper(const std::string& lease_namespace)
    : lease_namespace_(lease_namespace) {
    initLeaseName();
}

void K8sMasterViewHelper::initLeaseName() {
    std::string cluster_id;
    const char* cluster_id_env = std::getenv("MC_STORE_CLUSTER_ID");
    if (cluster_id_env != nullptr && strlen(cluster_id_env) > 0) {
        cluster_id = cluster_id_env;
    } else {
        cluster_id = "mooncake";
    }
    // Ensure the cluster_id ends with '-' if not empty
    if (!cluster_id.empty() && cluster_id.back() != '-') {
        cluster_id += '-';
    }
    lease_name_ = "mooncake-store-" + cluster_id + "master-lease";
    LOG(INFO) << "K8s lease name: " << lease_name_;
}

ErrorCode K8sMasterViewHelper::ElectLeader(const std::string& master_address,
                                           ViewVersionId& version) {
    LOG(INFO) << "Attempting to elect self as leader with address: "
              << master_address;

    auto err_code = k8s_helper_.InitLeaderElector(lease_name_, master_address);
    if (err_code != ErrorCode::OK) {
        LOG(ERROR) << "Failed to init k8s leader elector: " << err_code;
        return err_code;
    }

    err_code = k8s_helper_.ElectLeader();
    if (err_code != ErrorCode::OK) {
        LOG(ERROR) << "Failed to elect self as leader: " << err_code;
        return err_code;
    }

    version = 0; // Set dummy version since k8s doesn't use it
    return ErrorCode::OK;
}

void K8sMasterViewHelper::KeepLeader() {
    LOG(INFO) << "Starting to keep leader role";
    auto err_code = k8s_helper_.KeepAlive();
    if (err_code != ErrorCode::OK) {
        LOG(ERROR) << "Failed to keep leader: " << err_code;
    }
    LOG(INFO) << "Leader role no longer maintained";
}

ErrorCode K8sMasterViewHelper::CancelKeepLeader() {
    return k8s_helper_.StopLeaderElector();
}

ErrorCode K8sMasterViewHelper::GetMasterView(std::string& master_address,
                                             ViewVersionId& version) {
    // For k8s, we don't use version like etcd
    // Just call the existing method and set dummy version
    auto err_code = k8s_helper_.GetMasterAddress(lease_namespace_, lease_name_,
                                                 master_address);
    if (err_code != ErrorCode::OK) {
        LOG(ERROR) << "Failed to get master address due to k8s error: "
                   << err_code;
        return err_code;
    }
    LOG(INFO) << "Get master address: " << master_address;
    version = 0;  // Set dummy version since k8s doesn't use it
    return ErrorCode::OK;
}

}  // namespace mooncake