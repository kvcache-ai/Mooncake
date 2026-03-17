#include "k8s_helper.h"

#ifdef STORE_USE_K8S
#include "libk8s_wrapper.h"
#endif

#include <glog/logging.h>

namespace mooncake {

#ifdef STORE_USE_K8S
ErrorCode K8sHelper::InitLeaderElector(const std::string& lease_name,
                                       const std::string& identity) {
    std::lock_guard<std::mutex> lock(k8s_mutex_);
    if (k8s_initialized_) {
        LOG(INFO) << "K8s leader elector already initialized";
        return ErrorCode::OK;
    }
    char* err_msg = nullptr;
    long long ret = InitLeaderElectorGo((char*)lease_name.c_str(),
                                        (char*)identity.c_str(), &err_msg);
    if (ret != 0) {
        if (err_msg) {
            LOG(ERROR) << "Failed to initialize k8s leader elector: "
                   << err_msg;
            free(err_msg);
        }else {
            LOG(ERROR) << "Failed to initialize k8s leader elector with empty err_msg";
        }
        return ErrorCode::K8S_OPERATION_ERROR;
    }
    k8s_initialized_ = true;
    return ErrorCode::OK;
}

ErrorCode K8sHelper::ElectLeader() {
    char* err_msg = nullptr;
    long long ret = ElectLeaderGo(&err_msg);
    if (ret != 0) {
        if (err_msg) {
            LOG(ERROR) << "Failed to elect leader: " << err_msg;
            free(err_msg);
        }else {
            LOG(ERROR) << "Failed to elect leader with empty err_msg";
        }
        return ErrorCode::K8S_LEADER_ELECTION_FAILED;
    }
    return ErrorCode::OK;
}

ErrorCode K8sHelper::KeepAlive() {
    long long ret = KeepAliveGo();
    if (ret != 0) {
        LOG(ERROR) << "Failed to keep leader alive";
        return ErrorCode::K8S_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode K8sHelper::StopLeaderElector() {
    char* err_msg = nullptr;
    long long ret = StopLeaderElectorGo(&err_msg);
    if (ret != 0) {
        if (err_msg) {
            LOG(ERROR) << "Failed to stop leader elector: " << err_msg;
            free(err_msg);
        }else {
            LOG(ERROR) << "Failed to stop leader elector with empty err_msg";
        }
        return ErrorCode::K8S_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode K8sHelper::GetMasterAddress(const std::string& lease_namespace,
                                      const std::string& lease_name,
                                      std::string& address) {
    char* err_msg = nullptr;
    char* addr_ptr = nullptr;
    int addr_size = 0;
    long long ret = GetMasterAddressGo((char*)lease_namespace.c_str(),
                                       (char*)lease_name.c_str(), &addr_ptr,
                                       &addr_size, &err_msg);
    if (ret != 0 || addr_ptr == nullptr) {
        if (err_msg) {
            LOG(ERROR) << "Failed to get master address: " << err_msg;
            free(err_msg);
        }else {
            LOG(ERROR) << "Failed to get master address with empty err_msg";
        }
        return ErrorCode::K8S_OPERATION_ERROR;
    }
    address = std::string(addr_ptr, addr_size);
    return ErrorCode::OK;
}
#else
ErrorCode K8sHelper::InitLeaderElector(const std::string& lease_name,
                                       const std::string& identity) {
    LOG(FATAL) << "K8s is not enabled in compilation";
    return ErrorCode::INTERNAL_ERROR;
}

ErrorCode K8sHelper::ElectLeader() {
    LOG(FATAL) << "K8s is not enabled in compilation";
    return ErrorCode::INTERNAL_ERROR;
}

ErrorCode K8sHelper::KeepAlive() {
    LOG(FATAL) << "K8s is not enabled in compilation";
    return ErrorCode::INTERNAL_ERROR;
}

ErrorCode K8sHelper::StopLeaderElector() {
    LOG(FATAL) << "K8s is not enabled in compilation";
    return ErrorCode::INTERNAL_ERROR;
}

ErrorCode K8sHelper::GetMasterAddress(const std::string& lease_namespace,
                                      const std::string& lease_name,
                                      std::string& address) {
    LOG(FATAL) << "K8s is not enabled in compilation";
    return ErrorCode::INTERNAL_ERROR;
}

#endif

}  // namespace mooncake