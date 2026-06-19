#include "k8s_lease_helper.h"

#ifdef STORE_USE_K8S_LEASE
#include "libk8s_lease_wrapper.h"
#endif

#include <glog/logging.h>

namespace mooncake {

std::mutex K8sLeaseHelper::init_mutex_;
bool K8sLeaseHelper::initialized_ = false;

#ifdef STORE_USE_K8S_LEASE

ErrorCode K8sLeaseHelper::Init() {
    std::lock_guard<std::mutex> lock(init_mutex_);
    if (initialized_) {
        return ErrorCode::OK;
    }
    char* err_msg = nullptr;
    int ret = K8sLeaseInit(&err_msg);
    if (ret != 0) {
        LOG(ERROR) << "Failed to initialize K8s client: " << err_msg;
        free(err_msg);
        return ErrorCode::K8S_LEASE_OPERATION_ERROR;
    }
    initialized_ = true;
    return ErrorCode::OK;
}

ErrorCode K8sLeaseHelper::RunElection(const std::string& ns,
                                      const std::string& lease,
                                      const std::string& identity,
                                      int lease_dur, int renew_deadline,
                                      int retry_period) {
    char* err_msg = nullptr;
    int ret = K8sLeaseRunElection(
        const_cast<char*>(ns.c_str()), const_cast<char*>(lease.c_str()),
        const_cast<char*>(identity.c_str()), lease_dur, renew_deadline,
        retry_period, &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "RunElection failed: " << err_msg;
        free(err_msg);
        return ErrorCode::K8S_LEASE_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode K8sLeaseHelper::WaitElected(const std::string& ns,
                                      const std::string& lease, int timeout_sec,
                                      int64_t& lease_transitions) {
    char* err_msg = nullptr;
    long long transitions = 0;
    int ret = K8sLeaseWaitElected(const_cast<char*>(ns.c_str()),
                                  const_cast<char*>(lease.c_str()), timeout_sec,
                                  &transitions, &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "WaitElected failed: " << err_msg;
        free(err_msg);
        return ErrorCode::K8S_LEASE_OPERATION_ERROR;
    }
    lease_transitions = static_cast<int64_t>(transitions);
    return ErrorCode::OK;
}

ErrorCode K8sLeaseHelper::WaitLost(const std::string& ns,
                                   const std::string& lease) {
    char* err_msg = nullptr;
    int ret = K8sLeaseWaitLost(const_cast<char*>(ns.c_str()),
                               const_cast<char*>(lease.c_str()), &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "WaitLost failed: " << err_msg;
        free(err_msg);
        return ErrorCode::K8S_LEASE_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode K8sLeaseHelper::CancelElection(const std::string& ns,
                                         const std::string& lease) {
    char* err_msg = nullptr;
    int ret =
        K8sLeaseCancelElection(const_cast<char*>(ns.c_str()),
                               const_cast<char*>(lease.c_str()), &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "CancelElection failed: " << err_msg;
        free(err_msg);
        return ErrorCode::K8S_LEASE_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode K8sLeaseHelper::GetHolder(const std::string& ns,
                                    const std::string& lease,
                                    std::string& holder,
                                    int64_t& lease_transitions) {
    char* err_msg = nullptr;
    char* holder_ptr = nullptr;
    long long transitions = 0;
    int ret = K8sLeaseGetHolder(const_cast<char*>(ns.c_str()),
                                const_cast<char*>(lease.c_str()), &holder_ptr,
                                &transitions, &err_msg);
    if (ret == 1) {
        holder.clear();
        lease_transitions = 0;
        return ErrorCode::K8S_LEASE_NOT_FOUND;
    }
    if (ret != 0) {
        LOG(ERROR) << "GetHolder failed: " << err_msg;
        free(err_msg);
        return ErrorCode::K8S_LEASE_OPERATION_ERROR;
    }
    if (holder_ptr != nullptr) {
        holder = std::string(holder_ptr);
        free(holder_ptr);
    } else {
        holder.clear();
    }
    lease_transitions = static_cast<int64_t>(transitions);
    return ErrorCode::OK;
}

ErrorCode K8sLeaseHelper::WatchHolder(
    const std::string& ns, const std::string& lease, void* callback_context,
    void (*callback_func)(void*, const char*, size_t, int64_t)) {
    char* err_msg = nullptr;
    int ret = K8sLeaseWatchHolder(const_cast<char*>(ns.c_str()),
                                  const_cast<char*>(lease.c_str()),
                                  callback_context, callback_func, &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "WatchHolder failed: " << err_msg;
        free(err_msg);
        return ErrorCode::K8S_LEASE_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode K8sLeaseHelper::CancelWatch(const std::string& ns,
                                      const std::string& lease) {
    char* err_msg = nullptr;
    int ret = K8sLeaseCancelWatch(const_cast<char*>(ns.c_str()),
                                  const_cast<char*>(lease.c_str()), &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "CancelWatch failed: " << err_msg;
        free(err_msg);
        return ErrorCode::K8S_LEASE_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode K8sLeaseHelper::SetPodLabel(const std::string& ns,
                                      const std::string& pod,
                                      const std::string& key,
                                      const std::string& value) {
    char* err_msg = nullptr;
    int ret = K8sPatchPodLabel(const_cast<char*>(ns.c_str()),
                               const_cast<char*>(pod.c_str()),
                               const_cast<char*>(key.c_str()),
                               const_cast<char*>(value.c_str()), &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "SetPodLabel failed: " << err_msg;
        free(err_msg);
        return ErrorCode::K8S_LEASE_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode K8sLeaseHelper::ClearPodLabel(const std::string& ns,
                                        const std::string& pod,
                                        const std::string& key) {
    char* err_msg = nullptr;
    int ret = K8sRemovePodLabel(const_cast<char*>(ns.c_str()),
                                const_cast<char*>(pod.c_str()),
                                const_cast<char*>(key.c_str()), &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "ClearPodLabel failed: " << err_msg;
        free(err_msg);
        return ErrorCode::K8S_LEASE_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

#else  // !STORE_USE_K8S_LEASE

ErrorCode K8sLeaseHelper::Init() {
    LOG(FATAL) << "K8s Lease is not enabled in compilation";
    return ErrorCode::K8S_LEASE_OPERATION_ERROR;
}

ErrorCode K8sLeaseHelper::RunElection(const std::string&, const std::string&,
                                      const std::string&, int, int, int) {
    LOG(FATAL) << "K8s Lease is not enabled in compilation";
    return ErrorCode::K8S_LEASE_OPERATION_ERROR;
}

ErrorCode K8sLeaseHelper::WaitElected(const std::string&, const std::string&,
                                      int, int64_t&) {
    LOG(FATAL) << "K8s Lease is not enabled in compilation";
    return ErrorCode::K8S_LEASE_OPERATION_ERROR;
}

ErrorCode K8sLeaseHelper::WaitLost(const std::string&, const std::string&) {
    LOG(FATAL) << "K8s Lease is not enabled in compilation";
    return ErrorCode::K8S_LEASE_OPERATION_ERROR;
}

ErrorCode K8sLeaseHelper::CancelElection(const std::string&,
                                         const std::string&) {
    LOG(FATAL) << "K8s Lease is not enabled in compilation";
    return ErrorCode::K8S_LEASE_OPERATION_ERROR;
}

ErrorCode K8sLeaseHelper::GetHolder(const std::string&, const std::string&,
                                    std::string&, int64_t&) {
    LOG(FATAL) << "K8s Lease is not enabled in compilation";
    return ErrorCode::K8S_LEASE_OPERATION_ERROR;
}

ErrorCode K8sLeaseHelper::WatchHolder(const std::string&, const std::string&,
                                      void*,
                                      void (*)(void*, const char*, size_t,
                                               int64_t)) {
    LOG(FATAL) << "K8s Lease is not enabled in compilation";
    return ErrorCode::K8S_LEASE_OPERATION_ERROR;
}

ErrorCode K8sLeaseHelper::CancelWatch(const std::string&, const std::string&) {
    LOG(FATAL) << "K8s Lease is not enabled in compilation";
    return ErrorCode::K8S_LEASE_OPERATION_ERROR;
}

ErrorCode K8sLeaseHelper::SetPodLabel(const std::string&, const std::string&,
                                      const std::string&, const std::string&) {
    LOG(FATAL) << "K8s Lease is not enabled in compilation";
    return ErrorCode::K8S_LEASE_OPERATION_ERROR;
}

ErrorCode K8sLeaseHelper::ClearPodLabel(const std::string&, const std::string&,
                                        const std::string&) {
    LOG(FATAL) << "K8s Lease is not enabled in compilation";
    return ErrorCode::K8S_LEASE_OPERATION_ERROR;
}

#endif  // STORE_USE_K8S_LEASE

}  // namespace mooncake
