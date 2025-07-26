#include "etcd_helper.h"

#ifdef STORE_USE_ETCD
#include "libetcd_wrapper.h"
#endif

#include <glog/logging.h>

namespace mooncake {

std::string EtcdHelper::connected_endpoints_ = "";
std::mutex EtcdHelper::etcd_mutex_;
bool EtcdHelper::etcd_connected_ = false;

#ifdef STORE_USE_ETCD
ErrorCode EtcdHelper::ConnectToEtcdStoreClient(
    const std::string& etcd_endpoints) {
    std::lock_guard<std::mutex> lock(etcd_mutex_);
    if (etcd_connected_) {
        if (connected_endpoints_ != etcd_endpoints) {
            LOG(ERROR) << "Etcd client already connected to "
                       << connected_endpoints_
                       << ", while trying to connect to " << etcd_endpoints;
            return ErrorCode::INVALID_PARAMS;
        }
        return ErrorCode::OK;
    } else {
        char* err_msg = nullptr;
        int ret = NewStoreEtcdClient((char*)etcd_endpoints.c_str(), &err_msg);
        // ret == -2 means the etcd client has already been initialized
        if (ret != 0 && ret != -2) {
            LOG(ERROR) << "Failed to initialize etcd client: " << err_msg;
            free(err_msg);
            err_msg = nullptr;
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
        // Record the connection to avoid future connection attempts
        connected_endpoints_ = etcd_endpoints;
        etcd_connected_ = true;
        return ErrorCode::OK;
    }
}

ErrorCode EtcdHelper::Get(const char* key, const size_t key_size,
                          std::string& value, EtcdRevisionId& revision_id) {
    char* err_msg = nullptr;
    char* value_ptr = nullptr;
    int value_size = 0;
    int ret = EtcdStoreGetWrapper((char*)key, (int)key_size, &value_ptr,
                                  &value_size, &revision_id, &err_msg);
    if (ret == -2) {
        LOG(ERROR) << "key=" << std::string(key, key_size)
                   << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_KEY_NOT_EXIST;
    }
    if (ret != 0) {
        LOG(ERROR) << "key=" << std::string(key, key_size)
                   << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    value = std::string(value_ptr, value_size);
    free(value_ptr);
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::CreateWithLease(const char* key, const size_t key_size,
                                      const char* value,
                                      const size_t value_size,
                                      EtcdLeaseId lease_id,
                                      EtcdRevisionId& revision_id) {
    char* err_msg = nullptr;
    int ret = EtcdStoreCreateWithLeaseWrapper((char*)key, (int)key_size,
                                              (char*)value, (int)value_size,
                                              lease_id, &revision_id, &err_msg);
    if (ret == -2) {
        VLOG(1) << "key=" << std::string(key, key_size)
                << ", lease_id=" << lease_id << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_TRANSACTION_FAIL;
    } else if (ret != 0) {
        LOG(ERROR) << "key=" << std::string(key, key_size)
                   << ", lease_id=" << lease_id << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    } else {
        return ErrorCode::OK;
    }
}

ErrorCode EtcdHelper::GrantLease(int64_t lease_ttl, EtcdLeaseId& lease_id) {
    char* err_msg = nullptr;
    if (0 != EtcdStoreGrantLeaseWrapper(lease_ttl, &lease_id, &err_msg)) {
        LOG(ERROR) << "lease_ttl=" << lease_ttl << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::WatchUntilDeleted(const char* key,
                                        const size_t key_size) {
    char* err_msg = nullptr;
    int err_code =
        EtcdStoreWatchUntilDeletedWrapper((char*)key, (int)key_size, &err_msg);
    if (err_code != 0) {
        LOG(ERROR) << "key=" << std::string(key, key_size)
                   << ", error=" << err_msg;
        free(err_msg);
        if (err_code == -2) {
            return ErrorCode::ETCD_CTX_CANCELLED;
        } else {
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
    }
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::CancelWatch(const char* key, const size_t key_size) {
    char* err_msg = nullptr;
    if (0 != EtcdStoreCancelWatchWrapper((char*)key, (int)key_size, &err_msg)) {
        LOG(ERROR) << "key=" << std::string(key, key_size)
                   << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::KeepAlive(EtcdLeaseId lease_id) {
    char* err_msg = nullptr;
    int err_code = EtcdStoreKeepAliveWrapper(lease_id, &err_msg);
    if (err_code != 0) {
        LOG(ERROR) << "lease_id=" << lease_id << ", error=" << err_msg;
        free(err_msg);
        if (err_code == -2) {
            return ErrorCode::ETCD_CTX_CANCELLED;
        } else {
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
    }
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::CancelKeepAlive(EtcdLeaseId lease_id) {
    char* err_msg = nullptr;
    if (0 != EtcdStoreCancelKeepAliveWrapper(lease_id, &err_msg)) {
        LOG(ERROR) << "Failed to cancel keep lease: " << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}
#else
ErrorCode EtcdHelper::ConnectToEtcdStoreClient(
    const std::string& etcd_endpoints) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::Get(const char* key, const size_t key_size,
                          std::string& value, EtcdRevisionId& revision_id) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::CreateWithLease(const char* key, const size_t key_size,
                                      const char* value,
                                      const size_t value_size,
                                      EtcdLeaseId lease_id,
                                      EtcdRevisionId& revision_id) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::GrantLease(int64_t lease_ttl, EtcdLeaseId& lease_id) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::WatchUntilDeleted(const char* key,
                                        const size_t key_size) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::CancelWatch(const char* key, const size_t key_size) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::KeepAlive(EtcdLeaseId lease_id) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::CancelKeepAlive(EtcdLeaseId lease_id) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

#endif

}  // namespace mooncake