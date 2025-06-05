#include <glog/logging.h>

#include "etcd_helper.h"

namespace mooncake {

std::string EtcdHelper::connected_endpoints_ = "";
std::mutex EtcdHelper::etcd_mutex_;
bool EtcdHelper::etcd_connected_ = false;

ErrorCode EtcdHelper::ConnectToEtcdStoreClient(const std::string& etcd_endpoints) {
    std::lock_guard<std::mutex> lock(etcd_mutex_);
    if (etcd_connected_) {
        if (connected_endpoints_ != etcd_endpoints) {
            LOG(ERROR) << "Etcd client already connected to " << connected_endpoints_ <<
                ", while trying to connect to " << etcd_endpoints;
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

ErrorCode EtcdHelper::EtcdGet(const char* key, const size_t key_size, std::string& value, GoInt64& revision_id) {
    char* err_msg = nullptr;
    char* value_ptr = nullptr;
    int value_size = 0;
    int ret = EtcdStoreGetWrapper((char*)key, (int)key_size, &value_ptr, &value_size, &revision_id, &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "key=" << std::string(key, key_size) << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    if (value_ptr == nullptr) {
        VLOG(1) << "key=" << std::string(key, key_size) << ", error=etcd_key_not_exist";
        return ErrorCode::ETCD_KEY_NOT_EXIST;
    }
    value = std::string(value_ptr, value_size);
    free(value_ptr);
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::EtcdCreateWithLease(const char* key, const size_t key_size,
     const char* value, const size_t value_size, GoInt64 lease_id, GoInt64& revision_id) {
    char* err_msg = nullptr;
    int tx_success = 0; // transaction success or not
    int ret = EtcdStoreCreateWithLeaseWrapper((char*)key, (int)key_size,
        (char*)value, (int)value_size, lease_id, &tx_success, &revision_id, &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "key=" << std::string(key, key_size)
             << ", lease_id=" << lease_id << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    } else if (tx_success == 0) {
        VLOG(1) << "key=" << std::string(key, key_size)
             << ", lease_id=" << lease_id << ", error=etcd_transaction_fail";
        return ErrorCode::ETCD_TRANSACTION_FAIL;
    } else {
        return ErrorCode::OK;
    } 
}

ErrorCode EtcdHelper::EtcdGrantLease(int64_t lease_ttl, GoInt64& lease_id) {
    char* err_msg = nullptr;
    if (0 != EtcdStoreGrantLeaseWrapper(lease_ttl, &lease_id, &err_msg)) {
        LOG(ERROR) << "lease_ttl=" << lease_ttl << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::EtcdWatchUntilDeleted(const char* key, const size_t key_size) {
    char* err_msg = nullptr;
    if (0 != EtcdStoreWatchUntilDeletedWrapper(
        (char*)key, (int)key_size, &err_msg)) {
        LOG(ERROR) << "Error watching key: " << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::EtcdKeepAlive(int64_t lease_id) {
    char* err_msg = nullptr;
    if (0 != EtcdStoreKeepAliveWrapper(lease_id, &err_msg)) {
        LOG(ERROR) << "Failed to keep lease: " << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

}  // namespace mooncake