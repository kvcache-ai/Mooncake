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

ErrorCode EtcdHelper::Put(const char* key, const size_t key_size,
                          const char* value, const size_t value_size) {
    char* err_msg = nullptr;
    int ret = EtcdStorePutWrapper((char*)key, (int)key_size, (char*)value,
                                  (int)value_size, &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "key=" << std::string(key, key_size)
                   << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::Create(const char* key, const size_t key_size,
                             const char* value, const size_t value_size) {
    char* err_msg = nullptr;
    int ret = EtcdStoreCreateWrapper((char*)key, (int)key_size, (char*)value,
                                     (int)value_size, &err_msg);
    if (ret == -2) {
        free(err_msg);
        return ErrorCode::ETCD_TRANSACTION_FAIL;
    }
    if (ret != 0) {
        LOG(ERROR) << "key=" << std::string(key, key_size)
                   << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::GetWithPrefix(const char* prefix,
                                    const size_t prefix_size,
                                    std::vector<std::string>& keys,
                                    std::vector<std::string>& values) {
    // TODO: Implement GetWithPrefix - need to simplify Go wrapper interface
    // first For now, return error as this requires complex memory management
    LOG(ERROR) << "GetWithPrefix not yet implemented - requires Go wrapper "
                  "interface simplification";
    return ErrorCode::INTERNAL_ERROR;
}

ErrorCode EtcdHelper::GetRangeAsJson(const char* start_key,
                                     const size_t start_key_size,
                                     const char* end_key,
                                     const size_t end_key_size, size_t limit,
                                     std::string& json,
                                     EtcdRevisionId& revision_id) {
    char* err_msg = nullptr;
    char* json_ptr = nullptr;
    int json_size = 0;
    // Go wrapper takes int limit.
    int ret = EtcdStoreGetRangeAsJsonWrapper(
        (char*)start_key, (int)start_key_size, (char*)end_key,
        (int)end_key_size, (int)limit, &json_ptr, &json_size,
        (GoInt64*)&revision_id, &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "start_key=" << std::string(start_key, start_key_size)
                   << ", end_key=" << std::string(end_key, end_key_size)
                   << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    json = std::string(json_ptr, json_size);
    free(json_ptr);
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::GetFirstKeyWithPrefix(const char* prefix,
                                            const size_t prefix_size,
                                            std::string& first_key) {
    char* err_msg = nullptr;
    char* first_key_ptr = nullptr;
    int first_key_size = 0;
    int ret = EtcdStoreGetFirstKeyWithPrefixWrapper(
        (char*)prefix, (int)prefix_size, &first_key_ptr, &first_key_size,
        &err_msg);
    if (ret == -2) {
        free(err_msg);
        return ErrorCode::ETCD_KEY_NOT_EXIST;
    }
    if (ret != 0) {
        LOG(ERROR) << "prefix=" << std::string(prefix, prefix_size)
                   << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    first_key = std::string(first_key_ptr, first_key_size);
    free(first_key_ptr);
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::GetLastKeyWithPrefix(const char* prefix,
                                           const size_t prefix_size,
                                           std::string& last_key) {
    char* err_msg = nullptr;
    char* last_key_ptr = nullptr;
    int last_key_size = 0;
    int ret = EtcdStoreGetLastKeyWithPrefixWrapper(
        (char*)prefix, (int)prefix_size, &last_key_ptr, &last_key_size,
        &err_msg);
    if (ret == -2) {
        free(err_msg);
        return ErrorCode::ETCD_KEY_NOT_EXIST;
    }
    if (ret != 0) {
        LOG(ERROR) << "prefix=" << std::string(prefix, prefix_size)
                   << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    last_key = std::string(last_key_ptr, last_key_size);
    free(last_key_ptr);
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::DeleteRange(const char* start_key,
                                  const size_t start_key_size,
                                  const char* end_key,
                                  const size_t end_key_size) {
    char* err_msg = nullptr;
    int ret = EtcdStoreDeleteRangeWrapper((char*)start_key, (int)start_key_size,
                                          (char*)end_key, (int)end_key_size,
                                          &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "start_key=" << std::string(start_key, start_key_size)
                   << ", end_key=" << std::string(end_key, end_key_size)
                   << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::WatchWithPrefix(const char* prefix,
                                      const size_t prefix_size,
                                      void* callback_context,
                                      void (*callback_func)(void*, const char*,
                                                            size_t, const char*,
                                                            size_t, int)) {
    char* err_msg = nullptr;
    // Convert function pointer to void* for passing to Go function
    // Note: This is safe because we're just passing the pointer, not calling it
    void* callback_func_ptr = reinterpret_cast<void*>(callback_func);
    int ret = EtcdStoreWatchWithPrefixWrapper((char*)prefix, (int)prefix_size,
                                              callback_context,
                                              callback_func_ptr, &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "prefix=" << std::string(prefix, prefix_size)
                   << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::WatchWithPrefixFromRevision(
    const char* prefix, const size_t prefix_size, EtcdRevisionId start_revision,
    void* callback_context,
    void (*callback_func)(void*, const char*, size_t, const char*, size_t, int,
                          int64_t)) {
    char* err_msg = nullptr;
    void* callback_func_ptr = reinterpret_cast<void*>(callback_func);
    int ret = EtcdStoreWatchWithPrefixFromRevisionV2Wrapper(
        (char*)prefix, (int)prefix_size, (GoInt64)start_revision,
        callback_context, callback_func_ptr, &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "prefix=" << std::string(prefix, prefix_size)
                   << ", start_revision=" << (int64_t)start_revision
                   << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::CancelWatchWithPrefix(const char* prefix,
                                            const size_t prefix_size) {
    char* err_msg = nullptr;
    int ret = EtcdStoreCancelWatchWithPrefixWrapper((char*)prefix,
                                                    (int)prefix_size, &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "prefix=" << std::string(prefix, prefix_size)
                   << ", error=" << err_msg;
        free(err_msg);
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdHelper::WaitWatchWithPrefixStopped(const char* prefix,
                                                 const size_t prefix_size,
                                                 int timeout_ms) {
    char* err_msg = nullptr;
    int ret = EtcdStoreWaitWatchWithPrefixStoppedWrapper(
        (char*)prefix, (int)prefix_size, timeout_ms, &err_msg);
    if (ret != 0) {
        LOG(ERROR) << "prefix=" << std::string(prefix, prefix_size)
                   << ", timeout_ms=" << timeout_ms
                   << ", error=" << (err_msg ? err_msg : "unknown");
        if (err_msg) {
            free(err_msg);
        }
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

ErrorCode EtcdHelper::Put(const char* key, const size_t key_size,
                          const char* value, const size_t value_size) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::Create(const char* key, const size_t key_size,
                             const char* value, const size_t value_size) {
    (void)key;
    (void)key_size;
    (void)value;
    (void)value_size;
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::GetWithPrefix(const char* prefix,
                                    const size_t prefix_size,
                                    std::vector<std::string>& keys,
                                    std::vector<std::string>& values) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::GetRangeAsJson(const char* start_key,
                                     const size_t start_key_size,
                                     const char* end_key,
                                     const size_t end_key_size, size_t limit,
                                     std::string& json,
                                     EtcdRevisionId& revision_id) {
    (void)start_key;
    (void)start_key_size;
    (void)end_key;
    (void)end_key_size;
    (void)limit;
    (void)json;
    (void)revision_id;
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}
ErrorCode EtcdHelper::GetFirstKeyWithPrefix(const char* prefix,
                                            const size_t prefix_size,
                                            std::string& first_key) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::GetLastKeyWithPrefix(const char* prefix,
                                           const size_t prefix_size,
                                           std::string& last_key) {
    (void)prefix;
    (void)prefix_size;
    (void)last_key;
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::DeleteRange(const char* start_key,
                                  const size_t start_key_size,
                                  const char* end_key,
                                  const size_t end_key_size) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::WatchWithPrefix(const char* prefix,
                                      const size_t prefix_size,
                                      void* callback_context,
                                      void (*callback_func)(void*, const char*,
                                                            size_t, const char*,
                                                            size_t, int)) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::WatchWithPrefixFromRevision(
    const char* prefix, const size_t prefix_size, EtcdRevisionId start_revision,
    void* callback_context,
    void (*callback_func)(void*, const char*, size_t, const char*, size_t, int,
                          int64_t)) {
    (void)prefix;
    (void)prefix_size;
    (void)start_revision;
    (void)callback_context;
    (void)callback_func;
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

ErrorCode EtcdHelper::CancelWatchWithPrefix(const char* prefix,
                                            const size_t prefix_size) {
    LOG(FATAL) << "Etcd is not enabled in compilation";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

#endif

}  // namespace mooncake
